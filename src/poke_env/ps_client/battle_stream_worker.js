'use strict';

const {execSync} = require('child_process');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

function formatError(error) {
    if (error && typeof error.stack === 'string') {
        return error.stack;
    }
    return String(error);
}

async function writeEvent(event) {
    const payload = JSON.stringify(event) + '\n';
    const flushed = process.stdout.write(payload);
    if (!flushed) {
        await new Promise(resolve => process.stdout.once('drain', resolve));
    }
}

function ensureBuilt(showdownDir) {
    const distPath = path.join(showdownDir, 'dist', 'sim', 'battle-stream.js');
    if (fs.existsSync(distPath)) {
        return;
    }
    execSync('node build', {
        cwd: showdownDir,
        stdio: ['ignore', 'ignore', 'inherit'],
    });
}

function validateLines(lines, commandType) {
    if (!Array.isArray(lines)) {
        throw new Error(`${commandType} command must provide a lines array.`);
    }
    for (const line of lines) {
        if (typeof line !== 'string' || !line.startsWith('>')) {
            throw new Error(`Invalid battle command for ${commandType}: ${JSON.stringify(line)}`);
        }
    }
}

const showdownDir = process.argv[2];
if (!showdownDir) {
    throw new Error('Expected showdown_dir as the first argument.');
}

ensureBuilt(showdownDir);

const {BattleTextStream} = require(path.join(showdownDir, 'dist', 'sim', 'battle-stream.js'));

let activeStream = null;
let closing = false;
let commandQueue = Promise.resolve();

async function consumeBattle(stream) {
    try {
        while (true) {
            const chunk = await stream.read();
            if (chunk === null) {
                break;
            }
            await writeEvent({type: 'chunk', payload: chunk});
        }
    } catch (error) {
        await writeEvent({type: 'error', detail: formatError(error)});
    } finally {
        if (activeStream === stream) {
            activeStream = null;
        }
        await writeEvent({type: 'battle-ended'});
        if (closing) {
            process.exit(0);
        }
    }
}

function startBattle(lines) {
    if (activeStream) {
        throw new Error('Cannot start a new battle while another battle is active.');
    }

    validateLines(lines, 'start');
    const stream = new BattleTextStream({noCatch: true});
    activeStream = stream;
    void consumeBattle(stream);
    for (const line of lines) {
        stream.write(`${line}\n`);
    }
}

function writeBattleLines(lines) {
    if (!activeStream) {
        throw new Error('Received a write command without an active battle.');
    }

    validateLines(lines, 'write');
    for (const line of lines) {
        activeStream.write(`${line}\n`);
    }
}

function closeBattle() {
    if (activeStream) {
        activeStream.destroy();
        activeStream = null;
    }
}

async function handleCommand(command) {
    switch (command.type) {
    case 'start':
        startBattle(command.lines);
        break;
    case 'write':
        writeBattleLines(command.lines);
        break;
    case 'close-battle':
        closeBattle();
        break;
    case 'shutdown':
        closing = true;
        if (activeStream) {
            closeBattle();
        } else {
            process.exit(0);
        }
        break;
    default:
        throw new Error(`Unsupported worker command type: ${JSON.stringify(command.type)}`);
    }
}

function enqueueCommand(rawLine) {
    if (!rawLine.trim()) {
        return;
    }

    commandQueue = commandQueue.then(async () => {
        const command = JSON.parse(rawLine);
        await handleCommand(command);
    });
    commandQueue = commandQueue.catch(async error => {
        await writeEvent({type: 'error', detail: formatError(error)});
        process.exit(1);
    });
}

process.stdin.setEncoding('utf8');
const rl = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
});

rl.on('line', enqueueCommand);
rl.on('close', () => enqueueCommand(JSON.stringify({type: 'shutdown'})));

void writeEvent({type: 'ready'});