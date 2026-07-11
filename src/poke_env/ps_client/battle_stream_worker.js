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

async function writeRawEvent(event) {
    const payload = JSON.stringify(event) + '\n';
    const flushed = process.stdout.write(payload);
    if (!flushed) {
        await new Promise(resolve => process.stdout.once('drain', resolve));
    }
}

const pendingEvents = [];
let flushScheduled = false;
let flushPromise = null;

function isProtocolBatchEvent(event) {
    return (
        event &&
        event.type === 'protocol-batch' &&
        typeof event.battleId === 'string' &&
        Array.isArray(event.messages)
    );
}

function compressQueuedEvents(events) {
    const compressed = [];
    const protocolBatches = new Map();
    const protocolBattleOrder = [];

    function flushProtocolBatches() {
        for (const battleId of protocolBattleOrder) {
            const messages = protocolBatches.get(battleId);
            if (messages && messages.length > 0) {
                compressed.push({type: 'protocol-batch', battleId, messages});
            }
        }
        protocolBatches.clear();
        protocolBattleOrder.length = 0;
    }

    for (const event of events) {
        if (isProtocolBatchEvent(event)) {
            if (!protocolBatches.has(event.battleId)) {
                protocolBatches.set(event.battleId, []);
                protocolBattleOrder.push(event.battleId);
            }
            protocolBatches.get(event.battleId).push(...event.messages);
            continue;
        }

        flushProtocolBatches();
        compressed.push(event);
    }

    flushProtocolBatches();
    return compressed;
}

async function flushQueuedEvents() {
    flushScheduled = false;
    const events = compressQueuedEvents(pendingEvents.splice(0, pendingEvents.length));
    if (events.length === 0) {
        return;
    }

    if (events.length === 1) {
        await writeRawEvent(events[0]);
    } else {
        await writeRawEvent({type: 'event-batch', events});
    }
}

function queueEvent(event) {
    pendingEvents.push(event);
    if (!flushScheduled) {
        flushScheduled = true;
        flushPromise = new Promise((resolve, reject) => {
            setImmediate(() => {
                flushQueuedEvents().then(resolve, reject);
            });
        });
    }
}

async function flushEvents() {
    while (pendingEvents.length > 0 || flushScheduled) {
        if (flushScheduled && flushPromise) {
            await flushPromise;
        } else {
            await flushQueuedEvents();
        }
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

function splitProtocolMessages(lines) {
    return lines.map(line => line.split('|'));
}

function writeBattleCommandLines(stream, lines) {
    if (lines.length > 0) {
        stream.write(lines.join('\n'));
    }
}

const showdownDir = process.argv[2];
if (!showdownDir) {
    throw new Error('Expected showdown_dir as the first argument.');
}

const maxActiveBattlesRaw = Number.parseInt(process.argv[3] || '0', 10);
const maxActiveBattles = Number.isFinite(maxActiveBattlesRaw) && maxActiveBattlesRaw > 0 ? maxActiveBattlesRaw : 0;

ensureBuilt(showdownDir);

const {BattleStream} = require(path.join(showdownDir, 'dist', 'sim', 'battle-stream.js'));
const {extractChannelMessages} = require(path.join(showdownDir, 'dist', 'sim', 'battle.js'));

const activeStreams = new Map();
let closing = false;
let shutdownStarted = false;

function queueProtocolMessages(battleId, messages) {
    if (messages.length > 0) {
        queueEvent({type: 'protocol-batch', battleId, messages});
    }
}

function queueBattleError(battleId, error) {
    queueEvent({type: 'error', battleId, detail: formatError(error)});
}

function finishBattle(battleId, state) {
    if (activeStreams.get(battleId) !== state) {
        return;
    }
    activeStreams.delete(battleId);
    queueEvent({type: 'battle-ended', battleId});
    if (closing && activeStreams.size === 0) {
        void finishShutdown();
    }
}

async function consumeBattle(battleId, state) {
    const stream = state.stream;
    try {
        while (true) {
            const chunk = await stream.read();
            if (chunk === null) {
                break;
            }
            const newlineIndex = chunk.indexOf('\n');
            const chunkType = newlineIndex >= 0 ? chunk.slice(0, newlineIndex) : chunk;
            const payload = newlineIndex >= 0 ? chunk.slice(newlineIndex + 1) : '';

            switch (chunkType) {
            case 'update': {
                const channelMessages = extractChannelMessages(payload, [1, 2]);
                queueProtocolMessages(battleId, [{
                    type: 'split-chunk',
                    p1_messages: splitProtocolMessages(channelMessages[1]),
                    p2_messages: splitProtocolMessages(channelMessages[2]),
                }]);
                break;
            }
            case 'sideupdate': {
                const sideBreak = payload.indexOf('\n');
                const player = sideBreak >= 0 ? payload.slice(0, sideBreak) : payload;
                const sidePayload = sideBreak >= 0 ? payload.slice(sideBreak + 1) : '';
                queueProtocolMessages(battleId, [{
                    type: 'side-chunk',
                    player,
                    messages: sidePayload ? splitProtocolMessages(sidePayload.split('\n')) : [],
                }]);
                break;
            }
            case 'end':
                queueProtocolMessages(battleId, [{type: 'end', payload}]);
                break;
            case 'requesteddata':
                break;
            default:
                queueProtocolMessages(battleId, [chunk]);
                break;
            }
        }
    } catch (error) {
        if (activeStreams.get(battleId) === state) {
            queueBattleError(battleId, error);
        }
    } finally {
        finishBattle(battleId, state);
    }
}

function startBattle(battleId, lines) {
    if (!battleId || typeof battleId !== 'string') {
        throw new Error('start command requires a string battleId.');
    }
    if (activeStreams.has(battleId)) {
        throw new Error(`Cannot start battle ${battleId} while it is already active.`);
    }
    if (maxActiveBattles > 0 && activeStreams.size >= maxActiveBattles) {
        throw new Error(
            `Cannot start battle ${battleId}; worker is at capacity ${maxActiveBattles}.`,
        );
    }

    validateLines(lines, 'start');
    const stream = new BattleStream({noCatch: true});
    const state = {stream};
    activeStreams.set(battleId, state);
    void consumeBattle(battleId, state);

    try {
        writeBattleCommandLines(stream, lines);
    } catch (error) {
        queueBattleError(battleId, error);
        closeBattle(battleId);
        return;
    }
    queueEvent({type: 'battle-started', battleId});
}

function writeBattleLines(battleId, lines) {
    const state = activeStreams.get(battleId);
    if (!state) {
        queueBattleError(
            battleId,
            new Error(`Received a write command without an active battle: ${battleId}`),
        );
        return;
    }

    validateLines(lines, 'write');
    try {
        writeBattleCommandLines(state.stream, lines);
    } catch (error) {
        queueBattleError(battleId, error);
        closeBattle(battleId);
        return;
    }
}

function closeBattle(battleId) {
    const state = activeStreams.get(battleId);
    if (!state) {
        queueEvent({type: 'battle-ended', battleId});
        if (closing && activeStreams.size === 0) {
            void finishShutdown();
        }
        return;
    }

    activeStreams.delete(battleId);
    try {
        state.stream.destroy();
    } catch (error) {
        queueBattleError(battleId, error);
    }
    queueEvent({type: 'battle-ended', battleId});
    if (closing && activeStreams.size === 0) {
        void finishShutdown();
    }
}

async function finishShutdown() {
    if (shutdownStarted) {
        return;
    }
    shutdownStarted = true;
    for (const battleId of Array.from(activeStreams.keys())) {
        closeBattle(battleId);
    }
    await flushEvents();
    process.exit(0);
}

async function handleCommand(command) {
    switch (command.type) {
    case 'start':
        startBattle(command.battleId, command.lines);
        break;
    case 'write':
        writeBattleLines(command.battleId, command.lines);
        break;
    case 'close-battle':
        closeBattle(command.battleId);
        break;
    case 'shutdown':
        closing = true;
        if (activeStreams.size > 0) {
            for (const battleId of Array.from(activeStreams.keys())) {
                closeBattle(battleId);
            }
        } else {
            void finishShutdown();
        }
        break;
    default:
        throw new Error(`Unsupported worker command type: ${JSON.stringify(command.type)}`);
    }
}

async function failWorker(error) {
    queueEvent({type: 'error', detail: formatError(error)});
    await flushEvents();
    process.exit(1);
}

let commandChain = Promise.resolve();

function enqueueCommand(rawLine) {
    if (!rawLine.trim()) {
        return;
    }

    let command;
    try {
        command = JSON.parse(rawLine);
    } catch (error) {
        void failWorker(error);
        return;
    }
    commandChain = commandChain
        .then(() => handleCommand(command))
        .catch(error => failWorker(error));
}

process.stdin.setEncoding('utf8');
const rl = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
});

rl.on('line', enqueueCommand);
rl.on('close', () => enqueueCommand(JSON.stringify({type: 'shutdown'})));

void writeRawEvent({type: 'ready'});
