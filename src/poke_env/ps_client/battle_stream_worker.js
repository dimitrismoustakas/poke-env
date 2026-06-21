'use strict';

const {execSync} = require('child_process');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const {Worker} = require('worker_threads');

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

function queueProtocolMessage(battleId, message) {
    queueEvent({type: 'protocol-batch', battleId, messages: [message]});
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

const showdownDir = process.argv[2];
if (!showdownDir) {
    throw new Error('Expected showdown_dir as the first argument.');
}

ensureBuilt(showdownDir);

const activeStreams = new Map();
const idleBattleWorkers = [];
const allBattleWorkers = new Set();
let closing = false;
let shutdownStarted = false;

function battleWorkerPath() {
    return path.join(__dirname, 'battle_stream_battle_worker.js');
}

function removeIdleBattleWorker(handle) {
    const index = idleBattleWorkers.indexOf(handle);
    if (index >= 0) {
        idleBattleWorkers.splice(index, 1);
    }
}

function createBattleWorker() {
    const handle = {
        worker: new Worker(battleWorkerPath(), {workerData: {showdownDir}}),
        activeBattleId: null,
        exited: false,
    };
    allBattleWorkers.add(handle);
    handle.worker.on('message', event => handleBattleWorkerEvent(handle, event));
    handle.worker.on('error', error => handleBattleWorkerError(handle, error));
    handle.worker.on('exit', code => handleBattleWorkerExit(handle, code));
    return handle;
}

function acquireBattleWorker() {
    while (idleBattleWorkers.length > 0) {
        const handle = idleBattleWorkers.pop();
        if (!handle.exited) {
            return handle;
        }
    }
    return createBattleWorker();
}

function releaseBattleWorker(handle) {
    if (!closing && !handle.exited) {
        idleBattleWorkers.push(handle);
    }
}

function handleBattleWorkerEvent(handle, event) {
    if (!event || typeof event !== 'object') {
        return;
    }
    if (event.type === 'ready') {
        return;
    }

    const battleId = handle.activeBattleId;
    if (!battleId) {
        if (event.type === 'error') {
            queueEvent({type: 'error', detail: event.detail || 'Idle battle worker failed'});
        }
        return;
    }

    if (event.type === 'battle-ended') {
        if (activeStreams.get(battleId) === handle) {
            activeStreams.delete(battleId);
        }
        handle.activeBattleId = null;
        queueEvent({type: 'battle-ended', battleId});
        releaseBattleWorker(handle);
        if (closing && activeStreams.size === 0) {
            void finishShutdown();
        }
        return;
    }

    event.battleId = battleId;
    queueEvent(event);
}

function handleBattleWorkerError(handle, error) {
    const battleId = handle.activeBattleId;
    if (battleId && activeStreams.get(battleId) === handle) {
        queueEvent({type: 'error', battleId, detail: formatError(error)});
    } else {
        queueEvent({type: 'error', detail: formatError(error)});
    }
}

function handleBattleWorkerExit(handle, code) {
    handle.exited = true;
    allBattleWorkers.delete(handle);
    removeIdleBattleWorker(handle);

    const battleId = handle.activeBattleId;
    handle.activeBattleId = null;
    if (battleId && activeStreams.get(battleId) === handle) {
        activeStreams.delete(battleId);
        if (code !== 0) {
            queueEvent({
                type: 'error',
                battleId,
                detail: `Battle worker thread exited with code ${code}`,
            });
        }
        queueEvent({type: 'battle-ended', battleId});
    }
    if (closing && activeStreams.size === 0) {
        void finishShutdown();
    }
}

async function finishShutdown() {
    if (shutdownStarted) {
        return;
    }
    shutdownStarted = true;
    const terminations = [];
    for (const handle of allBattleWorkers) {
        terminations.push(handle.worker.terminate());
    }
    await Promise.allSettled(terminations);
    await flushEvents();
    process.exit(0);
}

function startBattle(battleId, lines) {
    if (!battleId || typeof battleId !== 'string') {
        throw new Error('start command requires a string battleId.');
    }
    if (activeStreams.has(battleId)) {
        throw new Error(`Cannot start battle ${battleId} while it is already active.`);
    }

    validateLines(lines, 'start');
    const handle = acquireBattleWorker();
    handle.activeBattleId = battleId;
    activeStreams.set(battleId, handle);
    handle.worker.postMessage({type: 'start', lines});
}

function writeBattleLines(battleId, lines) {
    const handle = activeStreams.get(battleId);
    if (!handle) {
        queueEvent({
            type: 'error',
            battleId,
            detail: formatError(new Error(`Received a write command without an active battle: ${battleId}`)),
        });
        return;
    }

    validateLines(lines, 'write');
    handle.worker.postMessage({type: 'write', lines});
}

function closeBattle(battleId) {
    const handle = activeStreams.get(battleId);
    if (handle) {
        handle.worker.postMessage({type: 'close-battle'});
    } else {
        queueEvent({type: 'battle-ended', battleId});
    }
}

function handleCommand(command) {
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

function enqueueCommand(rawLine) {
    if (!rawLine.trim()) {
        return;
    }

    try {
        const command = JSON.parse(rawLine);
        handleCommand(command);
    } catch (error) {
        void failWorker(error);
    }
}

process.stdin.setEncoding('utf8');
const rl = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
});

rl.on('line', enqueueCommand);
rl.on('close', () => enqueueCommand(JSON.stringify({type: 'shutdown'})));

void writeRawEvent({type: 'ready'});
