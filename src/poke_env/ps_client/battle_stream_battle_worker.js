'use strict';

const path = require('path');
const {parentPort, workerData} = require('worker_threads');

function formatError(error) {
    if (error && typeof error.stack === 'string') {
        return error.stack;
    }
    return String(error);
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

if (!parentPort) {
    throw new Error('battle_stream_battle_worker.js must run as a worker thread.');
}

const showdownDir = workerData && workerData.showdownDir;
if (!showdownDir) {
    throw new Error('Expected showdownDir workerData.');
}

const {BattleStream} = require(path.join(showdownDir, 'dist', 'sim', 'battle-stream.js'));
const {extractChannelMessages} = require(path.join(showdownDir, 'dist', 'sim', 'battle.js'));

let activeStream = null;

function postEvent(event) {
    parentPort.postMessage(event);
}

function finishBattle(stream) {
    if (activeStream !== stream) {
        return;
    }
    activeStream = null;
    postEvent({type: 'battle-ended'});
}

async function consumeBattle(stream) {
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
                postEvent({
                    type: 'protocol-batch',
                    messages: [{
                        type: 'split-chunk',
                        p1_messages: splitProtocolMessages(channelMessages[1]),
                        p2_messages: splitProtocolMessages(channelMessages[2]),
                    }],
                });
                break;
            }
            case 'sideupdate': {
                const sideBreak = payload.indexOf('\n');
                const player = sideBreak >= 0 ? payload.slice(0, sideBreak) : payload;
                const sidePayload = sideBreak >= 0 ? payload.slice(sideBreak + 1) : '';
                postEvent({
                    type: 'protocol-batch',
                    messages: [{
                        type: 'side-chunk',
                        player,
                        messages: sidePayload ? splitProtocolMessages(sidePayload.split('\n')) : [],
                    }],
                });
                break;
            }
            case 'end':
                postEvent({type: 'protocol-batch', messages: [{type: 'end', payload}]});
                break;
            case 'requesteddata':
                break;
            default:
                postEvent({type: 'protocol-batch', messages: [chunk]});
                break;
            }
        }
    } catch (error) {
        postEvent({type: 'error', detail: formatError(error)});
    } finally {
        finishBattle(stream);
    }
}

function startBattle(lines) {
    if (activeStream) {
        throw new Error('Cannot start a battle while another battle is active in this worker thread.');
    }

    validateLines(lines, 'start');
    const stream = new BattleStream({noCatch: true});
    activeStream = stream;
    void consumeBattle(stream);
    for (const line of lines) {
        stream.write(line);
    }
}

function writeBattleLines(lines) {
    if (!activeStream) {
        postEvent({
            type: 'error',
            detail: formatError(new Error('Received a write command without an active battle.')),
        });
        return;
    }

    validateLines(lines, 'write');
    for (const line of lines) {
        activeStream.write(line);
    }
}

function closeBattle() {
    const stream = activeStream;
    if (!stream) {
        postEvent({type: 'battle-ended'});
        return;
    }

    activeStream = null;
    stream.destroy();
    postEvent({type: 'battle-ended'});
}

parentPort.on('message', command => {
    try {
        switch (command.type) {
        case 'start':
            startBattle(command.lines);
            break;
        case 'write':
            writeBattleLines(command.lines);
            break;
        case 'close-battle':
        case 'shutdown':
            closeBattle();
            break;
        default:
            throw new Error(`Unsupported battle worker command type: ${JSON.stringify(command.type)}`);
        }
    } catch (error) {
        postEvent({type: 'error', detail: formatError(error)});
        closeBattle();
    }
});

postEvent({type: 'ready'});
