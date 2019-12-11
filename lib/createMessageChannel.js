const cluster = require('cluster');

function createMessageChannel() {
    const callbacks = {};
    const MAX_CALLBACK_ID = 256 * 256 * 256 * 256 - 100;

    let cid = 0;

    process.on('message', (msg) => {
        if (!msg || !msg.callbackId) {
            return;
        }
        const { callbackId } = msg;
        if (callbacks[callbackId]) {
            callbacks[callbackId](msg);
            delete callbacks[callbackId];
        }
    });

    return {
        postMessage(msg, cb) {
            if (cid >= MAX_CALLBACK_ID) {
                cid = 0;
            }
            let callbackId;
            if (cb) {
                callbackId = ++cid;
                callbacks[callbackId] = cb;
            } else {
                callbackId = -1;
            }
            process.send({
                ...msg,
                callbackId,
                workerId: cluster.worker.id
            });
        }
    };
}

module.exports = createMessageChannel;