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

        callbacks[callbackId](msg);
        delete callbacks[callbackId];
    });

    return {
        postMessage(msg, cb) {
            if (cid >= MAX_CALLBACK_ID) {
                cid = 0;
            }

            const callbackId = ++cid;
            callbacks[callbackId] = cb;

            process.send({
                ...msg,
                callbackId,
                workerId: cluster.worker.id
            });
        }
    };
}

module.exports = createMessageChannel;