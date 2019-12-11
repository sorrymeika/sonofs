function onReceiveBlock(socket, handleBuffer) {
    let type;
    let allBuf;
    let total;
    let reading = false;
    let readingSize = false;

    const HEAD_LENGTH = 7;

    function readBuffer(chunk) {
        if (!reading) {
            type = chunk.readUInt8(0);
            if (type == 255) {
                handleBuffer(type, chunk);
                return;
            } else {
                reading = true;
                allBuf = chunk;
                readingSize = false;

                if (allBuf.length < HEAD_LENGTH) {
                    readingSize = true;
                    return;
                } else {
                    total = allBuf.readUIntBE(1, 6) + HEAD_LENGTH;
                }
            }
        } else {
            allBuf = Buffer.concat([allBuf, chunk]);
        }

        if (readingSize && allBuf.length > HEAD_LENGTH) {
            readingSize = false;
            total = allBuf.readUIntBE(1, 6) + HEAD_LENGTH;
        }

        if (allBuf.length >= total) {
            reading = false;
            if (allBuf.length > total) {
                handleBuffer(type, allBuf.slice(HEAD_LENGTH, total));
                readBuffer(allBuf.slice(total));
            } else {
                handleBuffer(type, allBuf.slice(HEAD_LENGTH));
            }
            allBuf = null;
        }
    }

    socket.on('data', readBuffer);
}
exports.onReceiveBlock = onReceiveBlock;


function sendBlock(socket, type, buf, cb) {
    const head = Buffer.alloc(7);
    head.writeUInt8(type);
    head.writeUIntBE(buf ? buf.length : 0, 1, 6);

    if (!buf) {
        return socket.write(head, cb);
    } else {
        return socket.write(head) && socket.write(buf, cb);
    }
}

exports.sendBlock = sendBlock;

function sendSuccessBlock(socket, cb) {
    return sendBlock(socket, 1, null, cb);
}
exports.sendSuccessBlock = sendSuccessBlock;


function sendHeartbeat(socket) {
    socket.write(Buffer.from([255]));
}
exports.sendHeartbeat = sendHeartbeat;