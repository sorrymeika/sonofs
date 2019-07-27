
const cluster = require('cluster');
const net = require('net');
const fs = require('fs');
const fsPromises = fs.promises;
const path = require('path');
const { formatFile, pad } = require('./util');

const MAX_FILE_ID = 256 * 256 * 256 - 1;
const MAX_FILE_SIZE = 1024 * 1024 * 1024 - 1;

function createMaster(cfg, callback) {
    if (!cluster.isMaster) throw new Error('createMaster must in cluster master');

    const { serverId, root, groupId, rootId } = cfg;
    const serverCfgPath = path.join(root, 'server.cfg');
    const filesIdxPath = path.join(root, 'files.idx');

    let currentBundleId;
    let currentBundleSize;
    let idxFileSize;

    function idManager(cfgFd, filesIdxFd) {
        const messageHandler = (msg) => {
            if (!msg || !msg.workerId || !msg.fileSize || !msg.callbackId) {
                return;
            }
            console.log('new file:', currentBundleSize, currentBundleId, msg);

            const { workerId, fileSize, callbackId } = msg;
            let newFileSize = currentBundleSize + fileSize;

            if (newFileSize > MAX_FILE_SIZE) {
                currentBundleId++;
                currentBundleSize = newFileSize = fileSize;
            } else {
                currentBundleSize = newFileSize;
            }

            const bundleId = currentBundleId;
            const idxFileOffest = idxFileSize;
            idxFileSize += 12;

            const buf = Buffer.alloc(8);
            buf.writeIntBE(serverId, 0, 1);
            buf.writeIntBE(bundleId, 1, 3);
            buf.writeIntBE(newFileSize, 4, 4);

            const worker = cluster.workers[workerId];

            // 写入cfg文件
            fs.write(cfgFd, buf, 0, 8, 0, (err) => {
                if (err) {
                    worker.send({
                        success: false,
                        callbackId,
                        error: err
                    });
                    return;
                }

                // files.idx: 单文件12byte : dir:1 subdir:1 file:1 mime:1 fileStart:4 fileSize:4
                const idxBuf = Buffer.alloc(12);
                const dirId = bundleId >> 16;
                const subDirId = (bundleId & 65535) >> 8;
                const fileId = bundleId & 255;
                const { mime } = msg;
                const fileStart = newFileSize - fileSize;

                idxBuf.writeIntBE(dirId, 0, 1);
                idxBuf.writeIntBE(subDirId, 1, 1);
                idxBuf.writeIntBE(fileId, 2, 1);
                idxBuf.writeIntBE(mime, 3, 1);
                idxBuf.writeIntBE(fileStart, 4, 4);
                idxBuf.writeIntBE(fileSize, 8, 4);

                fs.write(filesIdxFd, idxBuf, 0, 12, idxFileOffest, (err) => {
                    if (err) {
                        worker.send({
                            success: false,
                            callbackId,
                            error: err
                        });
                        return;
                    }

                    const dir = pad(dirId.toString(16), 2);
                    const subDir = pad(subDirId.toString(16), 2);
                    const file = pad(fileId.toString(16), 2);
                    const fileDir = path.join(root, dir, subDir);
                    const filePath = path.join(fileDir, file + '.snf');

                    fs.mkdir(fileDir, { recursive: true }, () => {
                        const fileName = formatFile({
                            groupId,
                            rootId,
                            dir,
                            subDir,
                            file,
                            mime,
                            fileStart,
                            fileSize
                        });

                        worker.send({
                            success: true,
                            callbackId,
                            fileStart,
                            fileName,
                            fileSize,
                            bundleId,
                            mime,
                            dir,
                            subDir,
                            file,
                            filePath
                        });
                    });
                });
            });
        };

        console.log('currentBundleId:', currentBundleId, 'currentBundleSize:', currentBundleSize, 'idxFileSize:', idxFileSize);

        for (const id in cluster.workers) {
            cluster.workers[id].on('message', messageHandler);
        }
    }

    function cb(err, fd) {
        if (err) {
            callback && callback(err);
            return;
        }

        const success = (cfgFd, filesIdxFd) => {
            callback && callback(null);
            idManager(cfgFd, filesIdxFd);
        };

        fs.open(filesIdxPath, 'a', (err, filesIdxFd) => {
            if (err) {
                callback && callback(err);
                return;
            }

            fs.stat(filesIdxPath, (err, stats) => {
                if (err) {
                    callback && callback(err);
                    return;
                }

                idxFileSize = stats.size;

                if (!fd) {
                    fs.open(serverCfgPath, 'a', (err, fd) => {
                        if (err) {
                            callback && callback(err);
                        } else {
                            success(fd, filesIdxFd);
                        }
                    });
                } else {
                    success(fd, filesIdxFd);
                }
            });
        });
    }

    fs.open(serverCfgPath, 'wx', (err, fd) => {
        if (err) {
            if (err.code === 'EEXIST') {
                fs.readFile(serverCfgPath, (err, buf) => {
                    console.log(err, buf);

                    if (err) {
                        cb(err);
                        return;
                    }
                    const sid = buf.readUInt8();
                    if (sid !== serverId) {
                        cb(err);
                        return;
                    }

                    // buf[0] : serverId
                    // buf[1~3] : bundleId
                    // buf[4～7] : fileSize
                    currentBundleId = buf.readIntBE(1, 3);
                    currentBundleSize = buf.readIntBE(4, 4);
                    cb(null);
                });
                return;
            }
            cb(err);
        }
        fs.write(fd, Buffer.from([serverId, 0, 0, 0, 0, 0, 0, 0]), (err, written, buf) => {
            if (err) {
                cb(err);
                return;
            }
            currentBundleId = 0;
            currentBundleSize = 0;
            cb(buf, fd);
        });
    });
}

exports.createMaster = createMaster;

function createServer(cfg) {
    if (!cluster.isWorker) throw new Error('createServer必须在子线程中运行!');

    const { groupId, serverId, root, port } = cfg;

    let cid = 0;
    const callbacks = {};

    process.on('message', (msg) => {
        if (!msg || !msg.callbackId) {
            return;
        }
        const { callbackId } = msg;

        callbacks[callbackId](msg);
        delete callbacks[callbackId];
    });

    const server = net.createServer((socket) => {
        console.log('client connected! workerId:', cluster.worker.id);

        socket.on('end', () => {
            console.log('client disconnected! workerId:', cluster.worker.id);
        });

        socket.on('data', (buf) => {
            console.log('file buf:', buf);

            const mime = buf.readUInt8(0);
            const fileSize = buf.readIntBE(1, 3);

            const callbackId = ++cid;
            callbacks[callbackId] = (msg) => {
                console.log('callback:', msg, 'workerId:', cluster.worker.id);
                const { fileStart, filePath, fileName } = msg;

                fs.open(filePath, 'a', (err, fd) => {
                    if (err) {
                        socket.write(Buffer.from([0]));
                        return;
                    }

                    fs.write(fd, buf, 4, buf.length - 4, fileStart, (err) => {
                        if (err) {
                            socket.write(Buffer.from([0]));
                            return;
                        }
                        socket.write(Buffer.concat([Buffer.from([1]), Buffer.from(fileName, 'utf8')]));
                    });
                });

            };

            process.send({
                callbackId,
                workerId: cluster.worker.id,
                mime,
                fileSize
            });
        });
    });
    server.on('error', (err) => {
        throw err;
    });
    server.listen(port, () => {
        console.log('server bound workerId:', cluster.worker.id);
    });
}

exports.createServer = createServer;