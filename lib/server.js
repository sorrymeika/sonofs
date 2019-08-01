
const cluster = require('cluster');
const net = require('net');
const fs = require('fs');
const path = require('path');
const { formatFile, pad, parseFileName } = require('./util');

const MAX_BUNDLE_ID = 256 * 256 * 256 - 1;
const MAX_FILE_SIZE = 1024 * 1024 * 1024 - 1;

function createMaster(cfg, callback) {
    if (!cluster.isMaster) throw new Error('createMaster must in cluster master');

    const { serverId, root, groupId, logger = console } = cfg;
    const serverCfgPath = path.join(root, 'server.cfg');
    const filesIdxPath = path.join(root, 'files.idx');

    let currentBundleId;
    let currentBundleSize;
    let idxFileSize;

    function cb(err, fd) {
        if (err) {
            callback && callback(err);
            return;
        }

        const success = (cfgFd, filesIdxFd) => {
            callback && callback(null);
            idManager(cfgFd, filesIdxFd);
            registerServer(cfg);
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

    function idManager(cfgFd, filesIdxFd) {
        const messageHandler = (msg) => {
            if (!msg || !msg.workerId || !msg.fileSize || !msg.callbackId) {
                return;
            }
            logger.info('create new file:', currentBundleSize, currentBundleId, msg);

            const { workerId, fileSize, callbackId } = msg;
            let newFileSize = currentBundleSize + fileSize;

            if (currentBundleSize < MAX_FILE_SIZE) {
                currentBundleSize = newFileSize;
            } else {
                currentBundleId++;
                currentBundleSize = newFileSize = fileSize;
                if (currentBundleId > MAX_BUNDLE_ID) {
                    throw new Error('文件数量大于' + MAX_BUNDLE_ID + '，请更换磁盘!');
                }
            }

            const bundleId = currentBundleId;
            const idxFileOffest = idxFileSize;
            idxFileSize += 12;

            const buf = Buffer.alloc(8);
            buf.writeUIntBE(serverId, 0, 1);
            buf.writeUIntBE(bundleId, 1, 3);
            buf.writeUIntBE(newFileSize, 4, 4);

            const worker = cluster.workers[workerId];

            // 将文件信息写入cfg文件
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

                idxBuf.writeUIntBE(dirId, 0, 1);
                idxBuf.writeUIntBE(subDirId, 1, 1);
                idxBuf.writeUIntBE(fileId, 2, 1);
                idxBuf.writeUIntBE(mime, 3, 1);
                idxBuf.writeUIntBE(fileStart, 4, 4);
                idxBuf.writeUIntBE(fileSize, 8, 4);

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
                            serverId,
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

        logger.info('currentBundleId:', currentBundleId, 'currentBundleSize:', currentBundleSize, 'idxFileSize:', idxFileSize);

        for (const id in cluster.workers) {
            cluster.workers[id].on('message', messageHandler);
        }
    }

    let hbTimeout;
    let hbClient;

    function registerServer() {
        // console.log('server registerServer');
        if (hbTimeout) {
            clearTimeout(hbTimeout);
            hbTimeout = null;
        }

        const { groupId, serverId, registry, port } = cfg;

        const serverBuf = Buffer.from([1, groupId, serverId, 0, 0, 0, 0, 0, 0, 0]);
        serverBuf.writeUInt32BE(port, 3);
        serverBuf.writeUIntBE(currentBundleId, 7, 3);

        if (!hbClient) {
            hbClient = net.createConnection({
                host: registry.host,
                port: registry.port
            }, () => {
                hbClient.write(serverBuf);
            })
                .on('timeout', () => {
                    hbClient.end();
                })
                .on('error', handleUnkowError)
                .on('end', handleUnkowError)
                .on('close', handleUnkowError)
                .on('data', (buf) => {
                    let timeout;
                    // 失败
                    if (buf.length == 1 && buf.readUInt8() === 0) {
                        timeout = 3000;
                    } else {
                        timeout = 5000;
                    }
                    hbTimeout = setTimeout(() => {
                        registerServer();
                    }, timeout);
                });
        } else {
            hbClient.write(serverBuf);
        }
    }

    function handleUnkowError() {
        hbClient = null;
        if (!hbTimeout) {
            logger.info('disconnected from register!');
            hbTimeout = setTimeout(registerServer, 5000);
        }
    }

    fs.open(serverCfgPath, 'wx', (err, fd) => {
        if (err) {
            if (err.code === 'EEXIST') {
                fs.readFile(serverCfgPath, (err, buf) => {
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
                    currentBundleId = buf.readUIntBE(1, 3);
                    currentBundleSize = buf.readUIntBE(4, 4);
                    cb(null);
                });
                return;
            }
            cb(err);
        }
        fs.write(fd, Buffer.from([serverId, 0, 0, 0, 0, 0, 0, 0]), (err) => {
            if (err) {
                cb(err);
                return;
            }
            currentBundleId = 0;
            currentBundleSize = 0;
            cb(null, fd);
        });
    });
}

exports.createMaster = createMaster;

function createServer(cfg) {
    if (!cluster.isWorker) throw new Error('createServer必须在子线程中运行!');

    const { root, port, logger = console } = cfg;

    let cid = 0;
    let filePromise;

    const callbacks = {};

    process.on('message', (msg) => {
        if (!msg || !msg.callbackId) {
            return;
        }
        const { callbackId } = msg;

        callbacks[callbackId](msg);
        delete callbacks[callbackId];
    });

    function getFileInfo(mime, fileSize) {
        return new Promise((resolve, reject) => {
            const callbackId = ++cid;
            callbacks[callbackId] = (msg) => {
                logger.info('getFileInfo callback:', msg, 'workerId:', cluster.worker.id);

                const { success, filePath } = msg;
                if (success) {
                    fs.open(filePath, 'a', (err, fd) => {
                        if (err) return reject(err);

                        fs.fstat(fd, (err, stats) => {
                            if (err) return reject(err);

                            if (stats.size === 0) {
                                // 预先分配1GB空间
                                fs.write(fd, Buffer.from([0]), 0, 1, MAX_FILE_SIZE, (err) => {
                                    if (err) return reject(err);
                                    resolve({ msg, fd });
                                });
                            } else {
                                resolve({ msg, fd });
                            }
                        });
                    });
                } else {
                    reject(msg);
                }
            };

            process.send({
                callbackId,
                workerId: cluster.worker.id,
                mime,
                fileSize
            });
        });
    }

    const server = net.createServer((socket) => {
        logger.info('client connected! workerId:', cluster.worker.id);

        socket.on('end', () => {
            logger.info('client disconnected! workerId:', cluster.worker.id);
        });

        socket.on('data', (buf) => {
            // 操作类型: enum { 0: '读', 1: '写', 2: '断点续传' }
            const type = buf.readUInt8(0);

            if (type == 0) {
                const fileName = buf.toString('utf8', 1);
                const {
                    dir,
                    subDir,
                    file,
                    fileStart,
                    fileSize
                } = parseFileName(fileName);

                const filePath = path.join(root, dir, subDir, file);

                fs.open(filePath, 'r', (err, fd) => {
                    if (err) {
                        logger.error(err);
                        socket.write(Buffer.from([0]));
                        return;
                    }

                    const readBuf = Buffer.alloc(fileSize);
                    fs.read(fd, readBuf, 0, fileSize, fileStart, (err, bytesRead, buffer) => {
                        if (err) {
                            logger.error(err);
                            socket.write(Buffer.from([0]));
                            return;
                        }

                        socket.cork();
                        socket.write(Buffer.from([1]));
                        socket.write(buffer);
                        socket.uncork();
                    });
                });
            } else if (type === 1) {
                // 小文件(小于256*256*256)直接保存
                const mime = buf.readUInt8(1);
                const fileSize = buf.readUIntBE(2, 3);
                const headBuf = Buffer.from([0, 0, 0, 0, 0]);
                headBuf.writeUInt32BE(fileSize, 1);

                getFileInfo(mime, fileSize)
                    .then(({ fd, msg }) => {
                        const { fileStart, fileName } = msg;
                        fs.write(fd, buf, 5, buf.length - 5, fileStart, (err) => {
                            if (err) {
                                socket.write(headBuf);
                                return;
                            }
                            headBuf.writeUInt8(1, 0);
                            socket.write(Buffer.concat([headBuf, Buffer.from(fileName, 'utf8')]));
                        });
                    })
                    .catch(() => {
                        socket.write(headBuf);
                    });
            } else if (type === 2) {
                // 较大文件断点续传
                const mime = buf.readUInt8(1);
                const fileSize = buf.readUIntBE(2, 4);

                if (!filePromise) {
                    filePromise = getFileInfo(mime, fileSize);
                }

                let sliceSize = 0;
                do {
                    const offset = buf.readUIntBE(sliceSize + 6, 4);
                    const rangeSize = buf.readUIntBE(sliceSize + 10, 3);
                    const rangeOffset = sliceSize + 13;

                    const headBuf = Buffer.from([0, 0, 0, 0, 0]);
                    headBuf.writeUInt32BE(rangeSize, 1);

                    filePromise
                        .then(({ fd, msg }) => {
                            const { fileStart, fileName } = msg;

                            fs.write(fd, buf, rangeOffset, rangeSize, fileStart + offset, (err) => {
                                if (err) {
                                    socket.write(headBuf);
                                    return;
                                }
                                headBuf.writeUInt8(1, 0);
                                socket.write(Buffer.concat([headBuf, Buffer.from(fileName, 'utf8')]));
                            });
                        })
                        .catch(() => {
                            socket.write(headBuf);
                        });

                    sliceSize = rangeOffset + rangeSize;
                } while (sliceSize < buf.length);
            }
        });
    });
    server.on('error', (err) => {
        logger.error(err);
    });
    server.listen(port, () => {
        logger.info('server bound workerId:', cluster.worker.id);
    });
}

exports.createServer = createServer;