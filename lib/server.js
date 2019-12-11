
const cluster = require('cluster');
const net = require('net');
const fs = require('fs');
const path = require('path');
const { formatFile, pad, parseFileName, getBundlePath } = require('./util');
const socketUtils = require('./socket-utils');
const { startSlave } = require('./slave');

const MAX_BUNDLE_ID = 256 * 256 * 256 - 1;
const MAX_FILE_SIZE = 1024 * 1024 * 1024 - 1;
const MAX_CALLBACK_ID = 256 * 256 * 256 * 256 - 100;

function startMaster(cfg, readyFn) {
    if (!cluster.isMaster) throw new Error('startMaster must in cluster master');

    const { serverId, root, groupId, logger = console, isSlave = false, port, registry } = cfg;
    const serverCfgPath = path.join(root, 'server.cfg');
    const filesIdxPath = path.join(root, 'files.idx');

    let currentBundleId;
    let currentBundleSize;
    let savedBundleSize;
    let idxFileSize;

    function start() {
        loadCfg(serverCfgPath, (err, cfgFd) => {
            if (err) {
                readyFn && readyFn(err);
                return;
            }

            fs.open(filesIdxPath, 'a', (err, filesIdxFd) => {
                if (err) {
                    readyFn && readyFn(err);
                    return;
                }

                fs.stat(filesIdxPath, (err, stats) => {
                    if (err) {
                        readyFn && readyFn(err);
                        return;
                    }
                    readyFn && readyFn(null);

                    idxFileSize = stats.size;
                    registerWorkerListener(cfgFd, filesIdxFd);
                    startServerRegistryHb();

                    if (isSlave) {
                        startSlave({
                            logger,
                            cfg,
                            cfgFd,
                            bundle: {
                                get currentBundleId() {
                                    return currentBundleId;
                                },
                                set currentBundleId(val) {
                                    currentBundleId = val;
                                },
                                get currentBundleSize() {
                                    return currentBundleSize;
                                },
                                set currentBundleSize(val) {
                                    currentBundleSize = val;
                                }
                            }
                        });
                    }
                });
            });
        });
    }

    start();

    function loadCfg(serverCfgPath, complete) {
        fs.open(serverCfgPath, 'wx', (err, fd) => {
            if (err) {
                if (err.code === 'EEXIST') {
                    fs.readFile(serverCfgPath, (err, buf) => {
                        if (err) {
                            return complete(err);
                        }
                        console.log(buf);

                        // buf[0] : serverId
                        // buf[1~3] : bundleId
                        // buf[4～7] : bundleSize
                        const sid = buf.readUInt8();
                        if (sid !== serverId) {
                            return complete(new Error('服务器ID错误!'));
                        }
                        currentBundleId = buf.readUIntBE(1, 3);
                        savedBundleSize = currentBundleSize = buf.readUIntBE(4, 4);

                        fs.open(serverCfgPath, fs.constants.O_WRONLY | fs.constants.O_CREAT, (err, fd) => {
                            if (err) {
                                complete(err);
                            } else {
                                complete(null, fd);
                            }
                        });
                    });
                } else {
                    complete(err);
                }
            } else {
                fs.write(fd, Buffer.from([serverId, 0, 0, 0, 0, 0, 0, 0]), (err) => {
                    if (err) {
                        return complete(err);
                    }
                    currentBundleId = 0;
                    savedBundleSize = currentBundleSize = 0;
                    complete(null, fd);
                });
            }
        });
    }

    const uploadingFiles = [];

    function registerWorkerListener(cfgFd, filesIdxFd) {
        const messageListener = (msg) => {
            if (!msg || !msg.workerId || !msg.callbackId) {
                logger.info('bad message:', msg);
                return;
            }
            // logger.info('proccess handler:', msg);

            const { type, workerId, callbackId } = msg;
            const worker = cluster.workers[workerId];

            if (isSlave) {
                worker.send({
                    success: false,
                    callbackId,
                    error: 'slave机不可上传文件!'
                });
                return;
            }

            if (type === 'GET_CURRENT_BUNDLE') {
                worker.send({
                    success: true,
                    callbackId,
                    bundleSize: savedBundleSize,
                    bundleId: currentBundleId
                });
            } else if (type === "ALLOC_FILE_SPACE") {
                const { fileSize } = msg;
                let newFileSize = currentBundleSize + fileSize;

                if (currentBundleSize < MAX_FILE_SIZE) {
                    currentBundleSize = newFileSize;
                } else {
                    currentBundleId++;
                    currentBundleSize = newFileSize = fileSize;
                    if (currentBundleId > MAX_BUNDLE_ID) {
                        worker.send({
                            success: false,
                            callbackId,
                            error: '文件数量大于' + MAX_BUNDLE_ID + '，请更换磁盘!'
                        });
                        return;
                    }
                }

                const bundleId = currentBundleId;
                const idxFileOffest = idxFileSize;
                idxFileSize += 12;

                const buf = Buffer.alloc(8);
                buf.writeUIntBE(serverId, 0, 1);
                buf.writeUIntBE(bundleId, 1, 3);
                buf.writeUIntBE(newFileSize, 4, 4);

                logger.info("serverId", serverId);
                logger.info("newFileSize", newFileSize);
                logger.info("write", buf);

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

                    // files.idx: 文件索引，每个文件索引占12byte : dir:1 subdir:1 file:1 mime:1 fileStart:4 fileSize:4
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
                            uploadingFiles.push(fileName);

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
            } else if (type === 'UPLOAD_SUCCESS') {
                const { fileName } = msg;
                for (let i = uploadingFiles.length; i >= 0; i--) {
                    if (uploadingFiles[i] == fileName) {
                        uploadingFiles.splice(i, 1);
                    }
                }
                if (uploadingFiles.length == 0) {
                    logger.info('all upload success - oldBundleSize:', savedBundleSize, 'currentBundleSize:', currentBundleSize);
                    savedBundleSize = currentBundleSize;
                }
            }
        };

        logger.info('currentBundleId:', currentBundleId, 'currentBundleSize:', currentBundleSize, 'idxFileSize:', idxFileSize);

        for (const id in cluster.workers) {
            cluster.workers[id].on('message', messageListener);
        }
    }

    /**
     * 启动服务器注册服务
     */
    function startServerRegistryHb() {
        let hbTimeout;
        let registryClient;

        function registerServer() {
            if (hbTimeout) {
                clearTimeout(hbTimeout);
                hbTimeout = null;
            }

            const serverBuf = Buffer.from([1, groupId, serverId, 0, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3]);
            serverBuf.writeUInt8(isSlave ? 1 : 0, 3);
            serverBuf.writeUInt32BE(port, 4);
            serverBuf.writeUIntBE(currentBundleId, 8, 3);
            serverBuf.writeUInt32BE(currentBundleSize, 11);

            if (!registryClient) {
                registryClient = net.createConnection({
                    host: registry.host,
                    port: registry.port
                }, () => {
                    socketUtils.sendBlock(registryClient, 1, serverBuf);
                })
                    .on('timeout', () => {
                        registryClient.end();
                    })
                    .on('error', handleRegistryUnkowError)
                    .on('end', handleRegistryUnkowError)
                    .on('close', handleRegistryUnkowError)
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
                socketUtils.sendBlock(registryClient, 1, serverBuf);
            }
        }

        function handleRegistryUnkowError() {
            registryClient = null;
            if (!hbTimeout) {
                logger.info('disconnected from register!');
                hbTimeout = setTimeout(registerServer, 5000);
            }
        }

        registerServer();
    }
}

exports.startMaster = startMaster;

function startWorker(cfg) {
    if (!cluster.isWorker) throw new Error('startWorker必须在子线程中运行!');

    const { root, port, logger = console } = cfg;
    const {
        allocFileSpace,
        getCurrentBundle
    } = createMessageChannel();

    let filePromise;

    const server = net.createServer((socket) => {
        logger.info('client connected! workerId:', cluster.worker.id);

        let uploadingFileName;

        // 操作类型: enum { 0: '读', 1: '写', 2: '断点续传', 3: '同步文件给slave' }
        let type;
        let allBuf;
        let total;
        let reading = false;
        let readingSize = false;

        socket
            .on('end', () => {
                if (type == 1 || type == 2) {
                    process.send({
                        type: 'UPLOAD_SUCCESS',
                        fileName: uploadingFileName,
                        callbackId: -1,
                        workerId: cluster.worker.id,
                    });
                }
                logger.info('client disconnected! workerId:', cluster.worker.id);
            })
            .on('error', (e) => {
                logger.error('server error! workerId:', cluster.worker.id, 'error:', e);
            });

        function readBuffer(chunk) {
            if (!reading) {
                reading = true;
                type = chunk.readUInt8(0);
                allBuf = chunk;
                readingSize = false;

                if (type == 0) {
                    // 文件信息18字节
                    total = 18;
                } else if (type == 3) {
                    // slave服务器信息12字节
                    total = 12;
                } else if (allBuf.length < 5) {
                    readingSize = true;
                    return;
                } else {
                    total = allBuf.readUInt32BE(1);
                }
            } else {
                allBuf = Buffer.concat([allBuf, chunk]);
            }

            if (readingSize && allBuf.length > 5) {
                readingSize = false;
                total = allBuf.readUInt32BE(1);
            }

            if (allBuf.length >= total) {
                reading = false;
                if (allBuf.length > total) {
                    handleBuffer(allBuf.slice(0, total));
                    readBuffer(allBuf.slice(total));
                } else {
                    handleBuffer(allBuf);
                }
                allBuf = null;
            }
        }

        function handleBuffer(buf) {
            if (type == 0) {
                // 获取文件
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
                const CLIENT_HEAD_LENGTH = 9;
                const mime = buf.readUInt8(5);
                const fileSize = buf.readUIntBE(6, 3);
                const headBuf = Buffer.from([0, 0, 0, 0, 0]);
                headBuf.writeUInt32BE(fileSize, 1);

                allocFileSpace(mime, fileSize)
                    .then(({ fd, msg }) => {
                        const { fileStart, fileName } = msg;
                        uploadingFileName = fileName;
                        fs.write(fd, buf, CLIENT_HEAD_LENGTH, buf.length - CLIENT_HEAD_LENGTH, fileStart, (err) => {
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
                const mime = buf.readUInt8(5);
                const fileSize = buf.readUIntBE(6, 4);

                if (!filePromise) {
                    filePromise = allocFileSpace(mime, fileSize);
                }

                let sliceSize = 0;
                do {
                    const offset = buf.readUIntBE(sliceSize + 10, 4);
                    const rangeSize = buf.readUIntBE(sliceSize + 14, 3);
                    const rangeOffset = sliceSize + 17;

                    const headBuf = Buffer.from([0, 0, 0, 0, 0]);
                    headBuf.writeUInt32BE(rangeSize, 1);

                    filePromise
                        .then(({ fd, msg }) => {
                            const { fileStart, fileName } = msg;
                            uploadingFileName = fileName;

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
            } else if (type === 3) {
                // 同步文件给slave
                const slaveBundleId = buf.readUIntBE(1, 3);
                const slaveBundleSize = buf.readUInt32BE(4);
                const callbackSyncId = buf.readUInt32BE(8);

                getCurrentBundle((currentBundle) => {
                    if (slaveBundleId == currentBundle.bundleId) {
                        // 未生成新bundle
                        if (slaveBundleSize == currentBundle.bundleSize) {
                            // 无新文件
                            socket.write(Buffer.from([0]));
                        } else {
                            // 当前bundle有新文件
                            sendBundleToSlave(callbackSyncId, currentBundle.bundleId, getBundlePath(root, slaveBundleId), slaveBundleSize, currentBundle.bundleSize, socket);
                        }
                    } else if (slaveBundleId < currentBundle.bundleId) {
                        // 已有新bundle
                        const bundlePath = getBundlePath(root, slaveBundleId);

                        fs.stat(bundlePath, (err, stats) => {
                            if (err) {
                                socket.write(Buffer.from([0]));
                                return;
                            }

                            if (slaveBundleSize >= stats.size) {
                                // 读取新bundle
                                const newBundleId = slaveBundleId + 1;
                                if (newBundleId === currentBundle.bundleId) {
                                    sendBundleToSlave(callbackSyncId, newBundleId, getBundlePath(root, newBundleId), 0, currentBundle.bundleSize, socket);
                                } else {
                                    sendNewBundleToSlave(callbackSyncId, newBundleId, getBundlePath(root, newBundleId), socket);
                                }
                            } else {
                                // 同步slave bundle
                                sendBundleToSlave(callbackSyncId, slaveBundleId, bundlePath, slaveBundleSize, stats.size, socket);
                            }
                        });
                    } else {
                        // slave 的 bundleId 不应该大于 master 的 bundleId
                        socket.write(Buffer.from([0]));
                    }
                });
            }
        }

        socket.on('data', readBuffer);
    });

    server.on('error', (err) => {
        logger.error(err);
    });

    server.listen(port, () => {
        logger.info('server bound workerId:', cluster.worker.id);
    });
}

function sendNewBundleToSlave(syncId, newBundleId, newBundlePath, socket) {
    fs.stat(newBundlePath, (err, stats) => {
        if (err) {
            socket.write(Buffer.from([0]));
            return;
        }
        sendBundleToSlave(syncId, newBundleId, newBundlePath, 0, stats.size, socket);
    });
}

function sendBundleToSlave(syncId, syncingBundleId, syncingBundlePath, slaveBundleSize, bundleFileSize, socket) {
    const head = Buffer.alloc(12);

    head.writeUInt8(1);
    head.writeUIntBE(syncingBundleId, 1, 3);
    head.writeInt32BE(bundleFileSize, 4);
    head.writeInt32BE(syncId, 8);

    socket.write(head);

    const rs = fs.createReadStream(syncingBundlePath, {
        start: slaveBundleSize,
        end: bundleFileSize - 1,
        autoClose: true
    });

    rs.on('data', (chunk) => {
        socket.write(chunk);
    });
}

function createMessageChannel() {
    const callbacks = {};

    let cid = 0;

    // 写入前先向主线程申请文件空间
    function allocFileSpace(mime, fileSize) {
        return new Promise((resolve, reject) => {
            if (cid >= MAX_CALLBACK_ID) {
                cid = 0;
            }

            const callbackId = ++cid;
            callbacks[callbackId] = (msg) => {
                console.log('allocFileSpace callback:', msg, 'workerId:', cluster.worker.id);

                const { success, filePath } = msg;
                if (success) {
                    fs.open(filePath, fs.constants.O_WRONLY | fs.constants.O_CREAT, (err, fd) => {
                        if (err) return reject(err);

                        fs.fstat(fd, (err, stats) => {
                            if (err) return reject(err);

                            if (stats.size === 0) {
                                // 预先分配1GB空间
                                fs.write(fd, Buffer.from([1]), 0, 1, MAX_FILE_SIZE, (err) => {
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
                type: 'ALLOC_FILE_SPACE',
                callbackId,
                workerId: cluster.worker.id,
                mime,
                fileSize
            });
        });
    }

    function getCurrentBundle(cb) {
        if (cid >= MAX_CALLBACK_ID) {
            cid = 0;
        }

        const callbackId = ++cid;
        callbacks[callbackId] = cb;

        process.send({
            type: 'GET_CURRENT_BUNDLE',
            callbackId,
            workerId: cluster.worker.id,
        });
    }

    process.on('message', (msg) => {
        if (!msg || !msg.callbackId) {
            return;
        }
        const { callbackId } = msg;

        callbacks[callbackId](msg);
        delete callbacks[callbackId];
    });

    return {
        allocFileSpace,
        getCurrentBundle
    };
}

exports.startWorker = startWorker;