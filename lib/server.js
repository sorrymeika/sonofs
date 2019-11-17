
const cluster = require('cluster');
const net = require('net');
const fs = require('fs');
const path = require('path');
const { formatFile, pad, parseFileName } = require('./util');

const MAX_BUNDLE_ID = 256 * 256 * 256 - 1;
const MAX_FILE_SIZE = 1024 * 1024 * 1024 - 1;
const MAX_SYNC_ID = 256 * 256 * 256 * 256 - 100;

function startServer(cfg, callback) {
    if (!cluster.isMaster) throw new Error('startServer must in cluster master');

    const { serverId, root, groupId, logger = console, isSlave = false, port, registry } = cfg;
    const serverCfgPath = path.join(root, 'server.cfg');
    const filesIdxPath = path.join(root, 'files.idx');

    let currentBundleId;
    let currentBundleSize;
    let idxFileSize;

    fs.open(serverCfgPath, 'wx', (err, fd) => {
        if (err) {
            if (err.code === 'EEXIST') {
                fs.readFile(serverCfgPath, (err, buf) => {
                    if (err) {
                        start(err);
                        return;
                    }
                    console.log(buf);
                    const sid = buf.readUInt8();
                    if (sid !== serverId) {
                        start(err);
                        return;
                    }

                    // buf[0] : serverId
                    // buf[1~3] : bundleId
                    // buf[4～7] : bundleSize
                    currentBundleId = buf.readUIntBE(1, 3);
                    currentBundleSize = buf.readUIntBE(4, 4);
                    start(null);
                });
                return;
            }
            start(err);
        }
        fs.write(fd, Buffer.from([serverId, 0, 0, 0, 0, 0, 0, 0]), (err) => {
            if (err) {
                start(err);
                return;
            }
            currentBundleId = 0;
            currentBundleSize = 0;
            start(null, fd);
        });
    });

    function start(err, fd) {
        if (err) {
            callback && callback(err);
            return;
        }

        const success = (cfgFd, filesIdxFd) => {
            callback && callback(null);
            fileIdManager(cfgFd, filesIdxFd);
            startServerRegistryHb();
            if (isSlave) startSlave(cfgFd);
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
                    fs.open(serverCfgPath, fs.constants.O_WRONLY | fs.constants.O_CREAT, (err, fd) => {
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

    function fileIdManager(cfgFd, filesIdxFd) {
        const messageHandler = (msg) => {
            if (!msg || !msg.workerId || !msg.callbackId) {
                logger.info('bad message:', msg);
                return;
            }
            logger.info('proccess handler:', msg);

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

            if (type === 'getCurrentBundle') {
                worker.send({
                    success: true,
                    callbackId,
                    bundleSize: currentBundleSize,
                    bundleId: currentBundleId
                });
                return;
            }

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
                    registryClient.write(serverBuf);
                })
                    .on('timeout', () => {
                        registryClient.end();
                    })
                    .on('error', handleRegistryUnkowError)
                    .on('end', handleRegistryUnkowError)
                    .on('close', handleRegistryUnkowError)32: 37
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
                registryClient.write(serverBuf);
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

    /**
     * slave机需启动文件同步服务
     */
    function startSlave(cfgFd) {
        // 获取Master
        function getMasterCfg(cb) {
            let serverCfg;
            let error;

            const callback = () => cb(error, serverCfg);
            const registryClient = net.createConnection({
                host: registry.host,
                port: registry.port
            }, () => {
                registryClient.write(Buffer.from([4, groupId, serverId]));
            })
                .on('timeout', () => {
                    registryClient.end();
                })
                .on('error', (e) => {
                    error = e;
                })
                .on('close', callback)
                .on('data', (buf) => {
                    if (buf.length && buf.readUInt8() === 1) {
                        serverCfg = JSON.parse(buf.toString('utf8', 1));
                    }
                    registryClient.end();
                });
        }

        let heartBeat;
        let masterClient;
        let syncId = 0;
        let isSyncing = false;
        let isDownloading = false;
        let bundleId = 0;
        let bundleSize = 0;
        let chunkEnd = 0;
        let currentBundleFd;
        let fdPromise;
        let syncError;

        function syncFromMaster() {
            if (isSyncing) {
                return;
            }
            isSyncing = true;

            if (heartBeat) {
                clearTimeout(heartBeat);
                heartBeat = null;
            }

            if (syncId >= MAX_SYNC_ID) {
                syncId = 0;
            } else {
                syncId++;
            }

            const slaveBuf = Buffer.from([3, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]);
            slaveBuf.writeUIntBE(currentBundleId, 1, 3);
            slaveBuf.writeUInt32BE(currentBundleSize, 4);
            slaveBuf.writeUInt32BE(syncId, 8);

            if (masterClient) {
                masterClient.write(slaveBuf);
            } else {
                getMasterCfg((error, masterCfg) => {
                    if (error || !masterCfg) {
                        return handleDisconnect();
                    }

                    // console.log('masterCfg:', masterCfg);

                    masterClient = net.createConnection({
                        host: masterCfg.host,
                        port: masterCfg.port
                    }, () => {
                        masterClient.write(slaveBuf);
                    })
                        .on('timeout', () => {
                            masterClient.end();
                        })
                        .on('error', handleDisconnect)
                        .on('close', handleDisconnect)
                        .on('data', (buf) => {
                            if (isDownloading) {
                                chunkEnd += buf.length;
                                if (currentBundleFd) {
                                    saveBundle(currentBundleFd, bundleId, bundleSize, buf, chunkEnd);
                                } else {
                                    fdPromise.then((fd) => {
                                        saveBundle(fd, bundleId, bundleSize, buf, chunkEnd);
                                    });
                                }
                                return;
                            }

                            const status = buf.readUInt8();
                            if (status == 0) {
                                // 无新文件
                                finishDownloading();
                            } else {
                                bundleId = buf.readUIntBE(1, 3);
                                bundleSize = buf.readUInt32BE(4);
                                const callbackSyncId = buf.readUInt32BE(8);

                                if (callbackSyncId !== syncId) {
                                    finishDownloading();
                                } else {
                                    // 当前bundle有新文件
                                    isDownloading = true;

                                    fdPromise = new Promise((resolve, reject) => {
                                        const bundlePath = getBundlePath(root, bundleId);
                                        const bundleDir = path.dirname(bundlePath);

                                        if (bundleId === currentBundleId) {
                                            chunkEnd = currentBundleSize + buf.length - 13;
                                        } else {
                                            chunkEnd = buf.length - 13;
                                        }

                                        fs.mkdir(bundleDir, { recursive: true }, () => {
                                            fs.open(bundlePath, 'a', (err, fd) => {
                                                if (err) reject(err);
                                                else {
                                                    if (buf.length > 13) {
                                                        const chunkSize = buf.length - 13;
                                                        fs.write(fd, buf, 13, chunkSize, chunkEnd - chunkSize, (err) => {
                                                            if (err) syncError = err;
                                                            syncBundleComplete(bundleId, bundleSize, chunkEnd);
                                                        });
                                                    } else {
                                                        resolve(currentBundleFd = fd);
                                                    }
                                                }
                                            });
                                        });
                                    });
                                }
                            }
                        });
                });
            }
        }

        function saveBundle(fd, bundleId, bundleSize, chunk, chunkEnd) {
            fs.write(fd, chunk, 0, chunk.length, chunkEnd - chunk.length, (err) => {
                if (err) {
                    syncError = err;
                }
                syncBundleComplete(bundleId, bundleSize, chunkEnd);
            });
        }

        function syncBundleComplete(bundleId, bundleSize, chunkEnd) {
            if (chunkEnd >= bundleSize) {
                if (syncError) {
                    finishDownloading();
                } else {
                    console.log('syncBundleComplete:', bundleId, bundleSize);
                    saveCfg(bundleId, bundleSize, finishDownloading);
                }
            }
        }

        function saveCfg(bundleId, bundleSize, cb) {
            currentBundleId = bundleId;
            currentBundleSize = bundleSize;

            const buf = Buffer.alloc(8);
            buf.writeUIntBE(serverId, 0, 1);
            buf.writeUIntBE(bundleId, 1, 3);
            buf.writeUIntBE(bundleSize, 4, 4);

            // 将文件信息写入cfg文件
            fs.write(cfgFd, buf, 0, 8, 0, cb);
        }

        function finishDownloading() {
            isSyncing = false;
            isDownloading = false;
            fdPromise = null;
            syncError = null;

            if (heartBeat) {
                clearTimeout(heartBeat);
            }
            heartBeat = setTimeout(syncFromMaster, 1000);
        }

        function handleDisconnect() {
            masterClient = null;
            isSyncing = false;

            if (!heartBeat) {
                logger.info('disconnected from master!');
                heartBeat = setTimeout(syncFromMaster, 5000);
            }
        }

        syncFromMaster();
    }
}

exports.startServer = startServer;

function startChildThread(cfg) {
    if (!cluster.isWorker) throw new Error('startChildThread必须在子线程中运行!');

    const { root, port, logger = console } = cfg;
    const {
        allocFileSpace,
        getCurrentBundle
    } = useMessageChannel();

    let filePromise;

    const server = net.createServer((socket) => {
        logger.info('client connected! workerId:', cluster.worker.id);

        socket.on('end', () => {
            logger.info('client disconnected! workerId:', cluster.worker.id);
        });

        socket.on('data', (buf) => {
            // 操作类型: enum { 0: '读', 1: '写', 2: '断点续传', 3: '同步文件给slave' }
            const type = buf.readUInt8(0);

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
                const mime = buf.readUInt8(1);
                const fileSize = buf.readUIntBE(2, 3);
                const headBuf = Buffer.from([0, 0, 0, 0, 0]);
                headBuf.writeUInt32BE(fileSize, 1);

                allocFileSpace(mime, fileSize)
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
                    filePromise = allocFileSpace(mime, fileSize);
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
                            syncBundle(callbackSyncId, currentBundle.bundleId, getBundlePath(root, slaveBundleId), slaveBundleSize, currentBundle.bundleSize, socket);
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
                                    syncBundle(callbackSyncId, newBundleId, getBundlePath(root, newBundleId), 0, currentBundle.bundleSize, socket);
                                } else {
                                    syncNewBundle(callbackSyncId, newBundleId, getBundlePath(root, newBundleId), socket);
                                }
                            } else {
                                // 同步slave bundle
                                syncBundle(callbackSyncId, slaveBundleId, bundlePath, slaveBundleSize, stats.size, socket);
                            }
                        });
                    } else {
                        // slave 的 bundleId 不应该大于 master 的 bundleId
                        socket.write(Buffer.from([0]));
                    }
                });
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

function getBundlePath(root, bundleId) {
    const dirId = bundleId >> 16;
    const subDirId = (bundleId & 65535) >> 8;
    const fileId = bundleId & 255;

    const dir = pad(dirId.toString(16), 2);
    const subDir = pad(subDirId.toString(16), 2);
    const bundleName = pad(fileId.toString(16), 2) + '.snf';
    return path.join(root, dir, subDir, bundleName);
}

function syncNewBundle(syncId, newBundleId, newBundlePath, socket) {
    fs.stat(newBundlePath, (err, stats) => {
        if (err) {
            socket.write(Buffer.from([0]));
            return;
        }
        syncBundle(syncId, newBundleId, newBundlePath, 0, stats.size, socket);
    });
}

function syncBundle(syncId, syncingBundleId, syncingBundlePath, slaveBundleSize, bundleFileSize, socket) {
    const head = Buffer.from([1, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]);

    head.writeUIntBE(syncingBundleId, 1, 3);
    head.writeInt32BE(bundleFileSize, 4);
    head.writeInt32BE(syncId, 8);

    socket.write(head);

    const rs = fs.createReadStream(syncingBundlePath, {
        start: slaveBundleSize,
        end: bundleFileSize,
        autoClose: true
    });

    rs.on('data', (chunk) => {
        socket.write(chunk);
    });
}

function useMessageChannel() {
    const callbacks = {};

    let cid = 0;

    // 写入前先向主线程申请文件空间
    function allocFileSpace(mime, fileSize) {
        return new Promise((resolve, reject) => {
            if (cid >= MAX_SYNC_ID) {
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
                callbackId,
                workerId: cluster.worker.id,
                mime,
                fileSize
            });
        });
    }

    function getCurrentBundle(cb) {
        if (cid >= MAX_SYNC_ID) {
            cid = 0;
        }

        const callbackId = ++cid;
        callbacks[callbackId] = cb;

        process.send({
            type: 'getCurrentBundle',
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

exports.startChildThread = startChildThread;