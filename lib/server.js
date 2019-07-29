
const cluster = require('cluster');
const net = require('net');
const fs = require('fs');
const path = require('path');
const { formatFile, pad, parseFileName } = require('./util');

const MAX_FILE_ID = 256 * 256 * 256 - 1;
const MAX_FILE_SIZE = 1024 * 1024 * 1024 - 1;

function createMaster(cfg, callback) {
    if (!cluster.isMaster) throw new Error('createMaster must in cluster master');

    const { serverId, root, groupId } = cfg;
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
            console.log('new file:', currentBundleSize, currentBundleId, msg);

            const { workerId, fileSize, callbackId } = msg;
            let newFileSize = currentBundleSize + fileSize;

            if (newFileSize > MAX_FILE_SIZE) {
                currentBundleId++;
                currentBundleSize = newFileSize = fileSize;
                if (currentBundleId > MAX_FILE_ID) {
                    throw new Error('文件数量大于' + MAX_FILE_ID + '，请更换磁盘!');
                }
            } else {
                currentBundleSize = newFileSize;
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

        console.log('currentBundleId:', currentBundleId, 'currentBundleSize:', currentBundleSize, 'idxFileSize:', idxFileSize);

        for (const id in cluster.workers) {
            cluster.workers[id].on('message', messageHandler);
        }
    }

    let hbStarted;
    let hbTimeout;
    let hbClient;

    function registerServer() {
        if (hbTimeout) {
            clearTimeout(hbTimeout);
            hbTimeout = null;
        }
        hbStarted = false;

        const { groupId, serverId, registry, port } = cfg;
        const portBuf = Buffer.alloc(4);
        portBuf.writeUInt32BE(port);
        const sendData = Buffer.concat([Buffer.from([1, groupId, serverId]), portBuf]);
        if (!hbClient) {
            hbClient = net.createConnection({
                host: registry.host,
                port: registry.port
            }, () => {
                // console.log(info);
                hbClient.write(sendData);
            })
                .on('timeout', () => {
                    hbClient.end();
                })
                .on('error', handleUnkowError)
                .on('end', handleUnkowError)
                .on('close', handleUnkowError)
                .on('data', (buf) => {
                    console.log(buf);
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
            hbClient.write(sendData);
        }
    }

    function handleUnkowError() {
        hbClient = null;
        if (!hbTimeout) {
            console.log('handleUnkowError');
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

    const { root, port } = cfg;

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
            // 操作类型: enum { 0: '读', 1: '写' }
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
                        socket.write(Buffer.from([0]));
                        return;
                    }

                    const readBuf = Buffer.alloc(fileSize);
                    fs.read(fd, readBuf, 0, fileSize, fileStart, (err, bytesRead, buffer) => {
                        if (err) {
                            socket.write(Buffer.from([0]));
                            return;
                        }

                        socket.cork();
                        socket.write(Buffer.from([1]));
                        socket.write(buffer);
                        socket.uncork();
                    });
                });
            } else {
                const mime = buf.readUInt8(1);
                const fileSize = buf.readUIntBE(2, 3);

                const callbackId = ++cid;
                callbacks[callbackId] = (msg) => {
                    console.log('callback:', msg, 'workerId:', cluster.worker.id);
                    const { success, fileStart, filePath, fileName } = msg;
                    if (success) {
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
                    } else {
                        socket.write(Buffer.from([0]));
                    }
                };

                process.send({
                    callbackId,
                    workerId: cluster.worker.id,
                    mime,
                    fileSize
                });
            }
        });
    });
    server.on('error', (err) => {
        console.error(err);
    });
    server.listen(port, () => {
        console.log('server bound workerId:', cluster.worker.id);
    });
}

exports.createServer = createServer;