const cluster = require('cluster');
const net = require('net');
const fs = require('fs');
const path = require('path');
const { parseFileName, uuid } = require('./util');
const socketUtils = require('./socket-utils');
const { mimeMaps, getMime } = require('./mime');

const MAX_CALLBACK_ID = 256 * 256 * 256 - 1;
const MAX_MEM_FILE_SIZE = 256 * 256 * 256;

function callbackOnce(callback) {
    return (err, data) => {
        if (callback) {
            callback(err, data);
            callback = null;
        }
    };
}

function createClient(cfg) {
    const {
        tmpDir,
        registry,
        logger = console
    } = cfg;

    if (!tmpDir) throw new Error('`tmpDir` can not be null!');

    const getFileServer = useGetFileServer(registry);

    /**
     * 获取文件
     * @param {string} fileName
     * @param {Function} callback
     */
    function getFile(fileName, callback) {
        if (!callback) {
            return new Promise((resolve, reject) => {
                getFile(fileName, (err, file) => {
                    err ? reject(err) : resolve(file);
                });
            });
        }

        const cb = callbackOnce(callback);

        getFileServer({
            type: 1,
            fileName
        }, (err, server) => {
            if (err) {
                cb(err);
                return;
            }

            const fileInfo = parseFileName(fileName);
            const { fileSize, mime } = fileInfo;

            const client = net.createConnection({
                host: server.host,
                port: server.port,
                // 以 1M/s 计算过期时间
                timeout: Math.min(30000, Math.max(2000, (fileSize / (1024 * 1024)) * 1000))
            }, () => {
                client.cork();
                client.write(Buffer.from([0]));
                client.write(fileName, 'utf8');
                client.uncork();
            })
                .on('timeout', () => {
                    client.end();
                })
                .on('error', cb)
                .on('close', () => {
                    cb(new Error('CONNECTION_CLOSED'));
                });

            let reading = false;
            let allBuf;

            function readBuffer(chunk) {
                if (!reading) {
                    const success = chunk.readUInt8();
                    if (success === 0) {
                        cb(new Error('GET_FILE_FAILURE'));
                        client.end();
                        return;
                    }
                    reading = true;
                    allBuf = chunk;
                } else {
                    allBuf = Buffer.concat([allBuf, chunk]);
                }

                if (allBuf.length >= fileSize + 1) {
                    reading = false;
                    const fileBuf = allBuf.slice(1);
                    cb(null, {
                        mime: getMime(mime),
                        buffer: fileBuf
                    });
                }
            }

            client.on('data', (chunk) => {
                try {
                    readBuffer(chunk);
                } catch (e) {
                    cb(e);
                    client.end();
                }
            });
        });
    }

    /**
     * 上传文件
     * @param {string|number} mime 文件mime类型
     * @param {*} stream 文件流
     * @param {*} callback
     */
    function upload(mime, stream, callback) {
        if (!callback) {
            return new Promise((resolve, reject) => {
                upload(mime, stream, (err, file) => {
                    err ? reject(err) : resolve(file);
                });
            });
        }

        if (typeof mime === 'string') {
            mime = mimeMaps[mime] || 6;
        }

        const cb = callbackOnce(callback);

        Promise.all([
            getFileServer({ type: 2 }),
            getFileInfo(stream)
        ])
            .then(([server, fileInfo]) => {
                let fileRangeSize = 0;
                let tempFilePath;
                const fileSize = fileInfo.fileSize;

                const client = net.createConnection({
                    host: server.host,
                    port: server.port,
                    // 以 1M/s 计算过期时间
                    timeout: Math.min(30000, Math.max(2000, (fileSize / (1024 * 1024)) * 1000))
                }, () => {
                    if (fileInfo.type === 'buffer') {
                        // 一次性上传
                        const headBuf = Buffer.alloc(9);
                        headBuf.writeUInt8(1);
                        headBuf.writeUInt32BE(fileSize + 9, 1);
                        headBuf.writeUInt8(mime, 5);
                        headBuf.writeUIntBE(fileSize, 6, 3);
                        const buf = fileInfo.buffer;

                        client.cork();
                        client.write(headBuf);
                        client.write(buf);
                        client.uncork();
                    } else {
                        // 分片上传
                        let rangeSize = MAX_MEM_FILE_SIZE;
                        let offset = 0;

                        tempFilePath = fileInfo.path;

                        fs.open(fileInfo.path, 'r', (err, fd) => {
                            if (err) {
                                return client.end();
                            }

                            for (; offset < fileSize; offset += rangeSize) {
                                let bufSize = Math.min(rangeSize, fileSize - offset);
                                const headBuf = Buffer.alloc(17);
                                headBuf.writeUInt8(2);
                                headBuf.writeUInt32BE(17 + bufSize, 1);
                                headBuf.writeUInt8(mime, 5);
                                headBuf.writeUIntBE(fileSize, 6, 4);
                                headBuf.writeUIntBE(offset, 10, 4);
                                headBuf.writeUIntBE(bufSize, 14, 3);

                                let readBuf = Buffer.alloc(bufSize);
                                fs.read(fd, readBuf, 0, bufSize, offset, (err, bytesRead, buf) => {
                                    if (err) return client.end();

                                    client.cork();
                                    client.write(headBuf);
                                    client.write(buf);
                                    client.uncork();
                                });
                            }
                        });
                    }
                })
                    .on('timeout', () => {
                        logger.info('timeout');
                        client.end();
                    })
                    .on('error', cb)
                    .on('close', () => {
                        tempFilePath && fs.unlink(tempFilePath, () => { });
                        cb(new Error('CONNECTION_CLOSED'));
                    })
                    .on('data', (buf) => {
                        try {
                            let cursor = 0;
                            do {
                                const success = buf.readUInt8(cursor);
                                fileRangeSize += buf.readUInt32BE(cursor + 1);
                                cursor += 5;
                                if (success) {
                                    cursor += 17;
                                }
                                if (fileRangeSize === fileSize) {
                                    if (success === 0) {
                                        cb(new Error('UPLOAD_FAILURE'));
                                    } else {
                                        cb(null, buf.toString('utf8', cursor - 17, cursor));
                                    }
                                    client.end();
                                }
                            } while (cursor < buf.length);
                        } catch (e) {
                            logger.error(e);
                            cb(e);
                            client.end();
                        }
                    });
            })
            .catch(cb);
    }


    let tmpFileId = 0;
    function openTempFile(cb) {
        const tmpFilePath = path.join(tmpDir, uuid() + '.' + process.pid + '.' + (cluster.isMaster ? 0 : cluster.worker.id) + '.' + tmpFileId);
        tmpFileId++;

        if (tmpFileId > MAX_CALLBACK_ID) {
            tmpFileId = 0;
        }

        fs.open(tmpFilePath, 'a', (err, fd) => {
            if (err) return cb(err);
            cb(null, {
                fd,
                path: tmpFilePath
            });
        });
    }

    function getFileInfo(stream, cb) {
        if (!cb) {
            return new Promise((resolve, reject) => {
                getFileInfo(stream, (err, file) => {
                    err ? reject(err) : resolve(file);
                });
            });
        }

        let fileSize = 0;
        let error = null;
        let type = 'buffer';
        let buffers = [];
        let tmpFileCreating = false;
        let tmpFilePath;
        let tmpFd;
        let tmpFilePosition;

        let promises = [];

        stream.on('data', (chunk) => {
            fileSize += chunk.length;
            if (fileSize > MAX_MEM_FILE_SIZE) {
                type = 'file';

                if (!tmpFileCreating && !tmpFd) {
                    tmpFileCreating = true;
                    buffers.push(chunk);

                    promises.push(
                        new Promise((resolve, reject) => {
                            openTempFile((err, res) => {
                                if (err) return reject(err);
                                const buffer = Buffer.concat(buffers);
                                tmpFd = res.fd;
                                tmpFilePath = res.path;
                                tmpFilePosition = buffer.length;
                                fs.write(tmpFd, buffer, (err) => {
                                    if (err) return reject(err);
                                    resolve();
                                });
                                buffers = null;
                            });
                        })
                    );
                } else if (tmpFd) {
                    promises.push(
                        new Promise((resolve, reject) => {
                            fs.write(tmpFd, chunk, 0, chunk.length, tmpFilePosition, (err) => {
                                err ? reject(err) : resolve();
                            });
                            tmpFilePosition += chunk.length;
                        })
                    );
                } else {
                    buffers.push(chunk);
                }
            } else {
                buffers.push(chunk);
            }
        });

        stream.on('error', (err) => {
            error = err;
        });

        stream.on('end', () => {
            if (promises.length) {
                Promise.all(promises)
                    .then(() => {
                        cb(null, {
                            type,
                            fd: tmpFd,
                            fileSize,
                            path: tmpFilePath
                        });
                    })
                    .catch(cb);
            } else {
                error ? cb(error) : cb(null, {
                    type,
                    fd: tmpFd,
                    fileSize,
                    buffer: Buffer.concat(buffers),
                    path: tmpFilePath
                });
            }
        });
    }

    return {
        getFile,
        upload,
        parseFileName
    };
}

function useGetFileServer(registry) {
    let registryClient;
    let registryCallbackId = 0;
    let registryCallbacks = {};
    let registryTimer;
    let isGettingServer = false;
    let serverQueue = [];

    function getFileServer(opt, cb) {
        if (!cb) {
            return new Promise((resolve, reject) => {
                getFileServer(opt, (err, result) => {
                    err ? reject(err) : resolve(result);
                });
            });
        }

        if (!registryTimer) {
            // 定时器定期判断方法回调时间是否过期
            registryTimer = setInterval(() => {
                const now = Date.now();
                for (let key in registryCallbacks) {
                    const cbInfo = registryCallbacks[key];
                    // 3秒过期
                    if (now - cbInfo.time > 3000) {
                        delete registryCallbacks[key];
                        cbInfo.callback(new Error('UNABLE_TO_CONNECT_TO_THE_REGISTRY'));
                    }
                }
            }, 3000);
        }

        if (registryCallbackId >= MAX_CALLBACK_ID) {
            registryCallbackId = 0;
        }
        registryCallbackId++;
        registryCallbacks[registryCallbackId] = {
            time: Date.now(),
            callback: cb
        };

        // callbackId需要传给registry并回传
        const cidBuf = Buffer.alloc(3);
        cidBuf.writeUIntBE(registryCallbackId, 0, 3);

        let sendBuf;
        let actionType;
        if (opt.type == 1) {
            // 文件下载
            const { fileName } = opt;
            actionType = 2;
            sendBuf = Buffer.concat([cidBuf, Buffer.from(fileName, 'utf8')]);
        } else {
            // 文件上传
            actionType = 3;
            sendBuf = cidBuf;
        }

        if (!registryClient) {
            isGettingServer = true;
            // 创建与注册中心的连接，获取文件服务器配置信息
            registryClient = net.createConnection({
                host: registry.host,
                port: registry.port
            }, () => {
                socketUtils.sendBlock(registryClient, actionType, sendBuf);
            })
                .on('timeout', () => {
                    registryClient.end();
                })
                .on('error', handleRegistryUnkowError)
                .on('end', handleRegistryUnkowError)
                .on('close', handleRegistryUnkowError);

            socketUtils.onReceiveBlock(registryClient, (resultType, buf) => {
                const callbackId = buf.readUIntBE(0, 3);
                const cbInfo = registryCallbacks[callbackId];
                if (cbInfo) {
                    delete registryCallbacks[callbackId];
                    if (resultType === 1) {
                        cbInfo.callback(null, JSON.parse(buf.toString('utf8', 3)));
                    } else {
                        cbInfo.callback(new Error('NO_SERVER'));
                    }
                }

                if (serverQueue.length) {
                    isGettingServer = true;
                    socketUtils.sendBlock(registryClient, actionType, serverQueue.shift());
                } else {
                    isGettingServer = false;
                }
            });
        } else if (!isGettingServer) {
            isGettingServer = true;
            socketUtils.sendBlock(registryClient, actionType, sendBuf);
        } else {
            serverQueue.push(sendBuf);
        }
    }

    function handleRegistryUnkowError() {
        registryClient = null;
    }

    return getFileServer;
}

exports.createClient = createClient;