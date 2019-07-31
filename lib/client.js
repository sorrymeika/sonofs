const cluster = require('cluster');
const net = require('net');
const fs = require('fs');
const path = require('path');
const { parseFileName } = require('./util');
const { mimeMaps, getMime } = require('./mime');

function uuid() {
    var chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'.split(''),
        uuid = '',
        rnd = 0,
        len = 36,
        r;
    for (var i = 0; i < len; i++) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            uuid += '-';
        } else if (i == 14) {
            uuid += '4';
        } else {
            if (rnd <= 0x02) rnd = 0x2000000 + (Math.random() * 0x1000000) | 0;
            r = rnd & 0xf;
            rnd >>= 4;
            uuid += chars[(i == 19) ? (r & 0x3) | 0x8 : r];
        }
    }
    return uuid;
}

const MAX_CALLBACK_ID = 256 * 256 * 256 - 1;
const MAX_MEM_FILE_SIZE = 256 * 256 * 256;

function createClient(cfg) {
    const {
        tmpDir,
        registry
    } = cfg;

    if (!tmpDir) throw new Error('`tmpDir` can not be null!');

    let registryClient;
    let registryCallbackId = 0;
    let registryCallbacks = {};
    let registryTimer;

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
                        cbInfo.callback(new Error('UNKOW_ERROR'));
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
        if (opt.type == 1) {
            const { fileName } = opt;
            sendBuf = Buffer.concat([Buffer.from([2]), cidBuf, Buffer.from(fileName, 'utf8')]);
        } else {
            sendBuf = Buffer.concat([Buffer.from([3]), cidBuf]);
        }

        if (!registryClient) {
            // 创建与注册中心的连接，获取文件服务器配置信息
            registryClient = net.createConnection({
                host: registry.host,
                port: registry.port
            }, () => {
                registryClient.write(sendBuf);
            })
                .on('timeout', () => {
                    registryClient.end();
                })
                .on('error', handleRegistryUnkowError)
                .on('end', handleRegistryUnkowError)
                .on('close', handleRegistryUnkowError)
                .on('data', (buf) => {
                    const success = buf.readUInt8();
                    const callbackId = buf.readUIntBE(1, 3);
                    const cbInfo = registryCallbacks[callbackId];
                    if (cbInfo) {
                        delete registryCallbacks[callbackId];
                        if (success === 1) {
                            cbInfo.callback(null, JSON.parse(buf.toString('utf8', 4)));
                        } else {
                            cbInfo.callback(new Error('NO_SERVER'));
                        }
                    }
                });
        } else {
            registryClient.write(sendBuf);
        }
    }

    function handleRegistryUnkowError() {
        registryClient = null;
    }

    function getFile(fileName, cb) {
        if (!cb) {
            return new Promise((resolve, reject) => {
                getFile(fileName, (err, file) => {
                    err ? reject(err) : resolve(file);
                });
            });
        }

        getFileServer({
            type: 1,
            fileName
        }, (err, server) => {
            // console.log('getFileServer:', err, server);
            if (err) {
                cb && cb(err);
                return;
            }

            function handleDestroy() {
                cb && cb(new Error('CONNECTION_CLOSED'));
            }

            const { fileSize, mime } = parseFileName(fileName);
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
                .on('error', (e) => {
                    cb && cb(e);
                })
                .on('close', handleDestroy)
                .on('data', (buf) => {
                    try {
                        const success = buf.readUInt8();
                        if (success === 0) {
                            cb && cb(new Error('GET_FILE_FAILURE'));
                        } else {
                            const fileBuf = buf.slice(1);
                            cb && cb(null, {
                                mime: getMime(mime),
                                buffer: fileBuf
                            });
                        }
                        cb = null;
                        client.end();
                    } catch (e) {
                        cb && cb(e);
                        cb = null;
                        client.end();
                    }
                });
        });
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

    function upload(mime, stream, cb) {
        if (!cb) {
            return new Promise((resolve, reject) => {
                upload(mime, stream, (err, file) => {
                    err ? reject(err) : resolve(file);
                });
            });
        }

        if (typeof mime === 'string') {
            mime = mimeMaps[mime] || 6;
        }

        Promise.all([
            getFileServer({ type: 2 }),
            getFileInfo(stream)
        ])
            .then(([server, fileInfo]) => {
                function handleDestroy() {
                    cb && cb(new Error('CONNECTION_CLOSED'));
                }

                let fileRangeSize = 0;
                const fileSize = fileInfo.fileSize;

                const client = net.createConnection({
                    host: server.host,
                    port: server.port,
                    // 以 1M/s 计算过期时间
                    // timeout: Math.min(30000, Math.max(2000, (fileSize / (1024 * 1024)) * 1000))
                }, () => {
                    if (fileInfo.type === 'buffer') {
                        const buf = fileInfo.buffer;
                        console.log('一次性上传 mime:', mime, 'buf len', buf.length);

                        // 一次性上传
                        client.cork();
                        const headBuf = Buffer.alloc(5);
                        headBuf.writeUInt8(1);
                        headBuf.writeUInt8(mime, 1);
                        headBuf.writeUIntBE(fileSize, 2, 3);
                        client.write(headBuf);
                        client.write(buf);
                        client.uncork();
                    } else {
                        console.log('分片上传 mime:', mime);
                        // 分片上传

                        let rangeSize = MAX_MEM_FILE_SIZE;
                        let offset = 0;

                        fs.open(fileInfo.path, 'r', (err, fd) => {
                            if (err) {
                                return client.end();
                            }

                            for (; offset < fileSize; offset += rangeSize) {
                                let bufSize = Math.min(rangeSize, fileSize - offset);
                                const headBuf = Buffer.alloc(13);
                                headBuf.writeUInt8(2);
                                headBuf.writeUInt8(mime, 1);
                                headBuf.writeUIntBE(fileSize, 2, 4);
                                headBuf.writeUIntBE(offset, 6, 4);
                                headBuf.writeUIntBE(bufSize, 10, 3);

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
                        console.log('timeout');
                        client.end();
                    })
                    .on('error', (e) => {
                        cb && cb(e);
                    })
                    .on('close', handleDestroy)
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
                                        cb && cb(new Error('UPLOAD_FAILURE'));
                                    } else {
                                        cb && cb(null, buf.toString('utf8', cursor - 17, cursor));
                                    }
                                    cb = null;
                                    client.end();
                                }
                            } while (cursor < buf.length);
                        } catch (e) {
                            console.log('err', e);
                            cb && cb(e);
                            cb = null;
                            client.end();
                        }
                    });
            })
            .catch(cb);
    }

    return {
        getFile,
        upload
    };
}

exports.createClient = createClient;