const net = require('net');

const MAX_CALLBACK_ID = 256 * 256 * 256 - 1;

function createClient(cfg) {
    const {
        registry
    } = cfg;

    let registryClient;
    let registryCallbackId = 0;
    let registryCallbacks = {};
    let registryTimer = {};

    function getFileServer(opt, cb) {
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
            sendBuf = Buffer.concat(Buffer.from([2]), cidBuf, Buffer.from(fileName, 'utf8'));
        } else {
            sendBuf = Buffer.concat(Buffer.from([3]), cidBuf);
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
                    console.log(buf);
                    const success = buf.readUInt8();
                    if (success === 1) {
                        const callbackId = buf.readUIntBE(1, 3);
                        const cbInfo = registryCallbacks[callbackId];
                        if (cbInfo) {
                            delete registryCallbacks[callbackId];
                            cbInfo.callback(null, JSON.parse(buf.toString('utf8', 4)));
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
            console.log(server);
            function handleDestroy() {
                cb && cb(new Error('CONNECTION_CLOSED'));
            }

            const client = net.createConnection({
                host: server.host,
                port: server.port,
                timeout: 5000
            }, () => {
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
                        const res = buf.readUInt8();
                        if (res === 0) {
                            cb && cb(new Error('UPLOAD_FAILURE'));
                        } else {
                            cb && cb(buf.toString('utf8', 1));
                        }
                    } catch (e) {
                        cb && cb(e);
                    }
                    cb = null;
                });
        });
    }

    function upload(mime, fileBuffer, cb) {
        if (!cb) {
            return new Promise((resolve, reject) => {
                upload(mime, fileBuffer, (err, file) => {
                    err ? reject(err) : resolve(file);
                });
            });
        }

        const fileSize = fileBuffer.length;

        getFileServer({
            type: 2
        }, (err, server) => {
            console.log(server);
            function handleDestroy() {
                cb && cb(new Error('CONNECTION_CLOSED'));
            }
            const client = net.createConnection({
                host: server.host,
                port: server.port,
                // 以 1M/s 计算过期时间
                timeout: Math.min(30000, Math.max(2000, (fileSize / (1024 * 1024)) * 1000))
            }, () => {
                if (fileSize < 256 * 256 * 256) {
                    // 一次性上传
                    client.cork();
                    const headBuf = Buffer.from([1, mime, 0, 0, 0]);
                    headBuf.writeUIntBE(fileSize, 2, 3);
                    client.write(fileBuffer);
                    client.uncork();
                } else {
                    // 分片上传
                    client.cork();
                    const headBuf = Buffer.from([2, mime, 0, 0, 0, 0, 0, 0, 0, 0]);
                    headBuf.writeUIntBE(fileSize, 2, 4);
                    let offset = 0;
                    headBuf.writeUIntBE(offset, 6, 4);
                    client.uncork();
                }
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
                        const res = buf.readUInt8();
                        if (res === 0) {
                            cb && cb(new Error('UPLOAD_FAILURE'));
                        } else {
                            cb && cb(buf.toString('utf8', 1));
                        }
                    } catch (e) {
                        cb && cb(e);
                    }
                    cb = null;
                });
        });
    }

    return {
        getFile,
        upload
    };
}

exports.createClient = createClient;