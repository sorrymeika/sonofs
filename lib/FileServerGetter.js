const net = require('net');
const socketUtils = require('./socket-utils');

const MAX_CALLBACK_ID = 256 * 256 * 256 - 1;

class FileServerGetter {
    constructor({
        logger,
        registryCfg
    }) {
        this.logger = logger;
        this.registryCfg = registryCfg;
        this.registryClient = null;

        this.callbacks = {};
        this.callbackId = 0;
    }

    getFileServer(fileName) {
        return new Promise((resolve, reject) => {
            this.request({
                type: 1,
                fileName
            }, (err, server) => {
                if (err) reject(err);
                else resolve(server);
            });
        });
    }

    getUploadServer() {
        return new Promise((resolve, reject) => {
            this.request({
                type: 2
            }, (err, server) => {
                if (err) reject(err);
                else resolve(server);
            });
        });
    }

    request(opt, cb) {
        this._detectCallbackTimeout();

        const { id: callbackId } = this._newCallback(cb);

        // callbackId需要传给registry并回传
        const cidBuf = Buffer.alloc(3);
        cidBuf.writeUIntBE(callbackId, 0, 3);

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

        if (!this.registryClient) {
            const desposeRegistryClient = () => {
                this.registryClient = null;
            };

            // 创建与注册中心的连接，获取文件服务器配置信息
            this.registryClient = net.createConnection({
                host: this.registryCfg.host,
                port: this.registryCfg.port
            }, () => {
                socketUtils.sendBlock(this.registryClient, actionType, sendBuf);
            })
                .on('timeout', () => {
                    this.registryClient.end();
                })
                .on('error', desposeRegistryClient)
                .on('end', desposeRegistryClient)
                .on('close', desposeRegistryClient);

            socketUtils.onReceiveBlock(this.registryClient, (resultType, buf) => {
                const callbackId = buf.readUIntBE(0, 3);
                const callbackInfo = this.callbacks[callbackId];
                if (callbackInfo) {
                    delete this.callbacks[callbackId];
                    if (resultType === 1) {
                        callbackInfo.callback(null, JSON.parse(buf.toString('utf8', 3)));
                    } else {
                        callbackInfo.callback(new Error('NO_SERVER'));
                    }
                }
            });
        } else {
            socketUtils.sendBlock(this.registryClient, actionType, sendBuf);
        }
    }

    _newCallback(cb) {
        if (this.callbackId >= MAX_CALLBACK_ID) {
            this.callbackId = 0;
        }
        this.callbackId++;
        return this.callbacks[this.callbackId] = {
            id: this.callbackId,
            time: Date.now(),
            callback: cb
        };
    }

    _detectCallbackTimeout() {
        if (!this.callbackTimeoutDetector) {
            // 定时器定期判断方法回调时间是否过期
            this.callbackTimeoutDetector = setInterval(() => {
                const now = Date.now();
                for (let key in this.callbacks) {
                    const callback = this.callbacks[key];
                    // 3秒过期
                    if (now - callback.time > 3000) {
                        delete this.callbacks[key];
                        callback.callback(new Error('UNABLE_TO_CONNECT_TO_THE_REGISTRY'));
                    }
                }
            }, 3000);
        }
    }

    desposeRegistryClient() {
        this.registryClient = null;
    }
}

module.exports = FileServerGetter;