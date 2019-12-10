
const cluster = require('cluster');
const net = require('net');
const socketUtils = require('./socket-utils');
const { startSlave } = require('./slave');
const FileIndexManager = require('./FileIndexManager');

class FileServer {
    constructor({ logger, isSlave, ...cfg }) {
        this.logger = logger || console;
        this.isSlave = isSlave === true;
        this.cfg = cfg;

        this.fileIndexManager = new FileIndexManager({
            logger: this.logger,
            cfg: this.cfg
        });

        this.registerServer = this.registerServer.bind(this);
        this.handleDisconnectFromRegistry = this.handleDisconnectFromRegistry.bind(this);
    }

    start() {
        if (!cluster.isMaster) throw new Error('startMaster must in cluster master');

        this.fileIndexManager.init()
            .then(() => {
                this.registerServer();
                this.registerWorkerListener();
            });
    }

    registerWorkerListener() {
        const { logger } = this;

        const handleWorkerMessage = (msg) => {
            // logger.info('proccess handler:', msg);

            if (!msg || !msg.workerId || !msg.callbackId) {
                logger.info('bad message:', msg);
                return;
            }
            const { type, workerId, callbackId } = msg;
            const worker = cluster.workers[workerId];

            if (this.isSlave) {
                worker.send({
                    success: false,
                    callbackId,
                    error: 'slave机不可上传文件!'
                });
                return;
            }

            const { fileIndexManager } = this;

            if (type === 'getCurrentBundle') {
                worker.send({
                    success: true,
                    callbackId,
                    bundleSize: fileIndexManager.savedBundleSize,
                    bundleId: fileIndexManager.currentBundleId
                });
                return;
            }

            const { mime, fileSize } = msg;

            fileIndexManager.newFile(mime, fileSize)
                .then(res => {
                    worker.send({
                        success: true,
                        ...res,
                    });
                })
                .catch(e => {
                    worker.send({
                        success: false,
                        callbackId,
                        error: e
                    });
                });
        };

        for (const id in cluster.workers) {
            cluster.workers[id].on('message', handleWorkerMessage);
        }
    }

    /**
     * 启动服务器注册服务
     */
    registerServer() {
        if (this.hbTimeout) {
            clearTimeout(this.hbTimeout);
            this.hbTimeout = null;
        }

        const { isSlave, currentBundleId, currentBundleSize } = this;
        const { groupId, serverId, port, registry } = this.cfg;

        const serverBuf = Buffer.from([1, groupId, serverId, 0, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3]);
        serverBuf.writeUInt8(isSlave ? 1 : 0, 3);
        serverBuf.writeUInt32BE(port, 4);
        serverBuf.writeUIntBE(currentBundleId, 8, 3);
        serverBuf.writeUInt32BE(currentBundleSize, 11);

        let { registryClient, handleDisconnectFromRegistry } = this;

        if (!registryClient) {
            this.registryClient = registryClient = net.createConnection({
                host: registry.host,
                port: registry.port
            }, () => {
                socketUtils.sendBlock(registryClient, 1, serverBuf);
            })
                .on('timeout', () => {
                    registryClient.end();
                })
                .on('error', handleDisconnectFromRegistry)
                .on('end', handleDisconnectFromRegistry)
                .on('close', handleDisconnectFromRegistry);

            socketUtils.onReceiveBlock(registryClient, (type) => {
                let timeout;
                if (type == 1) {
                    timeout = 5000;
                } else {
                    // 失败
                    timeout = 3000;
                }
                this.hbTimeout = setTimeout(this.registerServer, timeout);
            });
        } else {
            socketUtils.sendBlock(registryClient, 1, serverBuf);
        }
    }

    handleDisconnectFromRegistry() {
        this.registryClient = null;
        if (!this.hbTimeout) {
            this.logger.info('disconnected from register!');
            this.hbTimeout = setTimeout(this.registerServer, 5000);
        }
    }
}

exports.FileServer = FileServer;