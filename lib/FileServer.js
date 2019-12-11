
const cluster = require('cluster');
const net = require('net');
const socketUtils = require('./socket-utils');
const FileSync = require('./FileSync');
const FileIndexManager = require('./FileIndexManager');
const FileWorker = require('./FileWorker');

class FileServer {

    static start(cfg) {
        if (cluster.isMaster) {
            const master = new FileServer(cfg);
            master.start((err) => {
                if (err) {
                    master.logger.info(err);
                } else {
                    const threads = cfg.maxWorkers || require('os').cpus().length;
                    for (let i = 0; i < threads; i++) {
                        cluster.fork();
                    }
                    cluster.on('exit', (worker, code, signal) => {
                        console.log(`worker ${worker.process.pid} died`, code, signal);
                        cluster.fork();
                    });
                }
            });
        } else {
            const worker = new FileWorker(cfg);
            worker.start();
        }
    }

    constructor({ logger, isSlave, ...cfg }) {
        this.logger = logger || console;
        this.isSlave = isSlave === true;
        this.cfg = cfg;

        this.uploadingFiles = [];

        this.fileIndexManager = new FileIndexManager({
            logger: this.logger,
            cfg: this.cfg
        });

        this.registerServer = this.registerServer.bind(this);
        this.handleDisconnectFromRegistry = this.handleDisconnectFromRegistry.bind(this);
    }

    start(cb) {
        if (!cluster.isMaster) throw new Error('startMaster must in cluster master');

        this.fileIndexManager.init()
            .then(() => {
                cb(null);

                this.registerServer();
                this.registerWorkerListener();
                if (this.isSlave) {
                    this.fileSync = new FileSync({
                        cfg: this.cfg,
                        logger: this.logger,
                        fileIndexManager: this.fileIndexManager
                    });
                    this.fileSync.start();
                }
            })
            .catch(cb);
    }

    registerWorkerListener() {
        const { logger } = this;

        const handleWorkerMessage = (msg) => {
            // console.info('proccess handler:', msg);

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

            if (type === 'GET_CURRENT_BUNDLE') {
                worker.send({
                    success: true,
                    callbackId,
                    bundleSize: fileIndexManager.savedBundleSize,
                    bundleId: fileIndexManager.currentBundleId
                });
            } else if (type === 'ALLOC_FILE_SPACE') {
                const { mime, fileSize } = msg;

                fileIndexManager.newFile(mime, fileSize)
                    .then(res => {
                        this.uploadingFiles.push(res.fileName);
                        worker.send({
                            success: true,
                            callbackId,
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
            } else if (type === 'UPLOAD_SUCCESS') {
                const { fileName } = msg;
                for (let i = this.uploadingFiles.length; i >= 0; i--) {
                    if (this.uploadingFiles[i] == fileName) {
                        this.uploadingFiles.splice(i, 1);
                    }
                }
                if (this.uploadingFiles.length == 0) {
                    fileIndexManager.savedBundleSize = fileIndexManager.currentBundleSize;
                }
            }
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

        const { isSlave } = this;
        const { groupId, serverId, port, registry } = this.cfg;

        const serverBuf = Buffer.from([1, groupId, serverId, 0, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3]);
        serverBuf.writeUInt8(isSlave ? 1 : 0, 3);
        serverBuf.writeUInt32BE(port, 4);
        serverBuf.writeUIntBE(this.fileIndexManager.currentBundleId, 8, 3);
        serverBuf.writeUInt32BE(this.fileIndexManager.currentBundleSize, 11);

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

module.exports = FileServer;