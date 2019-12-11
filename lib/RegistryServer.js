const net = require('net');
const { parseFileName } = require('./util');
const socketUtils = require('./socket-utils');

const { MAX_BUNDLE_ID } = require('./consts');

const MAX_CONNECTIONS = Math.pow(2, 32);

class RegistryServer {

    static start(cfg, cb) {
        const registryServer = new RegistryServer(cfg);
        registryServer.start(cb);
    }

    constructor({ logger = console, ...cfg }) {
        this.logger = logger;
        this.cfg = cfg;
        this.servers = [];

        this.server = net.createServer((socket) => {
            let type;
            let serverCfg;

            socket.on('timeout', () => {
                this.logger.info('socket timeout');
                socket.end();
            });

            socketUtils.onReceiveBlock(socket, (actionType, buf) => {
                type = actionType;
                if (type === 1) {
                    const groupId = buf.readUInt8(1);
                    const serverId = buf.readUInt8(2);
                    const isSlave = buf.readUInt8(3);
                    const port = buf.readUInt32BE(4);
                    const bundleId = buf.readUIntBE(8, 3);
                    const bundleSize = buf.readUInt32BE(11);
                    serverCfg = {
                        bundleId,
                        bundleSize,
                        groupId,
                        serverId,
                        isSlave,
                        port,
                        host: socket.remoteAddress
                    };
                    // console.log('registerServer:', cfg);
                    this.registerServer(serverCfg);
                    socketUtils.sendBlock(socket, 1);
                } else if (type === 4) {
                    // 获取Master
                    const groupId = buf.readUInt8(0);
                    const serverId = buf.readUInt8(1);
                    const master = this.getMaster(groupId, serverId);

                    socketUtils.sendBlock(socket, 1, Buffer.from(JSON.stringify(master), 'utf8'));
                } else {
                    const callbackId = buf.readUIntBE(0, 3);
                    let server;

                    if (type === 2) {
                        // Client Get File
                        const fileName = buf.toString('utf8', 3);
                        server = this.getReadServer(fileName);
                    } else if (type === 3) {
                        // Client Upload File
                        server = this.getUploadServer();
                    }

                    const callbackIdBuf = Buffer.alloc(3);
                    callbackIdBuf.writeUIntBE(callbackId, 0, 3);

                    if (server == null) {
                        socketUtils.sendBlock(socket, 0, callbackIdBuf);
                    } else {
                        socketUtils.sendBlock(socket, 1, Buffer.concat([callbackIdBuf, Buffer.from(JSON.stringify(server), 'utf8')]));
                    }
                }
            });

            socket.on('close', () => {
                if (type === 1 && serverCfg) {
                    // 文件服务器断联后需要移除服务器
                    const { groupId, serverId, host, port } = serverCfg;
                    for (let i = this.servers.length - 1; i >= 0; i--) {
                        const srv = this.servers[i];
                        if (srv.groupId === groupId && srv.serverId === serverId && srv.host === host && srv.port === port) {
                            // remove server
                            this.servers.splice(i, 1);
                            this.logger.info('remove server', serverCfg);
                        }
                    }
                }
            });
        });
    }

    start(cb) {
        const { server } = this;
        server.listen(this.cfg.port, () => {
            this.logger.info('start registry on', server.address());
            cb && cb();
        });
    }

    registerServer({ groupId, serverId, isSlave, host, port, bundleId, bundleSize }) {
        // Register Server
        let server = this.servers.find((srv) => (srv.groupId === groupId && serverId === srv.serverId && srv.host === host && srv.port === port));
        if (!server) {
            server = {
                groupId,
                serverId,
                host,
                port,
                connections: 0
            };
            this.servers.push(server);
        }
        server.isSlave = isSlave;
        server.bundleId = bundleId;
        server.bundleSize = bundleSize;
        server.r_expireAt = Date.now() + 10000;
    }

    getMaster(groupId, serverId) {
        for (let i = 0; i < this.servers.length; i++) {
            const server = this.servers[i];
            if (server.serverId === serverId && server.groupId === groupId && server.r_expireAt > Date.now() && !server.isSlave) {
                return server;
            }
        }
        return null;
    }

    getReadServer(fileName) {
        const { groupId, serverId, bundleId, fileStart, fileSize } = parseFileName(fileName);

        let result;
        let minConnections = -1;

        for (let i = 0; i < this.servers.length; i++) {
            const server = this.servers[i];
            if (server.serverId === serverId && server.groupId === groupId && server.r_expireAt > Date.now()) {
                // 取连接数最小的服务器
                if (minConnections == -1 || server.connections < minConnections) {
                    // 如果是Slave机，需要判断文件是否已从Master同步
                    if (!server.isSlave || server.bundleId > bundleId || (server.bundleId == bundleId && server.bundleSize >= (fileStart + fileSize))) {
                        minConnections = server.connections;
                        result = server;
                    }
                }
            }
        }

        if (!result) return null;

        result.connections += 1;
        if (result.connections >= MAX_CONNECTIONS) {
            result.connections = 0;
        }
        return result;
    }

    getUploadServer() {
        let result;
        let minConnections = -1;

        for (let i = 0; i < this.servers.length; i++) {
            const server = this.servers[i];
            // Slave机不可上传
            if (server.r_expireAt > Date.now() && !server.isSlave) {
                // 文件服务器文件超限不可上传
                if (server.bundleId < MAX_BUNDLE_ID && (minConnections == -1 || server.connections < minConnections)) {
                    minConnections = server.connections;
                    result = server;
                }
            }
        }

        if (!result) return null;

        result.connections += 1;
        if (result.connections >= MAX_CONNECTIONS) {
            result.connections = 0;
        }
        return result;
    }
}

module.exports = RegistryServer;