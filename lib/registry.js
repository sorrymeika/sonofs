const net = require('net');
const { parseFileName } = require('./util');

const MAX_CONNECTIONS = Math.pow(2, 32);
const MAX_BUNDLE_ID = 256 * 256 * 256 - 1;

function startRegistry(cfg, cb) {
    const { port, logger = console } = cfg;
    const servers = [];

    const registryServer = net.createServer((socket) => {
        let type;
        let cfg;

        socket.on('timeout', () => {
            logger.info('socket timeout');
            socket.end();
        });

        socket.on('data', (buf) => {
            type = buf.readUInt8();

            if (type === 1) {
                // Server
                const groupId = buf.readUInt8(1);
                const serverId = buf.readUInt8(2);
                const port = buf.readUInt32BE(3);
                const bundleId = buf.readUIntBE(7, 3);
                cfg = {
                    bundleId,
                    groupId,
                    serverId,
                    port,
                    host: socket.remoteAddress
                };
                // console.log('registerServer:', cfg);
                registerServer(cfg);
                socket.write(Buffer.from([1]));
            } else {
                const callbackId = buf.readUIntBE(1, 3);
                let server;

                if (type === 2) {
                    // Client Get File
                    const fileName = buf.toString('utf8', 4);
                    server = getReadServer(fileName);
                } else if (type === 3) {
                    // Client Upload File
                    server = getUploadServer();
                }

                if (server == null) {
                    socket.write(Buffer.from([0]));
                } else {
                    socket.cork();
                    socket.write(Buffer.from([1]));
                    const callbackIdBuf = Buffer.alloc(3);
                    callbackIdBuf.writeUIntBE(callbackId, 0, 3);
                    socket.write(callbackIdBuf);
                    socket.write(JSON.stringify(server));
                    socket.uncork();
                }
            }
        });

        socket.on('close', () => {
            if (type === 1 && cfg) {
                // 文件服务器断联后需要移除服务器
                const { groupId, serverId, host, port } = cfg;
                for (let i = servers.length - 1; i >= 0; i--) {
                    const srv = servers[i];
                    if (srv.groupId === groupId && srv.serverId === serverId && srv.host === host && srv.port === port) {
                        // remove server
                        servers.splice(i, 1);
                        console.log('remove server', cfg);
                    }
                }
            }
        });
    });

    registryServer.listen(port, () => {
        logger.info('opened registry on', registryServer.address());
        cb && cb();
    });

    function registerServer({ groupId, serverId, host, port, bundleId }) {
        let server = servers.find((srv) => (srv.groupId === groupId && serverId === srv.serverId && srv.host === host && srv.port === port));
        if (!server) {
            server = {
                groupId,
                serverId,
                host,
                port,
                connections: 0
            };
            servers.push(server);
        }
        server.bundleId = bundleId;
        server.r_expireAt = Date.now() + 10000;
    }

    function getReadServer(fileName) {
        const { groupId, serverId } = parseFileName(fileName);

        let result;
        let minConnections = -1;

        for (let i = 0; i < servers.length; i++) {
            const server = servers[i];
            if (server.serverId === serverId && server.groupId === groupId && server.r_expireAt > Date.now()) {
                if (minConnections == -1 || server.connections < minConnections) {
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

    function getUploadServer() {
        let result;
        let minConnections = -1;

        for (let i = 0; i < servers.length; i++) {
            const server = servers[i];
            if (server.r_expireAt > Date.now()) {
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

exports.startRegistry = startRegistry;