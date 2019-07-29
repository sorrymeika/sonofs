const net = require('net');
const { parseFileName } = require('./util');

function startRegistry(cfg, cb) {
    const { port, logger = console } = cfg;
    const servers = [];

    const registryServer = net.createServer((socket) => {
        socket.on('timeout', () => {
            logger.info('socket timeout');
            socket.end();
        });

        socket.on('data', (buf) => {
            const type = buf.readUInt8();
            if (type === 1) {
                const groupId = buf.readUInt8(1);
                const serverId = buf.readUInt8(2);
                const port = buf.readUInt32BE(3);
                registerServer({
                    groupId,
                    serverId,
                    port,
                    host: socket.remoteAddress
                });
                socket.write(Buffer.from([1]));
            } else if (type === 2) {
                const fileName = buf.toString('utf8', 1);
                const server = getServer(fileName);

                if (server == null) {
                    socket.write(Buffer.from([0]));
                } else {
                    socket.cork();
                    socket.write(Buffer.from([1]));
                    socket.write(JSON.stringify(server));
                    socket.uncork();
                }
            }
        });
    });

    registryServer.listen(port, () => {
        logger.info('opened registry on', registryServer.address());
        cb && cb();
    });

    function registerServer({ groupId, serverId, host, port }) {
        let server = servers.find((srv) => (srv.name === groupId && srv.serverId && srv.host === host && srv.port === port));
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
        server.r_expireAt = Date.now() + 10000;
    }

    function getServer(fileName) {
        const { groupId, serverId } = parseFileName(fileName);
        const server = servers.find((server) => (server.serverId === serverId && server.groupId === groupId && server.r_expireAt > Date.now()));
        if (!server) return null;

        server.connections += 1;
        servers.sort((a, b) => a.connections - b.connections);
        return server;
    }
}

exports.startRegistry = startRegistry;