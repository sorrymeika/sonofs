const cluster = require('cluster');
// const numCPUs = require('os').cpus().length;

const { createMaster, createServer } = require('../lib/server');

const cfg = {
    groupId: 1,
    serverId: 1,
    root: '/Users/sunlu/Desktop/workspace/nodejs/data',
    port: 8124,
    registry: {
        port: 8123
    }
};

if (cluster.isMaster) {
    createMaster(cfg);

    for (let i = 0; i < 2; i++) {
        cluster.fork();
    }

    cluster.on('exit', (worker, code, signal) => {
        console.log(`worker ${worker.process.pid} died`, code, signal);
    });
} else {
    createServer(cfg);
}