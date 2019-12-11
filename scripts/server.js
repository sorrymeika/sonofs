const sonofs = require('../');

const cfg = {
    groupId: 1,
    serverId: 1,
    root: '/Users/sunlu/Desktop/workspace/nodejs/data',
    port: 8124,
    maxWorkers: 2,
    registry: {
        port: 8123
    }
};

sonofs.startFileServer(cfg);