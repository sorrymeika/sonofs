const sonofs = require('../');

const cfg = {
    groupId: 1,
    serverId: 1,
    root: '/Users/sunlu/Desktop/workspace/nodejs/data1',
    port: 8125,
    isSlave: true,
    maxWorkers: 2,
    registry: {
        port: 8123
    }
};

sonofs.startFileServer(cfg);