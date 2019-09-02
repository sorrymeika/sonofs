const { startServer, startChildThread } = require('./lib/server');
const { createClient } = require('./lib/client');
const { startRegistry } = require('./lib/registry');

exports.Server = {
    start: startServer,
    childThread: startChildThread
};
exports.createClient = createClient;
exports.startRegistry = startRegistry;