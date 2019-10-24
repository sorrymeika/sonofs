const { startServer, startChildThread } = require('./lib/server');
const { createClient } = require('./lib/client');
const { startRegistry } = require('./lib/registry');
const { parseFileName } = require('./lib/util');

exports.Server = {
    start: startServer,
    childThread: startChildThread
};
exports.createClient = createClient;
exports.startRegistry = startRegistry;
exports.parseFileName = parseFileName;