const { createMaster, createServer } = require('./lib/server');
const { createClient } = require('./lib/client');
const { startRegistry } = require('./lib/registry');

exports.startServerMaster = createMaster;
exports.startServer = createServer;
exports.createClient = createClient;
exports.startRegistry = startRegistry;