const Server = require('./lib/server');
const { createClient } = require('./lib/client');
const { startRegistry } = require('./lib/registry');
const { parseFileName } = require('./lib/util');


exports.startRegistryServer = require('./lib/RegistryServer').start;
exports.startFileServer = require('./lib/FileServer').start;
exports.createFileClient = require('./lib/FileClient').create;

exports.Server = Server;
exports.createClient = createClient;
exports.startRegistry = startRegistry;
exports.parseFileName = parseFileName;