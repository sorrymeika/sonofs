const { createMaster, createServer } = require('./lib/server');

exports.startUploadMaster = createMaster;
exports.startUploadServer = createServer;