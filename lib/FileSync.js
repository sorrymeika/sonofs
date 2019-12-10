const net = require('net');
const fs = require('fs');
const path = require('path');

const { getBundlePath } = require('./util');
const socketUtils = require('./socket-utils');

const MAX_SYNC_ID = 256 * 256 * 256 * 256 - 100;

class FileSync {
    constructor({ logger, cfg }) {
        this.logger = logger;
        this.cfg = cfg;
    }

    getMasterCfg() {
        const {
            registry,
            groupId,
            serverId
        } = this.cfg;

        return new Promise((resolve, reject) => {
            let serverCfg;
            let error;

            const registryClient = net.createConnection({
                host: registry.host,
                port: registry.port
            }, () => {
                socketUtils.sendBlock(registryClient, 4, Buffer.from([groupId, serverId]));
            })
                .on('timeout', () => {
                    registryClient.end();
                })
                .on('error', (e) => {
                    error = e;
                })
                .on('close', () => {
                    error ? reject(error) : resolve(serverCfg);
                });

            socketUtils.onReceiveBlock(registryClient, (type, buf) => {
                try {
                    serverCfg = JSON.parse(buf.toString('utf8'));
                } catch (e) {
                    error = e;
                }
                registryClient.end();
            });
        });
    }
}

module.exports = FileSync;