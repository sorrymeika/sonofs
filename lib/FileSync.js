const net = require('net');
const fs = require('fs');
const path = require('path');

const { getBundlePath } = require('./util');
const socketUtils = require('./socket-utils');

const MAX_SYNC_ID = 256 * 256 * 256 * 256 - 100;

class FileSync {
    constructor({ logger, cfg, fileIndexManager }) {
        this.logger = logger;
        this.cfg = cfg;
        this.fileIndexManager = fileIndexManager;
    }

    start() {
        this.interval = setInterval(() => {
            this.syncFromMaster();
        }, 1000);

        this.syncFromMaster();
    }

    stop() {
        if (this.interval) {
            clearInterval(this.interval);
            this.interval = null;
        }
    }

    syncFromMaster() {
        if (this.isSyncing) {
            return;
        }
        this.isSyncing = true;

        const syncId = this.newSyncId();

        const slaveBuf = Buffer.from([3, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]);
        slaveBuf.writeUIntBE(this.fileIndexManager.currentBundleId, 1, 3);
        slaveBuf.writeUInt32BE(this.fileIndexManager.currentBundleSize, 4);
        slaveBuf.writeUInt32BE(syncId, 8);

        if (this.master) {
            this.master.write(slaveBuf);
        } else {
            this.connectMaster(slaveBuf);
        }
    }

    async connectMaster(slaveBuf) {
        const masterCfg = await this.getMasterCfg();
        if (!masterCfg) {
            return;
        }

        let masterClient;
        let syncId = 0;
        let isDownloading = false;
        let isWriting = false;

        let downloadError;

        masterClient = net.createConnection({
            host: masterCfg.host,
            port: masterCfg.port
        }, () => {
            masterClient.write(slaveBuf);
        })
            .on('timeout', () => {
                masterClient.end();
            });

        this.master = masterClient;

        const HEADER_SIZE = 12;

        let allBuf;
        let bundleId;
        let bundleSize;

        const readBuffer = (chunk) => {
            if (!isDownloading) {
                const status = chunk.readUInt8();
                if (status == 0) {
                    // 无新文件
                    return finishDownloading();
                }

                allBuf = chunk;
                isDownloading = true;
                isWriting = false;

                if (allBuf.length >= HEADER_SIZE) {
                    startWrite(chunk);
                }
            } else if (!isWriting) {
                allBuf = Buffer.concat([allBuf, chunk]);
                if (allBuf.length >= HEADER_SIZE) {
                    startWrite(allBuf);
                }
            } else {
                saveChunk(chunk);
            }
        };

        let chunks;
        let currentBundleFd;
        let needDownloadSize = 0;
        let downloadedSize = 0;
        let currentPosition = 0;

        const startWrite = (buf) => {
            downloadedSize = 0;
            isWriting = true;
            currentBundleFd = null;
            chunks = [];
            bundleId = buf.readUIntBE(1, 3);
            bundleSize = buf.readUInt32BE(4);
            const remoteSyncId = buf.readInt32BE(8);
            if (syncId != remoteSyncId) {
                finishDownloading();
                return;
            }

            const bundlePath = getBundlePath(this.cfg.root, bundleId);
            const bundleDir = path.dirname(bundlePath);

            const bodySize = buf.length - HEADER_SIZE;

            if (bundleId === this.fileIndexManager.currentBundleId) {
                currentPosition = this.fileIndexManager.currentBundleSize;
                needDownloadSize = bundleSize - this.fileIndexManager.currentBundleSize;
            } else {
                currentPosition = 0;
                needDownloadSize = bundleSize;
            }
            const position = currentPosition;
            currentPosition += bodySize;

            this.logger.info('StartWrite - currentBundleSize:', this.fileIndexManager.currentBundleSize, 'bundleSize:', bundleSize, 'needDownloadSize:', needDownloadSize);

            fs.mkdir(bundleDir, { recursive: true }, () => {
                fs.open(bundlePath, fs.constants.O_WRONLY | fs.constants.O_CREAT, (err, fd) => {
                    if (err) {
                        return catchDownloadError(err);
                    } else {
                        currentBundleFd = fd;
                        if (buf.length > HEADER_SIZE) {
                            fs.write(fd, buf, HEADER_SIZE, bodySize, position, (err) => {
                                if (err) {
                                    return catchDownloadError(err);
                                }
                                downloadedSize += bodySize;
                                checkDownloadFinished();
                            });
                        }
                        chunks.forEach((chunkInfo) => {
                            saveBundle(chunkInfo.buf, chunkInfo.position);
                        });
                        chunks = null;
                    }
                });
            });
        };

        const saveChunk = (buf) => {
            const position = currentPosition;
            currentPosition += buf.length;
            if (currentBundleFd) {
                saveBundle(buf, position);
            } else {
                chunks.push({
                    position,
                    buf
                });
            }
        };

        const saveBundle = (chunk, position) => {
            fs.write(currentBundleFd, chunk, 0, chunk.length, position, (err) => {
                if (err) {
                    return catchDownloadError(err);
                }
                downloadedSize += chunk.length;
                checkDownloadFinished();
            });
        };

        const checkDownloadFinished = () => {
            if (downloadedSize >= needDownloadSize) {
                if (downloadError) {
                    catchDownloadError(downloadError);
                } else {
                    this.logger.info('DownloadFinished - bundleId:', bundleId, 'bundleSize:', bundleSize, ' downloadedSize:', downloadedSize, 'needDownloadSize:', needDownloadSize);

                    try {
                        this.fileIndexManager.saveCfg(bundleId, bundleSize);
                        finishDownloading();
                    } catch (err) {
                        catchDownloadError(err);
                    }
                }
            }
        };

        const handleDisconnect = () => {
            masterClient = null;
            finishDownloading();
        };

        const catchDownloadError = (err) => {
            downloadError = err;
            this.logger.error(err);
            masterClient.end();
            finishDownloading();
        };

        const finishDownloading = () => {
            this.isSyncing = false;
            isDownloading = false;
            downloadError = null;
            isWriting = false;
        };

        masterClient.on('data', readBuffer)
            .on('error', handleDisconnect)
            .on('close', handleDisconnect);
    }

    newSyncId() {
        if (this.syncId >= MAX_SYNC_ID) {
            this.syncId = 0;
        } else {
            this.syncId++;
        }
        return this.syncId;
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