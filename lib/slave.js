const net = require('net');
const fs = require('fs');
const path = require('path');

const { getBundlePath } = require('./util');

const MAX_SYNC_ID = 256 * 256 * 256 * 256 - 100;


/**
 * slave机需启动文件同步服务
 */
function startSlave({
    logger,
    cfgFd,
    cfg,
    bundle
}) {
    const {
        root,
        registry,
        groupId,
        serverId
    } = cfg;

    // 获取Master
    function getMasterCfg(cb) {
        let serverCfg;
        let error;

        const callback = () => cb(error, serverCfg);
        const registryClient = net.createConnection({
            host: registry.host,
            port: registry.port
        }, () => {
            registryClient.write(Buffer.from([4, groupId, serverId]));
        })
            .on('timeout', () => {
                registryClient.end();
            })
            .on('error', (e) => {
                error = e;
            })
            .on('close', callback)
            .on('data', (buf) => {
                if (buf.length && buf.readUInt8() === 1) {
                    serverCfg = JSON.parse(buf.toString('utf8', 1));
                }
                registryClient.end();
            });
    }

    let heartBeat;
    let masterClient;
    let syncId = 0;
    let isSyncing = false;
    let isDownloading = false;
    let isWriting = false;

    let downloadError;

    function syncFromMaster() {
        if (isSyncing) {
            return;
        }
        isSyncing = true;

        if (heartBeat) {
            clearTimeout(heartBeat);
            heartBeat = null;
        }

        if (syncId >= MAX_SYNC_ID) {
            syncId = 0;
        } else {
            syncId++;
        }

        const slaveBuf = Buffer.from([3, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]);
        slaveBuf.writeUIntBE(bundle.currentBundleId, 1, 3);
        slaveBuf.writeUInt32BE(bundle.currentBundleSize, 4);
        slaveBuf.writeUInt32BE(syncId, 8);

        if (masterClient) {
            masterClient.write(slaveBuf);
        } else {
            getMasterCfg((error, masterCfg) => {
                if (error || !masterCfg) {
                    return handleDisconnect();
                }

                masterClient = net.createConnection({
                    host: masterCfg.host,
                    port: masterCfg.port
                }, () => {
                    masterClient.write(slaveBuf);
                })
                    .on('timeout', () => {
                        masterClient.end();
                    })
                    .on('error', handleDisconnect)
                    .on('close', handleDisconnect);

                const HEADER_SIZE = 12;

                let allBuf;
                let bundleId;
                let bundleSize;

                function readBuffer(chunk) {
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
                }

                let chunks;
                let currentBundleFd;
                let needDownloadSize = 0;
                let downloadedSize = 0;
                let currentPosition = 0;

                function startWrite(buf) {
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

                    const bundlePath = getBundlePath(root, bundleId);
                    const bundleDir = path.dirname(bundlePath);

                    const bodySize = buf.length - HEADER_SIZE;

                    if (bundleId === bundle.currentBundleId) {
                        currentPosition = bundle.currentBundleSize;
                        needDownloadSize = bundleSize - bundle.currentBundleSize;
                    } else {
                        currentPosition = 0;
                        needDownloadSize = bundleSize;
                    }
                    const position = currentPosition;
                    currentPosition += bodySize;

                    console.log('StartWrite - currentBundleSize:', bundle.currentBundleSize, 'bundleSize:', bundleSize, 'needDownloadSize:', needDownloadSize);

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
                }

                function saveChunk(buf) {
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
                }

                function saveBundle(chunk, position) {
                    fs.write(currentBundleFd, chunk, 0, chunk.length, position, (err) => {
                        if (err) {
                            return catchDownloadError(err);
                        }
                        downloadedSize += chunk.length;
                        checkDownloadFinished();
                    });
                }

                function checkDownloadFinished() {
                    if (downloadedSize >= needDownloadSize) {
                        if (downloadError) {
                            catchDownloadError(downloadError);
                        } else {
                            logger.info('DownloadFinished - bundleId:', bundleId, 'bundleSize:', bundleSize, ' downloadedSize:', downloadedSize, 'needDownloadSize:', needDownloadSize);
                            saveCfg(bundleId, bundleSize, (err) => {
                                if (err) {
                                    catchDownloadError(err);
                                } else {
                                    finishDownloading();
                                }
                            });
                        }
                    }
                }

                function catchDownloadError(err) {
                    downloadError = err;
                    logger.error(err);
                    masterClient.end();
                    finishDownloading();
                }

                masterClient.on('data', readBuffer);
            });
        }
    }

    function saveCfg(bundleId, bundleSize, cb) {
        bundle.currentBundleId = bundleId;
        bundle.currentBundleSize = bundleSize;

        const buf = Buffer.alloc(8);
        buf.writeUIntBE(serverId, 0, 1);
        buf.writeUIntBE(bundleId, 1, 3);
        buf.writeUIntBE(bundleSize, 4, 4);

        // 将文件信息写入cfg文件
        fs.write(cfgFd, buf, 0, 8, 0, cb);
    }

    function finishDownloading() {
        resetStatus();

        if (heartBeat) {
            clearTimeout(heartBeat);
        }
        heartBeat = setTimeout(syncFromMaster, 1000);
    }

    function handleDisconnect() {
        masterClient = null;
        resetStatus();

        if (!heartBeat) {
            logger.info('disconnected from master!');
            heartBeat = setTimeout(syncFromMaster, 5000);
        }
    }

    function resetStatus() {
        isSyncing = false;
        isDownloading = false;
        downloadError = null;
        isWriting = false;
    }

    syncFromMaster();
}

exports.startSlave = startSlave;