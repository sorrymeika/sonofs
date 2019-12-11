const fs = require('fs');
const path = require('path');

const { formatFile, pad } = require('./util');
const { MAX_BUNDLE_ID, MAX_FILE_SIZE } = require('./consts');

class FileIndexManager {
    constructor({ cfg, logger }) {
        this.cfg = cfg;
        this.logger = logger;
        this.indexFilePath = path.join(this.cfg.root, 'files.idx');
        this.indexFileSize = 0;

        this.currentBundleId = 0;
        this.currentBundleSize = 0;
        this.savedBundleSize = 0;
    }

    async init() {
        const [cfgFd, indexFd] = await Promise.all([this._loadCfg(), this._loadIndex()]);

        this.cfgFd = cfgFd;
        this.indexFd = indexFd;

        this.logger.info('init - currentBundleId:', this.currentBundleId, 'currentBundleSize:', this.currentBundleSize, 'idxFileSize:', this.idxFileSize);
    }

    _loadCfg(complete) {
        if (!complete) {
            return new Promise((resolve, reject) => {
                this._loadCfg((err, res) => {
                    if (err) reject(err);
                    else resolve(res);
                });
            });
        }

        const { serverId, root } = this.cfg;
        const serverCfgPath = path.join(root, 'server.cfg');

        fs.open(serverCfgPath, 'wx', (err, fd) => {
            if (err) {
                if (err.code === 'EEXIST') {
                    fs.readFile(serverCfgPath, (err, buf) => {
                        if (err) {
                            return complete(err);
                        }

                        // buf[0] : serverId
                        // buf[1~3] : bundleId
                        // buf[4～7] : bundleSize
                        const sid = buf.readUInt8();
                        if (sid !== serverId) {
                            return complete(new Error('服务器ID错误!'));
                        }
                        this.currentBundleId = buf.readUIntBE(1, 3);
                        this.savedBundleSize = this.currentBundleSize = buf.readUIntBE(4, 4);

                        fs.open(serverCfgPath, fs.constants.O_WRONLY | fs.constants.O_CREAT, (err, fd) => {
                            if (err) {
                                complete(err);
                            } else {
                                complete(null, fd);
                            }
                        });
                    });
                } else {
                    complete(err);
                }
            } else {
                fs.write(fd, Buffer.from([serverId, 0, 0, 0, 0, 0, 0, 0]), (err) => {
                    if (err) {
                        return complete(err);
                    }
                    this.currentBundleId = 0;
                    this.savedBundleSize = this.currentBundleSize = 0;
                    complete(null, fd);
                });
            }
        });
    }

    _loadIndex() {
        return new Promise((resolve, reject) => {
            fs.open(this.indexFilePath, fs.constants.O_WRONLY | fs.constants.O_CREAT, (err, indexFd) => {
                if (err) {
                    return reject(err);
                }

                fs.stat(indexFd, (err, stats) => {
                    if (err) {
                        return reject(err);
                    }
                    this.indexFileSize = stats.size;
                    resolve(indexFd);
                });
            });
        });
    }

    saveCfg(bundleId, bundleSize) {
        const { serverId } = this.cfg;
        const buf = Buffer.alloc(8);
        buf.writeUIntBE(serverId, 0, 1);
        buf.writeUIntBE(bundleId, 1, 3);
        buf.writeUIntBE(bundleSize, 4, 4);

        this.currentBundleId = bundleId;
        this.currentBundleSize = bundleSize;

        return new Promise((resolve, reject) => {
            // 将文件信息写入cfg文件
            fs.write(this.cfgFd, buf, 0, 8, 0, (err) => {
                if (err) {
                    return reject(err);
                }
                resolve();
            });
        });
    }

    async newFile(mime, fileSize) {
        let newFileSize = this.currentBundleSize + fileSize;

        if (this.currentBundleSize < MAX_FILE_SIZE) {
            this.currentBundleSize = newFileSize;
        } else {
            this.currentBundleId++;
            this.currentBundleSize = newFileSize = fileSize;
            if (this.currentBundleId > MAX_BUNDLE_ID) {
                throw new Error('文件数量大于' + MAX_BUNDLE_ID + '，请更换磁盘!');
            }
        }
        const bundleId = this.currentBundleId;
        const fileOffest = this.fileSize;
        this.indexFileSize += 12;

        await this.saveCfg(bundleId, this.currentBundleSize);

        return new Promise((reject, resolve) => {
            const fileStart = newFileSize - fileSize;

            // files.idx: 文件索引，每个文件索引占12byte : dir:1 subdir:1 file:1 mime:1 fileStart:4 fileSize:4
            const idxBuf = Buffer.alloc(12);
            const dirId = bundleId >> 16;
            const subDirId = (bundleId & 65535) >> 8;
            const fileId = bundleId & 255;

            idxBuf.writeUIntBE(dirId, 0, 1);
            idxBuf.writeUIntBE(subDirId, 1, 1);
            idxBuf.writeUIntBE(fileId, 2, 1);
            idxBuf.writeUIntBE(mime, 3, 1);
            idxBuf.writeUIntBE(fileStart, 4, 4);
            idxBuf.writeUIntBE(fileSize, 8, 4);

            fs.write(this.indexFd, idxBuf, 0, 12, fileOffest, (err) => {
                if (err) return reject(err);

                const dir = pad(dirId.toString(16), 2);
                const subDir = pad(subDirId.toString(16), 2);
                const file = pad(fileId.toString(16), 2);
                const fileDir = path.join(this.cfg.root, dir, subDir);
                const filePath = path.join(fileDir, file + '.snf');

                const {
                    groupId,
                    serverId
                } = this.cfg;

                fs.mkdir(fileDir, { recursive: true }, () => {
                    const fileName = formatFile({
                        groupId,
                        serverId,
                        dir,
                        subDir,
                        file,
                        mime,
                        fileStart,
                        fileSize
                    });

                    resolve({
                        fileStart,
                        fileName,
                        fileSize,
                        bundleId,
                        mime,
                        dir,
                        subDir,
                        file,
                        filePath
                    });
                });
            });
        });
    }
}

module.exports = FileIndexManager;