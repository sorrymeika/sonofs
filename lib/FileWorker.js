const cluster = require('cluster');
const fs = require('fs');
const net = require('net');
const path = require('path');
const { parseFileName, getBundlePath } = require('./util');
const socketUtils = require('./socket-utils');

const { MAX_FILE_SIZE } = require('./consts');

const createMessageChannel = require('./createMessageChannel');

class FileWorker {
    constructor({ logger, ...cfg }) {
        this.cfg = cfg;
        this.logger = logger || console;
        this.messageChannel = createMessageChannel();
    }

    start() {
        if (!cluster.isWorker) throw new Error('startWorker必须在子线程中运行!');

        let filePromise;

        const server = net.createServer((socket) => {
            this.logger.info('client connected! workerId:', cluster.worker.id);

            let uploadingFileName;

            // 操作类型: enum { 0: '读', 1: '写', 2: '断点续传', 3: '同步文件给slave' }
            let type;
            let allBuf;
            let total;
            let reading = false;
            let readingSize = false;

            socket
                .on('end', () => {
                    if (type == 1 || type == 2) {
                        this.messageChannel.postMessage({
                            type: 'UPLOAD_SUCCESS',
                            fileName: uploadingFileName
                        });
                    }
                    this.logger.info('client disconnected! workerId:', cluster.worker.id);
                })
                .on('error', (e) => {
                    this.logger.error('server error! workerId:', cluster.worker.id, 'error:', e);
                });

            const readBuffer = (chunk) => {
                if (!reading) {
                    reading = true;
                    type = chunk.readUInt8(0);
                    allBuf = chunk;
                    readingSize = false;

                    if (type == 0) {
                        // 文件信息18字节
                        total = 18;
                    } else if (type == 3) {
                        // slave服务器信息12字节
                        total = 12;
                    } else if (allBuf.length < 5) {
                        readingSize = true;
                        return;
                    } else {
                        total = allBuf.readUInt32BE(1);
                    }
                } else {
                    allBuf = Buffer.concat([allBuf, chunk]);
                }

                if (readingSize && allBuf.length > 5) {
                    readingSize = false;
                    total = allBuf.readUInt32BE(1);
                }

                if (allBuf.length >= total) {
                    const buf = allBuf;
                    reading = false;
                    allBuf = null;

                    if (buf.length > total) {
                        handleBuffer(buf.slice(0, total));
                        readBuffer(buf.slice(total));
                    } else {
                        handleBuffer(buf);
                    }
                }
            };

            const handleBuffer = (buf) => {
                if (type == 0) {
                    // 获取文件
                    const fileName = buf.toString('utf8', 1);
                    this.sendFile(socket, fileName);
                } else if (type === 1) {
                    // 小文件上传，小于256*256*256直接保存
                    const CLIENT_HEAD_LENGTH = 9;
                    const mime = buf.readUInt8(5);
                    const fileSize = buf.readUIntBE(6, 3);
                    const headBuf = Buffer.from([0, 0, 0, 0]);
                    headBuf.writeUInt32BE(fileSize);

                    this.logger.info('receive file - mime:', mime, 'fileSize:', fileSize);

                    this.allocFileSpace(mime, fileSize)
                        .then(({ fd, msg }) => {
                            const { fileStart, fileName } = msg;
                            uploadingFileName = fileName;

                            this.logger.info('save file:', uploadingFileName);

                            fs.write(fd, buf, CLIENT_HEAD_LENGTH, buf.length - CLIENT_HEAD_LENGTH, fileStart, (err) => {
                                if (err) {
                                    socketUtils.sendBlock(socket, 0, headBuf);
                                    return;
                                }
                                socketUtils.sendBlock(socket, 1, Buffer.concat([headBuf, Buffer.from(fileName, 'utf8')]));
                            });
                        })
                        .catch(() => {
                            socketUtils.sendBlock(socket, 0, headBuf);
                        });
                } else if (type === 2) {
                    // 较大文件断点续传
                    const mime = buf.readUInt8(5);
                    const fileSize = buf.readUIntBE(6, 4);

                    if (!filePromise) {
                        filePromise = this.allocFileSpace(mime, fileSize);
                    }

                    const offset = buf.readUIntBE(10, 4);
                    const rangeSize = buf.readUIntBE(14, 3);
                    const CLIENT_HEADER_SIZE = 17;

                    const headBuf = Buffer.from([0, 0, 0, 0]);
                    headBuf.writeUInt32BE(rangeSize);

                    filePromise
                        .then(({ fd, msg }) => {
                            const { fileStart, fileName } = msg;
                            uploadingFileName = fileName;

                            fs.write(fd, buf, CLIENT_HEADER_SIZE, rangeSize, fileStart + offset, (err) => {
                                if (err) {
                                    socketUtils.sendBlock(socket, 0, headBuf);
                                    return;
                                }
                                socketUtils.sendBlock(socket, 1, Buffer.concat([headBuf, Buffer.from(fileName, 'utf8')]));
                            });
                        })
                        .catch(() => {
                            socketUtils.sendBlock(socket, 0, headBuf);
                        });
                } else if (type === 3) {
                    // 同步文件给slave
                    const slaveBundleId = buf.readUIntBE(1, 3);
                    const slaveBundleSize = buf.readUInt32BE(4);
                    const callbackSyncId = buf.readUInt32BE(8);

                    this.getCurrentBundle()
                        .then((currentBundle) => {
                            if (slaveBundleId == currentBundle.bundleId) {
                                // 未生成新bundle
                                if (slaveBundleSize == currentBundle.bundleSize) {
                                    // 无新文件
                                    socket.write(Buffer.from([0]));
                                } else {
                                    // 当前bundle有新文件
                                    this.sendBundleToSlave(socket, callbackSyncId, currentBundle.bundleId, getBundlePath(this.cfg.root, slaveBundleId), slaveBundleSize, currentBundle.bundleSize);
                                }
                            } else if (slaveBundleId < currentBundle.bundleId) {
                                // 已有新bundle
                                const bundlePath = getBundlePath(this.cfg.root, slaveBundleId);

                                fs.stat(bundlePath, (err, stats) => {
                                    if (err) {
                                        socket.write(Buffer.from([0]));
                                        return;
                                    }

                                    if (slaveBundleSize >= stats.size) {
                                        // 读取新bundle
                                        const newBundleId = slaveBundleId + 1;
                                        if (newBundleId === currentBundle.bundleId) {
                                            this.sendBundleToSlave(socket, callbackSyncId, newBundleId, getBundlePath(this.cfg.root, newBundleId), 0, currentBundle.bundleSize);
                                        } else {
                                            this.sendNewBundleToSlave(socket, callbackSyncId, newBundleId, getBundlePath(this.cfg.root, newBundleId));
                                        }
                                    } else {
                                        // 同步slave bundle
                                        this.sendBundleToSlave(socket, callbackSyncId, slaveBundleId, bundlePath, slaveBundleSize, stats.size);
                                    }
                                });
                            } else {
                                // slave 的 bundleId 不应该大于 master 的 bundleId
                                socket.write(Buffer.from([0]));
                            }
                        });
                }
            };

            socket.on('data', readBuffer);
        });

        server.on('error', (err) => {
            this.logger.error(err);
        });

        server.listen(this.cfg.port, () => {
            this.logger.info('server bound workerId:', cluster.worker.id);
        });
    }

    sendFile(socket, fileName) {
        const {
            dir,
            subDir,
            file,
            fileStart,
            fileSize
        } = parseFileName(fileName);

        const filePath = path.join(this.cfg.root, dir, subDir, file);

        fs.open(filePath, 'r', (err, fd) => {
            if (err) {
                this.logger.error(err);
                socket.write(Buffer.from([0]));
                return;
            }

            const readBuf = Buffer.alloc(fileSize);
            fs.read(fd, readBuf, 0, fileSize, fileStart, (err, bytesRead, buffer) => {
                if (err) {
                    this.logger.error(err);
                    socket.write(Buffer.from([0]));
                    return;
                }

                socket.cork();
                socket.write(Buffer.from([1]));
                socket.write(buffer);
                socket.uncork();
            });
        });
    }

    sendNewBundleToSlave(socket, syncId, newBundleId, newBundlePath) {
        fs.stat(newBundlePath, (err, stats) => {
            if (err) {
                socket.write(Buffer.from([0]));
                return;
            }
            this.sendBundleToSlave(socket, syncId, newBundleId, newBundlePath, 0, stats.size);
        });
    }

    sendBundleToSlave(socket, syncId, syncingBundleId, syncingBundlePath, slaveBundleSize, bundleFileSize) {
        const head = Buffer.alloc(12);

        head.writeUInt8(1);
        head.writeUIntBE(syncingBundleId, 1, 3);
        head.writeInt32BE(bundleFileSize, 4);
        head.writeInt32BE(syncId, 8);

        socket.write(head);

        const rs = fs.createReadStream(syncingBundlePath, {
            start: slaveBundleSize,
            end: bundleFileSize - 1,
            autoClose: true
        });

        rs.on('data', (chunk) => {
            socket.write(chunk);
        });
    }

    // 写入前先向主线程申请文件空间
    allocFileSpace(mime, fileSize) {
        return new Promise((resolve, reject) => {
            this.messageChannel.postMessage({
                type: 'ALLOC_FILE_SPACE',
                mime,
                fileSize
            }, (msg) => {
                // this.logger.info('allocFileSpace:', msg, 'workerId:', cluster.worker.id);
                const { success, filePath } = msg;
                if (success) {
                    fs.open(filePath, fs.constants.O_WRONLY | fs.constants.O_CREAT, (err, fd) => {
                        if (err) return reject(err);

                        fs.fstat(fd, (err, stats) => {
                            if (err) return reject(err);

                            if (stats.size === 0) {
                                // 预先分配1GB空间
                                fs.write(fd, Buffer.from([1]), 0, 1, MAX_FILE_SIZE, (err) => {
                                    if (err) return reject(err);
                                    resolve({ msg, fd });
                                });
                            } else {
                                resolve({ msg, fd });
                            }
                        });
                    });
                } else {
                    reject(msg);
                }
            });
        });
    }

    getCurrentBundle() {
        return new Promise((resolve, reject) => {
            this.messageChannel.postMessage({
                type: 'GET_CURRENT_BUNDLE',
            }, (res) => {
                if (res.success) {
                    resolve(res);
                } else {
                    reject(res);
                }
            });
        });
    }
}

module.exports = FileWorker;