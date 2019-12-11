const cluster = require('cluster');
const fs = require('fs');
const path = require('path');

const { uuid } = require('./util');
const { MAX_MEM_FILE_SIZE, MAX_CALLBACK_ID } = require('./consts');

class UploadTemporary {
    constructor({ logger, cfg }) {
        this.logger = logger;
        this.cfg = cfg;
        this.tmpFileId = 0;
    }

    read(stream) {
        let fileSize = 0;
        let error = null;
        let type = 'buffer';
        let buffers = [];
        let tmpFileCreating = false;
        let tmpFilePath;
        let tmpFd;
        let tmpFilePosition;

        let promises = [];

        stream.on('data', (chunk) => {
            fileSize += chunk.length;
            if (fileSize > MAX_MEM_FILE_SIZE) {
                type = 'file';

                if (!tmpFileCreating && !tmpFd) {
                    tmpFileCreating = true;
                    buffers.push(chunk);

                    promises.push(
                        this.createTempFile()
                            .then((res) => {
                                const buffer = Buffer.concat(buffers);
                                tmpFd = res.fd;
                                tmpFilePath = res.path;
                                tmpFilePosition = buffer.length;

                                return new Promise((resolve, reject) => {
                                    fs.write(tmpFd, buffer, (err) => {
                                        if (err) return reject(err);
                                        resolve();
                                    });
                                    buffers = null;
                                });
                            })
                    );
                } else if (tmpFd) {
                    promises.push(
                        new Promise((resolve, reject) => {
                            fs.write(tmpFd, chunk, 0, chunk.length, tmpFilePosition, (err) => {
                                err ? reject(err) : resolve();
                            });
                            tmpFilePosition += chunk.length;
                        })
                    );
                } else {
                    buffers.push(chunk);
                }
            } else {
                buffers.push(chunk);
            }
        });

        stream.on('error', (err) => {
            error = err;
        });

        return new Promise((resolve, reject) => {
            stream.on('end', () => {
                if (promises.length) {
                    Promise.all(promises)
                        .then(() => {
                            resolve({
                                type,
                                fd: tmpFd,
                                fileSize,
                                path: tmpFilePath
                            });
                        })
                        .catch(reject);
                } else {
                    error
                        ? reject(error)
                        : resolve({
                            type,
                            fd: tmpFd,
                            fileSize,
                            buffer: Buffer.concat(buffers),
                            path: tmpFilePath
                        });
                }
            });
        });
    }

    createTempFile() {
        const tmpFilePath = path.join(this.cfg.tmpDir, uuid() + '.' + process.pid + '.' + (cluster.isMaster ? 0 : cluster.worker.id) + '.' + this.tmpFileId);
        this.tmpFileId++;

        if (this.tmpFileId > MAX_CALLBACK_ID) {
            this.tmpFileId = 0;
        }

        return new Promise((resolve, reject) => {
            fs.open(tmpFilePath, 'a', (err, fd) => {
                if (err) return reject(err);
                resolve({
                    fd,
                    path: tmpFilePath
                });
            });
        });
    }
}

module.exports = UploadTemporary;