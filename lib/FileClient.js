const net = require('net');
const fs = require('fs');
const { parseFileName } = require('./util');
const { MAX_MEM_FILE_SIZE } = require('./consts');
const socketUtils = require('./socket-utils');
const { mimeMaps, getMime } = require('./mime');
const FileServerGetter = require('./FileServerGetter');
const UploadTemporary = require('./UploadTemporary');

class FileClient {

    static create(cfg) {
        return new FileClient(cfg);
    }

    constructor({ logger, ...cfg }) {
        if (!cfg.tmpDir) throw new Error('`cfg.tmpDir` can not be null!');

        this.parseFileName = parseFileName;
        this.logger = logger || console;
        this.cfg = cfg;
        this.fileServerGetter = new FileServerGetter({
            logger: this.logger,
            registryCfg: cfg.registry
        });
        this.uploadTemporary = new UploadTemporary({
            logger,
            cfg
        });
    }

    async getFile(fileName) {
        const server = await this.fileServerGetter.getFileServer(fileName);

        const fileInfo = parseFileName(fileName);
        const { fileSize, mime } = fileInfo;

        return new Promise((resolve, reject) => {
            const client = net.createConnection({
                host: server.host,
                port: server.port,
                // 以 1M/s 计算过期时间
                timeout: Math.min(30000, Math.max(2000, (fileSize / (1024 * 1024)) * 1000))
            }, () => {
                client.cork();
                client.write(Buffer.from([0]));
                client.write(fileName, 'utf8');
                client.uncork();
            })
                .on('timeout', () => {
                    client.end();
                })
                .on('error', reject)
                .on('close', () => {
                    reject(new Error('CONNECTION_CLOSED'));
                });

            let reading = false;
            let allBuf;

            function readBuffer(chunk) {
                if (!reading) {
                    const success = chunk.readUInt8();
                    if (success === 0) {
                        reject(new Error('GET_FILE_FAILURE'));
                        client.end();
                        return;
                    }
                    reading = true;
                    allBuf = chunk;
                } else {
                    allBuf = Buffer.concat([allBuf, chunk]);
                }

                if (allBuf.length >= fileSize + 1) {
                    reading = false;
                    const fileBuf = allBuf.slice(1);
                    resolve({
                        mime: getMime(mime),
                        buffer: fileBuf
                    });
                }
            }

            client.on('data', (chunk) => {
                try {
                    readBuffer(chunk);
                } catch (e) {
                    reject(e);
                    client.end();
                }
            });
        });
    }

    /**
     * 上传文件
     * @param {string|number} mime 文件mime类型
     * @param {*} stream 文件流
     * @param {*} callback
     */
    async upload(mime, stream) {
        if (typeof mime === 'string') {
            mime = mimeMaps[mime] || 6;
        }

        const [server, fileInfo] = await Promise.all([
            this.fileServerGetter.getUploadServer(),
            this.uploadTemporary.read(stream)
        ]);

        let tempFilePath;
        const fileSize = fileInfo.fileSize;

        const client = net.createConnection({
            host: server.host,
            port: server.port,
            // 以 1M/s 计算过期时间
            timeout: Math.min(30000, Math.max(2000, (fileSize / (1024 * 1024)) * 1000))
        }, () => {
            if (fileInfo.type === 'buffer') {
                // 一次性上传
                const headBuf = Buffer.alloc(9);
                headBuf.writeUInt8(1);
                headBuf.writeUInt32BE(fileSize + 9, 1);
                headBuf.writeUInt8(mime, 5);
                headBuf.writeUIntBE(fileSize, 6, 3);
                const buf = fileInfo.buffer;

                client.cork();
                client.write(headBuf);
                client.write(buf);
                client.uncork();
            } else {
                // 分片上传
                let rangeSize = MAX_MEM_FILE_SIZE;
                let offset = 0;

                tempFilePath = fileInfo.path;

                fs.open(fileInfo.path, 'r', (err, fd) => {
                    if (err) {
                        return client.end();
                    }

                    for (; offset < fileSize; offset += rangeSize) {
                        let bufSize = Math.min(rangeSize, fileSize - offset);
                        const headBuf = Buffer.alloc(17);
                        headBuf.writeUInt8(2);
                        headBuf.writeUInt32BE(17 + bufSize, 1);
                        headBuf.writeUInt8(mime, 5);
                        headBuf.writeUIntBE(fileSize, 6, 4);
                        headBuf.writeUIntBE(offset, 10, 4);
                        headBuf.writeUIntBE(bufSize, 14, 3);

                        let readBuf = Buffer.alloc(bufSize);
                        fs.read(fd, readBuf, 0, bufSize, offset, (err, bytesRead, buf) => {
                            if (err) return client.end();

                            client.write(headBuf);
                            client.write(buf);
                        });
                    }
                });
            }
        })
            .on('timeout', () => {
                this.logger.info('timeout');
                client.end();
            });

        return new Promise((resolve, reject) => {
            client
                .on('error', reject)
                .on('close', () => {
                    tempFilePath && fs.unlink(tempFilePath, () => { });
                    reject(new Error('CONNECTION_CLOSED'));
                });

            let success = true;
            let fileRangeSize = 0;

            socketUtils.onReceiveBlock(client, (type, buf) => {
                try {
                    success &= type == 1;
                    fileRangeSize += buf.readUInt32BE();
                    if (fileRangeSize === fileSize) {
                        if (success === 0) {
                            reject(new Error('UPLOAD_FAILURE'));
                        } else {
                            resolve(buf.toString('utf8', 4));
                        }
                        client.end();
                    }
                } catch (e) {
                    this.logger.error(e);
                    reject(e);
                    client.end();
                }
            });
        });
    }
}

module.exports = FileClient;