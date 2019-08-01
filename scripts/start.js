const fs = require('fs');
const net = require('net');

const client = net.createConnection({ port: 8124 }, () => {
    fs.readFile('test_upload.txt', (err, data) => {
        if (err) throw err;
        console.log(data);

        client.cork();
        client.write(Buffer.from([1]));
        client.write(Buffer.from([4]));

        const sizeBuf = Buffer.alloc(3);
        sizeBuf.writeUIntBE(data.length, 0, 3);
        client.write(sizeBuf);
        client.write(data);
        client.uncork();
    });

    client.on('data', (buf) => {
        console.log(buf);
        const success = buf.readUInt8();
        if (success) {
            const fileSize = buf.readUInt32BE(1);
            const fileName = buf.toString('utf8', 5);
            console.log(fileSize, fileName);
        } else {
            console.log('upload err!');
        }
        client.end();
    });
});