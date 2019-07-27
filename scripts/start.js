const fs = require('fs');
const net = require('net');

const client = net.createConnection({ port: 8124 }, () => {
    fs.readFile('config.loc', (err, data) => {
        if (err) throw err;
        console.log(data);

        client.cork();
        client.write(Buffer.from([1]));
        client.write(Buffer.from([1]));
        client.write(Buffer.from([1, 2, 34]));
        client.write(data);
        client.uncork();
    });

    client.on('data', (buf) => {
        console.log(buf);
        const success = buf.readUInt8();
        if (success) {
            const fileName = buf.toString('utf8', 1);
            console.log(fileName);
        } else {
            console.log('upload err!');
        }
        client.end();
    });
});