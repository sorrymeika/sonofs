
// filename: groupId:64/rootId:64 / (subdir:256/subsubdir:256/file.snf:256|255):255*255*255-1 / mime:64/start:1024*1024*1024-1/length:1024*1024*1024-1
// 例子: T/A/01/01/fe.snf
const fs = require('fs');

fs.open('00fffe.hd', 'wx', function (err) {
    if (err) return console.error(err);

    // 这里开始可以放心的读写文件
});

const upload = (file) => {
    const size = file.size;
};