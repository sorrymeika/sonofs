const fileNameEncodeChars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._';
const fileNameDecodeMap = { "A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "F": 5, "G": 6, "H": 7, "I": 8, "J": 9, "K": 10, "L": 11, "M": 12, "N": 13, "O": 14, "P": 15, "Q": 16, "R": 17, "S": 18, "T": 19, "U": 20, "V": 21, "W": 22, "X": 23, "Y": 24, "Z": 25, "a": 26, "b": 27, "c": 28, "d": 29, "e": 30, "f": 31, "g": 32, "h": 33, "i": 34, "j": 35, "k": 36, "l": 37, "m": 38, "n": 39, "o": 40, "p": 41, "q": 42, "r": 43, "s": 44, "t": 45, "u": 46, "v": 47, "w": 48, "x": 49, "y": 50, "z": 51, "0": 52, "1": 53, "2": 54, "3": 55, "4": 56, "5": 57, "6": 58, "7": 59, "8": 60, "9": 61, ".": 62, "_": 63 };

const i2s = (i) => {
    let num = i;
    let result = '';

    while (i != null) {
        if (num < 64) {
            result = fileNameEncodeChars[num] + result;
            break;
        } else {
            result = fileNameEncodeChars[num % 64] + result;
        }
        num = num >> 6;
    }

    return result;
};

const s2i = (s) => {
    if (!s) return null;
    let result = fileNameDecodeMap[s[s.length - 1]];
    let end = s.length - 1;
    let i = 1;

    while (i <= end) {
        result += fileNameDecodeMap[s[end - i]] << (6 * i);
        i++;
    }

    return result;
};

const pad = (origin, num) => {
    return ('000000000000000000000000' + origin).slice(-num);
};

const parseFileName = fileName => {
    const groupId = fileNameDecodeMap[fileName[0]];
    const rootId = fileNameDecodeMap[fileName[1]];
    const pack = s2i(fileName.slice(2, 6));
    const dir = (pack >> 16).toString(16);
    const subDir = ((pack & 65535) >> 8).toString(16);
    const file = (pack & 255).toString(16);
    const mime = fileNameDecodeMap[fileName.slice(6, 7)];

    const fileStart = s2i(fileName.slice(7, 12));
    const fileSize = s2i(fileName.slice(12, 17));

    return {
        fileName,
        groupId,
        rootId,
        dir,
        subDir,
        file: file + '.snf',
        mime,
        fileStart,
        fileSize
    };
};

const MAX_FILE_SIZE = 1024 * 1024 * 1024 - 1;

const formatFile = ({ groupId, rootId, dir, subDir, file, mime, fileStart, fileSize }) => {
    if (groupId < 0 || groupId >= 64) throw new Error('`groupId`必须在0-63之间!');
    if (rootId < 0 || rootId >= 64) throw new Error('`rootId`必须在0-63之间!');

    if (typeof dir === 'string') dir = parseInt(dir, 16);
    if (dir < 0 || dir >= 256) throw new Error('`dir`必须在0-255之间!');

    if (typeof subDir === 'string') subDir = parseInt(subDir, 16);
    if (subDir < 0 || subDir >= 256) throw new Error('`subDir`必须在0-255之间!');

    if (typeof file === 'string') file = parseInt(file.replace('.snf', ''), 16);
    if (file < 0 || file >= 256) throw new Error('`file`必须在0-255之间!');

    if (mime < 0 || mime >= 64) throw new Error('`mime`必须在0-63之间!');
    if (fileStart < 0 || fileStart >= MAX_FILE_SIZE) throw new Error('`fileStart`必须在0-' + (MAX_FILE_SIZE - 1) + '之间!');
    if (fileSize < 0 || fileSize > MAX_FILE_SIZE) throw new Error('`fileSize`必须在0-' + MAX_FILE_SIZE + '之间!');

    const fileName = i2s(groupId)
        + i2s(rootId)
        + i2s((dir << 16) + (subDir << 8) + file)
        + fileNameDecodeMap[mime]
        + pad(i2s(fileStart), 5)
        + pad(i2s(fileSize), 5);

    return fileName;
};

const s = i2s(1024 * 1024 * 1024 - 1);
console.log(s);
console.log(s2i(s));

const fileName = formatFile({
    groupId: 1,
    rootId: 1,
    dir: '5',
    subDir: '12',
    file: '16.snf',
    mime: 1,
    fileStart: 0,
    fileSize: 1024
});

console.log(fileName);

console.log(parseFileName(fileName));


function test1(a, b, c) {
    return (a << 16) + (b << 8) + c;
}

function test2(num) {
    return {
        a: num >> 16,
        b: (num & 65535) >> 8,
        c: (num & 255)
    };
}

let ta = test1(1, 5, 6);

console.log(ta);
console.log(test2(ta));