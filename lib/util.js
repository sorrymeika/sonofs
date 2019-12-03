const path = require('path');

const fileNameEncodeChars = '0ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789._';
const fileNameDecodeMap = { "0": 0, "1": 53, "2": 54, "3": 55, "4": 56, "5": 57, "6": 58, "7": 59, "8": 60, "9": 61, "A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7, "H": 8, "I": 9, "J": 10, "K": 11, "L": 12, "M": 13, "N": 14, "O": 15, "P": 16, "Q": 17, "R": 18, "S": 19, "T": 20, "U": 21, "V": 22, "W": 23, "X": 24, "Y": 25, "Z": 26, "a": 27, "b": 28, "c": 29, "d": 30, "e": 31, "f": 32, "g": 33, "h": 34, "i": 35, "j": 36, "k": 37, "l": 38, "m": 39, "n": 40, "o": 41, "p": 42, "q": 43, "r": 44, "s": 45, "t": 46, "u": 47, "v": 48, "w": 49, "x": 50, "y": 51, "z": 52, ".": 62, "_": 63 };

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
    const serverId = fileNameDecodeMap[fileName[1]];
    const bundleId = s2i(fileName.slice(2, 6));
    const dir = pad((bundleId >> 16).toString(16), 2);
    const subDir = pad(((bundleId & 65535) >> 8).toString(16), 2);
    const file = pad((bundleId & 255).toString(16), 2);
    const mime = fileNameDecodeMap[fileName.slice(6, 7)];

    const fileStart = s2i(fileName.slice(7, 12));
    const fileSize = s2i(fileName.slice(12, 17));

    return {
        fileName,
        bundleId,
        groupId,
        serverId,
        dir,
        subDir,
        file: file + '.snf',
        mime,
        fileStart,
        fileSize
    };
};

const MAX_FILE_SIZE = 1024 * 1024 * 1024 - 1;

const formatFile = ({ groupId, serverId, dir, subDir, file, mime, fileStart, fileSize }) => {
    if (groupId < 0 || groupId >= 64) throw new Error('`groupId`必须在0-63之间!');
    if (serverId < 0 || serverId >= 64) throw new Error('`serverId`必须在0-63之间!');

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
        + i2s(serverId)
        + pad(i2s((dir << 16) + (subDir << 8) + file), 4)
        + fileNameEncodeChars[mime]
        + pad(i2s(fileStart), 5)
        + pad(i2s(fileSize), 5);

    return fileName;
};


function getBundlePath(root, bundleId) {
    const dirId = bundleId >> 16;
    const subDirId = (bundleId & 65535) >> 8;
    const fileId = bundleId & 255;

    const dir = pad(dirId.toString(16), 2);
    const subDir = pad(subDirId.toString(16), 2);
    const bundleName = pad(fileId.toString(16), 2) + '.snf';
    return path.join(root, dir, subDir, bundleName);
}

module.exports = {
    pad,
    s2i,
    i2s,
    parseFileName,
    getBundlePath,
    formatFile
};