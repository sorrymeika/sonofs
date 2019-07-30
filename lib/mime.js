const mimeMaps = {
    'image/jpeg': 1,
    'image/png': 2,
    'image/gif': 3,
    'text/plain': 4,
    'text/html': 5,
    'application/octet-stream': 6
};

const mimeKeys = Object.keys(mimeMaps);

exports.mimeKeys = mimeKeys;
exports.mimeMaps = mimeMaps;
exports.getMime = (mime) => {
    for (let i = 0; i < mimeKeys.length; i++) {
        const name = mimeKeys[i];
        if (mimeMaps[name] === mime) {
            return name;
        }
    }
    return 'application/octet-stream';
};