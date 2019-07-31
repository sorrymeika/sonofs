# sonofs
sono nodejs distributed file system

## 简介

sonofs是一个轻量级、高性能的分布式文件系统。提供小文件合并、负载均衡、文件自动备份等功能


## 如何使用


### 启动注册中心服务

scripts/registry.js

```js
const { startRegistry } = require('sonofs');

startRegistry({
    port: 8123
}, () => {
});
```

### 启动文件服务

scripts/server.js

```js
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const { startServerMaster, startServer } = require('sonofs');

const cfg = {
    // 服务分组id
    groupId: 1,
    // 服务id，同一`serverId`只能分配给一个进程
    serverId: 1,
    // 文件存储主目录，同一文件夹只能分配给一个进程
    root: '/data/upload',
    // 服务器端口号
    port: 8124,
    // 注册中心配置
    registry: {
        host: '127.0.0.1',
        port: 8123
    }
};

if (cluster.isMaster) {
    startServerMaster(cfg, () => {
        for (let i = 0; i < numCPUs; i++) {
            cluster.fork();
        }
    });
    cluster.on('exit', (worker, code, signal) => {
        console.log(`worker ${worker.process.pid} died`, code, signal);
    });
} else {
    startServer(cfg);
}
```

### 上传/访问文件

client.js

```js
const { Controller } = require("egg");
const { createClient } = require('sonofs');

// 创建客户端
const fsClient = createClient({
    tmpDir: '/data/tmp',
    registry: {
        port: 8123
    }
});

class UploadController extends Controller {
    // 上传文件
    async testUpload() {
        const { ctx } = this;
        const stream = await ctx.getFileStream();
        // 调用上传方法
        const result = await fsClient.upload(stream.mime, stream);

        console.log(result);

        ctx.body = {
            fileName: result
        };
    }

    // 访问文件
    async testGetFile() {
        const { ctx } = this;
        try {
            // 调用获取文件方法
            const result = await fsClient.getFile(ctx.query.file);

            ctx.type = result.mime;
            ctx.body = result.buffer;
        } catch (e) {
            console.error(e);
            ctx.body = {};
        }
    }
}
```

# 技术细节

## server config

* `groupId` 服务分组id
* `serverId` 服务id，同一`serverId`只能分配给一个进程
* `root` 文件存储主目录，同一文件夹只能分配给一个进程
* `port` 端口号
* `[master]` 仅`slave`机需要配置，master的ip地址+端口号

* 文件名格式: groupId+serverId+dir+subDir+bundle+mime+fileStart+fileSize
* 文件初始化时会预先分配 1gb空间

## mime

* `image/png` : `01`
* `image/jpeg` : `02`
