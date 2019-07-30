# sonofs
sono nodejs distributed file system

# mime

* `image/png` : `01`
* `image/jpeg` : `02`


# createServer

`groupId` 服务分组id
`serverId` 服务id，同一`serverId`只能分配给一个进程
`root` 文件存储主目录，同一文件夹只能分配给一个进程
`port` 端口号
`[master]` 仅`slave`机需要配置，master的ip地址