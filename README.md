# DHT

分别使用 Chord 算法和 Kademlia 算法实现了分布式哈希表（DHT）。在此基础上制作了类似于 BitTorrent 的 P2P 文件分享系统 **Bite**Torrent。

# Report

## 一些优化

`myrpc` 实现了支持 gracefully shutdown 的 RPC server 和支持 TCP 连接复用的 RPC client。

`throttler` 实现了可以限制并发数的 WaitGroup。

## Chord

节点通过保存它的前驱与后继形成了一个环，finger table 存了倍增的列表来加速环上的定位。后继和前驱保证了正确性，finger table 降低了时间复杂度。

Force Quit 的处理：每个节点备份前驱的数据，同时维护一定长度的后继列表，使得后继列表中只有部分节点失效时环不会断裂。每个点检测到前驱失效时启用备份数据。

## Kademlia

定义节点间的距离为 Node ID 的异或。每个节点使用 k-buckets 存放它的联系人，k-buckets 中也使用倍增的思想存放了不同距离的联系人，以减少寻找节点的时间。

每个数据会在节点周围被保存多份，当节点失效时可以启用备份数据。这通过数据的 RepublishTime 和 ExpireTime 来控制。


## BiteTorrent

参考了 [Building a BitTorrent client from the ground up in Go](https://blog.jse.li/posts/torrent/#putting-it-all-together)。

将文件切成小 Piece，把每个 PieceHash, Piece 作为键值对放入 DHT。种子存的是文件对应的 PieceHash 的列表，因此可以用种子来下载文件。种子本身也被放入 DHT，其 key 为种子的哈希。磁力链接保存的是种子的哈希，因此可以用一个比较简短的磁力链接获取到种子，进而下载文件。
