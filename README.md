# imagine

这要解决的是 大结构体 的修改问题，要考虑 结构体嵌套复用 的问题。
每个修改都是大结构体中一个小字段的修改。

sqlite 的模式可以参考，相当于自己基于文件系统之上又实现了一套文件系统，只是这套文件系统就可以自己指定 page(block) 大小，以适应不同数据表的需求。

我希望解决一些问题：
1. 大结构体的频繁小修改问题 (期望是 imagine 这个项目解决)
2. 嵌套结构体的直接存取问题, 而不是用 tables 存储 (imagine 同时也尝试解决这个问题)
3. 前后端一体化的文档存储 (reactive 是解决这个的一个尝试)
4. 小 kv 的易维护的本地化存储 (diskv 解决) [diskv](https://github.com/iamlongalong/diskv)

当前这个项目，diskmap 数据部分，采用的分 block 的存储方式，index 采用的完全内存的方式(dump 到 index 文件中)，分 block 的目的是为了能原地修改(如果大小足够)。

始终要权衡两个问题： 总文件大小 和 读写性能。 

大结构体的存储，若要实现快速读写，就要尝试用 block 的方式，而不能紧凑存，目的是为了快速索引、原地修改。

还有一个特征: 大结构体不是每个字段都设置了值，大多数时候都采用默认值的方式，只有当修改时才会有值。

我们之前的方案，是紧凑存储，类似于 pb 的方案，然后加了 log，这导致修改时内存中需要有一份展开的数据 (有部分逻辑判断)，在内存淘汰机制不完善的情况下，很容易导致内存中展开的数据很多。
log 是用来在一段时间后做一次整体的更新，在内存中修改完数据后又重新 dump 到磁盘上。

有些文件很大，在磁盘上大约有 100MB，这才内存中展开后能达到 800MB 的大小，这样体积的文件在频繁修改时就会造成频繁地重新更新。

实际上，操作逻辑上看，一项资源的修改在短期内是比较集中的，一方面，若能按块读取，内存中就无须存在太多展开数据，另一方面，如果能按块修改，那么就无须整个文件重新 dump。


imagine is a diskmap implementation

for some case, we has map which got a large number of members, and then lack of RAM happens……

for the purpose of saving shortage RAM resource, we designed this diskmap project. Just like its name, we use cheap disk storage for the most records in a map, and high-performance, expensive ram for cache.

# usage

you can use it as : 

```golang
ctx := context.Background()

dmaps, err := NewDmaps(DmapsOption{Dir: "testdata"})
if err != nil {
  log.Println(err)
  return
}

defer dmaps.Close(ctx)

m := dmaps.MustGetMap(ctx, "users")

m.Set(ctx, "longalong", []byte("i am longalong, nice to meet you ~"))

v, err := m.Get(ctx, "longalong")
if err != nil {
  log.Println(err)
  return
}

log.Println(v)

```


# TODOs
- [x] ~~add valuer registry~~ [2023-03-26]
- [x] ~~memmap save to diskmap~~ [2023-03-26]
- [ ] implement merger
- [ ] wal for recover
</br>

- [x] ~~force valuer type (bind valuer with map namespace)~~ [2023-03-26]
- [ ] generate valuer with pb/~~gob/json~~/msgpack
- [ ] implement compresser
- [ ] valuer with wasm
- [ ] use pb for internal struct marshaller (using json and gob now)
</br>

- [ ] add server implement
- [ ] add server client
- [ ] implement listener
- [ ] support package db and index
- [ ] use mmap for disk cache
- [ ] dmap cache with ttl
- [ ] benchmark and more tests
- [ ] examples and documents
</br>

- [ ] optimize the index file structure (data file also)
- [ ] with relations define in model (like join query)
- [ ] UI viewer for monitoring
- [ ] add remote storage map
- [ ] add primary and standby structure
- [ ] add cluster structure
- [ ] with crdt support

# other

这个项目的一些前置信息可以参考 [我的一次内部分享](https://blog.longalong.cn/posts/23_03_28_15_01_a_record_of_sharing_of_database.html)
