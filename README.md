# imagine

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
