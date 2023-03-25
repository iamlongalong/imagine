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
- [ ] add valuer registry
- [ ] memmap save to diskmap
- [ ] wal for recover

- [ ] force valuer type (bind valuer with map namespace)
- [ ] generat valuer with pb/gob/json/msgpack
- [ ] implement compresser
- [ ] default valuer (maybe……)

- [ ] add server implement
- [ ] add server client
- [ ] implement listener
- [ ] support package db and index
- [ ] use mmap for disk cache
- [ ] benchmark and more tests
- [ ] examples and documents

- [ ] optimize the index file structure (data file also)
- [ ] with relations define in model (like join query)
- [ ] UI viewer for monitoring
- [ ] add remote storage map
- [ ] add primary and standby structure
- [ ] add cluster structure
- [ ] with crdt support
