// Copyright 2023 The imagine Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// imagine is a diskmap implementation
// for some case, we has map which got a large number of members, and then lack of RAM happens……
// for the purpose of saving shortage RAM resource, we designed this diskmap project. Just like its name, we use cheap disk storage for the most records in a map, and high-performance, expensive ram for cache.
package imagine

// you can use it as :

// ctx := context.Background()

// dmaps, err := NewDmaps(DmapsOption{Dir: "testdata"})
// if err != nil {
//   log.Println(err)
//   return
// }

// defer dmaps.Close(ctx)

// m := dmaps.MustGetMap(ctx, "users")

// m.Set(ctx, "longalong", []byte("i am longalong, nice to meet you ~"))

// v, err := m.Get(ctx, "longalong")
// if err != nil {
//   log.Println(err)
//   return
// }

// log.Println(v)
