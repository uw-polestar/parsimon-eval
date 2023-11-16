let NodeKind = < Host | Switch >

let Node = { id : Natural, kind : NodeKind }

let Link = { a : Natural, b : Natural, bandwidth : Natural, delay : Natural }

let Flow =
      { id : Natural
      , src : Natural
      , dst : Natural
      , size : Natural
      , start : Natural
      }

in  { Node, NodeKind, Link, Flow }
