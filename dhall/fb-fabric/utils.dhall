let List/concatMap = https://prelude.dhall-lang.org/List/concatMap.dhall

let List/unzip = https://prelude.dhall-lang.org/List/unzip.dhall

let T = ./types.dhall

let C = ../common/types.dhall

let Rack/nodes = \(r : T.Rack) -> [ r.tor ] # r.hosts

let Pod/nodes =
      \(p : T.Pod) ->
        let torsAndHosts =
              List/concatMap
                T.Rack
                C.Node
                (\(r : T.Rack) -> Rack/nodes r)
                p.racks

        in  p.fabs # torsAndHosts

let Plane/nodes = \(p : T.Plane) -> p

let Cluster/nodes =
      \(c : T.Cluster) ->
        let spines =
              List/concatMap
                T.Plane
                C.Node
                (\(p : T.Plane) -> Plane/nodes p)
                c.planes

        let fabsTorsAndHosts =
              List/concatMap T.Pod C.Node (\(p : T.Pod) -> Pod/nodes p) c.pods

        in  spines # fabsTorsAndHosts

let Rack/links = \(r : T.Rack) -> r.host2tor

let Pod/links =
      \(p : T.Pod) ->
        let rack_links =
              List/concatMap
                T.Rack
                C.Link
                (\(r : T.Rack) -> Rack/links r)
                p.racks

        in  p.tor2fab # rack_links

let Cluster/links =
      \(c : T.Cluster) ->
        let pod_links =
              List/concatMap T.Pod C.Link (\(p : T.Pod) -> Pod/links p) c.pods

        in  c.fab2spine # pod_links

in  { Rack/nodes
    , Pod/nodes
    , Plane/nodes
    , Cluster/nodes
    , Rack/links
    , Pod/links
    , Cluster/links
    }
