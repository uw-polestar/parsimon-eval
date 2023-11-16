let List/generate = https://prelude.dhall-lang.org/List/generate.dhall

let List/map = https://prelude.dhall-lang.org/List/map.dhall

let List/concatMap = https://prelude.dhall-lang.org/List/concatMap.dhall

let List/index = https://prelude.dhall-lang.org/List/index.dhall

let C = ../common/types.dhall

let T = ./types.dhall

let mkLink =
      \(a : Natural) ->
      \(b : Natural) ->
      \(bw : Natural) ->
        { a, b, bandwidth = bw, delay = 1000 }

let mkRack =
      \(p : T.Params) ->
      \(next_id : Natural) ->
        let hosts =
              List/generate
                p.nr_hosts_per_rack
                C.Node
                (\(i : Natural) -> { id = next_id + i, kind = C.NodeKind.Host })

        let tor_id = next_id + p.nr_hosts_per_rack

        let tor = { id = tor_id, kind = C.NodeKind.Switch }

        let host2tor =
              List/map
                C.Node
                C.Link
                (\(n : C.Node) -> mkLink n.id tor_id 10000000000)
                hosts

        in  { tor, hosts, host2tor } : T.Rack

let mkPod =
      \(p : T.Params) ->
      \(next_id : Natural) ->
        let fabs =
              List/generate
                p.nr_fabs_per_pod
                C.Node
                ( \(i : Natural) ->
                    { id = next_id + i, kind = C.NodeKind.Switch }
                )

        let next_id = next_id + p.nr_fabs_per_pod

        let nr_ids_per_rack = p.nr_hosts_per_rack + 1

        let racks =
              List/generate
                p.nr_racks_per_pod
                T.Rack
                (\(i : Natural) -> mkRack p (next_id + nr_ids_per_rack * i))

        let tor2fab =
              List/concatMap
                T.Rack
                C.Link
                ( \(r : T.Rack) ->
                    List/map
                      C.Node
                      C.Link
                      (\(n : C.Node) -> mkLink r.tor.id n.id 40000000000)
                      fabs
                )
                racks

        in  { fabs, racks, tor2fab } : T.Pod

let mkPlane =
      \(p : T.Params) ->
      \(next_id : Natural) ->
        List/generate
          p.nr_spines_per_plane
          C.Node
          (\(i : Natural) -> { id = next_id + i, kind = C.NodeKind.Switch })

let mkCluster =
      \(p : T.Params) ->
      \(next_id : Natural) ->
        let nr_planes = p.nr_fabs_per_pod

        let planes =
              List/generate
                nr_planes
                (List C.Node)
                ( \(i : Natural) ->
                    mkPlane p (next_id + p.nr_spines_per_plane * i)
                )

        let next_id = p.nr_spines_per_plane * nr_planes

        let nr_ids_per_rack = p.nr_hosts_per_rack + 1

        let nr_ids_per_pod =
              p.nr_racks_per_pod * nr_ids_per_rack + p.nr_fabs_per_pod

        let pods =
              List/generate
                p.nr_pods
                T.Pod
                (\(i : Natural) -> mkPod p (next_id + nr_ids_per_pod * i))

        let fab2spine =
              List/concatMap
                T.Pod
                C.Link
                ( \(p : T.Pod) ->
                    let IndexedNode = { index : Natural, value : C.Node }

                    in  List/concatMap
                          IndexedNode
                          C.Link
                          ( \(fab : IndexedNode) ->
                              let spines =
                                  -- EFFICIENCY: there are only a small number of planes
                                  -- CORRECTNESS: `None` should be impossible
                                    merge
                                      { Some = \(ns : List C.Node) -> ns
                                      , None = [] : List C.Node
                                      }
                                      ( List/index
                                          fab.index
                                          (List C.Node)
                                          planes
                                      )

                              in  List/map
                                    C.Node
                                    C.Link
                                    ( \(spine : C.Node) ->
                                        mkLink fab.value.id spine.id 40000000000
                                    )
                                    spines
                          )
                          (List/indexed C.Node p.fabs)
                )
                pods

        in  { planes, pods, fab2spine } : T.Cluster

in  { mkRack, mkPod, mkCluster }
