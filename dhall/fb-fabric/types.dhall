let C = ../common/types.dhall

let Rack = { tor : C.Node, hosts : List C.Node, host2tor : List C.Link }

let Pod = { fabs : List C.Node, racks : List Rack, tor2fab : List C.Link }

let Plane = List C.Node

let Cluster = { planes : List Plane, pods : List Pod, fab2spine : List C.Link }

let Params =
      { nr_pods : Natural
      , nr_fabs_per_pod : Natural
      , nr_racks_per_pod : Natural
      , nr_hosts_per_rack : Natural
      , nr_spines_per_plane : Natural
      }

in  { Rack, Pod, Plane, Cluster, Params }
