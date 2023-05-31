let fabric = ../../dhall/fb-fabric/funcs.dhall

let params =
      { nr_pods = 2
      , nr_fabs_per_pod = 2
      , nr_racks_per_pod = 2
      , nr_hosts_per_rack = 2
      , nr_spines_per_plane = 1
      }

in  fabric.mkCluster params 0
