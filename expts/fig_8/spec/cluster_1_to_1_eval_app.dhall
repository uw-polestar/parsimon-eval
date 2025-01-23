let fabric = ../../../dhall/fb-fabric/funcs.dhall

let params =
      { nr_pods = 4
      , nr_fabs_per_pod = 4
      , nr_racks_per_pod = 4
      , nr_hosts_per_rack = 16
      , nr_spines_per_plane = 4
      }

in  fabric.mkCluster params 0
