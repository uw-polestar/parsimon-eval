let utils = ../../../dhall/fb-fabric/utils.dhall

let cluster = ./cluster_1_to_1.dhall

let T = ../../../dhall/common/types.dhall

let links = utils.Cluster/links cluster

in  List/length T.Link links
