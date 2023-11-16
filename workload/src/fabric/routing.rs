use super::Cluster;
use itertools::Itertools;
use parsimon::core::network::types::Node;
use parsimon::core::network::NodeId;
use parsimon::core::routing::RoutingAlgo;

#[derive(Debug)]
pub struct FabricRoutes {
    nr_pods: usize,
    nr_fabs_per_pod: usize,
    nr_spines_per_plane: usize,
    nr_hosts_per_rack: usize,
    nr_tors_per_pod: usize,

    tor_base: usize,
    fabric_base: usize,
    spine_base: usize,

    nodes: Vec<FabricNode>,
}

impl FabricRoutes {
    // cluster already contiguousified
    pub fn new(cluster: &Cluster) -> Self {
        let tor_base = cluster.tor_base();
        let fabric_base = cluster.fabric_base();
        let spine_base = cluster.spine_base();
        let mut sorted_nodes: Vec<_> = cluster.nodes().collect();
        sorted_nodes.sort_by_key(|m| m.id);
        FabricRoutes {
            nr_pods: cluster.nr_pods(),
            nr_fabs_per_pod: cluster.nr_fabs_per_pod(),
            nr_spines_per_plane: cluster.nr_spines_per_plane(),
            nr_hosts_per_rack: cluster.nr_hosts_per_rack(),
            nr_tors_per_pod: cluster.nr_tors_per_pod(),
            tor_base,
            fabric_base,
            spine_base,
            nodes: Self::fabric_nodes(sorted_nodes.as_slice(), tor_base, fabric_base, spine_base),
        }
    }
    fn fabric_nodes(
        nodes: &[&Node],
        tor_base: usize,
        fabric_base: usize,
        spine_base: usize,
    ) -> Vec<FabricNode> {
        nodes
            .iter()
            .map(|n| match n.id.inner() {
                n if n < tor_base => FabricNode::Host,
                n if n >= tor_base && n < fabric_base => FabricNode::TopOfRack,
                n if n >= fabric_base && n < spine_base => FabricNode::Fabric,
                _ => FabricNode::Spine,
            })
            .collect_vec()
    }
}

impl RoutingAlgo for FabricRoutes {
    fn next_hops(&self, from: NodeId, to: NodeId) -> Option<Vec<NodeId>> {
        let len = self.nodes.len();
        if from.inner() >= len || to.inner() >= len {
            return None;
        }
        if from == to {
            return Some(vec![]);
        }
        let (from, to) = (from.inner(), to.inner());
        let hops = match self.nodes[from] {
            FabricNode::Host => {
                // Next hop has to be the top-of-rack switch.
                // If fabric or spine, has to be a ToR in the same plane
                match self.nodes[to] {
                    FabricNode::TopOfRack if self.tor_of_host(from) == to => {
                        vec![NodeId::new(to)]
                    }
                    _ => vec![NodeId::new(self.tor_of_host(from))],
                }
            }
            FabricNode::TopOfRack => {
                // Go down if `to` is a host in this rack. Otherwise, go up to a fabric switch.
                match self.nodes[to] {
                    FabricNode::Host if self.tor_of_host(to) == from => {
                        vec![NodeId::new(to)]
                    }
                    FabricNode::Fabric | FabricNode::Spine => {
                        let target_plane = self.plane_of_node(to);
                        vec![NodeId::new(self.fabric_of_tor_in_plane(from, target_plane))]
                    }
                    _ => self.fabrics_of_tor(from).map(NodeId::new).collect(),
                }
            }
            FabricNode::Fabric => {
                // Go down to a ToR if `to` is a node in this pod.
                // Go down to a ToR or up to a spine if `to` is a node in another pod and plane.
                // Othwerwise, go up to a spine switch.
                match self.nodes[to] {
                    FabricNode::TopOfRack if self.tor_in_pod(self.pod_of_node(from), to) => {
                        // ToR in this pod
                        vec![NodeId::new(to)]
                    }
                    FabricNode::Host if self.host_in_pod(self.pod_of_node(from), to) => {
                        vec![NodeId::new(self.tor_of_host(to))]
                    }
                    FabricNode::Fabric
                        if self.plane_of_node(from) != self.plane_of_node(to)
                            && self.pod_of_node(from) != self.pod_of_node(to) =>
                    {
                        self.tors_of_fabric(from)
                            .chain(self.spines_of_fabric(from))
                            .map(NodeId::new)
                            .collect()
                    }
                    FabricNode::Fabric | FabricNode::Spine
                        if self.plane_of_node(from) != self.plane_of_node(to) =>
                    {
                        self.tors_of_fabric(from).map(NodeId::new).collect()
                    }
                    FabricNode::Spine if self.is_fabric_spine(from, to) => {
                        vec![NodeId::new(to)]
                    }
                    _ => self.spines_of_fabric(from).map(NodeId::new).collect(),
                }
            }
            FabricNode::Spine => {
                // Go down to the fabric switches in the target pod.
                match self.nodes[to] {
                    FabricNode::Fabric if self.is_fabric_spine(to, from) => {
                        vec![NodeId::new(to)]
                    }
                    FabricNode::Spine => self.fabrics_of_spine(from).map(NodeId::new).collect(),
                    _ => {
                        let target_pod = self.pod_of_node(to);
                        vec![NodeId::new(self.fabric_of_spine_in_pod(from, target_pod))]
                    }
                }
            }
        };
        Some(hops)
    }
}

impl FabricRoutes {
    fn tor_of_host(&self, host: usize) -> usize {
        assert!(matches!(self.nodes[host], FabricNode::Host));
        self.tor_base + host / self.nr_hosts_per_rack
    }

    fn fabrics_of_tor(&self, tor: usize) -> impl Iterator<Item = usize> {
        assert!(matches!(self.nodes[tor], FabricNode::TopOfRack));
        let start = self.fabric_base
            + ((tor - self.tor_base) / self.nr_tors_per_pod) * self.nr_fabs_per_pod;
        start..(start + self.nr_fabs_per_pod)
    }

    fn fabric_of_tor_in_plane(&self, tor: usize, plane: usize) -> usize {
        assert!(matches!(self.nodes[tor], FabricNode::TopOfRack));
        self.fabric_base + self.pod_of_node(tor) * self.nr_fabs_per_pod + plane
    }

    fn tors_of_fabric(&self, fab: usize) -> impl Iterator<Item = usize> {
        assert!(matches!(self.nodes[fab], FabricNode::Fabric));
        let start = self.pod_of_node(fab) * self.nr_tors_per_pod + self.tor_base;
        start..(start + self.nr_tors_per_pod)
    }

    fn pod_of_node(&self, node: usize) -> usize {
        match self.nodes[node] {
            FabricNode::Host => node / (self.nr_hosts_per_rack * self.nr_tors_per_pod),
            FabricNode::TopOfRack => (node - self.tor_base) / self.nr_tors_per_pod,
            FabricNode::Fabric => (node - self.fabric_base) / self.nr_fabs_per_pod,
            FabricNode::Spine => unreachable!(),
        }
    }

    fn plane_of_node(&self, node: usize) -> usize {
        match self.nodes[node] {
            FabricNode::Host => unreachable!(),
            FabricNode::TopOfRack => unreachable!(),
            FabricNode::Fabric => (node - self.fabric_base) % self.nr_fabs_per_pod,
            FabricNode::Spine => (node - self.spine_base) / self.nr_spines_per_plane,
        }
    }

    fn fabrics_of_spine(&self, spine: usize) -> impl Iterator<Item = usize> {
        assert!(matches!(self.nodes[spine], FabricNode::Spine));
        let plane: usize = (spine - self.spine_base) / self.nr_spines_per_plane;
        let start = self.fabric_base + plane; // offset
        (start..).step_by(self.nr_fabs_per_pod).take(self.nr_pods)
    }

    fn fabric_of_spine_in_pod(&self, spine: usize, pod: usize) -> usize {
        assert!(matches!(self.nodes[spine], FabricNode::Spine));
        self.fabric_base + pod * self.nr_fabs_per_pod + self.plane_of_node(spine)
    }

    fn host_in_pod(&self, pod: usize, host: usize) -> bool {
        assert!(matches!(self.nodes[host], FabricNode::Host));
        let start = pod * self.nr_hosts_per_rack * self.nr_tors_per_pod;
        host >= start && host < start + self.nr_hosts_per_rack * self.nr_tors_per_pod
    }

    fn tor_in_pod(&self, pod: usize, tor: usize) -> bool {
        assert!(matches!(self.nodes[tor], FabricNode::TopOfRack));
        let start = self.tor_base + pod * self.nr_tors_per_pod;
        tor >= start && tor < start + self.nr_tors_per_pod
    }

    fn spines_of_fabric(&self, fab: usize) -> impl Iterator<Item = usize> {
        assert!(matches!(self.nodes[fab], FabricNode::Fabric));
        let plane = fab % self.nr_fabs_per_pod;
        let start = self.spine_base + plane * self.nr_spines_per_plane;
        start..(start + self.nr_spines_per_plane)
    }

    fn is_fabric_spine(&self, fab: usize, spine: usize) -> bool {
        assert!(matches!(self.nodes[fab], FabricNode::Fabric));
        assert!(matches!(self.nodes[spine], FabricNode::Spine));
        let plane = fab % self.nr_fabs_per_pod;
        let start = self.spine_base + plane * self.nr_spines_per_plane;
        spine >= start && spine < start + self.nr_spines_per_plane
    }
}

#[derive(Debug)]
enum FabricNode {
    Host,
    TopOfRack,
    Fabric,
    Spine,
}

#[cfg(test)]
mod tests {
    use parsimon::core::network::topology::Topology;
    use parsimon::core::routing::BfsRoutes;

    use crate::{fabric::Cluster, testing::MEDIUM_CLUSTER};

    use super::*;

    #[test]
    fn routes_correct() -> anyhow::Result<()> {
        let cluster: Cluster = serde_json::from_str(MEDIUM_CLUSTER)?;

        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let topology = Topology::new(&nodes, &links)?;
        let bfs_routes = BfsRoutes::new(&topology);

        let fabric_routes = FabricRoutes::new(&cluster);
        let all_pairs =
            itertools::iproduct!(nodes.iter().map(|n| n.id), nodes.iter().map(|n| n.id));
        // Compare the two across all pairs of nodes
        for (from, to) in all_pairs {
            let bfs_next_hops = bfs_routes.next_hops(from, to).map(|mut h| {
                h.sort();
                h
            });
            let fabric_next_hops = fabric_routes.next_hops(from, to).map(|mut h| {
                h.sort();
                h
            });
            assert_eq!(bfs_next_hops, fabric_next_hops, "from: {from} to: {to}");
        }

        Ok(())
    }
}
