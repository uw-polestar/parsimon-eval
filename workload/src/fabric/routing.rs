use parsimon::core::network::NodeId;
use parsimon::core::routing::RoutingAlgo;

use super::Cluster;

const NR_TORS_PER_POD: usize = 48;

#[derive(Debug)]
pub struct FabricRoutes {
    nr_pods: usize,
    nr_fabs_per_pod: usize,
    nr_spines_per_plane: usize,
    nr_hosts_per_rack: usize,

    tor_base: usize,
    fabric_base: usize,
    spine_base: usize,

    nodes: Vec<FabricNode>,
}

impl FabricRoutes {
    pub fn new(cluster: &Cluster) -> Self {
        // TODO: implement me
        todo!()
    }
}

impl RoutingAlgo for FabricRoutes {
    fn next_hops(&self, from: NodeId, to: NodeId) -> Option<Vec<NodeId>> {
        let len = self.nodes.len();
        if from.inner() >= len || to.inner() >= len {
            return None;
        }
        if from == to {
            return Some(vec![from]);
        }
        let (from, to) = (from.inner(), to.inner());
        let hops = match self.nodes[from] {
            FabricNode::Host => {
                // Next hop has to be the top-of-rack switch.
                vec![NodeId::new(self.tor_of_host(from))]
            }
            FabricNode::TopOfRack => {
                // Go down if `to` is a host in this rack. Otherwise, go up to a fabric switch.
                match self.nodes[to] {
                    FabricNode::Host if self.tor_of_host(to) == from => {
                        vec![NodeId::new(to)]
                    }
                    FabricNode::Fabric if self.fabrics_of_tor(from).any(|f| f == to) => {
                        vec![NodeId::new(to)]
                    }
                    _ => self.fabrics_of_tor(from).map(NodeId::new).collect(),
                }
            }
            FabricNode::Fabric => {
                // Go down to a ToR if `to` is a node in this pod. Othwerwise, go up to a spine switch.
                match self.nodes[to] {
                    FabricNode::TopOfRack if self.tor_in_pod(self.pod_of_fabric(from), to) => {
                        vec![NodeId::new(to)]
                    }
                    FabricNode::Host if self.host_in_pod(self.pod_of_fabric(from), to) => {
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
                    _ => self.fabrics_of_spine(from).map(NodeId::new).collect(),
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
        let start =
            self.fabric_base + ((tor - self.tor_base) / NR_TORS_PER_POD) * self.nr_fabs_per_pod;
        start..(start + self.nr_fabs_per_pod)
    }

    fn tors_of_fabric(&self, fab: usize) -> impl Iterator<Item = usize> {
        assert!(matches!(self.nodes[fab], FabricNode::Fabric));
        let start = self.pod_of_fabric(fab) * NR_TORS_PER_POD + self.tor_base;
        start..(start + NR_TORS_PER_POD)
    }

    fn pod_of_fabric(&self, fab: usize) -> usize {
        assert!(matches!(self.nodes[fab], FabricNode::Fabric));
        (fab - self.fabric_base) / self.nr_fabs_per_pod
    }

    fn fabrics_of_spine(&self, spine: usize) -> impl Iterator<Item = usize> {
        let plane = (spine - self.spine_base) / self.nr_spines_per_plane;
        let start = self.fabric_base + plane; // offset
        let end: usize = self.fabric_base * self.nr_pods + plane;
        (start..end).step_by(self.nr_fabs_per_pod)
    }

    fn host_in_pod(&self, pod: usize, host: usize) -> bool {
        let start = pod * self.nr_fabs_per_pod;
        host >= start && host <= start + self.nr_hosts_per_rack
    }

    fn tor_in_pod(&self, pod: usize, tor: usize) -> bool {
        let start = self.tor_base + pod * self.nr_fabs_per_pod;
        tor >= start && tor <= start + NR_TORS_PER_POD
    }

    fn spines_of_fabric(&self, fab: usize) -> impl Iterator<Item = usize> {
        assert!(matches!(self.nodes[fab], FabricNode::Fabric));
        let plane = fab % self.nr_fabs_per_pod;
        let start = self.spine_base + plane * self.nr_spines_per_plane;
        start..(start + self.nr_spines_per_plane)
    }

    fn is_fabric_spine(&self, fab: usize, spine: usize) -> bool {
        let plane = fab % self.nr_fabs_per_pod;
        let start = self.spine_base + plane * self.nr_spines_per_plane;
        spine >= start && spine <= start + self.nr_spines_per_plane
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

    use crate::{fabric::Cluster, testing::TINY_CLUSTER};

    use super::*;

    #[test]
    fn routes_correct() -> anyhow::Result<()> {
        let mut cluster: Cluster = serde_json::from_str(TINY_CLUSTER)?;
        cluster.contiguousify();

        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let topology = Topology::new(&nodes, &links)?;
        let bfs_routes = BfsRoutes::new(&topology);

        let fabric_routes = FabricRoutes::new(&cluster);

        // TODO: Compare the two across all pairs of nodes;

        Ok(())
    }
}
