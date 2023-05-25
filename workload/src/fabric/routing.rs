use parsimon::core::network::NodeId;

const NR_TORS_PER_POD: usize = 48;

#[derive(Debug)]
pub struct Routes {
    nr_pods: usize,
    nr_fabs_per_pod: usize,
    nr_spines_per_plane: usize,
    nr_hosts_per_rack: usize,

    tor_base: usize,
    fabric_base: usize,
    spine_base: usize,

    nodes: Vec<FabricNode>,
}

impl Routes {
    pub fn next_hops_unchecked(&self, from: NodeId, to: NodeId) -> Vec<NodeId> {
        if from == to {
            return vec![from];
        }
        let (from, to) = (from.inner(), to.inner());
        match self.nodes[from] {
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
                    _ => self.fabrics_of_tor(from).map(NodeId::new).collect(),
                }
            }
            FabricNode::Fabric => {
                // Go down to a ToR if `to` is a node in this pod. Othwerwise, go up to a spine switch.
                match self.nodes[to] {
                    FabricNode::Fabric if self.pod_of_fabric(from) == self.pod_of_fabric(to) => {
                        self.tors_of_fabric(from).map(NodeId::new).collect()
                    }
                    _ => self.spines_of_fabric(from).map(NodeId::new).collect(),
                }
            }
            FabricNode::Spine => {
                // Go down to the fabric switches in the target pod.
                self.fabrics_of_pod(self.pod_of_fabric(to))
                    .map(NodeId::new)
                    .collect()
            }
        }
    }

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

    fn fabrics_of_pod(&self, pod: usize) -> impl Iterator<Item = usize> {
        let start = self.fabric_base + pod * self.nr_fabs_per_pod;
        start..(start + self.nr_fabs_per_pod)
    }

    fn spines_of_fabric(&self, fab: usize) -> impl Iterator<Item = usize> {
        assert!(matches!(self.nodes[fab], FabricNode::Fabric));
        let plane = fab % self.nr_fabs_per_pod;
        let start = self.spine_base + plane * self.nr_spines_per_plane;
        start..(start + self.nr_spines_per_plane)
    }
}

#[derive(Debug)]
enum FabricNode {
    Host,
    TopOfRack,
    Fabric,
    Spine,
}
