use parsimon::core::network::NodeId;

#[derive(Debug)]
pub struct Routes {
    nr_pods: usize,
    nr_fabs_per_pod: usize,
    nr_hosts_per_rack: usize,

    tor_base: usize,
    fabric_base: usize,

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
                    FabricNode::Host if self.tor_of_host(to) == from => todo!(),
                    _ => self.fabrics_of_tor(from).map(NodeId::new).collect(),
                }
            }
            FabricNode::Fabric => {
                // Go down to a ToR if `to` is a node in this pod. Othwerwise, go up to a spine switch.
                todo!()
            }
            FabricNode::Spine => {
                // Go down to the fabric switches in the target pod.
                todo!()
            }
        }
    }

    fn tor_of_host(&self, host: usize) -> usize {
        assert!(matches!(self.nodes[host], FabricNode::Host));
        self.tor_base + host / self.nr_hosts_per_rack
    }

    fn fabrics_of_tor(&self, tor: usize) -> impl Iterator<Item = usize> {
        assert!(matches!(self.nodes[tor], FabricNode::TopOfRack));
        let start = self.fabric_base + (tor / self.nr_pods) * self.nr_fabs_per_pod;
        start..(start + self.nr_fabs_per_pod)
    }
}

#[derive(Debug)]
enum FabricNode {
    Host,
    TopOfRack,
    Fabric,
    Spine,
}
