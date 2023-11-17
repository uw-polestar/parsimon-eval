use std::iter;

use parsimon::core::network::{
    types::{Link, Node},
    NodeId,
};
use rustc_hash::FxHashMap;

pub const NR_RACKS_PER_POD: usize = 48;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Cluster {
    pub planes: Vec<Plane>,
    pub pods: Vec<Pod>,
    pub fab2spine: Vec<Link>,
}

impl Cluster {
    pub fn nr_pods(&self) -> usize {
        self.pods.len()
    }

    pub fn nr_tors_per_pod(&self) -> usize {
        self.pods.first().map(|p| p.racks.len()).unwrap_or(0)
    }

    pub fn nr_fabs_per_pod(&self) -> usize {
        self.pods.first().map(|p| p.fabs.len()).unwrap_or(0)
    }

    pub fn nr_spines_per_plane(&self) -> usize {
        self.planes.first().map(|p| p.len()).unwrap_or(0)
    }

    pub fn nr_hosts_per_rack(&self) -> usize {
        self.pods
            .first()
            .map(|p| p.nr_hosts_per_rack())
            .unwrap_or(0)
    }

    pub fn tor_base(&self) -> usize {
        self.pods.first().map(|p| p.tor_base()).unwrap_or(0)
    }

    pub fn fabric_base(&self) -> usize {
        self.pods.first().map(|p| p.fabric_base()).unwrap_or(0)
    }

    pub fn spine_base(&self) -> usize {
        self.planes
            .first()
            .map(|p| p.first().map(|s| s.id.inner()).unwrap_or(0))
            .unwrap_or(0) // Q? default value here?
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.planes
            .iter()
            .flat_map(|pl| pl.iter())
            .chain(self.pods.iter().flat_map(|p| p.nodes()))
    }

    pub fn links(&self) -> impl Iterator<Item = &Link> {
        self.fab2spine
            .iter()
            .chain(self.pods.iter().flat_map(|p| p.links()))
    }

    pub fn contiguousify(&mut self) {
        // Collect nodes in a very specific way: hosts first, then ToRs, the fabric switches, then
        // spine switches, preserving all ordering.
        let spines = self.planes.iter().flat_map(|plane| plane.iter());
        let mut fabs = Vec::new();
        let mut tors = Vec::new();
        let mut hosts = Vec::new();
        for pod in &self.pods {
            for fab in &pod.fabs {
                fabs.push(fab);
            }
            for rack in &pod.racks {
                tors.push(&rack.tor);
                for host in &rack.hosts {
                    hosts.push(host);
                }
            }
        }
        let nodes = hosts
            .into_iter()
            .chain(tors.into_iter().chain(fabs.into_iter().chain(spines)));

        // Now rename node IDs.
        let old2new = nodes
            .into_iter()
            .enumerate()
            .map(|(i, n)| (n.id, NodeId::new(i)))
            .collect::<FxHashMap<_, _>>();
        for plane in &mut self.planes {
            for spine in plane {
                rename_node(spine, &old2new);
            }
        }
        for pod in &mut self.pods {
            pod.rename(&old2new);
        }
        for link in &mut self.fab2spine {
            rename_link(link, &old2new);
        }
    }
}

pub type Plane = Vec<Node>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Pod {
    pub fabs: Vec<Node>,
    pub racks: Vec<Rack>,
    pub tor2fab: Vec<Link>,
}

impl Pod {
    pub fn nr_hosts_per_rack(&self) -> usize {
        self.racks.first().map(|r| r.hosts.len()).unwrap_or(0)
    }

    pub fn tor_base(&self) -> usize {
        self.racks.first().map(|r| r.tor.id.inner()).unwrap_or(0) // Q? what should default value be
    }

    pub fn fabric_base(&self) -> usize {
        self.fabs.first().map(|f| f.id.inner()).unwrap_or(0) // Q? what should default value be
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.fabs
            .iter()
            .chain(self.racks.iter().flat_map(|r| r.nodes()))
    }

    pub fn links(&self) -> impl Iterator<Item = &Link> {
        self.tor2fab
            .iter()
            .chain(self.racks.iter().flat_map(|r| r.links()))
    }

    fn rename(&mut self, old2new: &FxHashMap<NodeId, NodeId>) {
        for fab in &mut self.fabs {
            rename_node(fab, old2new);
        }
        for rack in &mut self.racks {
            rack.rename(old2new);
        }
        for link in &mut self.tor2fab {
            rename_link(link, old2new);
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Rack {
    pub tor: Node,
    pub hosts: Vec<Node>,
    pub host2tor: Vec<Link>,
}

impl Rack {
    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        iter::once(&self.tor).chain(self.hosts.iter())
    }

    pub fn links(&self) -> impl Iterator<Item = &Link> {
        self.host2tor.iter()
    }

    fn rename(&mut self, old2new: &FxHashMap<NodeId, NodeId>) {
        rename_node(&mut self.tor, old2new);
        for host in &mut self.hosts {
            rename_node(host, old2new);
        }
        for link in &mut self.host2tor {
            rename_link(link, old2new);
        }
    }
}

fn rename_node(node: &mut Node, old2new: &FxHashMap<NodeId, NodeId>) {
    node.id = *old2new.get(&node.id).unwrap();
}

fn rename_link(link: &mut Link, old2new: &FxHashMap<NodeId, NodeId>) {
    link.a = *old2new.get(&link.a).unwrap();
    link.b = *old2new.get(&link.b).unwrap();
}

#[cfg(test)]
mod tests {
    use crate::testing::TINY_CLUSTER_UNORDERED;

    use super::*;

    #[test]
    fn contiguousify_correct() -> anyhow::Result<()> {
        let mut cluster: Cluster = serde_json::from_str(TINY_CLUSTER_UNORDERED)?;
        cluster.contiguousify();
        insta::assert_yaml_snapshot!(&cluster);
        Ok(())
    }
}
