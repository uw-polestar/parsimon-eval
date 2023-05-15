use std::iter;

use parsimon::core::network::types::{Link, Node};

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
        self.pods.get(0).map(|p| p.racks.len()).unwrap_or(0)
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
}

pub type Plane = Vec<Node>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Pod {
    pub fabs: Vec<Node>,
    pub racks: Vec<Rack>,
    pub tor2fab: Vec<Link>,
}

impl Pod {
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
}
