use crate::{
    fabric::{Cluster, FabricRoutes},
    spatial::{SpatialData, SpatialWorkload},
};
use parsimon::core::{
    network::{Channel, Flow, FlowId, Network},
    units::{BitsPerSec, Bytes, Nanosecs, Secs},
};
use rand::prelude::*;
use rand_distr::LogNormal;
use utils::Ecdf;

#[derive(Debug, typed_builder::TypedBuilder)]
pub struct FlowGenerator {
    spatial_data: SpatialData,
    cluster: Cluster,
    size_dist: Ecdf,
    lognorm_sigma: f64,
    #[builder(default = Secs::ONE)]
    start_time: Secs,
    max_load: f64,
    stop_when: StopWhen,
    #[builder(default = FlowId::ZERO)]
    id_start: FlowId,
    #[builder(default = 0)]
    seed: u64,
}

impl FlowGenerator {
    pub fn generate(&self) -> Vec<Flow> {
        let mut rng = StdRng::seed_from_u64(self.seed);

        // Get the spatial workload
        let spatial_wk = self.spatial_data.map_to(&self.cluster, &mut rng).unwrap();

        // Compute the rate required to achieve the specified max link load
        let nr_test_flows = match self.stop_when {
            StopWhen::Elapsed(_) => self.cluster.links().count() * 10_000,
            StopWhen::NrFlows(nr_flows) => nr_flows,
        };
        let chan = Self::most_loaded_channel(&spatial_wk, &self.cluster, nr_test_flows, &mut rng);
        let total_rate = chan
            .bandwidth
            .scale_by(self.max_load)
            .scale_by(chan.frac.recip());

        // Get inter-arrival distribution
        let mean_f = Bytes::new(self.size_dist.mean().round() as u64);
        let mean_i = utils::mean_i_for_r(total_rate, mean_f);
        let lognorm_mu = utils::lognorm_mean_to_mu(mean_i.into_f64(), self.lognorm_sigma);
        let delta_dist = LogNormal::new(lognorm_mu, self.lognorm_sigma).unwrap();

        // Generate flows
        Self::do_generate(
            &spatial_wk,
            &self.size_dist,
            delta_dist,
            self.start_time,
            self.stop_when,
            self.id_start,
            &mut rng,
        )
    }

    fn most_loaded_channel(
        spatial_wk: &SpatialWorkload,
        cluster: &Cluster,
        nr_test_flows: usize,
        mut rng: impl Rng,
    ) -> ChannelInfo {
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new_with_routes(&nodes, &links, FabricRoutes::new(cluster))
            .expect("invalid cluster specification");
        let flows = (0..nr_test_flows)
            .map(|i| {
                let (src, dst) = spatial_wk.sample(&mut rng);
                Flow {
                    id: FlowId::new(i),
                    src,
                    dst,
                    size: Bytes::default(),
                    start: Nanosecs::default(),
                }
            })
            .collect::<Vec<_>>();
        let network = network.into_simulations(flows);
        let (nr_link_flows, bandwidth) = network
            .channels()
            .map(|chan| {
                let nr_flows = chan.nr_flows();
                let bandwidth = chan.bandwidth();
                let nr_flows_per_gbps = nr_flows as f64 / (bandwidth.into_f64() / 1e9);
                (nr_flows_per_gbps, nr_flows, bandwidth)
            })
            .max_by(|(a, _, _), (b, _, _)| a.partial_cmp(b).unwrap())
            .map(|(_, nr_flows, bandwidth)| (nr_flows, bandwidth))
            .unwrap();
        ChannelInfo {
            bandwidth,
            frac: nr_link_flows as f64 / nr_test_flows as f64,
        }
    }

    fn do_generate(
        spatial_wk: &SpatialWorkload,
        size_dist: impl Distribution<f64>,
        delta_dist: impl Distribution<f64>,
        start_time: Secs,
        stop_when: StopWhen,
        id_start: FlowId,
        mut rng: impl Rng,
    ) -> Vec<Flow> {
        let start_time: Nanosecs = start_time.into();
        let mut flows = Vec::new();
        let mut nr_flows = 0;
        let mut cur = start_time;
        let (end, max_nr_flows) = match stop_when {
            StopWhen::Elapsed(duration) => (start_time + duration.into(), usize::MAX),
            StopWhen::NrFlows(max_nr_flows) => (Nanosecs::MAX, max_nr_flows),
        };
        while cur < end && nr_flows < max_nr_flows {
            let (src, dst) = spatial_wk.sample(&mut rng);
            let size = Bytes::new(size_dist.sample(&mut rng).round() as u64);
            let delta = Nanosecs::new(delta_dist.sample(&mut rng).round() as u64);
            let flow = Flow {
                id: id_start + FlowId::new(flows.len()),
                src,
                dst,
                size,
                start: cur,
            };
            flows.push(flow);
            nr_flows += 1;
            cur += delta;
        }
        flows
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StopWhen {
    Elapsed(Secs),
    NrFlows(usize),
}

#[derive(Debug)]
struct ChannelInfo {
    bandwidth: BitsPerSec,
    frac: f64,
}
