use std::path::PathBuf;

use rand::{prelude::SliceRandom, Rng};

use crate::ns3::CcKind;
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MixSpace {
    pub spatials: Vec<PathBuf>,
    pub size_dists: Vec<PathBuf>,
    pub lognorm_sigmas: Vec<f64>,
    pub max_loads: LoadRange,
    pub clusters: Vec<PathBuf>,
    pub bfszs: ParamRange,
    pub windows: ParamRange,
    pub pfcs: Vec<f64>,
    pub ccs: Vec<CcKind>,
    pub params: Vec<ParamRange>,
}

impl MixSpace {
    pub fn to_mixes(&self, count: usize, mut rng: impl Rng, mut rng_2: impl Rng) -> Vec<Mix> {
        (0..count)
            .map(|i| Mix {
                id: i,
                spatial: self.spatials.choose(&mut rng).unwrap().clone(),
                size_dist: self.size_dists.choose(&mut rng).unwrap().clone(),
                lognorm_sigma: *self.lognorm_sigmas.choose(&mut rng).unwrap(),
                max_load: rng.gen_range(self.max_loads.low..=self.max_loads.high),
                cluster: self.clusters.choose(&mut rng).unwrap().clone(),
                bfsz: rng_2.gen_range(self.bfszs.low..=self.bfszs.high),
                window: rng_2.gen_range(self.windows.low..=self.windows.high),
                enable_pfc: *self.pfcs.choose(&mut rng_2).unwrap(),
                cc: self.ccs[i % self.ccs.len()],
                param_1: rng_2.gen_range(self.params[i % self.ccs.len()*2].low..=self.params[i % self.ccs.len()*2].high),
                param_2: rng_2.gen_range(self.params[i % self.ccs.len()*2+1].low..=self.params[i % self.ccs.len()*2+1].high),
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct LoadRange {
    low: f64,
    high: f64,
}
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct ParamRange {
    low: f64,
    high: f64,
}

pub type MixId = usize;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Mix {
    pub id: MixId,
    pub spatial: PathBuf,
    pub size_dist: PathBuf,
    pub lognorm_sigma: f64,
    pub max_load: f64,
    pub cluster: PathBuf,
    #[serde(default = "default_bfsz")]
    pub bfsz: f64,
    #[serde(default = "default_window")]
    pub window: f64,
    #[serde(default = "default_enable_pfc")]
    pub enable_pfc: f64,
    #[serde(default = "default_cc")]
    pub cc: CcKind,
    #[serde(default = "default_param_cc")]
    pub param_1: f64,
    #[serde(default = "default_param_cc")]
    pub param_2: f64,
    
}

fn default_bfsz() -> f64 {
    30.0
}
fn default_window() -> f64 {
    18.0
}
fn default_enable_pfc() -> f64 {
    1.0
}
fn default_cc() -> CcKind {
    CcKind::Dctcp
}
fn default_param_cc() -> f64 {
    30.0
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MixParam {
    pub cc: CcKind,
    pub window: u64,
}

#[cfg(test)]
mod tests {
    #[test]
    fn mix_serde() {
        let data= r#"{"id":0,"spatial":"../../workload/spatials/cluster_b_2_16.json","size_dist":"../../workload/distributions/facebook/hadoop-all.txt","lognorm_sigma":1.0,"max_load":0.5523434279086952,"cluster":"spec/cluster_2_to_1.json"}"#;
        let mix = serde_json::from_str::<super::Mix>(data).unwrap();
        // assert_eq!(mix.cc, super::CcKind::Dctcp);
        assert_eq!(mix.id, 0);
        // assert_eq!(mix.cc.get_int_value(), 1);
    }

    fn mix_param_serde() {
        let data= r#"{"cc":"dctcp","window":42000}"#;
        let mix_param = serde_json::from_str::<super::MixParam>(data).unwrap();
        assert_eq!(mix_param.cc, super::CcKind::Dctcp);
        assert_eq!(mix_param.window, 42000);
        assert_eq!(mix_param.cc.get_int_value(), 1);

        let data= r#"{"cc":"dcqcn","window":18000}"#;
        let mix_param = serde_json::from_str::<super::MixParam>(data).unwrap();
        assert_eq!(mix_param.cc, super::CcKind::Dcqcn);
        assert_eq!(mix_param.window, 18000);
        assert_eq!(mix_param.cc.get_int_value(), 3);
    }
}
