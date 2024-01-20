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
}

impl MixSpace {
    pub fn to_mixes(&self, count: usize, mut rng: impl Rng) -> Vec<Mix> {
        (0..count)
            .map(|i| Mix {
                id: i,
                spatial: self.spatials.choose(&mut rng).unwrap().clone(),
                size_dist: self.size_dists.choose(&mut rng).unwrap().clone(),
                lognorm_sigma: *self.lognorm_sigmas.choose(&mut rng).unwrap(),
                max_load: rng.gen_range(self.max_loads.low..=self.max_loads.high),
                cluster: self.clusters.choose(&mut rng).unwrap().clone(),
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct LoadRange {
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
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MixParam {
    pub cc: CcKind,
    pub dctcp_k: u32,
}

#[cfg(test)]
mod tests {
    #[test]
    fn mix_param_serde() {
        let data= r#"{"cc":"dctcp","dctcp_k":42}"#;
        let mix_param = serde_json::from_str::<super::MixParam>(data).unwrap();
        assert_eq!(mix_param.cc, super::CcKind::Dctcp);
        assert_eq!(mix_param.dctcp_k, 42);
        assert_eq!(mix_param.cc.get_int_value(), 1);

        let data= r#"{"cc":"dcqcn","dctcp_k":30}"#;
        let mix_param = serde_json::from_str::<super::MixParam>(data).unwrap();
        assert_eq!(mix_param.cc, super::CcKind::Dcqcn);
        assert_eq!(mix_param.dctcp_k, 30);
        assert_eq!(mix_param.cc.get_int_value(), 3);
    }
}
