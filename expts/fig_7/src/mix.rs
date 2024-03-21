use std::path::PathBuf;

use parsimon::core::units::Secs;
use crate::ns3::CcKind;

pub type MixId = usize;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Mix {
    pub id: MixId,
    pub spatial: PathBuf,
    pub size_dist: PathBuf,
    pub lognorm_sigma: f64,
    pub max_load: f64,
    pub cluster: PathBuf,
    pub duration: Secs,
    #[serde(default = "default_param_id")]
    pub param_id: MixId,
    #[serde(default = "default_bfsz")]
    pub bfsz: f64,
    #[serde(default = "default_window")]
    pub window: u64,
    #[serde(default = "default_enable_pfc")]
    pub enable_pfc: f64,
    #[serde(default = "default_cc")]
    pub cc: CcKind,
    #[serde(default = "default_param_cc")]
    pub param_1: f64,
    #[serde(default = "default_param_cc")]
    pub param_2: f64,

}
fn default_param_id() -> MixId {
    0
}

fn default_bfsz() -> f64 {
    50.0
}
fn default_window() -> u64 {
    18000
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
