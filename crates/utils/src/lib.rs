use std::{collections::BTreeMap, fs, path::Path};

use anyhow::Context;
use ordered_float::OrderedFloat;
use parsimon::core::units::{BitsPerSec, Bytes, Nanosecs};
use rand::prelude::*;

#[derive(Debug, Clone)]
pub struct Ecdf {
    ecdf: Vec<(f64, f64)>,
}

impl Ecdf {
    pub fn from_ecdf(ecdf: Vec<(f64, f64)>) -> Result<Self, EcdfError> {
        if ecdf.is_empty() {
            return Err(EcdfError::InvalidEcdf);
        }
        let len = ecdf.len();
        if (ecdf[len - 1].1 - 100.0).abs() > f64::EPSILON {
            return Err(EcdfError::InvalidEcdf);
        }
        for i in 1..len {
            if ecdf[i].1 <= ecdf[i - 1].1 || ecdf[i].0 <= ecdf[i - 1].0 {
                return Err(EcdfError::InvalidEcdf);
            }
        }
        Ok(Self { ecdf })
    }

    pub fn from_values(values: &[f64]) -> Result<Self, EcdfError> {
        if values.is_empty() {
            return Err(EcdfError::NoValues);
        }
        let mut values = values
            .iter()
            .map(|&val| OrderedFloat(val))
            .collect::<Vec<_>>();
        values.sort();
        let points = values
            .iter()
            .enumerate()
            .map(|(i, &size)| (size, (i + 1) as f64 / values.len() as f64))
            .collect::<Vec<_>>();
        let mut map = BTreeMap::new();
        for (x, y) in points {
            // Updates if key exists, kicking out the old value
            map.insert(x, y);
        }
        let ecdf = map
            .into_iter()
            .map(|(x, y)| (x.into_inner(), y * 100.0))
            .collect();
        Self::from_ecdf(ecdf)
    }

    pub fn mean(&self) -> f64 {
        let mut s = 0.0;
        let (mut last_x, mut last_y) = self.ecdf[0];
        for &(x, y) in self.ecdf.iter().skip(1) {
            s += (x + last_x) / 2.0 * (y - last_y);
            last_x = x;
            last_y = y;
        }
        s / 100.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EcdfError {
    #[error("EDist is invalid")]
    InvalidEcdf,

    #[error("No values provided")]
    NoValues,
}

impl Distribution<f64> for Ecdf {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> f64 {
        let y = rng.gen_range(0.0..=100.0);
        let mut i = 0;
        while y > self.ecdf[i].1 {
            i += 1;
        }
        match i {
            0 => self.ecdf[0].0,
            _ => {
                let (x0, y0) = self.ecdf[i - 1];
                let (x1, y1) = self.ecdf[i];
                x0 + (x1 - x0) / (y1 - y0) * (y - y0)
            }
        }
    }
}

pub fn read_ecdf(path: impl AsRef<Path>) -> anyhow::Result<Ecdf> {
    let s = fs::read_to_string(path).context("failed to read CDF file")?;
    let v = s
        .lines()
        .map(|line| {
            let words = line.split_whitespace().collect::<Vec<_>>();
            anyhow::ensure!(words.len() == 2, "invalid CDF file");
            let x = words[0].parse::<f64>().context("invalid CDF x-val")?;
            let y = words[1].parse::<f64>().context("invalid CDF y-val")?;
            Ok((x, y))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Ecdf::from_ecdf(v)?)
}

/// Converts log-normal mean to mu
pub fn lognorm_mean_to_mu(mean: f64, sigma: f64) -> f64 {
    mean.ln() - (sigma.powi(2) / 2_f64)
}

/// Converts log-normal mu to mean
pub fn lognorm_mu_to_mean(mu: f64, sigma: f64) -> f64 {
    (mu + (sigma.powi(2) / 2_f64)).exp()
}

/// Mean interarrival to hit a target utilization, with a minimum of
/// intermediate rounding
pub fn mean_i_for_u(u: f64, bw: impl Into<BitsPerSec>, mean_f: Bytes) -> Nanosecs {
    let bw: BitsPerSec = bw.into();
    let bps = bw.scale_by(u).into_f64();
    let mean_f = mean_f.into_f64();
    let delta = (bps / 8.0 / mean_f).recip() * 1e9;
    Nanosecs::new(delta.round() as u64)
}

/// Mean interarrival to hit a target rate
pub fn mean_i_for_r(r: impl Into<BitsPerSec>, mean_f: Bytes) -> Nanosecs {
    let bps: BitsPerSec = r.into();
    let bps = bps.into_f64();
    let mean_f = mean_f.into_f64();
    let delta = (bps / 8.0 / mean_f).recip() * 1e9;
    Nanosecs::new(delta.round() as u64)
}
