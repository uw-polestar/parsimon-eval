//! An interface to the backend ns-3 simulation.
//!
//! This crate is tightly coupled to interface provided by the ns-3 scripts.

#![warn(unreachable_pub, missing_debug_implementations, missing_docs)]

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::io;

use parsimon::core::{
    network::Flow,
    // network::types::{Link, Node},
    units::Bytes,
};
// use rand::prelude::*;
use crate::ns3::CcKind;
/// An ns-3 simulation.
#[derive(Debug, typed_builder::TypedBuilder)]
pub struct Mlsys {
    /// The directory in the flowsim source tree containing the `main_flowsim_mmf.py`.
    #[builder(setter(into))]
    pub script_path: PathBuf,
    /// The directory in which to write simulation configs and data.
    #[builder(setter(into))]
    pub data_dir: PathBuf,
    /// The flows to simulate.
    /// PRECONDITION: `flows` must be sorted by start time
    pub flows: Vec<Flow>,
    /// The random seed for sampling target percentiles.
    pub seed: u64,
    /// The input percentiles.
    pub input_percentiles: Vec<f32>,
    /// The number of size buckets for the feature map from mlsys.
    pub nr_size_buckets: usize,
    /// The number of output percentiles.
    pub output_length: usize,
    /// The buffer size factor.
    #[builder(default = 30.0)]
    pub bfsz: f64,
    /// The sencing window.
    #[builder(default = Bytes::new(18000))]
    pub window: Bytes,
    /// Enable PFC.
    #[builder(default = 1.0)]
    pub enable_pfc: f64,
    /// The congestion control protocol.
    #[builder(default)]
    pub cc_kind: CcKind,
    /// The congestion control parameter.
    #[builder(default = 30.0)]
    pub param_1: f64,
    /// The congestion control parameter.
    #[builder(default = 0.0)]
    pub param_2: f64,
    /// ML model ID
    #[builder(default="".to_string())]
    pub model_suffix: String,
}

impl Mlsys {
    /// Run the simulation, returning a vector of [FctRecord]s.
    ///
    /// This routine can fail due to IO errors or errors parsing ns-3 data.
    pub fn run(&self, n_hosts: usize) -> Result<Vec<Vec<f32>>, Error> {
        // Set up directory
        let mk_path = |dir, file| [dir, file].into_iter().collect::<PathBuf>();
        fs::create_dir_all(&self.data_dir)?;

        // Set up the flows
        let flows = translate_flows(&self.flows);
        fs::write(
            mk_path(self.data_dir.as_path(), "flows.txt".as_ref()),
            flows,
        )?;

        // Run flowsim
        self.invoke_mlsys(n_hosts)?;

        // Parse and return results
        let s = fs::read_to_string(mk_path(
            self.data_dir.as_path(),
            format!("fct_mlsys.txt").as_ref(),
        ))?;
        let records = self.parse_mlsys_record(s.lines().next().unwrap())?;
        Ok(records)
        // Ok(())
    }

    fn invoke_mlsys(&self, n_hosts: usize) -> io::Result<()> {
        // We need to canonicalize the directories because we run `cd` below.
        let data_dir = std::fs::canonicalize(&self.data_dir)?;
        let data_dir = data_dir.display();
        let script_path = std::fs::canonicalize(&self.script_path)?;
        let script_path = script_path.display();

        // Build the command that runs the C script.
        let n_hosts = n_hosts;
        let model_suffix = self.model_suffix.clone();
        let bfsz = self.bfsz;
        let window = self.window.into_u64();
        let enable_pfc = self.enable_pfc;
        let cc = self.cc_kind.get_int_value();
        let param_1 = self.param_1;
        let param_2 = self.param_2;
        let c_command = format!(
            "./run ../ckpts/model_llama{model_suffix}.bin ../ckpts/model_mlp{model_suffix}.bin {data_dir} -b 10 -e 576 -n {n_hosts} -t 1 -f {bfsz} -k {window} -p {enable_pfc} -c {cc} -x {param_1} -y {param_2} > {data_dir}/output.txt 2>&1"
        );

        // println!("{}", c_command);
        // Execute the command in a child process.
        let _output = Command::new("sh")
            .arg("-c")
            .arg(format!("cd {script_path}; {c_command}; rm {data_dir}/flows.txt"))
            .output()?;
        Ok(())
    }

    fn interpolate_values(
        &self,
        input_values: Vec<Vec<f32>>,
    ) -> Vec<Vec<f32>> {
        let input_sets = input_values.len();
        assert!(input_sets == self.nr_size_buckets);
        let mut result = Vec::with_capacity(input_sets);
        for set_index in 0..input_sets {
            let mut input_set = input_values[set_index].clone();
            for i in 1..input_set.len() {
                if input_set[i] < input_set[i - 1] {
                    input_set[i]=input_set[i - 1];
                }
            }
            let set_result=input_set;
            result.push(set_result);
        }
        result
    }
    
    fn parse_mlsys_record(&self, s: &str) -> Result<Vec<Vec<f32>>, ParseMlsysError> {
        // sip, dip, sport, dport, size (B), start_time, fct (ns), standalone_fct (ns)
        let mut fields = s.split_whitespace().map(|x| x.parse::<f32>().unwrap()).collect::<Vec<f32>>();
        let nr_fields = fields.len();
        let nr_mlsys_fields=self.nr_size_buckets*(self.input_percentiles.len());
        if nr_fields != nr_mlsys_fields {
            return Err(ParseMlsysError::WrongNrFields {
                expected: nr_mlsys_fields,
                got: nr_fields,
            });
        }
        let feat_vecs:Vec<_>=fields.chunks_mut(self.input_percentiles.len()).map(|row| row.to_vec()).collect();
        let output_feat=self.interpolate_values(feat_vecs);
        Ok(output_feat)
    }
}

/// The error type for [flowsimSimulation::run].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error parsing ns-3 formats.
    #[error("failed to parse ns-3 format")]
    ParseMlsys(#[from] ParseMlsysError),

    /// IO error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// The function used to delete the data direction for storage.
pub fn ns3_clean(data_dir:PathBuf) -> io::Result<()> {
    let data_dir = std::fs::canonicalize(data_dir)?;
    let data_dir = data_dir.display();
    let _output = Command::new("sh")
        .arg("-c")
        .arg(format!("rm -rf {data_dir}"))
        .output()?;
    Ok(())
}

fn translate_flows(flows: &[Flow]) -> String {
    let nr_flows = flows.len();
    // First line: # of flows
    // src0 dst0 3 dst_port0 size0 start_time0
    // src1 dst1 3 dst_port1 size1 start_time1
    let lines = std::iter::once(nr_flows.to_string())
        .chain(flows.iter().map(|f| {
            format!(
                "{} {} {} 3 100 {} {}",
                f.id,
                f.src,
                f.dst,
                f.size.into_u64(),
                f.start.into_u64() as f64 / 1e9 // in seconds, for some reason
            )
        }))
        .collect::<Vec<_>>();
    lines.join("\n")
}

/// Error parsing ns-3 formats.
#[derive(Debug, thiserror::Error)]
pub enum ParseMlsysError {
    /// Incorrect number of fields.
    #[error("Wrong number of fields (expected {expected}, got {got}")]
    WrongNrFields {
        /// Expected number of fields.
        expected: usize,
        /// Actual number of fields.
        got: usize,
    },

    /// Error parsing field value.
    #[error("Failed to parse field")]
    ParseInt(#[from] std::num::ParseIntError),
}


#[cfg(test)]
mod tests {
    use super::*;

    use parsimon::core::{
        network::{Flow, FlowId, NodeId},
        testing,
        units::{Bytes, Nanosecs},
    };

    // #[test]
    // fn translate_topology_correct() -> anyhow::Result<()> {
    //     let (nodes, links) = testing::eight_node_config();
    //     let s = translate_topology(&nodes, &links);
    //     insta::assert_snapshot!(s, @r###"
    //     8 4 8
    //     4 5 6 7
    //     0 4 10000000000bps 1000ns 0
    //     1 4 10000000000bps 1000ns 0
    //     2 5 10000000000bps 1000ns 0
    //     3 5 10000000000bps 1000ns 0
    //     4 6 10000000000bps 1000ns 0
    //     4 7 10000000000bps 1000ns 0
    //     5 6 10000000000bps 1000ns 0
    //     5 7 10000000000bps 1000ns 0
    //     "###);
    //     Ok(())
    // }
    #[test]
    fn translate_flows_correct() -> anyhow::Result<()> {
        let flows = vec![
            Flow {
                id: FlowId::new(0),
                src: NodeId::new(0),
                dst: NodeId::new(1),
                size: Bytes::new(1234),
                start: Nanosecs::new(1_000_000_000),
            },
            Flow {
                id: FlowId::new(1),
                src: NodeId::new(0),
                dst: NodeId::new(2),
                size: Bytes::new(5678),
                start: Nanosecs::new(2_000_000_000),
            },
        ];
        let s = translate_flows(&flows);
        insta::assert_snapshot!(s, @r###"
        2
        0 0 1 3 100 1234 1
        1 0 2 3 100 5678 2
        "###);
        Ok(())
    }
}
