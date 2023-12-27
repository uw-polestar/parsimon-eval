//! An interface to the backend ns-3 simulation.
//!
//! This crate is tightly coupled to interface provided by the ns-3 scripts.

#![warn(unreachable_pub, missing_debug_implementations, missing_docs)]

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::io;

use derivative::Derivative;
use parsimon::core::{
    network::Flow,
    // network::types::{Link, Node},
    // units::{Bytes, Nanosecs},
};

/// An ns-3 simulation.
#[derive(Debug, typed_builder::TypedBuilder)]
pub struct Mlsys {
    /// The directory in the flowsim source tree containing the `main_flowsim_mmf.py`.
    #[builder(setter(into))]
    pub script_path: PathBuf,
    /// The directory in which to write simulation configs and data.
    #[builder(setter(into))]
    pub data_dir: PathBuf,
    /// The congestion control protocol.
    #[builder(default)]
    pub cc_kind: CcKind,
    /// The flows to simulate.
    /// PRECONDITION: `flows` must be sorted by start time
    pub flows: Vec<Flow>,
}

impl Mlsys {
    /// Run the simulation, returning a vector of [FctRecord]s.
    ///
    /// This routine can fail due to IO errors or errors parsing ns-3 data.
    pub fn run(&self, n_hosts: usize) -> io::Result<()> {
        // Set up directory
        let mk_path = |dir, file| [dir, file].into_iter().collect::<PathBuf>();
        fs::create_dir_all(&self.data_dir)?;

        // Set up the topology
        // let topology = translate_topology(&self.nodes, &self.links);
        // fs::write(
        //     mk_path(self.data_dir.as_path(), "topology.txt".as_ref()),
        //     topology,
        // )?;

        // Set up the flows
        let flows = translate_flows(&self.flows);
        fs::write(
            mk_path(self.data_dir.as_path(), "flows.txt".as_ref()),
            flows,
        )?;

        // Run flowsim
        self.invoke_mlsys(n_hosts)?;

        // Parse and return results
        // let s = fs::read_to_string(mk_path(
        //     self.data_dir.as_path(),
        //     // format!("fct_mlsys_{}.txt", self.cc_kind.as_str()).as_ref(),
        //     format!("fct_mlsys.txt").as_ref(),
        // ))?;
        // let records = parse_mlsys_records(&s)?;
        // Ok(records)
        Ok(())
    }

    fn invoke_mlsys(&self, n_hosts: usize) -> io::Result<()> {
        // We need to canonicalize the directories because we run `cd` below.
        let data_dir = std::fs::canonicalize(&self.data_dir)?;
        let data_dir = data_dir.display();
        let script_path = std::fs::canonicalize(&self.script_path)?;
        let script_path = script_path.display();
        let n_hosts = n_hosts.to_string();
        // let cc = self.cc_kind.as_str();
        // Build the command that runs the Python script.
        // let python_command = format!(
        //     "{script_path}/python {script_path} --root {data_dir} -b 10 --nhost {n_hosts} --cc {cc}> {data_dir}/output.txt 2>&1"
        // );
        let c_command = format!(
            "run ../data_test/checkpoints/model_llama_pmn.bin ../data_test/checkpoints/model_mlp_pmn.bin {data_dir} -b 10 -e 288 -n {n_hosts} -p 30> {data_dir}/output.txt 2>&1"
        );
        // println!("{}", python_command);
        // Execute the command in a child process.
        let _output = Command::new("sh")
            .arg("-c")
            .arg(format!("cd {script_path}; {c_command}; rm {data_dir}/flows.txt"))
            .output()?;
        Ok(())
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

// fn translate_topology(nodes: &[Node], links: &[Link]) -> String {
//     let mut s = String::new();
//     let switches = nodes
//         .iter()
//         .filter(|&n| matches!(n.kind, NodeKind::Switch))
//         .collect::<Vec<_>>();
//     // First line: total node #, switch node #, link #
//     writeln!(s, "{} {} {}", nodes.len(), switches.len(), links.len()).unwrap();
//     // Second line: switch node IDs...
//     let switch_ids = switches
//         .iter()
//         .map(|&s| s.id.to_string())
//         .collect::<Vec<_>>()
//         .join(" ");
//     writeln!(s, "{switch_ids}").unwrap();
//     // src0 dst0 rate delay error_rate
//     // src1 dst1 rate delay error_rate
//     // ...
//     for link in links {
//         writeln!(
//             s,
//             "{} {} {} {} 0",
//             link.a, link.b, link.bandwidth, link.delay
//         )
//         .unwrap();
//     }
//     s
// }

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
    // let lines = flows
    //     .iter()
    //     .map(|f| {
    //         format!(
    //             "{} {} {} 3 100 {} {}",
    //             f.id,
    //             f.src,
    //             f.dst,
    //             f.size.into_u64(),
    //             f.start.into_u64() as f64 / 1e9 // in seconds, for some reason
    //         )
    //     })
    //     .collect::<Vec<_>>();
    lines.join("\n")
}

// fn parse_mlsys_records(s: &str) -> Result<Vec<Vec<f32>>, ParseMlsysError> {
//     parse_flowsim_record(s.lines().next().unwrap())
// }

// fn parse_flowsim_record(s: &str) -> Result<Vec<Vec<f32>>, ParseMlsysError> {
//     // sip, dip, sport, dport, size (B), start_time, fct (ns), standalone_fct (ns)
//     const NR_MLSYS_FIELDS: usize = 80;
//     const NR_SIZE_BUCKETS: usize = 4;
//     let mut fields = s.split_whitespace().map(|x| x.parse::<f32>().unwrap()).collect::<Vec<f32>>();
//     let nr_fields = fields.len();
//     if nr_fields != NR_MLSYS_FIELDS {
//         return Err(ParseMlsysError::WrongNrFields {
//             expected: NR_MLSYS_FIELDS,
//             got: nr_fields,
//         });
//     }
//     Ok(fields.chunks_mut(NR_SIZE_BUCKETS).map(|row| row.to_vec()).collect())
// }

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

/// Congestion control protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Derivative, serde::Serialize, serde::Deserialize)]
#[derivative(Default)]
#[serde(rename_all = "lowercase")]
pub enum CcKind {
    /// DCTCP.
    #[derivative(Default)]
    Dctcp,
    /// TIMELY.
    Timely,
    /// DCQCN.
    Dcqcn,
}

impl CcKind {
    fn as_str(&self) -> &'static str {
        match self {
            CcKind::Dctcp => "dctcp",
            CcKind::Timely => "timely_vwin",
            CcKind::Dcqcn => "dcqcn_paper_vwin",
        }
    }
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
