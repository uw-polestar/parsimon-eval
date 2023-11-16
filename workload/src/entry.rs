#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct FullEntry {
    pub timestamp: u64,
    pub packetlength: u64,
    pub srcip: String,
    pub dstip: String,
    pub srcport: String,
    pub dstport: String,
    pub ipprotocol: String,
    pub srchostprefix: String,
    pub dsthostprefix: String,
    pub srcrack: String,
    pub dstrack: String,
    pub srcpod: String,
    pub dstpod: String,
    pub intercluster: bool,
    pub interdatacenter: bool,
}

impl FullEntry {
    pub fn into_entry(self) -> Option<Entry> {
        (self.is_intracluster() && self.is_valid_enough()).then_some(Entry {
            timestamp: self.timestamp,
            srcip: self.srcip,
            dstip: self.dstip,
            srcrack: self.srcrack,
            dstrack: self.dstrack,
            srcpod: self.srcpod,
            dstpod: self.dstpod,
        })
    }

    fn is_intracluster(&self) -> bool {
        !(self.intercluster || self.interdatacenter)
    }

    fn is_valid_enough(&self) -> bool {
        let s = "\\N";
        !(self.srcip == s
            || self.dstip == s
            || self.srcrack == s
            || self.dstrack == s
            || self.srcpod == s
            || self.dstpod == s)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    pub timestamp: u64,
    pub srcip: String,
    pub dstip: String,
    pub srcrack: String,
    pub dstrack: String,
    pub srcpod: String,
    pub dstpod: String,
}
