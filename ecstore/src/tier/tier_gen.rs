use crate::tier::tier::TierConfigMgr;

impl TierConfigMgr {
    fn decode_msg(/*dc *msgp.Reader*/) -> Result<(), std::io::Error> {
        todo!();
    }

    fn encode_msg(/*en *msgp.Writer*/) -> Result<(), std::io::Error> {
        todo!();
    }

    pub fn marshal_msg(&self, b: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        todo!();
    }

    pub fn unmarshal_msg(buf: &[u8]) -> Result<Self, std::io::Error> {
        todo!();
    }

    pub fn msg_size(&self) -> usize {
        100
    }
}
