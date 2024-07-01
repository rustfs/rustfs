use crc32fast::Hasher;
use siphasher::sip::SipHasher;

pub fn sip_hash(key: &str, cardinality: usize, id: &[u8; 16]) -> usize {
    //  你的密钥，必须是16字节

    // 计算字符串的SipHash值
    let result = SipHasher::new_with_key(id).hash(key.as_bytes());

    result as usize % cardinality
}

pub fn crc_hash(key: &str, cardinality: usize) -> usize {
    let mut hasher = Hasher::new(); // 创建一个新的哈希器

    hasher.update(key.as_bytes()); // 更新哈希状态，添加数据

    let checksum = hasher.finalize();

    checksum as usize % cardinality
}
