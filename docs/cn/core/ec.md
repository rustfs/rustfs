# 对象存储中多副本和纠删码的区别？

 对象存储中的多副本（Multiple Replication）和纠删码（Erasure Coding）是两种不同的数据冗余策略，用于保证数据的可靠性和可用性。以下是两者的主要区别：

### 1.1 多副本：
1. **原理**：
   - 数据被复制多份，每一份都存储在不同的位置（通常是不同的服务器或数据中心）。
   
2. **冗余度**：
   - 冗余度较高，通常为3副本，即数据存储三份。
3. **读写性能**：
   - 读取性能好，因为可以从任何一个副本读取数据。
   - 写入性能相对较低，因为需要同时写入所有副本。
4. **空间利用率**：
   - 空间利用率低，因为存储的数据量是原始数据量的多倍。
5. **容错能力**：
   - 可以容忍多个副本同时丢失，具体数量取决于副本的个数。
6. **恢复速度**：
   - 恢复速度快，只需要复制一份丢失的副本即可。
### 1.2  纠删码：
1. **原理**：
   - 数据被分割成多个片段，然后通过编码算法生成校验片段，这些片段和原始数据片段一起存储。
   
2. **冗余度**：
   - 冗余度相对较低，通常是原始数据量的1/N，N为纠删码的冗余度。
3. **读写性能**：
   - 读取性能较差，因为需要从多个位置读取数据片段并进行解码。
   - 写入性能也较差，因为需要计算并存储额外的校验片段。
4. **空间利用率**：
   - 空间利用率高，因为只需要存储少量的校验数据。
5. **容错能力**：
   - 可以容忍一定数量的数据片段丢失或损坏，具体数量取决于纠删码的设计。
6. **恢复速度**：
   - 恢复速度较慢，因为需要使用剩余的数据片段和校验片段来重建丢失的数据。
### 1.3 总结：
- **多副本**适合对读写性能要求高，且存储空间充足的场景，通常的三副本在分布式环境下丢失数据的风险往往比50%的纠删码要高。
- **纠删码**适合对存储效率要求高，且能够容忍一定读写性能下降的场景。
- 



# RustFS 使用的是哪种存储安全技术？

在大规模存储的情况下，三副本的磁盘使用率是33%，而纠删码的使用为50%的情况下也比多副本安全。

分布式存储里面又使用多副本，又使用纠删码是对于程序员的浪费，并且会提升系统设计的复杂性。

因此，RustFS 使用的是更简洁、更安全的纠删码技术。

