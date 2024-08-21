# 1. What is the difference between Static Erasure Coding and Dynamic Erasure Coding?
Static Erasure Coding and Dynamic Erasure Coding are two distinct data protection techniques, each with its own characteristics in handling data redundancy and fault tolerance. Here are the main differences between the two:
1. **Encoding Method**:
   - **Static Erasure Coding**: The erasure code is generated along with the data at the time of writing and remains unchanged during data storage. Common static erasure coding techniques include Reed-Solomon coding, Cauchy Reed-Solomon coding, etc.
   - **Dynamic Erasure Coding**: The erasure code can be dynamically adjusted during the process of data writing and reading based on the current system load, reliability requirements, and other factors. This technology is more flexible and can optimize the encoding process based on actual conditions.
2. **Update Operations**:
   - **Static Erasure Coding**: When data is updated, it typically requires reading the original data blocks and corresponding parity blocks, recalculating, and writing new parity information. This process can lead to a large number of read-modify-write operations, which is less efficient.
   - **Dynamic Erasure Coding**: Can handle data updates more efficiently by dynamically adjusting the encoding strategy, reducing unnecessary read-modify-write operations, and improving system performance.
3. **Fault Tolerance**:
   - **Static Erasure Coding**: Its fault tolerance is determined at the time of encoding and is usually fixed.
   - **Dynamic Erasure Coding**: Can adjust the level of redundancy based on the actual situation of the storage system, thus dynamically providing different fault tolerance capabilities.
4. **Applicable Scenarios**:
   - **Static Erasure Coding**: Suitable for scenarios where data is not frequently updated or where high data reliability is required, such as cold storage, big data backup, etc.
   - **Dynamic Erasure Coding**: Suitable for scenarios with frequent data updates and significant changes in system load, such as distributed file systems, critical business databases, etc.
5. **System Complexity**:
   - **Static Erasure Coding**: Relatively simple to implement and easy to manage.
   - **Dynamic Erasure Coding**: Complex to implement, requiring more intelligent algorithms to dynamically adjust the encoding strategy, but it can better adapt to system changes.
Static Erasure Coding is widely used in many scenarios due to its stability and ease of management, while Dynamic Erasure Coding has advantages in environments that require frequent data operations due to its flexibility and efficiency.

# 2. What erasure coding does RustFS use?
To keep the system simpler and more reliable, RustFS uses static erasure coding technology.
