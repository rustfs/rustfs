# I. Disk
**Disk**: In object storage, a disk is the physical medium that actually stores the data. Similar to traditional storage systems, disks are used for persistent data storage. In distributed storage systems, data is typically distributed across multiple disks to improve performance and reliability.
# II. Erasure Coding
**EC (Erasure Coding)**: Erasure coding is a data protection mechanism used to enhance the reliability of storage systems. It works by dividing data into multiple fragments and adding some redundant fragments, which allows the original data to be recovered in the event of partial data loss. Erasure coding can provide data reliability similar to multiple replicas at a lower cost. For example, with erasure coding technology, the same level of reliability as three replicas can be achieved with approximately 33% redundant data.
# III. Stripe
**Stripe**: In the context of erasure coding, a stripe refers to a series of fragments into which data is divided, including data fragments and redundant fragments. These fragments can be distributed and stored across multiple disks. Striping can improve the performance of data writing and reading and plays a crucial role in data recovery.
# IV. Pool
**Pool**: In an object storage system, a pool usually refers to a logical collection of storage resources, which can include multiple disks or nodes. A pool provides an abstraction layer that makes data management more flexible. In distributed storage systems, data is allocated to different pools to achieve load balancing, performance optimization, and data protection.
