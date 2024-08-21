# Preface
Before participating in the development, many partners already have a basic concept of object storage.
If you are not familiar with the basic concepts of object storage, we will elaborate on the concept of object storage, hoping you can get started quickly.
First, let's understand what is object storage?
Object Storage (Object Storage) is a data storage architecture designed to manage large amounts of unstructured data. Unlike traditional block storage and file storage, object storage manages data as objects, with each object containing data, metadata, and a unique identifier.
# Advantages of Object Storage
Object storage, as a storage solution designed for large-scale unstructured data, has the following advantages:
1. **Scalability**:
   - Object storage can easily expand storage capacity and performance by adding more nodes, making it highly suitable for environments that require continuously growing storage space.
2. **High Availability**:
   - Object storage systems are typically designed for high availability, with data redundancy and automatic failover mechanisms ensuring service continuity even in the event of hardware failures.
3. **Durability**:
   - Through multiple replicas or erasure coding techniques, object storage can ensure the durability of data over the long term, resisting various fault conditions.
4. **Simplified Management**:
   - Object storage simplifies data management because it does not require complex file system structures; each object is independent, making maintenance and management tasks simpler.
5. **Cost-Effectiveness**:
   - Object storage is generally less costly because it can make more efficient use of hardware resources and reduces the complexity and management overhead of traditional storage systems.
6. **Data Access Flexibility**:
   - Object storage allows data access through simple HTTP interfaces, facilitating data sharing and distribution across networks.
7. **Compatibility**:
   - Many object storage solutions support standard APIs, such as Amazon S3, making it easier to migrate applications and services.
8. **Support for Diverse Use Cases**:
   - Object storage is suitable for a variety of application scenarios, including backup and archiving, big data analytics, media and content distribution, cloud storage services, and more.
9. **Geographically Distributed Storage**:
   - Object storage can be distributed across geographic locations, supporting global data access and disaster recovery.
10. **Security**:
    - Object storage provides various security features, including data encryption, access control, authentication, and log auditing, to protect data from unauthorized access.
11. **Ease of Integration**:
    - Object storage services can be easily integrated with existing applications and services, providing seamless data storage and retrieval.
12. **Support for RESTful APIs**:
    - Object storage typically supports RESTful APIs, allowing developers to use standard HTTP operations to manage stored data.
# Nouns Related to Object Storage
The technology of object storage involves many related terms, which we will display in a table as follows:

# Nouns Related to Object Storage
The technology of object storage involves many related terms, which we will display in a table as follows:

| Term | Description |
| --- | --- |
| Object | An object is the basic data unit in object storage, with each object containing data and metadata |
| Key | Each object in object storage has a unique key used to identify the object |
| Metadata | Metadata is information that describes the attributes of an object, consisting of a set of name-value pairs used as part of object management |
| Data | Data is organized in object storage, divided into a series of blocks, each wrapped into an object |
| Host style and Path style | These two access methods refer to virtual host style and path style, respectively. In S3, the bucket in a path-style URL is in the URL path, while the virtual host-style URL uses the bucket as a subdomain to improve access performance |
| IAM | Identity and Access Management (IAM) is a policy in object storage used to control access to objects, allowing you to specify who can access objects and what they can do |
| Bucket | A bucket is the basic management unit in object storage, with each bucket having a unique name used to identify stored objects |
| Access Control List (ACL) | An Access Control List (ACL) is a policy in object storage used to control access to objects, allowing you to specify who can access objects and what they can do |
| Storage Class | Storage Class is a policy in object storage used to control the type of object storage, allowing you to choose different storage types as needed |
| Lifecycle | Lifecycle is a policy in object storage used to control the lifecycle of objects, allowing you to set different storage types as needed |
| Bucket Policy | Bucket Policy is a policy in object storage used to control access permissions to buckets, allowing you to specify who can access buckets and what they can do |
| Cross-Region Replication | Cross-Region Replication is a policy in object storage used to replicate data from one region to another, allowing you to synchronize across multiple regions |
| Cross-Origin Resource Sharing | Cross-Origin Resource Sharing is a policy in object storage used to control cross-origin requests, allowing you to share resources across multiple domains |
| Versioning | Versioning is a policy in object storage used to manage multiple versions of objects, allowing you to retain multiple versions of the same object in object storage |
| Bucket Logging | Bucket Logging is a policy in object storage used to record access to buckets, allowing you to retain access logs for buckets in object storage |
| Static Website Hosting | Static Website Hosting is a policy in object storage used to deploy static websites to object storage, allowing you to host static websites in object storage |
| WORM (Write-Once-Read-Many) | Similar to read-only mode, the WORM feature ensures that an object is never deleted or overwritten within a set time period or permanently, primarily used in data protection scenarios |
| Object Lock | Object Lock is a policy in object storage used to control access to objects, allowing you to lock objects in object storage to prevent accidental modification or deletion |
| Object Version | Object Version is a policy in object storage used to manage multiple versions of objects, allowing you to retain multiple versions of the same object in object storage |
| Bucket Quotas | Bucket Quotas is a policy in object storage used to control the storage quota of buckets, allowing you to set storage quotas in object storage |
| S3 | Initially, S3 was the object storage service provided by Amazon Web Services (AWS); now, S3 is the standard protocol for object storage. AWS's object storage is still referred to as S3, and the essence and core of the S3 protocol are the HTTP communication protocol |
| Storage Class | Storage Class is a policy in object storage used to control the type of object storage, allowing you to choose different storage types as needed |
| S3-IA | S3-IA is a storage type of S3, which allows objects to be stored at a low cost in storage with infrequent access, suitable for long-term preservation of data that is not frequently accessed |
| S3-Standard (also known as Standard Storage) | S3-Standard is a storage type of S3, which allows objects to be stored at a low cost in standard storage, suitable for long-term preservation of data that is not frequently accessed |
| S3-Standard-IA | S3-Standard-IA is a storage type of S3, which allows objects to be stored at a low cost in standard storage, suitable for long-term preservation of data that is not frequently accessed |
| Security Token Service | A service used for temporary authorization to access object storage resources. Through STS, users can generate an access credential with a custom expiration time and permissions, thus enabling temporary access control to resources |

