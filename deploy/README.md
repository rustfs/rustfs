# RustFS Deploy

This directory contains the deployment scripts and configurations for the project.
The deployment process is divided into two main parts: the RustFS binary and the RustFS console. The RustFS binary is
responsible for the core functionality of the system, while the RustFS console provides a web-based interface for
managing and monitoring the system.

# Directory Structure

```text
|--data // data directory
|  |--vol1 // volume 1 not created 
|  |--vol2 // volume 2 not created
|  |--vol3 // volume 3 not created
|  |--vol4 // volume 4 not created
|  |--README.md // data directory readme
|--logs // log directory
|  |--rustfs.log // RustFS log
|  |--README.md // logs directory readme
|--build
|  |--rustfs.run.md // deployment script for RustFS
|  |--rustfs.run-zh.md // deployment script for RustFS in Chinese
|  |--rustfs.service // systemd service file
|  |--rustfs-zh.service.md // systemd service file in Chinese
|--certs
|  ├── rustfs_cert.pem        // Default｜fallback certificate
|  ├── rustfs_key.pem         // Default｜fallback private key
|  ├── rustfs.com/    // certificate directory of specific domain names
|  │   ├── rustfs_cert.pem
|  │   └── rustfs_key.pem
|  ├── api.rustfs.com/
|  │   ├── rustfs_cert.pem
|  │   └── rustfs_key.pem
|  └── cdn.rustfs.com/
|      ├── rustfs_cert.pem
|      └── rustfs_key.pem
|--config
|  |--rustfs.env // env config
|  |--rustfs-zh.env // env config in Chinese
```