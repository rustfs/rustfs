# How to compile RustFS

|  Must package  |  Version  |
| - |  - |
|  Rust | 1.8.5 |
| protoc | 27.0 |
| flatc |  24.0+ |


Download  Links:
https://github.com/google/flatbuffers/releases/download/v24.3.25/Linux.flatc.binary.g++-13.zip
https://github.com/protocolbuffers/protobuf/releases/download/v27.0/protoc-27.0-linux-x86_64.zip

Or use Docker:

- uses: arduino/setup-protoc@v3
        with:
          version: "27.0"

 - uses: Nugine/setup-flatc@v1
        with:
          version: "24.3.25"



# How to add Console web

1. Wget http://dl.rustfs.com/console/console.latest.tar.gz

2. Mkdir in this repos folder `./rustfs/static`

   
