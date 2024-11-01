# 流程

## 写入

http::Body -> HashReader -> ...(other reader) ->  ChuckedReader -> BitrotWriter -> FileWriter
