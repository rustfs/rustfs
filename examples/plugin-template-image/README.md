# RustFS 图像插件模板

## 给 AI coding agent 的任务提示词

> 你正在基于 RustFS OCI 插件模板开发一个独立的对象处理插件。先阅读本 README、`src/main.rs` 和 `Cargo.toml`，再按我的业务要求修改。保持 Runner 的 `rustfs.job/v2alpha1` 输入与本地制品输出合同：从 `/input/input.json` 读取完整 attempt identity、精确对象版本和有界参数；从 `/credentials/credential.json` 读取短期凭据；只读获取指定版本；只把产物和 `result.json` 写到 `/output`。使用 Rust 和 libvips 实现图像操作，为每个新分支补测试。拒绝缺失或 `null` versionId、未知字段、越界尺寸和不支持的格式。禁止把凭据放进日志、命令行、环境变量或结果文件。完成后运行 README 中的测试和 Docker 构建，报告实际命令、结果、支持范围和未实现能力。

请把这段提示词连同具体业务需求交给 agent。它会把本目录作为可运行模板，不需要先猜 Runner 的容器文件协议。

## 当前样例

样例接收已版本化的 RustFS 图片对象，使用 Runner 提供的短期只读 S3 凭据获取指定 `versionId`，再用 libvips 做缩略处理和格式转换。它支持：

- 等比缩小并限制在目标宽高内（`fit: "inside"`）；
- 等比缩小并居中裁剪（`fit: "cover"`）；源图不足时不放大，结果会小于目标边长；
- 根据图片方向元数据自动旋正；
- 输出 PNG、JPEG 或 WebP；JPEG/WebP 使用 libvips 默认编码质量。

这个测试插件只实现上述基础操作。腾讯云数据万象还覆盖水印、模糊、锐化、EXIF/主色调、动图和智能处理等能力；这些都不是本样例已支持的功能。后续可以按需把单项能力做成明确参数和测试，不要在 manifest 或 README 中宣称未实现的功能。

## Runner 文件合同

Runner 会把以下只读文件和可写目录挂进容器：

- `/input/input.json`：`rustfs.job/v2alpha1`，包含 `attempt`、`object` 和 `parameters`；`object.version` 必须是具体非空版本，不能是 S3 的 `null` 哨兵。
- `/credentials/credential.json`：`access_key`、`secret_key` 和可选 `session_token`。这是 attempt 级短期凭据，只用于读源对象。
- `/output/`：唯一可写目录。插件写入 `image.png`、`image.jpg` 或 `image.webp`，并写 `result.json` 声明文件名和原始完整 attempt identity。Runner 负责把本地产物上传到受控结果桶并绑定精确版本。

最小输入示例：

```json
{
  "schema": "rustfs.job/v2alpha1",
  "attempt": {
    "platform": "example",
    "run": "00000000-0000-4000-8000-000000000001",
    "attempt": "00000000-0000-4000-8000-000000000002",
    "epoch": "1",
    "session": "00000000-0000-4000-8000-000000000003",
    "control_generation": "generation-1"
  },
  "object": {
    "bucket": "images",
    "key": "uploads/photo.jpg",
    "version": "3Lg...exact-version"
  },
  "parameters": {
    "width": 320,
    "height": 240,
    "fit": "cover",
    "format": "webp"
  }
}
```

`parameters` 可省略：默认 320×240、inside、WebP。源对象上限为 32 MiB，解码像素上限为 50 MP，目标边长上限为 10,000 像素，输出上限为 Runner 的 16 MiB 本地制品预算。容器应通过 CLI `--endpoint` 和 `--region` 接收存储地址，不接受密钥参数；HTTPS 是默认要求，仅 loopback HTTP 允许本地 smoke test。

结果文件形状：

```json
{
  "schema": "rustfs.job/v2alpha1",
  "attempt": { "与输入完全相同的 identity" },
  "outcome": "ACCEPTED",
  "artifacts": [{ "name": "image", "file": "image.webp" }]
}
```

## 构建与测试

需要 Docker Compose v2 或 Docker CLI，并使用与 Runner 节点相同的 CPU 架构构建。直接运行 Cargo 测试还需要安装 libvips 开发包和 `pkg-config`；Docker 构建会准备这些依赖并运行单测，运行镜像只带二进制、CA 证书和 libvips 运行库：

```sh
cargo test --locked --manifest-path examples/plugin-template-image/Cargo.toml
docker build -t rustfs-image-transform:dev examples/plugin-template-image
```

把镜像构建并签名后，按 Runner 的受信镜像流程配置 digest 和发布者，不要让 Runner 通过 tag 或无约束 pull 取得镜像。任务命令示例：

```json
["/usr/local/bin/rustfs-image-transform", "--endpoint", "https://s3.example.internal:9000", "--region", "us-east-1"]
```

## 扩展边界

每次扩展先决定输入参数、尺寸/像素/帧/内存上限、输出格式与失败语义，然后为变换和边界条件加测试。增加水印或检测能力时，明确字体、图片资源从哪里来，以及是否需要额外权限；不要假设容器可写宿主文件或能访问任意网络。生产镜像应继续使用 Runner 实施的非 root、只读根文件系统、网络出口限制、CPU/内存/PID 和输出容量限制。

功能参考：[腾讯云数据万象图片处理](https://cloud.tencent.com/document/product/1246/45270)；处理引擎：[libvips 操作列表](https://github.com/libvips/libvips/blob/master/doc/function-list.md)；Rust bindings：[libvips crate](https://docs.rs/libvips/2.3.0/libvips/)。
