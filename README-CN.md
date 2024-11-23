# RustFS Quickstart Guide

[![RustFS](https://raw.githubusercontent.com/rustfs/rustfs/33a05c50cfaf8aaf613bf98826f9e55ab50a7c89/images/logo.svg)](https://rustfs.com)


[![Apache 2.0 licensed][license-badge]][license-url]
[![Unsafe Forbidden][unsafe-forbidden-badge]][unsafe-forbidden-url]

[license-badge]: https://img.shields.io/badge/license-Apache--2.0-blue.svg
[license-url]: ./LICENSE
[unsafe-forbidden-badge]: https://img.shields.io/badge/unsafe-forbidden-success.svg
[unsafe-forbidden-url]: https://github.com/rust-secure-code/safety-dance/


[English](README.md) | 简体中文
 


# 我们是谁？ 👋

RustFS 是一个使用 Rust 编程语言构建的高性能分布式对象存储软件，Rust 是全球最受欢迎的编程语言之一。它与 MinIO 一样，拥有一系列优势，如简洁性、与 S3 兼容、开源特性，以及对数据湖、人工智能和大数据的支持。此外，与其他存储系统相比，RustFS 具有更好的、更用户友好的开源许可证，因为它是在 Apache 许可证下构建的。由于 Rust 是其基础，RustFS 为高性能对象存储提供了更快的速度和更安全的分布式特性。

# 常见问题解答？

1. [为什么我们要用Rust重写MinIO？](/docs/cn/why-rust.md)
2. [MinIO的优点你们会继续保持吗？](/docs/cn/why-good.md )
3. [为什么选择重写MinIO不是重写Ceph?](/docs/cn/why-ceph.md)
4. [为什么是对象存储而不是块存储？](/docs/cn/why-object-storage.md)
5. [们支持哪些语言交流？](/docs/cn/why-language.md)
6. [开源协议的选择？](/docs/cn/how-opensource.md)
7. [如何加入RustFS开源？](/docs/cn/howtojoin.md)


# 我们的网站
https://RustFS.com/


# 我们的愿景和价值观
简单、诚信。
帮助全人类降低存储成本，实现数据安全。


# 开发者文档

1. [对象存储的基本概念](/docs/cn/core/start.md)
2. [多副本和EC的区别?](/docs/cn/core/ec.md)
3. [集中存储和分布式存储的区别?](/docs/cn/core/distributed.md)
4. [分布式存储的模式有哪些？](/docs/cn/core/modes.md)
5. [静态EC与动态EC的优缺点?](/docs/cn/core/ec-modes.md)
6. [RustFS的启动模式](/docs/cn/core/start-modes.md)
7. [磁盘、EC、条带、池](/docs/cn/core/disk-ec-stripes-pools.md)
8.  [我们的代码规范?](/docs/cn/core/code-style.md)
9.  [如何向我们报告Bug和提交新功能建议?](/docs/cn/core/report-bug.md)


# RustFS vs 其他对象存储

| RustFS |  其他对象存储|
| - | - |
|基于 Rust 语言开发，内存更安全 |   使用 Go 或者 C 语言开发，内存GC/内存泄漏等 |
| 不向其他第三国上报日志 | 向其他第三国上报日志，可能违反国家安全法 |
| Apache协议，商用支持更友好 | AGPL V3 等协议、污染开源和协议陷阱，侵犯知识产权|
| S3 支持和功能完善，支持国内、国际云厂商 | S3 支持和功能完善，不支持本地化云厂商 |
| Rust开发，安全信创设备支持良好 | 边缘网关和信创保密设备支持不好 |
| 商业价格稳定，社区支持免费 |  价格高昂，1PiB高达 170万元人民币 |
| 无长臂管辖的法律风险 | 存在指定国家禁止向中国出口和限制使用的断供风险 |

# 如何安装 RustFS

内部测试安装包已经布。
请联系：hello@rustfs.com





# 工作机会

  我们欢迎致力于改变世界存储架构的开源爱好者加入我们。

  可以通过邮箱：hello@rustfs.com 加入我们；

   也可以添加微信，微信ID为：mys3io （只接受开发者申请）

# 投资我们

我们的电子邮件是 hello@rustfs.com


<!--
**RustFS/RustFS** is a ✨ _special_ ✨ repository because its `README.md` (this file) appears on your GitHub profile.

Here are some ideas to get you started:

- 🔭 I’m currently working on ...
- 🌱 I’m currently learning ...
- 👯 I’m looking to collaborate on ...
- 🤔 I’m looking for help with ...
- 💬 Ask me about ...
- 📫 How to reach me: ...
- 😄 Pronouns: ...
- ⚡ Fun fact: ...
-->
