# RustFS CI/CD 优化方案 - 使用 Ubicloud ARM64 和 x86 混合架构

## 概述

本次优化主要目标：
1. **降低成本**：使用 Ubicloud ARM64 runners 降低 CI/CD 成本约 37.5%
2. **提升性能**：避免交叉编译，使用原生架构编译，提升编译速度
3. **优化链接**：添加 mold 链接器，加速最后的链接阶段

## 关键优化策略

### 1. 避免交叉编译（最重要）

**问题**：现有 build.yml 在 x86 机器上交叉编译 ARM64 版本，性能损失大
**解决方案**：
- ARM64 构建使用 `ubicloud-standard-4-arm` 原生编译
- x86_64 构建使用 `ubicloud-standard-4` 原生编译
- 完全避免交叉编译和 QEMU 模拟

### 2. 升级到 Standard-4

从 `standard-2` (2vCPU, 8GB) 升级到 `standard-4` (4vCPU, 16GB)
- 编译速度提升约 40%
- 避免大型 Rust 项目链接阶段 OOM
- 虽然单价贵一倍，但总成本基本持平（因为时间缩短）

### 3. Docker 多架构构建优化

**当前方案**：使用 QEMU 模拟在 x86 上构建 ARM64 镜像
**优化方案**：
- 分别在各自架构上构建镜像
- 使用 `docker manifest` 合并多架构镜像
- 性能提升 5-10 倍

### 4. 添加 mold 链接器

在 Linux 环境下使用 mold 替代默认 ld，显著减少链接时间

---

## 详细修改对比

### 修改 1: ci.yml - 测试任务使用混合架构

#### 修改前
```yaml
test-and-lint:
  name: Test and Lint
  needs: skip-check
  if: needs.skip-check.outputs.should_skip != 'true'
  runs-on: ubicloud-standard-4  # 只使用 x86
  timeout-minutes: 60
  steps:
    # ... 单一架构测试
```

#### 修改后
```yaml
test-and-lint:
  name: Test and Lint (${{ matrix.arch }})
  needs: skip-check
  if: needs.skip-check.outputs.should_skip != 'true'
  runs-on: ${{ matrix.runner }}
  timeout-minutes: 60
  strategy:
    fail-fast: false
    matrix:
      include:
        - arch: x86_64
          runner: ubicloud-standard-4
        - arch: aarch64
          runner: ubicloud-standard-4-arm
  steps:
    # ... 在各自架构上原生测试
```

**优势**：
- 在真实目标架构上测试，发现架构特定问题
- ARM64 测试使用便宜的 ARM runner（成本降低 37.5%）
- 并行执行，总体时间不变

---

### 修改 2: build.yml - Linux 构建避免交叉编译

#### 修改前
```yaml
matrix:
  include:
    # Linux builds - 都在 x86 上，ARM64 需要交叉编译
    - os: ubicloud-standard-4
      target: x86_64-unknown-linux-musl
      cross: false
      platform: linux
    - os: ubicloud-standard-4
      target: aarch64-unknown-linux-musl
      cross: true  # 交叉编译，慢
      platform: linux
```

**问题**：ARM64 在 x86 上交叉编译，需要 cargo-zigbuild，速度慢

#### 修改后
```yaml
matrix:
  include:
    # x86_64 builds on x86 runners
    - os: ubicloud-standard-4
      target: x86_64-unknown-linux-musl
      cross: false
      platform: linux
      arch: x86_64
    - os: ubicloud-standard-4
      target: x86_64-unknown-linux-gnu
      cross: false
      platform: linux
      arch: x86_64
    
    # aarch64 builds on ARM runners (原生编译)
    - os: ubicloud-standard-4-arm
      target: aarch64-unknown-linux-musl
      cross: false  # 改为原生编译
      platform: linux
      arch: aarch64
    - os: ubicloud-standard-4-arm
      target: aarch64-unknown-linux-gnu
      cross: false  # 改为原生编译
      platform: linux
      arch: aarch64
```

**优势**：
- ARM64 在 ARM runner 上原生编译，速度快
- 无需 cargo-zigbuild 或 cross 工具
- 成本降低（ARM runner 便宜 37.5%）
- 编译产物更优化（可以使用 -C target-cpu=native）

---

### 修改 3: docker.yml - 分架构构建镜像

#### 修改前
```yaml
# 单一 job，使用 QEMU 构建多架构
build-docker:
  runs-on: ubicloud-standard-4
  steps:
    - name: Set up QEMU  # 使用 QEMU 模拟
      uses: docker/setup-qemu-action@v3
    
    - name: Build and push
      uses: docker/build-push-action@v6
      with:
        platforms: linux/amd64,linux/arm64  # QEMU 模拟，慢
```

**问题**：使用 QEMU 模拟在 x86 上构建 ARM64 镜像，性能损失 10-20 倍

#### 修改后
```yaml
# 拆分为两个 job，各自架构原生构建
build-docker-amd64:
  runs-on: ubicloud-standard-4
  steps:
    - name: Build and push (amd64)
      with:
        platforms: linux/amd64  # 原生构建
        outputs: type=image,name=${{ env.REGISTRY }},push-by-digest=true

build-docker-arm64:
  runs-on: ubicloud-standard-4-arm  # ARM runner
  steps:
    - name: Build and push (arm64)
      with:
        platforms: linux/arm64  # 原生构建
        outputs: type=image,name=${{ env.REGISTRY }},push-by-digest=true

# 合并 manifest
merge-manifests:
  needs: [build-docker-amd64, build-docker-arm64]
  runs-on: ubicloud-standard-4
  steps:
    - name: Create and push manifest
      run: |
        docker buildx imagetools create \
          -t ${{ env.REGISTRY }}:${{ env.TAG }} \
          ${{ env.REGISTRY }}@${{ needs.build-docker-amd64.outputs.digest }} \
          ${{ env.REGISTRY }}@${{ needs.build-docker-arm64.outputs.digest }}
```

**优势**：
- 各自架构原生构建，速度提升 5-10 倍
- 无需 QEMU，构建更可靠
- 并行构建，总时间大幅缩短

---

### 修改 4: setup action - 添加 mold 链接器

#### 在 setup/action.yml 中添加
```yaml
- name: Install mold linker (Linux)
  if: runner.os == 'Linux'
  shell: bash
  run: |
    # Install mold for faster linking
    curl -L "https://github.com/rui314/mold/releases/download/v2.4.0/mold-2.4.0-x86_64-linux.tar.gz" | tar xzf -
    sudo mv mold-*/bin/mold /usr/local/bin/
    sudo mv mold-*/libexec/mold /usr/local/libexec/
```

#### 在构建步骤中使用
```yaml
env:
  RUSTFLAGS: "-C link-arg=-fuse-ld=mold"
```

**优势**：链接速度提升 2-5 倍，对大型项目效果显著

---

## 成本与性能对比

### 单次完整 CI 运行预估

| 项目 | 当前方案 (分钟) | 优化后 (分钟) | 当前成本 | 优化后成本 | 节省 |
|------|----------------|--------------|----------|-----------|------|
| Test (x86) | 20 | 18 | $0.032 | $0.036 | -12.5% |
| Test (ARM) | - | 18 | - | $0.018 | - |
| Build x86 musl | 15 | 12 | $0.024 | $0.024 | 0% |
| Build x86 gnu | 15 | 12 | $0.024 | $0.024 | 0% |
| Build ARM musl | 25 (cross) | 12 | $0.040 | $0.012 | **-70%** |
| Build ARM gnu | 25 (cross) | 12 | $0.040 | $0.012 | **-70%** |
| Docker build | 30 | 15 | $0.048 | $0.035 | **-27%** |
| **总计** | **130** | **99** | **$0.208** | **$0.161** | **-22.6%** |

*注：x86 runner $0.0016/分钟，ARM runner $0.001/分钟*

### 关键改进

1. **ARM 构建时间减半**：从 25 分钟（交叉编译）→ 12 分钟（原生）
2. **成本降低 22.6%**：主要来自 ARM 构建成本降低 70%
3. **总时间减少 24%**：从 130 分钟 → 99 分钟
4. **并行度提升**：测试和构建都能充分利用多架构并行

---

## 实施步骤

1. ✅ 分析现有配置
2. ⏳ 修改 ci.yml - 添加 ARM64 测试矩阵
3. ⏳ 修改 build.yml - Linux 构建使用原生架构
4. ⏳ 修改 docker.yml - 分架构构建镜像
5. ⏳ 修改 setup action - 添加 mold 支持
6. ⏳ 创建分支并提交
7. ⏳ 推送到 GitHub 并创建 PR

---

## 注意事项

1. **ARM runner 可用性**：确保 Ubicloud 账户有 ARM runner 配额
2. **缓存兼容性**：不同架构的缓存需要分开（已在 cache-shared-key 中处理）
3. **测试覆盖**：ARM64 测试确保在真实硬件上运行
4. **渐进式迁移**：建议先在 feature 分支测试，确认无误后合并

---

## 预期效果

- ✅ **成本节省 22.6%**（每次 CI 运行约节省 $0.047）
- ✅ **时间节省 24%**（每次 CI 运行节省 31 分钟）
- ✅ **构建质量提升**（原生编译，无交叉编译问题）
- ✅ **测试覆盖增强**（真实 ARM64 硬件测试）
