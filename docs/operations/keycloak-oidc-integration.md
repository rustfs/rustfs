# RustFS 对接 Keycloak OIDC 操作手册

本文给出 RustFS 后台管理控制台对接 Keycloak 的可执行配置步骤。推荐按本文先完成 `default` OIDC provider 的登录闭环，再按生产权限模型细化 Keycloak 分组和 RustFS policy。

## 1. 对接结论

RustFS 可以直接对接 Keycloak，推荐协议是 OpenID Connect Authorization Code Flow。当前 RustFS 代码实现的关键约束如下：

- RustFS 使用 OIDC 授权码模式，并在授权请求中生成 PKCE S256 challenge。
- RustFS 回调接口要求 Keycloak 返回 `code` 和 `state`，随后 RustFS 到 Keycloak token endpoint 换取 token。
- RustFS 必须拿到 `id_token`，并校验签名、issuer、audience、过期时间和 nonce。
- RustFS 不调用 Keycloak Authorization Services 做权限判定；权限来自 ID Token 里的 claim，再映射为 RustFS IAM policy。
- OIDC 配置变更后需要重启 RustFS 才能进入当前运行态。

## 2. 示例参数

现场替换下面这些值即可：

| 参数 | 示例值 | 说明 |
| --- | --- | --- |
| Keycloak 地址 | `https://keycloak.example.com` | Keycloak 对外 HTTPS 地址 |
| Realm | `rustfs` | Keycloak realm 名 |
| Keycloak issuer | `https://keycloak.example.com/realms/rustfs` | RustFS `config_url` 推荐填写这个 issuer |
| Discovery URL | `https://keycloak.example.com/realms/rustfs/.well-known/openid-configuration` | 用于验证 Keycloak OIDC 发现文档 |
| RustFS 控制台地址 | `https://rustfs.example.com` | 用户浏览器访问 RustFS 的外部地址 |
| RustFS provider id | `default` | 本文主流程使用 RustFS 默认 provider |
| RustFS callback URL | `https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default` | 必须配置到 Keycloak Valid redirect URIs |
| Keycloak client id | `rustfs-console` | 给 RustFS 控制台创建的 OIDC client |
| Keycloak client secret | `<KEYCLOAK_CLIENT_SECRET>` | Keycloak client 的 secret |
| RustFS scopes | `openid,profile,email` | 必须包含 `openid` |
| RustFS groups claim | `groups` | 推荐 Keycloak 输出扁平数组 claim |
| RustFS roles claim | `roles` | 如用角色映射，需在 Keycloak 输出扁平数组 claim |

## 3. Keycloak 配置

### 3.1 创建或确认 Realm

1. 登录 Keycloak Admin Console。
2. 进入 realm 下拉菜单，创建或选择 realm：`rustfs`。
3. 确认 discovery 地址可以访问：

```bash
curl -fsS "https://keycloak.example.com/realms/rustfs/.well-known/openid-configuration" \
  | jq '.issuer,.authorization_endpoint,.token_endpoint,.jwks_uri'
```

期望输出中的 issuer 为：

```text
https://keycloak.example.com/realms/rustfs
```

### 3.2 创建 RustFS Client

在 Keycloak Admin Console 中执行：

1. 进入 `Clients`，点击 `Create client`。
2. `Client type` 选择 `OpenID Connect`。
3. `Client ID` 填写 `rustfs-console`。
4. 打开 `Client authentication`。
5. 打开 `Standard flow`。
6. 关闭不需要的 flow：`Implicit flow`、`Direct access grants`、`Service accounts roles`。
7. `Valid redirect URIs` 填写精确回调地址：

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default
```

8. `Web origins` 填写：

```text
https://rustfs.example.com
```

9. 在高级配置中将 `Proof Key for Code Exchange Code Challenge Method` 设置为 `S256`。
10. 保存后进入 `Credentials`，复制 client secret。

说明：

- RustFS 使用授权码模式，并发送 PKCE S256；Keycloak 侧建议强制 `S256`。
- RustFS token 交换时会以 request body 方式提交 client secret。Keycloak 的 `Client Id and Secret` client authenticator 支持 `client_secret_basic` 和 `client_secret_post`，不要在 client policy 中禁用 `client_secret_post`。

### 3.3 创建用户、组和权限映射

RustFS 的后台管理权限最终由 RustFS policy 决定。Keycloak 只负责认证和输出 claim。

RustFS 内置 policy 名包括：

| Policy | 用途 |
| --- | --- |
| `consoleAdmin` | 后台管理、KMS、S3 全权限 |
| `readwrite` | S3 读写权限 |
| `readonly` | S3 只读权限 |
| `writeonly` | S3 写入权限 |
| `diagnostics` | 诊断类管理权限 |

推荐生产做法：

1. 在 Keycloak 创建 group，例如 `consoleAdmin`、`readonly`。
2. 将用户加入对应 group。
3. 为 `rustfs-console` client 添加 group mapper，把用户所属 group 输出到 ID Token 的顶层 `groups` claim。
4. RustFS 将 `groups` claim 中的值映射为同名 RustFS policy。

临时快速验证做法：

- RustFS 配置 `role_policy=consoleAdmin`，这样所有通过该 Keycloak client 登录的用户都会获得 `consoleAdmin`。
- 仅用于连通性验证；生产不建议长期使用。

### 3.4 配置 groups claim mapper

在 Keycloak Admin Console：

1. 进入 `Clients` -> `rustfs-console`。
2. 进入 `Client scopes`。
3. 打开 `rustfs-console-dedicated` 或该 client 专属 scope。
4. 点击 `Add mapper` -> `By configuration` -> `Group Membership`。
5. 配置：

| Keycloak mapper 字段 | 值 |
| --- | --- |
| Name | `rustfs-groups` |
| Token Claim Name | `groups` |
| Full group path | `Off` |
| Add to ID token | `On` |
| Add to access token | `On` |
| Add to userinfo | `On` |
| Multivalued | `On` |

必须关闭 `Full group path`。RustFS policy 名只允许简单安全字符，不能包含 `/`。如果 Keycloak 输出 `/consoleAdmin`，RustFS 不会把它当成有效 policy 名。

### 3.5 可选：配置 roles claim mapper

如果现场更习惯用 Keycloak realm role 或 client role：

1. 给用户分配 role，例如 `consoleAdmin` 或 `readonly`。
2. 在 `rustfs-console` 的 client scope 中添加 `User Realm Role` 或 `User Client Role` mapper。
3. 配置扁平 claim：

| Keycloak mapper 字段 | 值 |
| --- | --- |
| Name | `rustfs-roles` |
| Token Claim Name | `roles` |
| Add to ID token | `On` |
| Add to access token | `On` |
| Multivalued | `On` |

RustFS 不解析 Keycloak 默认的嵌套 claim `realm_access.roles`。如果要用 role 映射，必须额外输出顶层 `roles` 数组，并在 RustFS 配置 `RUSTFS_IDENTITY_OPENID_ROLES_CLAIM=roles`。

## 4. RustFS 配置

### 4.1 推荐方式：环境变量配置

在 RustFS 服务环境中加入以下配置：

```bash
export RUSTFS_IDENTITY_OPENID_ENABLE=on
export RUSTFS_IDENTITY_OPENID_CONFIG_URL="https://keycloak.example.com/realms/rustfs"
export RUSTFS_IDENTITY_OPENID_CLIENT_ID="rustfs-console"
export RUSTFS_IDENTITY_OPENID_CLIENT_SECRET="<KEYCLOAK_CLIENT_SECRET>"
export RUSTFS_IDENTITY_OPENID_SCOPES="openid,profile,email"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC=off
export RUSTFS_IDENTITY_OPENID_DISPLAY_NAME="Keycloak"
export RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM="groups"
export RUSTFS_IDENTITY_OPENID_ROLES_CLAIM="roles"
export RUSTFS_IDENTITY_OPENID_EMAIL_CLAIM="email"
export RUSTFS_IDENTITY_OPENID_USERNAME_CLAIM="preferred_username"
```

如果只是先验证登录链路，可临时增加：

```bash
export RUSTFS_IDENTITY_OPENID_ROLE_POLICY="consoleAdmin"
```

如果要按 Keycloak group/role 精细授权，生产建议不要配置 `RUSTFS_IDENTITY_OPENID_ROLE_POLICY`，而是确保 Keycloak 输出的 `groups` 或 `roles` 值与 RustFS policy 名一致。

配置后重启 RustFS：

```bash
systemctl restart rustfs
```

如果 RustFS 以容器运行，则将以上环境变量写入容器环境或编排平台 Secret/ConfigMap 后重建容器。

### 4.2 可选方式：admin config 命令

如果现场使用 RustFS 兼容的 admin config 命令，也可以写入持久化配置：

```bash
mc admin config set rustfs identity_openid \
  enable=on \
  config_url="https://keycloak.example.com/realms/rustfs" \
  client_id="rustfs-console" \
  client_secret="<KEYCLOAK_CLIENT_SECRET>" \
  scopes="openid,profile,email" \
  redirect_uri="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default" \
  redirect_uri_dynamic=off \
  display_name="Keycloak" \
  groups_claim="groups" \
  roles_claim="roles" \
  email_claim="email" \
  username_claim="preferred_username"

mc admin service restart rustfs
```

如需快速验证管理员登录，把上一条命令末尾两行改成：

```bash
  username_claim="preferred_username" \
  role_policy="consoleAdmin"
```

注意：

- 环境变量 provider 优先级高于持久化配置；同名 provider 同时存在时，以环境变量为准。
- 由环境变量管理的 provider 不能通过 OIDC 配置 API 修改。
- 持久化 OIDC 配置保存后也需要重启 RustFS。

### 4.3 可选：命名 provider

如果不想使用 `default` provider，可以使用 provider id `keycloak`。此时 Keycloak redirect URI 必须改为：

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/callback/keycloak
```

环境变量需要增加后缀，示例：

```bash
export RUSTFS_IDENTITY_OPENID_ENABLE_keycloak=on
export RUSTFS_IDENTITY_OPENID_CONFIG_URL_keycloak="https://keycloak.example.com/realms/rustfs"
export RUSTFS_IDENTITY_OPENID_CLIENT_ID_keycloak="rustfs-console"
export RUSTFS_IDENTITY_OPENID_CLIENT_SECRET_keycloak="<KEYCLOAK_CLIENT_SECRET>"
export RUSTFS_IDENTITY_OPENID_SCOPES_keycloak="openid,profile,email"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI_keycloak="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/keycloak"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC_keycloak=off
export RUSTFS_IDENTITY_OPENID_DISPLAY_NAME_keycloak="Keycloak"
export RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM_keycloak="groups"
export RUSTFS_IDENTITY_OPENID_ROLES_CLAIM_keycloak="roles"
```

provider id 是环境变量后缀本身，登录入口也会使用同一个 id。为减少现场差错，首次对接建议使用 `default`。

## 5. 验证流程

### 5.1 验证 Keycloak discovery

```bash
curl -fsS "https://keycloak.example.com/realms/rustfs/.well-known/openid-configuration" \
  | jq '.issuer,.authorization_endpoint,.token_endpoint,.jwks_uri'
```

必须确认：

- `issuer` 等于 RustFS `RUSTFS_IDENTITY_OPENID_CONFIG_URL` 对应的 issuer。
- `authorization_endpoint`、`token_endpoint`、`jwks_uri` 都是 Keycloak 当前 realm 下的地址。

### 5.2 验证 RustFS OIDC provider 可见

```bash
curl -fsS "https://rustfs.example.com/rustfs/admin/v3/oidc/providers" | jq
```

期望看到 `default` provider，显示名为 `Keycloak`，并且未被隐藏。

### 5.3 发起登录

浏览器打开：

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/authorize/default
```

正常流程：

1. RustFS 302 跳转到 Keycloak。
2. 用户在 Keycloak 登录。
3. Keycloak 回调 RustFS：

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default?code=...&state=...
```

4. RustFS 换取 token，校验 ID Token。
5. RustFS 创建 1 小时有效的 STS 临时凭证并跳回控制台。
6. 控制台以临时凭证访问后台 API。

### 5.4 验证权限

登录成功后做两类验证：

1. 后台管理页面能打开，说明 `consoleAdmin` 或对应管理 policy 生效。
2. 访问桶、对象、诊断页面时符合预期权限，说明 Keycloak claim 到 RustFS policy 的映射正确。

如果登录成功但无权限，优先检查：

- 是否配置了 `role_policy=consoleAdmin`，或 Keycloak 是否输出了 `groups`/`roles`。
- `groups`/`roles` 是否被加入 ID Token。
- claim 值是否为 `consoleAdmin`、`readonly` 等 RustFS 已存在 policy 名。
- claim 值是否包含 `/`、空格或其他不安全字符。
- Keycloak 默认的 `realm_access.roles` 是否没有被转换为顶层 `roles`。

## 6. 常见故障处理

| 现象 | 常见原因 | 处理方式 |
| --- | --- | --- |
| RustFS provider 列表为空 | RustFS 未加载 OIDC 配置或未重启 | 检查环境变量，重启 RustFS |
| RustFS 启动发现 Keycloak 失败 | `config_url` 错误、TLS 不可信、网络不通 | 使用 discovery curl 验证，修正 issuer/TLS/DNS |
| Keycloak 提示 invalid redirect_uri | Keycloak Valid redirect URIs 与 RustFS callback 不完全一致 | 使用精确 callback URL，provider id 必须一致 |
| callback 报缺少 code/state | 反向代理丢失 query string | 检查代理规则，确保完整转发 URL 和 query |
| token exchange failed | client secret 错误或 Keycloak 禁用了 client_secret_post | 重新复制 secret，确认 client authenticator 允许 Client Id and Secret |
| no id_token | scopes 未包含 `openid` 或 client 不是 OIDC 标准流 | RustFS scopes 加 `openid`，Keycloak 打开 Standard flow |
| ID token verification failed | issuer 不匹配、client_id/audience 不匹配、JWKS 不可取 | 使用 realm issuer，确认 client id，检查 discovery/JWKS |
| 登录成功但没有权限 | 未映射 RustFS policy | 配 `role_policy` 验证，或配置 groups/roles mapper |
| group 映射后仍无权限 | Keycloak 输出 `/group/path` | 关闭 mapper 的 `Full group path` |
| 修改配置不生效 | OIDC 运行态未刷新 | 重启 RustFS |

## 7. 生产配置建议

- Keycloak 和 RustFS 对外入口都使用 HTTPS。
- Keycloak `Valid redirect URIs` 使用精确地址，不要用宽泛通配符。
- 不要长期把 `role_policy` 配成 `consoleAdmin`；生产建议用 Keycloak group/role 映射到 RustFS policy。
- client secret 放到系统 Secret、Kubernetes Secret 或受控环境变量，不写入普通文档和仓库。
- Keycloak group/role 名建议直接使用 RustFS policy 名，或用 RustFS `claim_prefix` 做统一前缀。
- 如果使用反向代理，确保 `Host`、`X-Forwarded-Proto`、query string 都正确转发。本文建议显式配置 `redirect_uri` 并关闭动态回调，减少代理差异。

## 8. 交付检查清单

- [ ] Keycloak realm 已创建：`rustfs`
- [ ] Keycloak client 已创建：`rustfs-console`
- [ ] Client authentication 已开启
- [ ] Standard flow 已开启
- [ ] PKCE S256 已开启
- [ ] Valid redirect URI 已精确配置为 RustFS callback
- [ ] Keycloak discovery curl 验证通过
- [ ] Keycloak ID Token 中包含 `email`、`preferred_username`
- [ ] 如果走精细授权，ID Token 中包含顶层 `groups` 或 `roles`
- [ ] `groups`/`roles` 值与 RustFS policy 名一致
- [ ] RustFS OIDC 环境变量或持久化配置已生效
- [ ] RustFS 已重启
- [ ] `/rustfs/admin/v3/oidc/providers` 能看到 provider
- [ ] 浏览器能完成 Keycloak 登录并返回 RustFS 控制台
- [ ] 登录后的管理权限符合预期

## 9. RustFS 代码依据

- `crates/iam/src/oidc.rs`：OIDC provider discovery、授权 URL 构造、PKCE、code exchange、ID Token 校验、claim 提取、权限 claim 映射。
- `rustfs/src/admin/handlers/oidc.rs`：OIDC provider 列表、authorize、callback、logout、配置保存和校验接口。
- `crates/config/src/constants/oidc.rs`：OIDC 配置 key、环境变量名和默认值。
- `rustfs/src/admin/handlers/sts.rs`：OIDC claim 和 policy 写入 STS 临时凭证。
- `crates/iam/src/sys.rs`：STS claim policy 名安全字符校验。
- `crates/policy/src/policy/policy.rs`：RustFS 内置 policy 定义。

## 10. Keycloak 文档依据

- Keycloak OpenID Connect：`https://www.keycloak.org/securing-apps/oidc-layers`
- Keycloak Server Administration Guide：`https://www.keycloak.org/docs/latest/server_admin/index.html`
- Keycloak Authorization Services Guide：`https://www.keycloak.org/docs/latest/authorization_services/index.html`
- Keycloak Upgrading Guide 中关于 `client_secret_basic` 和 `client_secret_post` 的说明：`https://www.keycloak.org/docs/latest/upgrading/index.html`
