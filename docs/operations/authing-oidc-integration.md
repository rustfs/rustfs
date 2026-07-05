# RustFS 对接 Authing OIDC 操作手册

本文用于指导实施人员将 RustFS 后台管理控制台接入 Authing。主流程使用 RustFS 默认 OIDC provider：`default`。

## 1. 对接方式

RustFS 当前支持的是标准 OpenID Connect，不是 Authing 专用插件。Authing 侧必须按 OIDC 应用提供：

- Issuer
- `.well-known/openid-configuration`
- Authorization endpoint
- Token endpoint
- JWKS 或可验证的 ID Token 签名
- 授权码模式返回的 `id_token`

RustFS 代码侧实际流程：

1. 用户访问 RustFS OIDC 登录入口。
2. RustFS 生成 `state`、`nonce`、PKCE S256 challenge。
3. 浏览器跳转到 Authing 登录页。
4. Authing 登录成功后回调 RustFS，携带 `code` 和 `state`。
5. RustFS 使用 `code`、`client_id`、`client_secret`、PKCE verifier 到 Authing token endpoint 换 token。
6. RustFS 校验 `id_token` 的签名、issuer、audience、过期时间和 nonce。
7. RustFS 从 ID Token 中读取 `email`、`preferred_username`、`roles` 或 `groups`。
8. RustFS 将 claim 映射为同名 RustFS policy，再签发 1 小时有效的 STS 临时凭证给控制台。

## 2. 实施参数

实施前先确定以下值：

| 参数 | 示例 | 说明 |
| --- | --- | --- |
| RustFS 外部访问地址 | `https://rustfs.example.com` | 用户浏览器访问 RustFS 的公网或内网入口 |
| RustFS provider id | `default` | 本文主流程固定使用 |
| RustFS 回调地址 | `https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default` | 必须完整写入 Authing 回调 URL |
| Authing 应用域名 | `https://xxx.authing.cn` | 以现场 Authing 应用详情为准 |
| Authing Issuer | `<AUTHING_ISSUER>`，例如 `https://xxx.authing.cn/oidc` 或 `https://xxx.authing.cn/oauth/oidc` | 必须以 Authing 控制台应用详情展示的 Issuer 为准，RustFS `config_url` 使用这个值 |
| Authing discovery | `<AUTHING_ISSUER>/.well-known/openid-configuration` | 验证 OIDC 元数据 |
| Authing App ID | `<AUTHING_APP_ID>` | RustFS `client_id` |
| Authing App Secret | `<AUTHING_APP_SECRET>` | RustFS `client_secret` |
| RustFS scopes | `openid,profile,email,roles` | 必须包含 `openid`；按角色授权时包含 `roles` |

不同 Authing 版本或部署形态的 Issuer 路径可能是 `/oidc`，也可能是 `/oauth/oidc`。实施时不要手工拼路径，必须复制 Authing 控制台应用详情中的 Issuer，并确保 discovery 返回的 `issuer` 与 RustFS `config_url` 完全一致。

## 3. Authing 配置

### 3.1 创建自建应用

1. 登录 Authing 控制台。
2. 进入 `应用` -> `自建应用`。
3. 创建应用，应用名称建议填写 `RustFS Console`。
4. 进入应用详情页，记录：
   - `App ID`
   - `App Secret`
   - `应用域名`
   - `Issuer`
   - `服务发现地址`

### 3.2 配置 OIDC 协议

进入应用详情页：

1. 打开 `协议配置`。
2. 选择或启用 `OIDC`。
3. 授权模式选择或保留：`authorization_code`。
4. 返回类型选择：`code`。
5. Token 换取认证方式选择：`client_secret_post`。
6. PKCE 支持或认证方式中允许 `S256`。
7. ID Token 签名算法建议选择：`RS256`。
8. 保存配置。

说明：

- Authing 文档示例中默认 token 换取认证方式是 `client_secret_post`，与 RustFS 当前代码匹配。
- Authing 文档示例中 ID Token 签名算法可能默认是 `HS256`。RustFS 可以携带 client secret 校验 confidential client 的对称签名，但生产建议改为 `RS256`，这样 discovery/JWKS 验证路径更通用，排障也更直观。

### 3.3 配置回调 URL

在 Authing 应用的回调地址中添加：

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default
```

注意：

- 必须和 RustFS 实际外部访问协议、域名、端口完全一致。
- 本文使用 provider id `default`，所以路径最后必须是 `/default`。
- 不要把回调地址写成 Authing 示例里的 `/auth/cb`，也不要写成 RustFS 控制台首页。

### 3.4 配置授权范围和用户信息

RustFS 至少需要：

```text
openid profile email
```

如果使用 Authing 角色控制 RustFS 权限，增加：

```text
roles
```

最终建议 RustFS 请求：

```text
openid profile email roles
```

Authing 默认 OIDC scope 中：

- `openid` 返回 `sub`
- `email` 返回 `email`、`email_verified`
- `profile` 返回 `preferred_username` 等用户资料
- `roles` 返回用户角色列表

### 3.5 配置角色到 RustFS policy 的映射

RustFS 不会调用 Authing 的权限接口。它只读取 ID Token 中的 `roles` 或 `groups` claim，并把其中的值当成 RustFS policy 名。

建议在 Authing 中创建角色，角色 Code 或实际进入 ID Token 的角色名使用以下 RustFS policy 名：

| Authing 角色值 | RustFS policy | 用途 |
| --- | --- | --- |
| `consoleAdmin` | `consoleAdmin` | 后台管理、KMS、S3 全权限 |
| `readwrite` | `readwrite` | S3 读写 |
| `readonly` | `readonly` | S3 只读 |
| `writeonly` | `writeonly` | S3 写入 |
| `diagnostics` | `diagnostics` | 诊断类管理权限 |

实施建议：

1. 在 Authing 中创建角色 `consoleAdmin`。
2. 给测试用户分配 `consoleAdmin`。
3. 确认 ID Token 顶层有：

```json
{
  "roles": ["consoleAdmin"]
}
```

RustFS 的 `claim_prefix` 只会做固定前缀拼接，不能做任意角色映射。首次实施建议 Authing `roles` 值直接等于 RustFS policy 名称。只有当 RustFS policy 预先命名为 `authing-<role>` 这类固定模式时，才配置 `claim_prefix="authing-"`。

### 3.6 快速联调用临时授权

如果 Authing 角色 claim 暂时不好配置，可以先在 RustFS 侧配置：

```bash
RUSTFS_IDENTITY_OPENID_ROLE_POLICY="consoleAdmin"
```

这会让所有通过该 Authing 应用登录 RustFS 的用户都获得 `consoleAdmin`。只建议用于联调，不建议生产长期使用。

## 4. RustFS 配置

### 4.1 环境变量方式

在 RustFS 服务环境中加入：

```bash
export RUSTFS_IDENTITY_OPENID_ENABLE=on
export RUSTFS_IDENTITY_OPENID_CONFIG_URL="<AUTHING_ISSUER>"
export RUSTFS_IDENTITY_OPENID_CLIENT_ID="<AUTHING_APP_ID>"
export RUSTFS_IDENTITY_OPENID_CLIENT_SECRET="<AUTHING_APP_SECRET>"
export RUSTFS_IDENTITY_OPENID_SCOPES="openid,profile,email,roles"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC=off
export RUSTFS_IDENTITY_OPENID_DISPLAY_NAME="Authing"
export RUSTFS_IDENTITY_OPENID_EMAIL_CLAIM="email"
export RUSTFS_IDENTITY_OPENID_USERNAME_CLAIM="preferred_username"
export RUSTFS_IDENTITY_OPENID_ROLES_CLAIM="roles"
```

如果先做快速联调，临时增加：

```bash
export RUSTFS_IDENTITY_OPENID_ROLE_POLICY="consoleAdmin"
```

配置后重启 RustFS：

```bash
systemctl restart rustfs
```

容器部署时，将上述环境变量写入容器环境、Kubernetes Secret 或部署平台变量后重建实例。

### 4.2 admin config 方式

如果现场使用 RustFS 兼容的 admin config 命令：

```bash
mc admin config set rustfs identity_openid \
  enable=on \
  config_url="<AUTHING_ISSUER>" \
  client_id="<AUTHING_APP_ID>" \
  client_secret="<AUTHING_APP_SECRET>" \
  scopes="openid,profile,email,roles" \
  redirect_uri="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default" \
  redirect_uri_dynamic=off \
  display_name="Authing" \
  email_claim="email" \
  username_claim="preferred_username" \
  roles_claim="roles"

mc admin service restart rustfs
```

快速联调时，把上一条 `mc admin config set` 命令末尾改成：

```bash
  email_claim="email" \
  username_claim="preferred_username" \
  roles_claim="roles" \
  role_policy="consoleAdmin"
```

注意：

- 环境变量优先级高于持久化配置。
- 由环境变量管理的 provider 不能通过 OIDC 配置 API 修改。
- OIDC 配置变更后需要重启 RustFS。

## 5. 验证步骤

### 5.1 验证 Authing discovery

执行：

```bash
AUTHING_ISSUER="<AUTHING_ISSUER>"

curl -fsS "$AUTHING_ISSUER/.well-known/openid-configuration" | jq '{
  issuer,
  authorization_endpoint,
  token_endpoint,
  jwks_uri,
  id_token_signing_alg_values_supported,
  code_challenge_methods_supported,
  token_endpoint_auth_methods_supported,
  scopes_supported
}'
```

检查：

- `issuer` 等于 Authing 控制台展示的 Issuer，并且与 RustFS `config_url` 完全一致
- `authorization_endpoint` 存在
- `token_endpoint` 存在
- `jwks_uri` 存在
- `code_challenge_methods_supported` 包含 `S256`
- `token_endpoint_auth_methods_supported` 包含 `client_secret_post`
- `scopes_supported` 包含 `openid`、`profile`、`email`，按角色授权时包含 `roles`

### 5.2 验证 RustFS provider

执行：

```bash
curl -fsS "https://rustfs.example.com/rustfs/admin/v3/oidc/providers" | jq
```

期望能看到显示名为 `Authing` 的 provider。

### 5.3 发起登录

浏览器打开：

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/authorize/default
```

正常流程：

1. 浏览器跳转到 Authing 登录页。
2. 使用 Authing 用户登录。
3. Authing 回调 RustFS：

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default?code=...&state=...
```

4. RustFS 换取 token 并校验 ID Token。
5. RustFS 生成 STS 临时凭证并跳回控制台。
6. 控制台能正常访问对应权限范围内的功能。

### 5.4 验证权限

如果配置了临时：

```bash
RUSTFS_IDENTITY_OPENID_ROLE_POLICY="consoleAdmin"
```

登录后应具备后台管理能力。

如果使用 Authing `roles`：

1. 测试用户在 Authing 中分配 `consoleAdmin`。
2. RustFS 配置 `RUSTFS_IDENTITY_OPENID_ROLES_CLAIM="roles"`。
3. RustFS scopes 包含 `roles`。
4. 登录后应具备 `consoleAdmin` 对应权限。

如登录成功但没有权限，优先检查 ID Token 顶层是否存在：

```json
{
  "roles": ["consoleAdmin"]
}
```

## 6. 常见问题

| 现象 | 常见原因 | 处理方式 |
| --- | --- | --- |
| `/oidc/providers` 看不到 Authing | RustFS 未加载 OIDC 配置或未重启 | 检查环境变量并重启 |
| Authing 提示回调地址错误 | Authing 回调 URL 与 RustFS callback 不完全一致 | 使用完整 callback：`/rustfs/admin/v3/oidc/callback/default` |
| RustFS 报 `missing code/state` | 代理丢失 query string | 检查反向代理转发规则 |
| RustFS 报 token exchange failed | App Secret 错误或 token 认证方式不匹配 | 确认 Authing 使用 `client_secret_post` |
| RustFS 报 no id_token | scope 未包含 `openid` 或未使用 OIDC 授权码流程 | scopes 加 `openid`，Authing 使用 OIDC `authorization_code` |
| ID token verification failed | issuer、audience、签名算法或 JWKS 不匹配 | 核对 discovery；建议 Authing 使用 `RS256` |
| 登录成功但无权限 | `roles` 未进入 ID Token 或角色名不是 RustFS policy | 配置 `roles` scope/claim，角色值使用 `consoleAdmin` 等 |
| 角色值有空格或特殊字符 | RustFS 过滤不安全 policy 名 | 角色值只使用 `[a-zA-Z0-9_:.-]` 且至少包含一个字母或数字 |
| 配置修改不生效 | OIDC 运行态未刷新 | 重启 RustFS |

## 7. 生产建议

- Authing 和 RustFS 都使用 HTTPS。
- Authing 回调 URL 使用精确地址，不使用宽泛通配符。
- 生产建议使用 `RS256` 签名 ID Token。
- 不要长期使用 `role_policy=consoleAdmin`。
- 使用 Authing `roles` claim 做最小权限映射。
- Authing 角色值直接使用 RustFS policy 名，减少转换逻辑。
- RustFS `redirect_uri_dynamic` 建议关闭，并显式配置 `redirect_uri`。
- 反向代理必须保留 `Host`、`X-Forwarded-Proto` 和 query string。

## 8. 交付检查清单

- [ ] Authing 自建应用已创建
- [ ] OIDC 协议已启用
- [ ] 授权模式为 `authorization_code`
- [ ] 返回类型为 `code`
- [ ] token 认证方式为 `client_secret_post`
- [ ] 回调 URL 已配置为 RustFS callback
- [ ] Discovery URL 可访问
- [ ] Discovery 中 issuer 与 RustFS `config_url` 一致
- [ ] Discovery 中支持 `S256`
- [ ] RustFS OIDC 环境变量或 admin config 已配置
- [ ] RustFS 已重启
- [ ] `/rustfs/admin/v3/oidc/providers` 能看到 Authing
- [ ] 浏览器能完成 Authing 登录并回到 RustFS
- [ ] 临时授权或 `roles` 权限映射验证通过

## 9. RustFS 代码依据

- `crates/iam/src/oidc.rs`：OIDC discovery、授权 URL、PKCE、code exchange、ID Token 校验、claim 提取和 policy 映射。
- `rustfs/src/admin/handlers/oidc.rs`：OIDC 登录入口、callback、配置接口和重启提示。
- `crates/config/src/constants/oidc.rs`：OIDC 配置 key 与环境变量。
- `rustfs/src/admin/handlers/sts.rs`：将 OIDC claim 和 policy 写入 STS 临时凭证。
- `crates/iam/src/sys.rs`：JWT policy claim 名安全校验。
- `crates/policy/src/policy/policy.rs`：RustFS 内置 policy。

## 10. Authing 文档依据

- Authing 协议配置：`https://docs.authing.cn/v2/guides/app-new/create-app/protocol-config.html`
- Authing 授权码 + PKCE：`https://docs.authing.cn/v2/apn/more-oidc-tests/type2.html`
- Authing OIDC Scope 和 Claim：`https://docs.authing.cn/v2/guides/app-new/create-app/oidc-scope.html`
- Authing Express OIDC 集成示例：`https://docs.authing.cn/v2/frameworks/express-oidc-client/`
- Authing Token 验证与 RS256 JWKS：`https://docs.us.authing.co/v2/guides/faqs/how-to-validate-user-token.html`
