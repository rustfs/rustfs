# OIDC 接口标准兼容性确认与整改项

根据贵方提供的《OIDC认证标准服务接口》文档，目前接口形态更接近“OAuth2 Code 模式 + 私有 profile 接口”，尚不能确认符合标准 OpenID Connect。请补充或确认以下标准 OIDC 必需能力。

## 1. 缺少 OIDC Discovery

标准 OIDC 客户端需要通过以下地址获取 Provider Metadata：

```text
GET {issuer}/.well-known/openid-configuration
```

当前文档只提供了 `authorize`、`accessToken`、`profile` 三个业务接口，未说明 discovery 地址。

请提供标准 discovery 地址，并确保返回至少包含：

- `issuer`
- `authorization_endpoint`
- `token_endpoint`
- `jwks_uri`
- `response_types_supported`
- `subject_types_supported`
- `id_token_signing_alg_values_supported`

## 2. 缺少 JWKS 公钥端点

标准 OIDC 客户端必须校验 `id_token` 签名，需要通过 `jwks_uri` 获取公钥。

当前文档只说明 `id_token` 是 JWT，但未提供：

- `jwks_uri`
- 签名算法，例如 `RS256`
- key rotation 机制
- `kid` 与 JWKS key 的匹配规则

## 3. 授权请求参数不完整

标准 OIDC 授权请求通常需要支持：

- `scope=openid profile email`
- `response_type=code`
- `client_id`
- `redirect_uri`
- `state`
- `nonce`
- `code_challenge`
- `code_challenge_method=S256`

当前文档示例只包含 `client_id`、`response_type=code`、`redirect_uri`、`oauth_timestamp`，未说明是否支持 `scope`、`state`、`nonce`、PKCE。

## 4. 回调缺少 state 回传说明

标准 OAuth2/OIDC 流程中，认证中心必须原样回传应用传入的 `state`，用于防 CSRF。

当前 OIDC 文档 callback 示例只有：

```text
...?code=123456abcdef
```

未说明 `state` 回传。请确认是否支持：

```text
...?code=xxx&state=yyy
```

## 5. Token 接口返回字段不完整

标准 token response 应为 JSON，并至少包含：

- `access_token`
- `token_type`，通常为 `Bearer`
- `expires_in`
- `id_token`

当前文档只明确 `access_token`、`id_token`，未说明 `token_type`、`expires_in`。请确认实际响应是否符合标准 OAuth2/OIDC token response。

## 6. ID Token claims 未定义

标准 OIDC `id_token` 至少需要包含并可校验：

- `iss`
- `sub`
- `aud`
- `exp`
- `iat`
- 如请求带 `nonce`，需要返回 `nonce`

当前文档只笼统说明 `id_token` 是 JWT，未定义具体 claims。请提供 `id_token` 示例和 claims 字段说明。

## 7. UserInfo 调用方式不标准

标准 OIDC UserInfo 通常使用：

```text
GET /userinfo
Authorization: Bearer <access_token>
```

当前文档使用：

```text
/oidc/profile?access_token=xxx
```

这是私有接口形式，不是标准 OIDC UserInfo 调用方式。请确认是否提供标准 `userinfo_endpoint`，并在 discovery 中返回。

## 8. 登出接口不是标准 OIDC logout

当前文档提供的是私有 token 移除接口：

```text
/ssoSession/remove?accessToken=...
```

标准 OIDC 通常通过 discovery 暴露 `end_session_endpoint`，并支持 RP-Initiated Logout。请确认是否支持标准登出端点。

## 9. 认证与权限判断流程混在一起

OIDC 标准主要解决认证问题，即确认“当前用户是谁”。业务系统的权限判断通常由接入方系统根据本地权限模型完成。

当前文档描述的流程是：

- 应用通过 code 换取 `access_token` 和 `id_token`
- 应用调用 `profile` 获取用户信息
- 应用根据返回的用户字段匹配自身平台账号数据
- 如需统一注销，每次请求接口时调用 SSO 的 token 校验接口判断 token 是否有效

这个流程只能证明用户身份或 token 是否有效，不能表达对象存储系统需要的授权信息。例如：

- 用户是否可以访问某个 bucket
- 用户是否可以执行 `s3:GetObject`
- 用户是否可以执行 `s3:PutObject`
- 用户是否可以执行管理操作
- 用户属于哪些组
- 用户映射到哪些访问策略

标准 OIDC 对接时，认证中心需要在 `id_token` 或 `userinfo` 中提供可用于授权映射的 claim，例如：

- `groups`
- `roles`
- `policy`
- 其他双方约定的权限映射字段

接入方系统再根据这些 claim 映射到本地权限策略。

请供应商确认：

- 是否能在 `id_token` 中返回用户组、角色或策略 claim
- 是否能在标准 `userinfo_endpoint` 中返回用户组、角色或策略 claim
- claim 字段名、格式、示例值是什么
- claim 与业务权限之间的映射规则是什么
- 是否只提供 token 有效性校验，而不提供授权所需 claim

如果只提供 `uid` 或 token 是否有效，则只能完成登录认证，不能完成标准 OIDC 场景下的权限映射。
