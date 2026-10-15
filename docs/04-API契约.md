# 流程编排平台 · API 契约（API Contract）

> 本文是 ProcessEngine 流程编排平台 v1.0 的**接口唯一事实来源**，所有服务端实现、前端调用、第三方对接、联调测试均以本文为准。
> 本文严格遵循 [`docs/00-设计基线.md`](00-设计基线.md)：第 3 节服务拆分与端口、第 5 节流程模型 JSON 与节点类型、第 6.6/6.7/6.8/6.10 节统一响应、错误码、分页、鉴权、幂等与 MQ 约定。
> 技术栈：Java 21 + Spring Boot 3.3.x + Spring Cloud Gateway + OpenFeign + springdoc-openapi（OpenAPI 3）+ RocketMQ 5.3 + MySQL 8.0 + Redis 7.2。
> 约定：全链路 REST + JSON（`application/json; charset=UTF-8`）；分页表格中的「类型」为 Java 侧逻辑类型；所有长整型 ID 在 JSON 中为 **字符串**（见 1.7）。

| 项 | 值 |
| --- | --- |
| 文档版本 | v1.0（与设计基线同步冻结） |
| 契约风格 | REST + JSON，资源复数命名，动作用子资源表达 |
| 鉴权 | OAuth2 风格 JWT（RS256），网关统一校验 |
| 接口总数 | **260**（对外 235 + 内部 Feign 25），另有实时推送 2 通道与死信运维 6 接口 |
| 错误码总量 | **339** 条（11 个 DOMAIN） |
| 相关文档 | [设计基线](00-设计基线.md) / [需求规格说明书](01-需求规格说明书.md) / [系统设计方案](02-系统设计方案.md) / [数据库设计](03-数据库设计.md) / [迭代计划与项目骨架](05-迭代计划与项目骨架.md) |

---

## 1. 通用约定

### 1.1 Base URL 与路径规范

| 项 | 规范 |
| --- | --- |
| 生产入口 | `https://{domain}`（仅暴露 `process-gateway`，其余服务不出公网） |
| 测试入口 | `https://{domain}-test`，联调入口 `http://127.0.0.1:8080` |
| 路径模板 | `/api/{服务短名}/v{版本}/{资源复数}` |
| 服务短名 | `iam` / `design` / `form` / `engine` / `task` / `sched` / `integ` / `notify` / `trace`（`form` 物理归属 `process-design`，`task` 物理归属 `process-engine`，见基线 3「归属裁决」） |
| 版本段 | 路径级版本 `v1`；`v1` 内**只做向后兼容**变更（新增字段/新增接口/放宽校验）；破坏性变更升 `v2`（见第 7 节） |
| 资源命名 | 名词复数、小写、连字符分隔；多词资源用 kebab-case，如 `/node-instances` |
| 子资源 | 从属关系用路径嵌套，深度不超过 4 层：`/api/design/v1/process-defs/{defId}/versions` |
| 动作用子资源表示 | 非 CRUD 动作以动词子资源表达：`/instances/{id}/suspend`、`/tasks/{id}/claim`；**不使用** `?action=xxx` |
| 平台内部路径 | 服务间调用使用 `/api/{服务短名}/v1/internal/**`，网关**不暴露**该前缀（见第 6 节） |
| 尾斜杠 | 不接受尾斜杠；`/api/.../instances/` 返回 `COMMON_10007` |
| 大小写 | 路径与查询参数名大小写敏感；请求头大小写不敏感 |
| 编码 | 请求与响应统一 UTF-8；URL 中的中文必须 percent-encode |
| 健康检查 | `/{service}/actuator/health`（网关内网转发），`/actuator/health/liveness`、`/actuator/health/readiness` 分离 |
| 接口文档 | 网关聚合：`/doc.html`（Knife4j）、`/v3/api-docs/{服务短名}`（springdoc-openapi） |

**请求方法语义**

| 方法 | 语义 | 幂等 | 是否要求 `Idempotency-Key` |
| --- | --- | --- | --- |
| `GET` | 查询，无副作用 | 是 | 否 |
| `POST` | 创建资源 / 执行动作 | 否 | **创建型与引擎推进型必须携带**（见 1.8） |
| `PUT` | 全量替换 | 是 | 否（依赖 `If-Match`，见 1.8） |
| `PATCH` | 局部更新 | 否 | 否（依赖 `If-Match`） |
| `DELETE` | 删除（逻辑删除） | 是 | 否 |

### 1.2 网关路由表

网关 `process-gateway`（8080）按路径前缀路由，鉴权与限流策略如下。`限流策略` 为 Sentinel 资源名，括号内为阈值（v1.0 初始值，支持 Nacos 热更新）。

| # | 路径前缀（Predicate） | 目标服务 | 服务端口 | 是否鉴权 | 限流策略（资源名 / 阈值） | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `/api/iam/v1/auth/**` | `process-iam` | 8081 | 否（白名单） | `gw.iam.auth` / IP 30 QPS + 账号 10 次/分钟 | 登录、刷新令牌；失败计数入 Redis |
| 2 | `/api/{iam,design,form,engine,task,sched,integ,notify,trace}/v1/internal/**` | 不路由 | — | 拒绝 | `gw.internal.block` / 直接 403 | 网关丢弃，防内部接口外泄 |
| 3 | `/api/iam/**` | `process-iam` | 8081 | 是 | `gw.iam` / 用户 200 QPS、租户 1000 QPS | RBAC 管理面 |
| 4 | `/api/design/**` | `process-design` | 8082 | 是 | `gw.design` / 用户 100 QPS | 建模面 |
| 5 | `/api/form/**` | `process-design` | 8082 | 是 | `gw.form` / 用户 300 QPS | 表单渲染高频读 |
| 6 | `/api/engine/**` | `process-engine` | 8083 | 是 | `gw.engine` / 租户 500 QPS、用户 100 QPS | 引擎同步面；启动实例单独限流 |
| 7 | `/api/engine/v1/instances`（POST） | `process-engine` | 8083 | 是 | `gw.engine.start` / 租户 200 QPS、businessKey 1 QPS | 防止业务系统重试风暴 |
| 8 | `/api/task/**` | `process-engine` | 8083 | 是 | `gw.task` / 用户 200 QPS | 人任务读写 |
| 9 | `/api/sched/**` | `process-scheduler` | 8084 | 是 | `gw.sched` / 用户 50 QPS | 调度管理面 |
| 10 | `/api/integ/**` | `process-integration` | 8085 | 是 | `gw.integ` / 用户 100 QPS | 连接器管理面 |
| 11 | `/api/notify/**` | `process-notify` | 8086 | 是 | `gw.notify` / 用户 200 QPS | 站内信轮询 |
| 12 | `/api/trace/**` | `process-trace` | 8087 | 是 | `gw.trace` / 用户 50 QPS、重查询 5 QPS | 聚合查询单独限流 |
| 13 | `/ws/**`、`/sse/**` | `process-trace` | 8087 | 是（握手期校验） | `gw.ws` / 单用户 5 连接 | WebSocket/SSE 直连（见第 3 节） |
| 14 | `/actuator/**` | 各服务 | — | 否（仅内网 | `gw.actuator` / 1 QPS | 由运维网段访问 |
| 15 | `/doc.html`、`/v3/api-docs/**` | 各服务 | — | 是（`ADMIN`） | `gw.docs` / 10 QPS | 非生产环境默认放行 |

路由规则补充：

1. **鉴权过滤链顺序**：`CORS → 请求体大小限制 → traceId 注入 → JWT 校验 → 租户校验 → 权限码校验（@PreAuthorize 在服务侧二次执行）→ 限流 → 熔断 → 路由转发 → 响应包装`。
2. **灰度**：请求头 `X-Gray-Tag`（客户端可传，网关校验白名单）或用户属性命中灰度规则时，路由到带 `gray` 标签的实例组；灰度实例通过 Nacos 元数据 `version=2` 注册。
3. **CORS**：允许来源由 Nacos 配置 `gateway.cors.allowed-origins` 控制；允许方法 `GET,POST,PUT,PATCH,DELETE,OPTIONS`；允许头 `Authorization,Content-Type,X-Tenant-Id,X-Request-Id,Idempotency-Key,If-Match,Accept-Language,X-Gray-Tag`；`Access-Control-Max-Age=3600`。
4. **请求体上限**：默认 2 MB；`/api/integ/v1/connectors/debug` 与导入导出接口放宽至 10 MB（Nacos 可调）。
5. **响应头透传**：网关强制回写 `X-Request-Id`、`X-Trace-Id`、`X-RateLimit-*`、`Retry-After`。

### 1.3 请求头规范

| 请求头 | 必填 | 生产者 | 说明 | 校验规则 |
| --- | --- | --- | --- | --- |
| `Authorization` | 除白名单接口外必填 | 客户端 | `Bearer {accessToken}`，RS256 JWT，Access Token 2h | 网关校验签名/`exp`/`iss`/`aud`；黑名单（登出/踢下线）在 Redis `auth:blacklist:{jti}` |
| `X-Tenant-Id` | 否 | **网关注入** | 租户 ID（字符串形式的 Long） | **客户端传入一律被网关覆盖**；服务侧只信任网关注入值；直连服务（同网段调试）缺失即 `COMMON_20001` |
| `X-Request-Id` | 否 | 客户端（建议） | 调用方请求标识，用于日志与问题定位 | 1–64 位 `[A-Za-z0-9_-]`；缺失时网关注入 UUID；服务侧回写响应头 |
| `X-Trace-Id` | 否 | 网关注入 | OTel 链路 ID | 客户端传值仅在网关开启 `gateway.trace.trust-incoming=true` 时生效 |
| `Idempotency-Key` | **写接口必填**（见 1.8） | 客户端 | 幂等键，全局唯一 | 16–64 位；服务侧以 `{tenantId}:{path}:{key}` 落 `rt_idempotent`，TTL 24h |
| `If-Match` | 条件必填（乐观锁更新） | 客户端 | 期望的资源版本号，如 `"7"`（或弱校验 `W/"7"`） | 与资源 `version` 不一致返回 `COMMON_30003`（HTTP 412/409） |
| `X-Internal-Token` | 内部接口必填 | 网关/服务 | 服务间调用凭证，网关签发，短 TTL（5 min） | 服务侧校验签名、受众（目标服务名）、`exp`；非法返回 `COMMON_20003`（HTTP 401） |
| `Accept-Language` | 否 | 客户端 | 语言，如 `zh-CN`、`en-US` | 缺失默认 `zh-CN`；决定 `message` 渲染与枚举名称语言 |
| `Content-Type` | 有请求体时必填 | 客户端 | `application/json; charset=UTF-8`；文件上传 `multipart/form-data` | 不匹配返回 `COMMON_10005`（HTTP 415） |
| `X-Gray-Tag` | 否 | 客户端 | 灰度标签，如 `canary-a` | 需在 Nacos 白名单内，否则忽略 |
| `X-Client-Info` | 否 | 客户端 | 客户端版本，如 `web/1.4.0` | 用于兼容性降级判断（第 7 节） |
| `X-Forwarded-For` | 否 | 网关 | 真实客户端 IP（网关写入，客户端传入被覆盖） | 服务侧取首个非内网地址记入审计日志 |

**响应头规范**

| 响应头 | 说明 |
| --- | --- |
| `X-Request-Id` | 回显请求 ID（缺失时返回网关生成值） |
| `X-Trace-Id` | 本次链路 ID，用于向 `process-trace` 检索 |
| `X-RateLimit-Limit` / `X-RateLimit-Remaining` / `X-RateLimit-Reset` | 当前窗口配额 / 剩余 / 重置时间（秒级时间戳），限流维度命中时返回 |
| `Retry-After` | 仅 429 / 503 返回，单位秒，建议重试间隔（含随机抖动） |
| `ETag` | 详情类接口返回资源版本，形如 `"7"`，与 `version` 字段同源 |
| `Content-Language` | 实际渲染语言 |

### 1.4 统一响应体

所有接口（含错误）返回统一信封，HTTP 状态码同时保持语义（`200/201/400/401/403/404/409/412/415/422/429/500/502/504`）。

**成功（HTTP 200 / 201）**

```json
{
  "code": "0",
  "message": "ok",
  "data": {
    "instanceId": "1836427591029384756",
    "instanceNo": "PI2025052000000123",
    "processKey": "leave_approval",
    "version": 3,
    "status": "RUNNING",
    "businessKey": "LEAVE-2025-0001",
    "startTime": "2025-05-20 09:31:02.114"
  },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

**成功-分页**

```json
{
  "code": "0",
  "message": "ok",
  "data": { "pageNum": 1, "pageSize": 20, "total": 137, "records": [] },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

**成功-异步受理**

```json
{
  "code": "0",
  "message": "ok",
  "data": { "accepted": true, "instanceId": "1836427591029384756", "instanceNo": "PI2025052000000123", "commandId": "cmd-9f8e7d6c" },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

**失败**

```json
{
  "code": "ENGINE_40001",
  "message": "流程实例不存在或已结束",
  "data": null,
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

**失败-带字段级校验明细（HTTP 422，`data.errors` 为结构化错误列表）**

```json
{
  "code": "DESIGN_10003",
  "message": "流程模型校验未通过",
  "data": {
    "valid": false,
    "errors": [
      { "level": "ERROR", "code": "MODEL_EDGE_DANGLING", "nodeId": "n_approve", "edgeId": "e9", "field": "target", "message": "连线目标节点 n_gone 不存在", "hint": "删除该连线或恢复目标节点" }
    ]
  },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

**信封字段说明**

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `code` | String | 是 | 成功固定 `"0"`；失败为 `{DOMAIN}_{5位数字}`，见 1.5 |
| `message` | String | 是 | 成功为 `ok`；失败为按 `Accept-Language` 渲染的可读信息 |
| `data` | Object / Array / null | 是 | 业务数据；失败通常为 `null`，校验类错误携带 `errors` |
| `traceId` | String | 是 | 32 位十六进制链路 ID，等同响应头 `X-Trace-Id` |

> 说明：一次请求内信封只出现一次，**禁止**在 `data` 内嵌套第二层 `code/message`。批量接口的逐条结果放在 `data.results[]`，每条自带 `success/errorCode/errorMessage`。

### 1.5 错误码总表

**编码规则（基线 6.6）**

- 格式：`{DOMAIN}_{5位数字}`，`DOMAIN ∈ {COMMON, AUTH, IAM, DESIGN, FORM, ENGINE, TASK, SCHED, INTEG, NOTIFY, TRACE}`。
- 数字段首规则：`1xxxx` 参数/校验错误（HTTP 400/415/422）；`2xxxx` 权限/租户/鉴权错误（401/403）；`3xxxx` 业务状态冲突（409/412）；`4xxxx` 资源不存在（404）；`5xxxx` 系统/依赖故障（500/502/503/504）。
- `message` 从 `messages_{locale}.properties` 按 `code` 取值渲染；同一 `code` 在所有服务内唯一。
- 客户端应基于 `code` 分支处理，**不得**基于 `message` 文案判等。

**COMMON（通用）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `COMMON_10001` | 400 | 参数校验失败 | 缺少必填参数、类型转换失败、`@Valid` 不通过 |
| `COMMON_10002` | 400 | 请求体格式非法 | JSON 解析失败、字段类型不匹配 |
| `COMMON_10003` | 400 | 分页参数非法 | `pageNum < 1`、`pageSize > 200`、`pageSize <= 0` |
| `COMMON_10004` | 400 | 排序字段非法 | `orderBy` 不在白名单、含 SQL 片段 |
| `COMMON_10005` | 415 | 不支持的媒体类型 | `Content-Type` 非 JSON/表单 |
| `COMMON_10006` | 400 | 请求体超过大小限制 | 上传或 JSON 超过 2 MB / 10 MB |
| `COMMON_10007` | 404 | 请求路径不存在 | 路径拼写错误、版本段不存在、尾斜杠 |
| `COMMON_10008` | 400 | 时间范围非法 | `startTime > endTime`、超出最大查询跨度 |
| `COMMON_10009` | 400 | 枚举取值非法 | 枚举字符串不在允许集合内 |
| `COMMON_10010` | 400 | 批量操作数量超限 | 批量审批/批量删除条数 > 100 |
| `COMMON_10011` | 400 | 文件类型或大小非法 | 导入文件非 `.json/.bpmn/.xml`，或 > 10 MB |
| `COMMON_10012` | 422 | 字段级校验失败 | 业务规则校验（如流程模型校验、表单校验）需返回 `errors[]` |
| `COMMON_20001` | 401 | 未认证或缺少租户上下文 | 无 `Authorization`、`X-Tenant-Id` 缺失 |
| `COMMON_20002` | 403 | 无接口访问权限 | 权限码校验失败（RBAC 未授权） |
| `COMMON_20003` | 401 | 内部调用令牌非法或过期 | `X-Internal-Token` 缺失/签名错误/`exp` 过期 |
| `COMMON_20004` | 403 | 租户已禁用或已过期 | 租户 `status=DISABLED` 或超过 `expire_time` |
| `COMMON_20005` | 403 | 数据权限越权 | 目标资源不属于当前用户可见范围（`data_scope`） |
| `COMMON_20006` | 403 | 资源不属于当前租户 | 跨租户访问他人资源 |
| `COMMON_20007` | 401 | Token 已失效（黑名单/踢下线） | 登出、被管理员强制下线后的 Token 再次使用 |
| `COMMON_30001` | 409 | 资源已存在 | 唯一键冲突（编码/名称/键重复） |
| `COMMON_30002` | 409 | 资源已被引用，禁止删除 | 删除被他人引用的定义/角色/渠道 |
| `COMMON_30003` | 412 | 乐观锁冲突（版本不一致） | `If-Match` 与当前 `version` 不一致 |
| `COMMON_30004` | 409 | 操作状态不允许 | 当前状态不支持该动作（如已发布版本再保存草稿） |
| `COMMON_30005` | 409 | 重复请求（幂等命中） | 同一 `Idempotency-Key` 重复提交且首次仍在处理中 |
| `COMMON_30006` | 409 | 并发操作被拒绝 | 同实例/同资源正在被其他请求处理（分布式锁占用） |
| `COMMON_30007` | 409 | 数据已变更，请刷新 | 前端缓存过期，需重新拉取 |
| `COMMON_30008` | 429 | 触发限流 | 网关或服务端流控命中（另见 1.9） |
| `COMMON_30009` | 409 | 客户端版本过低 | `X-Client-Info` 低于最低支持版本（强制升级） |
| `COMMON_40001` | 404 | 资源不存在 | 通用按 ID 查询无结果 |
| `COMMON_40002` | 404 | 关联资源不存在 | 引用的父资源/外键对象不存在 |
| `COMMON_40003` | 404 | 接口版本不支持 | `/v2/...` 尚未开放 |
| `COMMON_50001` | 500 | 系统内部错误 | 未捕获异常，已记录 `traceId` |
| `COMMON_50002` | 503 | 依赖服务不可用 | 下游 Feign 熔断打开或注册中心无实例 |
| `COMMON_50003` | 504 | 依赖服务调用超时 | Feign 读超时（默认 3s） |
| `COMMON_50004` | 500 | 数据库操作失败 | 死锁、连接池耗尽、SQL 异常 |
| `COMMON_50005` | 500 | 缓存/Redis 操作失败 | Redis 不可用、序列化异常 |
| `COMMON_50006` | 500 | 消息投递失败 | Outbox 投递 MQ 失败（会重试，最终入 DLQ） |
| `COMMON_50007` | 503 | 服务正在降级 | 熔断降级返回兜底数据/提示 |
| `COMMON_50008` | 500 | 文件存储操作失败 | MinIO 上传/下载失败 |

**AUTH（认证）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `AUTH_10001` | 400 | 用户名或密码为空 | 登录表单缺字段 |
| `AUTH_10002` | 400 | 验证码错误或已过期 | 图形验证码/短信验证码校验失败 |
| `AUTH_10003` | 400 | 授权类型不支持 | `grantType` 非 `password/refresh_token` |
| `AUTH_10004` | 400 | 租户编码缺失 | 多租户登录未传 `tenantCode` 且无法从域名推断 |
| `AUTH_10005` | 400 | Token 格式非法 | 刷新令牌非 JWT 或结构损坏 |
| `AUTH_20001` | 401 | 用户名或密码错误 | 认证失败（连续失败 5 次锁定 10 分钟） |
| `AUTH_20002` | 401 | Access Token 已过期 | `exp` 超过 2h |
| `AUTH_20003` | 401 | Refresh Token 已过期 | `exp` 超过 7d，需重新登录 |
| `AUTH_20004` | 401 | 令牌签名校验失败 | 公钥不匹配、被篡改 |
| `AUTH_20005` | 401 | 令牌受众不匹配 | `aud` 非本服务 |
| `AUTH_20006` | 403 | 账号已停用 | 用户 `status=DISABLED` |
| `AUTH_20007` | 403 | 账号已锁定 | 多次登录失败触发的临时锁定 |
| `AUTH_20008` | 403 | 密码已过期需修改 | 企业密码策略强制改密 |
| `AUTH_20009` | 403 | 登录 IP 不在白名单 | 租户开启 IP 白名单 |
| `AUTH_30001` | 409 | 账号在其他端登录被抢占 | 单点登录策略 `SINGLE` 下新会话顶掉旧会话 |
| `AUTH_40001` | 404 | 租户不存在 | `tenantCode` 无匹配 |
| `AUTH_50001` | 500 | 令牌签发失败 | 密钥加载异常、Redis 写入失败 |
| `AUTH_50002` | 502 | 认证依赖不可用 | 短信/SSO/LDAP 供应商不可达 |

**IAM（身份与权限）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `IAM_10001` | 400 | 用户名格式非法 | 长度 4–32、`^[a-zA-Z][a-zA-Z0-9_.-]*$` 不满足 |
| `IAM_10002` | 400 | 密码强度不足 | 少于 8 位或未含大小写/数字/符号组合 |
| `IAM_10003` | 400 | 邮箱或手机号格式非法 | 正则校验失败 |
| `IAM_10004` | 400 | 部门树层级非法 | 层级 > 10 或父部门为自身子孙 |
| `IAM_10005` | 400 | 角色数据权限枚举非法 | `dataScope` 不在 `ALL/DEPT_AND_CHILD/DEPT/SELF/CUSTOM` |
| `IAM_10006` | 400 | 权限码格式非法 | 非 `{服务}:{资源}:{动作}` |
| `IAM_10007` | 400 | 用户组动态规则非法 | `rule_json` 不是合法 Aviator 表达式 JSON |
| `IAM_10008` | 400 | 批量导入数据非法 | Excel/CSV 表头或行数据错误（返回行号） |
| `IAM_10009` | 400 | 租户配额参数非法 | `maxUserNum <= 0` 或小于当前用户数 |
| `IAM_10010` | 400 | 初始密码为弱口令 | 命中弱口令字典 |
| `IAM_10011` | 400 | 用户组成员为空 | 静态用户组至少 1 名成员 |
| `IAM_20001` | 403 | 无用户管理权限 | 缺少 `iam:user:*` |
| `IAM_20002` | 403 | 无角色管理权限 | 缺少 `iam:role:*` |
| `IAM_20003` | 403 | 无权限/菜单管理权限 | 缺少 `iam:permission:*` |
| `IAM_20004` | 403 | 无部门管理权限 | 缺少 `iam:dept:*` |
| `IAM_20005` | 403 | 无用户组管理权限 | 缺少 `iam:user-group:*` |
| `IAM_20006` | 403 | 无租户管理权限 | 缺少 `iam:tenant:*`（平台级） |
| `IAM_20007` | 403 | 不允许操作超级管理员 | 目标为内置 `admin` 账号 |
| `IAM_20008` | 403 | 不允许操作自身账号 | 自我停用/自我删除 |
| `IAM_20009` | 403 | 超出数据权限范围 | 目标用户不在可见部门范围内 |
| `IAM_30001` | 409 | 用户名已存在 | 同租户 `username` 唯一冲突 |
| `IAM_30002` | 409 | 角色编码已存在 | `role_code` 唯一冲突 |
| `IAM_30003` | 409 | 部门名称同级重复 | 同 `parent_id` 下重名 |
| `IAM_30004` | 409 | 用户组编码已存在 | `group_code` 唯一冲突 |
| `IAM_30005` | 409 | 角色已被用户引用 | 删除角色前存在 `sys_user_role` |
| `IAM_30006` | 409 | 部门存在子部门或用户 | 级联删除校验未通过 |
| `IAM_30007` | 409 | 租户用户数超配额 | 新增用户超过 `max_user_num` |
| `IAM_30008` | 409 | 权限被角色引用 | 删除权限前存在 `sys_role_permission` |
| `IAM_30009` | 409 | 用户已被停用 | 对已停用用户重复停用/派发任务 |
| `IAM_30010` | 409 | 在线会话不存在或已失效 | 强制下线目标会话已被清理 |
| `IAM_30011` | 409 | 租户编码已存在 | `tenant_code` 全局唯一冲突（见 FR-IAM-001） |
| `IAM_40001` | 404 | 用户不存在 | 用户 ID 无匹配（或已在其他租户） |
| `IAM_40002` | 404 | 角色不存在 | 角色 ID 无匹配 |
| `IAM_40003` | 404 | 权限/菜单不存在 | 权限 ID 无匹配 |
| `IAM_40004` | 404 | 部门不存在 | 部门 ID 无匹配 |
| `IAM_40005` | 404 | 用户组不存在 | 用户组 ID 无匹配 |
| `IAM_40006` | 404 | 租户不存在 | 租户 ID/编码无匹配 |
| `IAM_40007` | 404 | 登录日志不存在 | 日志 ID 无匹配（或已归档） |
| `IAM_50001` | 500 | 口令加密失败 | BCrypt 计算异常 |
| `IAM_50002` | 502 | 短信/邮件通道不可用 | 发送初始密码依赖的通知服务故障 |
| `IAM_50003` | 500 | 权限缓存刷新失败 | Redis 或 Caffeine 本地缓存异常 |

**DESIGN（建模）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `DESIGN_10001` | 400 | 流程模型 JSON 结构非法 | 缺少 `key/name/nodes/edges` 或类型错误 |
| `DESIGN_10002` | 400 | 节点类型不支持 | `type` 不在基线 5.1 的 `NodeType` 全集内 |
| `DESIGN_10003` | 422 | 流程模型校验未通过 | 存在 ERROR 级校验问题（返回 `errors[]`） |
| `DESIGN_10004` | 400 | 流程 key 格式非法 | 非 `^[a-z][a-z0-9_]{2,63}$` |
| `DESIGN_10005` | 400 | 连线缺少条件或默认流 | 排他网关出口多条无 `condition.isDefault=true` |
| `DESIGN_10006` | 400 | BPMN XML 解析失败 | XML 语法错误或不支持的 BPMN 元素 |
| `DESIGN_10007` | 400 | 表单引用不存在 | 节点 `formRef.formKey` 未找到或版本未发布 |
| `DESIGN_10008` | 400 | 候选人策略参数非法 | `assigneeStrategy` 与 `assigneeValue` 不匹配 |
| `DESIGN_10009` | 400 | 表达式语法错误 | Aviator 解析失败（网关条件/变量映射） |
| `DESIGN_10010` | 400 | 模型哈希不一致 | 导入时 `modelHash` 与内容不符 |
| `DESIGN_10011` | 400 | 变量定义非法 | `settings.vars` 重复名或类型不支持 |
| `DESIGN_10012` | 400 | 分类层级或排序非法 | 分类层级 > 5、`parentId` 为自身子孙 |
| `DESIGN_20001` | 403 | 无流程分类管理权限 | 缺少 `design:category:*` |
| `DESIGN_20002` | 403 | 无流程定义设计权限 | 缺少 `design:process-def:*` |
| `DESIGN_20003` | 403 | 无发布/下线权限 | 缺少 `design:process-version:publish/offline` |
| `DESIGN_20004` | 403 | 无模板管理权限 | 缺少 `design:template:*` |
| `DESIGN_20005` | 403 | 非流程负责人不可操作 | 定义设置了 `maintainerIds`，当前用户不在其中 |
| `DESIGN_20006` | 403 | 无导入导出权限 | 缺少 `design:process-version:import` |
| `DESIGN_30001` | 409 | 流程 key 已存在 | 同租户 `process_key` 唯一冲突 |
| `DESIGN_30002` | 409 | 版本已发布不可修改 | 对 `PUBLISHED` 版本保存草稿/改模型 |
| `DESIGN_30003` | 409 | 无 DRAFT 版本可发布 | 发布时指定版本状态非 `DRAFT` |
| `DESIGN_30004` | 409 | 版本已下线不可发布 | `OFFLINE` 版本重复发布（需先复制） |
| `DESIGN_30005` | 409 | 运行中实例存在，禁止删除 | 删除定义/版本时存在 `RUNNING/SUSPENDED` 实例 |
| `DESIGN_30006` | 409 | 存在未完成草稿 | 重复创建草稿版本 |
| `DESIGN_30007` | 409 | 回滚目标版本非法 | 回滚目标非历史 `PUBLISHED` 版本 |
| `DESIGN_30008` | 409 | 存在运行实例，禁止结构性回滚 | 新旧版本节点集合差异过大且有待办 |
| `DESIGN_30009` | 409 | 分类下存在流程定义 | 删除非空分类 |
| `DESIGN_30010` | 409 | 模板键已存在 | `template_key` 冲突 |
| `DESIGN_40001` | 404 | 流程定义不存在 | `defId`/`processKey` 无匹配 |
| `DESIGN_40002` | 404 | 流程版本不存在 | `versionId` 或 `version` 无匹配 |
| `DESIGN_40003` | 404 | 流程分类不存在 | `categoryId` 无匹配 |
| `DESIGN_40004` | 404 | 模板不存在 | `template_key` 无匹配 |
| `DESIGN_40005` | 404 | 发布记录不存在 | 部署日志 ID 无匹配 |
| `DESIGN_50001` | 500 | 模型序列化失败 | JSON 序列化/压缩异常 |
| `DESIGN_50002` | 500 | 模型缓存刷新失败 | 发布后广播 Redis 缓存刷新失败 |
| `DESIGN_50003` | 500 | BPMN 转换失败 | 内部 JSON ↔ BPMN 映射器异常 |

**FORM（表单）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `FORM_10001` | 400 | 表单 schema 非法 | JSON Schema 结构不合法 |
| `FORM_10002` | 400 | 表单 key 格式非法 | 非 `^[a-z][a-z0-9_]{2,63}$` |
| `FORM_10003` | 400 | 表单字段 key 重复 | 同一 schema 内 `fieldKey` 冲突 |
| `FORM_10004` | 400 | 字段类型不支持 | `type` 不在 `input/textarea/number/date/...` 集内 |
| `FORM_10005` | 400 | 表单校验规则非法 | 正则/范围/表达式错误 |
| `FORM_10006` | 422 | 表单数据校验未通过 | 提交数据不满足 schema（返回字段级错误） |
| `FORM_10007` | 400 | 表单类型非法 | `formType` 不在 `CREATE/APPROVE/VIEW` |
| `FORM_10008` | 400 | 版本号非法 | `version <= 0` 或非数字 |
| `FORM_20001` | 403 | 无表单定义管理权限 | 缺少 `form:def:*` |
| `FORM_20002` | 403 | 无表单发布权限 | 缺少 `form:version:publish` |
| `FORM_30001` | 409 | 表单 key 已存在 | `form_key` 唯一冲突 |
| `FORM_30002` | 409 | 表单版本已发布不可修改 | 对 `PUBLISHED` 版本保存 |
| `FORM_30003` | 409 | 表单版本已被流程引用 | 删除被 `formRef` 引用的版本 |
| `FORM_30004` | 409 | 无草稿版本可发布 | 发布目标非 `DRAFT` |
| `FORM_30005` | 409 | 存在运行实例引用，禁止删除表单 | 表单被运行中流程版本引用 |
| `FORM_40001` | 404 | 表单定义不存在 | `formKey` 无匹配 |
| `FORM_40002` | 404 | 表单版本不存在 | `version` 无匹配，或 `LATEST` 无已发布版本 |
| `FORM_40003` | 404 | 表单 schema 不存在 | 版本存在但 `schema_json` 为空 |
| `FORM_50001` | 500 | schema 渲染元数据构建失败 | 字段字典/联动配置装载异常 |
| `FORM_50002` | 500 | 表单数据序列化失败 | 大字段序列化或附件引用异常 |

**ENGINE（执行内核）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `ENGINE_10001` | 400 | 启动参数缺失 | 未传 `processKey` 或 `versionId` |
| `ENGINE_10002` | 400 | 变量类型与定义不匹配 | 启动变量不满足 `settings.vars` 定义 |
| `ENGINE_10003` | 400 | businessKey 格式非法 | 长度 > 128 或含非法字符 |
| `ENGINE_10004` | 400 | 变量名非法或超长 | 名称非标识符，或值超过单变量上限 |
| `ENGINE_10005` | 400 | 大变量引用非法 | `valueRef` 不存在或已过期 |
| `ENGINE_10006` | 400 | 消息/信号名称缺失 | `messageName`/`signalName` 为空 |
| `ENGINE_10007` | 400 | 异步启动缺少回调地址 | 异步模式下未提供结果投递配置 |
| `ENGINE_10008` | 400 | 时间范围或过滤条件非法 | 列表查询条件冲突（如同时传 `completed=true` 与 `status=RUNNING`） |
| `ENGINE_10009` | 400 | 补偿节点未定义处理器 | 触发补偿但无 `COMPENSATION_HANDLER` |
| `ENGINE_20001` | 403 | 无实例启动权限 | 缺少 `engine:instance:start` |
| `ENGINE_20002` | 403 | 无实例管理权限 | 缺少 `engine:instance:*`（挂起/终止） |
| `ENGINE_20003` | 403 | 无变量写权限 | 缺少 `engine:variable:update` |
| `ENGINE_20004` | 403 | 无故障处理权限 | 缺少 `engine:incident:resolve` |
| `ENGINE_20005` | 403 | 非发起人不可终止实例 | 普通用户终止他人实例 |
| `ENGINE_30001` | 409 | 实例状态不允许该操作 | 对 `COMPLETED/TERMINATED` 实例挂起/终止 |
| `ENGINE_30002` | 409 | 实例已挂起 | 重复挂起 |
| `ENGINE_30003` | 409 | businessKey 重复启动 | 同 `processKey + businessKey` 已有活跃实例 |
| `ENGINE_30004` | 409 | 实例存在活跃令牌，禁止删除 | 删除运行中实例（须先终止） |
| `ENGINE_30005` | 409 | 并发推进冲突 | 同实例命令并发（分布式锁/乐观锁冲突），可重试 |
| `ENGINE_30006` | 409 | 无等待的事件订阅 | 投递 message/signal 但实例无匹配订阅 |
| `ENGINE_30007` | 409 | 信号已被消费 | 信号 `once` 语义下重复广播 |
| `ENGINE_30008` | 409 | 故障已处理 | 对 `RESOLVED/CANCELED` 的 incident 重试 |
| `ENGINE_30009` | 409 | 节点不处于可跳过状态 | 跳过非 `FAILED` 节点实例 |
| `ENGINE_30010` | 409 | 补偿不可执行 | 前序节点无补偿处理器或实例已结束 |
| `ENGINE_30011` | 409 | 定时器已触发或取消 | 删除非 `PENDING` 定时器 |
| `ENGINE_30012` | 409 | 引擎命令队列已满 | 背压保护，客户端按 `Retry-After` 重试 |
| `ENGINE_40001` | 404 | 流程实例不存在或已结束 | `instanceId` 无匹配或已归档 |
| `ENGINE_40002` | 404 | 节点实例不存在 | `nodeInstanceId` 无匹配 |
| `ENGINE_40003` | 404 | 令牌不存在 | `tokenId` 无匹配（已消费/归档） |
| `ENGINE_40004` | 404 | 故障记录不存在 | `incidentId` 无匹配 |
| `ENGINE_40005` | 404 | 定时器不存在 | `timerId` 无匹配 |
| `ENGINE_40006` | 404 | 流程版本未发布 | 按 `processKey` 启动但无 `PUBLISHED` 版本 |
| `ENGINE_40007` | 404 | 大变量对象不存在 | MinIO 对象缺失或已过期清理 |
| `ENGINE_40008` | 404 | 轨迹数据不存在 | 实例无轨迹（未开启轨迹或尚未同步） |
| `ENGINE_50001` | 500 | 令牌推进失败 | 节点行为异常，已生成 incident |
| `ENGINE_50002` | 500 | 表达式求值异常 | Aviator 运行时错误（空指针/类型不符） |
| `ENGINE_50003` | 502 | 连接器执行失败 | HTTP/SQL/MQ 节点执行异常（透传 INTEG 错误码） |
| `ENGINE_50004` | 503 | 引擎实例级锁获取失败 | 同实例并发，退避重试 |
| `ENGINE_50005` | 500 | 变量序列化失败 | 大对象外置或反序列化异常 |
| `ENGINE_50006` | 504 | 实例推进超时 | 同步启动超过 `engine.sync-start-timeout`（默认 5s） |
| `ENGINE_50007` | 500 | 补偿执行失败 | 补偿处理器抛异常，生成 incident |

**TASK（人任务）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `TASK_10001` | 400 | 办理意见超长 | `opinion` > 1000 字符 |
| `TASK_10002` | 400 | 批量条数超限 | 批量审批 > 100 条 |
| `TASK_10003` | 400 | 附件格式或大小非法 | 非白名单类型或单文件 > 50 MB |
| `TASK_10004` | 400 | 驳回目标节点非法 | 目标节点非历史可达节点或缺失 |
| `TASK_10005` | 400 | 加签人非法 | 目标用户不存在/已停用/为空 |
| `TASK_10006` | 400 | 委派人非法 | 委派给自己或已停用用户 |
| `TASK_10007` | 422 | 表单数据校验未通过 | 提交数据不满足表单 schema |
| `TASK_10008` | 400 | 转办/委托原因缺失 | 必填说明未提供 |
| `TASK_10009` | 400 | 抄送人列表非法 | 抄送人为空或超过 50 人 |
| `TASK_10010` | 400 | 催办频率超限 | 同一任务 24h 内催办 > 3 次 |
| `TASK_20001` | 403 | 无待办查看权限 | 缺少 `task:todo:list` |
| `TASK_20002` | 403 | 非任务办理人 | 非 `assignee` 且非候选人执行动作 |
| `TASK_20003` | 403 | 无任务管理权限 | 缺少 `task:task:manage`（管理视角列表） |
| `TASK_20004` | 403 | 无批量审批权限 | 缺少 `task:task:batch-approve` |
| `TASK_20005` | 403 | 无催办权限 | 缺少 `task:task:urge` |
| `TASK_20006` | 403 | 跨部门/跨租户任务不可见 | 数据权限限制 |
| `TASK_20007` | 403 | 非委托人或被委托人不可确认 | 委托确认权限校验失败 |
| `TASK_30001` | 409 | 任务已被他人认领 | `assignee` 已被占用 |
| `TASK_30002` | 409 | 任务状态不允许该操作 | 对 `COMPLETED/CANCELED` 任务操作 |
| `TASK_30003` | 409 | 任务不存在候选人 | 候选人为空且 `emptyStrategy` 未兜底 |
| `TASK_30004` | 409 | 重复认领/重复完成 | 幂等冲突或并发完成 |
| `TASK_30005` | 409 | 会签未全部完成 | `ALL` 模式下部分办理人未办结即推进 |
| `TASK_30006` | 409 | 或签已由他人完成 | `ANY` 模式下他人已办结 |
| `TASK_30007` | 409 | 撤回条件不满足 | 下游已办结或流程设置不允许撤回 |
| `TASK_30008` | 409 | 委托确认状态不允许 | 被委托人未完成即确认 |
| `TASK_30009` | 409 | 加签链已达到上限 | 加签层级 > 5 |
| `TASK_30010` | 409 | 任务已超时 | 超过 `due_time` 且策略禁止办理 |
| `TASK_30011` | 409 | 批量审批部分失败 | 全量事务回滚策略下存在失败项 |
| `TASK_30012` | 409 | 候选人数量超过单任务上限 | 展开后候选用户数 > 5000，须改用角色/部门/用户组策略或拆分节点（见 03 文档 4.3.6） |
| `TASK_40001` | 404 | 任务不存在 | `taskId` 无匹配 |
| `TASK_40002` | 404 | 候选人预览不可用 | 节点未配置候选人策略 |
| `TASK_40003` | 404 | 任务操作记录不存在 | 操作日志 ID 无匹配 |
| `TASK_40004` | 404 | 抄送记录不存在 | 抄送 ID 无匹配 |
| `TASK_50001` | 500 | 任务办理时引擎推进失败 | 下游节点异常，任务已回滚为待办 |
| `TASK_50002` | 502 | 候选人解析依赖不可用 | 调用 IAM 解析角色/用户组失败（降级缓存） |
| `TASK_50003` | 500 | 表单数据落库失败 | `form_data_json` 过大或序列化异常 |

**SCHED（调度）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `SCHED_10001` | 400 | cron 表达式非法 | Quartz/Spring cron 解析失败 |
| `SCHED_10002` | 400 | 作业名称或编码格式非法 | 非 `^[a-zA-Z][a-zA-Z0-9_-]{2,63}$` |
| `SCHED_10003` | 400 | 固定频率参数非法 | 同时配置 `cronExpr` 与 `fixedRate`，或 `fixedRate < 10s` |
| `SCHED_10004` | 400 | misfire 策略非法 | 不在 `SKIP/FIRE_ONCE/CATCH_UP` |
| `SCHED_10005` | 400 | 超时时间非法 | `timeoutSec <= 0` 或 > 86400 |
| `SCHED_10006` | 400 | 目标类型与引用不匹配 | `targetType=PROCESS` 但 `targetRef` 非流程 key |
| `SCHED_10007` | 400 | 作业参数 JSON 非法 | `paramsJson` 解析失败或含非法变量名 |
| `SCHED_10008` | 400 | 补跑时间范围非法 | `catchUpRanges` 重叠或跨度 > 31 天 |
| `SCHED_10009` | 400 | 并发策略非法 | 不在 `ALLOW/FORBID/REPLACE` |
| `SCHED_20001` | 403 | 无作业管理权限 | 缺少 `sched:job:*` |
| `SCHED_20002` | 403 | 无作业执行权限 | 缺少 `sched:job:trigger` |
| `SCHED_20003` | 403 | 无执行记录查看权限 | 缺少 `sched:execution:list` |
| `SCHED_30001` | 409 | 作业已暂停 | 对 `PAUSED` 作业执行暂停 |
| `SCHED_30002` | 409 | 作业已启用 | 对 `RUNNING` 作业执行恢复 |
| `SCHED_30003` | 409 | 作业编码已存在 | `job_code` 唯一冲突 |
| `SCHED_30004` | 409 | 作业正在执行中 | `concurrencyPolicy=FORBID` 且上一实例未结束 |
| `SCHED_30005` | 409 | 作业被其他节点抢占 | 分布式抢占锁未获取到 |
| `SCHED_30006` | 409 | 手动补跑与已有执行冲突 | 时间段内已有 `SUCCESS` 执行记录 |
| `SCHED_30007` | 409 | 执行记录不可重试 | 记录状态非 `FAILED/TIMEOUT` |
| `SCHED_40001` | 404 | 调度作业不存在 | `jobId`/`jobCode` 无匹配 |
| `SCHED_40002` | 404 | 执行记录不存在 | `executionId` 无匹配 |
| `SCHED_40003` | 404 | 作业日志不存在 | 日志 ID 无匹配或已归档 |
| `SCHED_50001` | 503 | 调度内核不可用 | Redis 时间轮/ZSet 异常 |
| `SCHED_50002` | 500 | 抢占锁释放失败 | 锁 owner 不匹配（由兜底对账修复） |
| `SCHED_50003` | 502 | 作业目标调用失败 | 触发流程实例或连接器失败（透传下游错误码） |

**INTEG（集成）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `INTEG_10001` | 400 | 连接器编码或名称非法 | 编码不满足 `^[a-z][a-z0-9_]{2,63}$` |
| `INTEG_10002` | 400 | 连接器类型不支持 | 不在 `HTTP/SQL/MQ/SCRIPT/GRPC/BEAN` |
| `INTEG_10003` | 400 | 操作编码重复或非法 | 同连接器下 `operationCode` 冲突 |
| `INTEG_10004` | 400 | 数据源连接串非法 | JDBC URL 协议与 `dsType` 不匹配 |
| `INTEG_10005` | 400 | 凭证类型非法 | 不在 `BASIC/BEARER/AK_SK/OAUTH2` |
| `INTEG_10006` | 400 | 认证方式与凭证类型不匹配 | HTTP 操作用 Bearer 但未绑定凭证 |
| `INTEG_10007` | 400 | configSchema 非法 | 非合法 JSON Schema |
| `INTEG_10008` | 400 | 超时或重试参数非法 | `timeoutMs` 不在 100–600000，重试次数 > 5 |
| `INTEG_10009` | 400 | 请求路径模板非法 | `pathTemplate` 含非法占位符或绝对 URL 越权 |
| `INTEG_10010` | 400 | 调试入参不满足 schema | 在线调试参数校验失败（返回字段级错误） |
| `INTEG_20001` | 403 | 无连接器管理权限 | 缺少 `integ:connector:*` |
| `INTEG_20002` | 403 | 无数据源管理权限 | 缺少 `integ:datasource:*` |
| `INTEG_20003` | 403 | 无凭证管理权限 | 缺少 `integ:credential:*` |
| `INTEG_20004` | 403 | 无连接器调试权限 | 缺少 `integ:connector:debug` |
| `INTEG_30001` | 409 | 连接器编码已存在 | `connector_code` 唯一冲突 |
| `INTEG_30002` | 409 | 连接器已禁用 | 对禁用连接器执行调试/被节点引用 |
| `INTEG_30003` | 409 | 连接器已被流程节点引用 | 删除被引用连接器/操作 |
| `INTEG_30004` | 409 | 内置连接器不可删除 | `builtin=1` |
| `INTEG_30005` | 409 | 连接器类型不匹配操作 | 对 `SQL` 连接器创建 HTTP 操作 |
| `INTEG_30006` | 409 | 数据源连通性测试失败 | 测试连接失败（含驱动/SQL 错误摘要） |
| `INTEG_30007` | 409 | 凭证已过期 | `expire_time` 已过 |
| `INTEG_30008` | 409 | 调试执行被限流 | 在线调试并发/频次超限 |
| `INTEG_40001` | 404 | 连接器不存在 | `connectorId/connectorCode` 无匹配 |
| `INTEG_40002` | 404 | 连接器操作不存在 | `operationId/operationCode` 无匹配 |
| `INTEG_40003` | 404 | 数据源不存在 | `dsId/dsCode` 无匹配 |
| `INTEG_40004` | 404 | 凭证不存在 | `credId` 无匹配 |
| `INTEG_40005` | 404 | 调用日志不存在 | 日志 ID 无匹配（或已归档至 ClickHouse） |
| `INTEG_50001` | 500 | 连接器执行器不可用 | SDK 初始化失败或 Bean 未注册 |
| `INTEG_50002` | 500 | 凭证解密失败 | AES-GCM 密钥不匹配/密文损坏 |
| `INTEG_50003` | 503 | 数据源连接池耗尽 | 连接等待超时 |
| `INTEG_50004` | 403 | 目标地址不在 SSRF 白名单 | HTTP 连接器目标域名未在 `int_connector` 配置 |
| `INTEG_50005` | 504 | 连接器调用超时 | 超过 `timeoutMs` |
| `INTEG_50006` | 502 | 连接器调用返回异常状态 | 上游 HTTP 4xx/5xx、SQL 执行错误 |
| `INTEG_50007` | 500 | 出参转换失败 | `transform_json` 表达式错误 |

**NOTIFY（通知）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `NOTIFY_10001` | 400 | 模板编码格式非法 | 非 `^[a-z][a-z0-9_]{2,63}$` |
| `NOTIFY_10002` | 400 | 模板内容为空 | `titleTpl`/`contentTpl` 均空 |
| `NOTIFY_10003` | 400 | 模板变量缺失 | 渲染时缺少 `vars_json` 声明的变量 |
| `NOTIFY_10004` | 400 | 渠道配置非法 | `config_cipher` 结构不匹配渠道类型 |
| `NOTIFY_10005` | 400 | 接收人表达式非法 | `receiverExpr` 非合法 Aviator 表达式 |
| `NOTIFY_10006` | 400 | 订阅规则非法 | `eventType` 与 `channel` 组合不合法 |
| `NOTIFY_10007` | 400 | Webhook 地址格式非法 | 非 http(s) URL 或命中 SSRF 白名单校验 |
| `NOTIFY_10008` | 400 | 渠道类型不支持 | 不在 `INBOX/EMAIL/SMS/WEBHOOK/DINGTALK/WECOM/FEISHU` |
| `NOTIFY_10009` | 400 | 模板标题或内容超长 | 标题 > 200、短信内容 > 500 字 |
| `NOTIFY_10010` | 400 | 限流速率配置非法 | `rateLimit` 非正整数或 > 100000/分钟 |
| `NOTIFY_20001` | 403 | 无通知配置权限 | 缺少 `notify:template:*` / `notify:channel:*` |
| `NOTIFY_20002` | 403 | 无消息查看权限 | 缺少 `notify:message:list` |
| `NOTIFY_30001` | 409 | 模板已发布不可修改 | 对已发布模板直接改内容（须新版本） |
| `NOTIFY_30002` | 409 | 渠道已禁用 | 向禁用渠道发送 |
| `NOTIFY_30003` | 409 | 模板编码已存在 | `template_code` 唯一冲突 |
| `NOTIFY_30004` | 409 | 渠道编码已存在 | 同 `channel` 已有启用配置 |
| `NOTIFY_30005` | 409 | 消息状态不可重发 | 非 `FAILED` 消息重发 |
| `NOTIFY_30006` | 429 | 发送触发限流 | 渠道 `rate_limit` 命中 |
| `NOTIFY_30007` | 409 | 订阅规则已存在 | 同 `event_type + biz_scope + channel` 重复 |
| `NOTIFY_40001` | 404 | 模板不存在 | `templateCode` 无匹配 |
| `NOTIFY_40002` | 404 | 渠道配置不存在 | `channelCode`/`channel` 无匹配 |
| `NOTIFY_40003` | 404 | 订阅规则不存在 | 订阅 ID 无匹配 |
| `NOTIFY_40004` | 404 | 消息不存在 | `msgNo`/消息 ID 无匹配 |
| `NOTIFY_50001` | 502 | 渠道供应商不可达 | 钉钉/企微/飞书/短信网关网络失败 |
| `NOTIFY_50002` | 502 | 邮件发送失败 | SMTP 认证/连接失败 |
| `NOTIFY_50003` | 502 | 短信发送失败 | 供应商返回失败码 |
| `NOTIFY_50004` | 502 | Webhook 回调失败 | 对端非 2xx 或超时 |
| `NOTIFY_50005` | 500 | 模板渲染失败 | 表达式求值异常、变量类型不符 |

**TRACE（轨迹与审计）**

| code | HTTP | 含义 | 触发场景 |
| --- | --- | --- | --- |
| `TRACE_10001` | 400 | 时间范围非法 | 缺失时间范围或 `start > end` |
| `TRACE_10002` | 400 | 时间跨度过大 | 单次查询跨度 > 31 天（需分段） |
| `TRACE_10003` | 400 | 事件类型非法 | `eventType` 不在轨迹事件枚举内 |
| `TRACE_10004` | 400 | 统计维度非法 | `groupBy` 不在 `processKey/nodeType/date/user/dept` |
| `TRACE_10005` | 400 | 告警规则表达式非法 | 表达式解析失败 |
| `TRACE_10006` | 400 | 告警阈值或周期非法 | `threshold <= 0`、周期不在允许集合 |
| `TRACE_10007` | 400 | 告警通知渠道未配置 | 规则启用但未绑定渠道/模板 |
| `TRACE_20001` | 403 | 无审计日志查询权限 | 缺少 `trace:oper-log:list` |
| `TRACE_20002` | 403 | 无跨租户查询权限 | 非平台管理员请求 `tenantId` 维度 |
| `TRACE_20003` | 403 | 无告警规则管理权限 | 缺少 `trace:alert-rule:*` |
| `TRACE_30001` | 409 | 告警规则已处于触发态 | 重复确认/重复静默 |
| `TRACE_30002` | 409 | 轨迹数据尚未就绪 | 事件入 ClickHouse 延迟（< 5s 正常） |
| `TRACE_30003` | 409 | 告警规则正在被其他请求修改 | 并发编辑冲突（`If-Match`） |
| `TRACE_40001` | 404 | 审计日志不存在 | 日志 ID 无匹配或已过保留期 |
| `TRACE_40002` | 404 | 告警规则不存在 | 规则 ID 无匹配 |
| `TRACE_40003` | 404 | 统计数据不存在 | 指定日期无物化统计 |
| `TRACE_50001` | 500 | ClickHouse 查询失败 | SQL 异常/集群不可用 |
| `TRACE_50002` | 504 | 查询超时 | 超过 `trace.query-timeout`（默认 10s） |
| `TRACE_50003` | 404 | WebSocket 会话不存在 | 向已断开会话推送 |
| `TRACE_50004` | 429 | SSE 连接数超限 | 单用户连接数 > 5 |
| `TRACE_50005` | 503 | 推送队列积压 | 客户端消费慢，服务端丢弃低优先级事件 |

> **统计（按上表逐行计）**：错误码共 **339** 条 —— `COMMON` 39、`AUTH` 18、`IAM` 41、`DESIGN` 36、`FORM` 20、`ENGINE` 41、`TASK` 36、`SCHED` 25、`INTEG` 34、`NOTIFY` 28、`TRACE` 21。其中 `1xxxx` 参数/校验 103 条、`2xxxx` 权限/租户 57 条、`3xxxx` 状态冲突 85 条、`4xxxx` 资源不存在 46 条（含 `COMMON_40001`~`COMMON_40003`）、`5xxxx` 系统/依赖故障 48 条；个别条目按业务语义映射为 `403`（`INTEG_50004` SSRF 拦截）或 `404`（`TRACE_50003` 会话不存在），HTTP 列以该行实际标注为准。新增错误码必须遵循 1.5 数字段规则并在本表登记。

### 1.6 分页规范

**入参（Query）**

| 参数 | 类型 | 必填 | 默认 | 约束 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `pageNum` | Integer | 否 | 1 | ≥ 1 | 页码，从 1 开始 |
| `pageSize` | Integer | 否 | 20 | 1–200 | 每页条数，> 200 返回 `COMMON_10003` |
| `orderBy` | String | 否 | 各接口默认 | 白名单字段 | 排序字段（下划线命名），非法返回 `COMMON_10004` |
| `orderDirection` | Enum | 否 | `DESC` | `ASC` / `DESC` | 排序方向 |
| `lastId` | String | 否 | — | 雪花 ID 字符串 | 游标分页锚点，与 `pageNum` 互斥（同时传以 `lastId` 优先） |
| `countTotal` | Boolean | 否 | `true` | — | 是否返回 `total`；`false` 时 `total=null` 以提速 |
| `searchAfter` | Boolean | 否 | `false` | — | 强制游标模式，由服务端在深分页时自动置为 `true` |

**出参（`data`）**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `pageNum` | Integer | 当前页码；游标模式回显请求值或 `0` |
| `pageSize` | Integer | 每页条数 |
| `total` | Long | 总条数（字符串序列化）；`countTotal=false` 时为 `null`；游标模式下为 `null` |
| `records` | Array | 数据列表 |
| `hasNext` | Boolean | 是否还有下一页（游标模式必返回） |
| `nextLastId` | String | 下一页游标（取 `records` 末条 `id`），`hasNext=false` 时为 `null` |
| `searchAfter` | Boolean | 本次是否走了游标模式，前端据此切换「加载更多」交互 |

**深分页游标模式**

- 触发条件：`pageNum * pageSize > 10000`（基线 6.7），或调用方显式传 `searchAfter=true` 或 `lastId`。
- 排序要求：必须存在稳定唯一排序键，服务端强制以 `id DESC` 作为最终 tie-breaker。
- 前端实现：首页正常分页；命中阈值后切换为「加载更多」模式，携带上一页返回的 `nextLastId`。
- 适用接口（标记 `CURSOR`）：实例列表、节点实例列表、任务列表、我的待办/已办、执行轨迹查询、调用日志、审计日志、消息列表。
- 禁止：游标模式下忽略 `pageNum` 跳页语义（传 `pageNum > 1` 且带 `lastId` 返回 `COMMON_10003`）。
- 不可变要求：游标依赖 `id` 单调递减，查询期间不得跨「历史归档切换」边界（实例归档接口不参与游标分页）。

**示例**

```json
{
  "code": "0",
  "message": "ok",
  "data": {
    "pageNum": 0,
    "pageSize": 50,
    "total": null,
    "records": [
      { "id": "1836427591029384756", "status": "RUNNING" },
      { "id": "1836427591029384755", "status": "COMPLETED" }
    ],
    "hasNext": true,
    "nextLastId": "1836427591029384755",
    "searchAfter": true
  },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

### 1.7 时间与数值序列化约定

| 项 | 约定 | 示例 |
| --- | --- | --- |
| 日期时间 | `LocalDateTime` 统一格式 `yyyy-MM-dd HH:mm:ss.SSS`（DATETIME(3)，UTC+8 入库），**不使用** ISO8601 带 `T`/时区偏移的写法 | `"2025-05-20 09:31:02.114"` |
| 纯日期 | `LocalDate` 格式 `yyyy-MM-dd` | `"2025-05-20"` |
| 纯时间 | `LocalTime` 格式 `HH:mm:ss` | `"09:31:02"` |
| 时间戳 | 仅 MQ 消息信封与 SSE 事件使用毫秒时间戳（Long） | `1747704662114` |
| 时区 | 服务端返回均为 UTC+8；前端按浏览器时区转换展示；跨时区调用方自行换算 | — |
| 空时间 | `null`（不返回空字符串） | `"endTime": null` |
| **Long** | 所有 Long 型（含雪花 ID、`total`、字节数、耗时毫秒）序列化为**字符串**，防 JS 精度丢失（基线 6.2） | `"1836427591029384756"` |
| Integer / Short | 保持 JSON Number | `"version": 7`、`"retryCount": 2` |
| BigDecimal / Double | 金额类使用 String（如 `"123.45"`）；比率类使用 Double（`0.8532`，保留 4 位） | — |
| Boolean | 严格 JSON Boolean，**不使用** `0/1` 或 `"true"` | `"suspended": true` |
| Array | 空集合返回 `[]`，不返回 `null` | `"candidateUsers": []` |
| Object | 空对象返回 `{}` | — |
| 枚举 | 返回 `UPPER_SNAKE` 字符串；需多语言展示时同时返回 `*Name` 字段（按 `Accept-Language` 渲染） | `"status": "RUNNING"`, `"statusName": "运行中"` |
| 大文本 | `model_json` / `schema_json` 等以**对象**返回；仅在 `?raw=true` 时返回字符串原文本 | — |
| 脱敏 | 手机号 `138****8888`、邮箱 `a***@b.com`、密码/凭证**永不返回** | — |
| 字符串长度 | 服务端按字符（非字节）校验；存储 utf8mb4 支持 emoji | — |

Jackson 全局配置（各服务 `JacksonConfig`）：`WRITE_DATES_AS_TIMESTAMPS=false`、`LocalDateTime` 使用上述 formatter、`Long`/`long` 使用 `ToStringSerializer`、`FAIL_ON_UNKNOWN_PROPERTIES=false`（向后兼容，见第 7 节）。

**枚举取值速查（跨服务公共部分）**

| 枚举 | 取值 |
| --- | --- |
| `ProcessStatus` | `RUNNING` / `SUSPENDED` / `COMPLETED` / `TERMINATED` |
| `VersionStatus` | `DRAFT` / `PUBLISHED` / `OFFLINE` |
| `TokenStatus` | `ACTIVE` / `WAITING` / `CONSUMED` / `DEAD` |
| `NodeInstanceStatus` | `ACTIVE` / `COMPLETED` / `FAILED` / `SKIPPED` / `COMPENSATED` |
| `TaskStatus` | `CREATED` / `CLAIMED` / `COMPLETED` / `DELEGATED` / `TRANSFERRED` / `CANCELED` / `TIMEOUT` |
| `TaskResult` | `APPROVE` / `REJECT` |
| `MultiMode` | `ALL` / `ANY` / `PERCENT` |
| `IncidentStatus` | `OPEN` / `RESOLVED` / `CANCELED` |
| `IncidentType` | `RETRY_EXHAUSTED` / `EXPRESSION_ERROR` / `CONNECTOR_ERROR` / `UNKNOWN` |
| `NodeType` | `START_EVENT` / `END_EVENT` / `TERMINATE_END_EVENT` / `TIMER_CATCH_EVENT` / `MESSAGE_CATCH_EVENT` / `SIGNAL_CATCH_EVENT` / `ERROR_CATCH_EVENT` / `TIMER_BOUNDARY_EVENT` / `ERROR_BOUNDARY_EVENT` / `MESSAGE_BOUNDARY_EVENT` / `SERVICE_TASK` / `HTTP_TASK` / `SQL_TASK` / `MQ_PRODUCE_TASK` / `SCRIPT_TASK` / `GRPC_TASK` / `DELAY_TASK` / `LOOP_TASK` / `SUB_PROCESS` / `CALL_ACTIVITY` / `COMPENSATION_HANDLER` / `USER_TASK` / `EXCLUSIVE_GATEWAY` / `PARALLEL_GATEWAY` / `INCLUSIVE_GATEWAY` / `EVENT_GATEWAY` |
| `AssigneeStrategy` | `USER` / `ROLE` / `DEPT` / `USER_GROUP` / `EXPRESSION` / `INITIATOR` / `INITIATOR_LEADER` / `FORM_FIELD` / `PREV_APPROVER` |
| `DataScope` | `ALL` / `DEPT_AND_CHILD` / `DEPT` / `SELF` / `CUSTOM` |
| `JobStatus` | `RUNNING` / `PAUSED` |
| `ExecutionStatus` | `SUCCESS` / `FAILED` / `RUNNING` / `TIMEOUT` / `SKIPPED` |
| `ConnectorType` | `HTTP` / `SQL` / `MQ` / `SCRIPT` / `GRPC` / `BEAN` |
| `Channel` | `INBOX` / `EMAIL` / `SMS` / `WEBHOOK` / `DINGTALK` / `WECOM` / `FEISHU` |
| `MessageStatus` | `PENDING` / `SENT` / `FAILED` / `READ` |

### 1.8 幂等与并发控制

**幂等要求分级**

| 级别 | 说明 | 要求携带 `Idempotency-Key` | 服务端实现 |
| --- | --- | --- | --- |
| `REQUIRED` | 创建型资源、引擎推进型、外部业务回调 | 是 | 缺失返回 `COMMON_10001`；命中未完成请求返回 `COMMON_30005`；命中已完成请求**重放首次响应**（HTTP 200 + 原 `data`） |
| `RECOMMENDED` | 可重复执行但代价较高的动作（发布、下线、重试） | 否 | 若携带则去重；否则依赖状态机与乐观锁 |
| `NONE` | 纯查询、天然幂等的 PUT/DELETE | 否 | 忽略该头 |

**幂等键落库与判定**

1. 键为 `{tenantId}:{HTTP方法}:{规范化路径}:{Idempotency-Key}`，写入 `rt_idempotent.biz_key`（唯一索引）。
2. `biz_type` 取业务类型（如 `START_PROCESS`、`COMPLETE_TASK`、`PUBLISH_VERSION`）；`result_json` 存首次响应的 `data` 快照；`expire_time` 默认 24h。
3. 并发同键：唯一索引冲突者返回 `COMMON_30005`（HTTP 409），建议 1s 后按同键重试。
4. 业务特殊幂等（优先级高于通用键）：
   - **启动实例**：`processKey + businessKey + tenantId` 在活跃实例内唯一，重复启动返回 `ENGINE_30003`，响应 `data` 内附已有 `instanceId`（便于调用方复用）。
   - **任务完成**：任务状态机 + `@Version`，重复完成返回 `TASK_30004`。
   - **MQ 消费**：幂等键 = `{instanceId}:{nodeInstanceId}:{attempt}`（基线 6.10），落 `rt_idempotent` 或消费位点表。
5. 幂等命中重放响应时，响应头回写 `X-Idempotent-Replay: true` 便于排障。

**并发控制（乐观锁 / 悲观锁）**

| 场景 | 机制 | 客户端要求 | 冲突表现 |
| --- | --- | --- | --- |
| 配置类资源编辑（流程定义、表单、连接器、渠道、作业） | 乐观锁 `version` + `If-Match` | 更新请求必须带 `If-Match: "当前version"` | `COMMON_30003`（HTTP 412），提示刷新 |
| 实例级状态推进（引擎命令） | Redis 分布式锁 `engine:inst:{instanceId}` + DB 行锁 + `@Version` | 无需传版本（服务端内部） | `ENGINE_30005` / `ENGINE_50004`，可重试 |
| 任务办理 | `rt_task.version` 乐观锁 + 状态机 CAS | 完成/认领可带 `If-Match` | `TASK_30001` / `TASK_30004` |
| 调度作业 | `sch_lock` 抢占 + `concurrencyPolicy` | 无需传版本 | `SCHED_30004` / `SCHED_30005` |
| 批量操作 | 单事务 + 逐条结果 | 幂等键建议携带 | `TASK_30011`（部分失败） |

**`If-Match` 使用规范**

```http
PUT /api/design/v1/process-defs/1836427591029384700
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
X-Tenant-Id: 1001
If-Match: "7"
Content-Type: application/json; charset=UTF-8
```

- 缺失 `If-Match` 时，允许的模式由接口标注：`If-Match: optional`（按最新覆盖）或 `If-Match: required`（必须传，缺失返回 `COMMON_10001`）。
- 详情类 `GET` 响应必须回写 `ETag` 与 `version` 字段，前端保存时把 `ETag` 原样放入 `If-Match`。
- 弱校验值 `W/"7"` 等价于 `"7"`（本平台不实现强/弱语义差异）。

### 1.9 限流、熔断与 429 语义

**限流维度（基线 6.8：IP / 用户 / 租户三维）**

| 维度 | Redis Key | 默认阈值 | 适用 |
| --- | --- | --- | --- |
| IP | `rl:ip:{ip}` | 500 QPS | 全部对外接口 |
| 用户 | `rl:user:{tenantId}:{userId}` | 200 QPS | 登录后接口 |
| 租户 | `rl:tenant:{tenantId}` | 1000 QPS | 全部接口 |
| 接口资源 | `rl:res:{资源名}` | 见 1.2 路由表 | 高频/重查询接口 |
| 租户配额 | `quota:tenant:{tenantId}:{metric}` | 由 `sys_tenant` 扩展配置决定 | 实例数、并发任务数、外部调用量 |
| 并发 | Sentinel 线程数 | 核心接口 200 并发 | 引擎同步接口 |

**429 响应**

```json
{
  "code": "COMMON_30008",
  "message": "请求过于频繁，请稍后重试",
  "data": { "limit": 200, "remaining": 0, "resetAfterSeconds": 3, "resource": "gw.engine.start" },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

响应头（429 与正常响应均按需返回）：

| 头 | 说明 |
| --- | --- |
| `X-RateLimit-Limit` | 当前窗口配额上限 |
| `X-RateLimit-Remaining` | 剩余配额（不足 10% 时服务端提前预警） |
| `X-RateLimit-Reset` | 配额重置的 Unix 秒时间戳 |
| `X-RateLimit-Resource` | 命中的限流资源名（便于定位规则） |
| `Retry-After` | 建议重试秒数 = `Reset - now + jitter(0~1s)` |

**客户端重试约定**

1. 仅对 `429`、`503`、`504` 与幂等接口（`GET/PUT/DELETE` 或带 `Idempotency-Key` 的写接口）重试。
2. 重试策略：指数退避 + 抖动，最多 3 次（首次间隔 = `Retry-After`，其后 ×2，上限 30s）。
3. 不重试的错误：`COMMON_1xxxx`、`COMMON_2xxxx`、`COMMON_30001/30002/30003/30004/30009`、各领域 `3xxxx` 状态冲突（除 `ENGINE_30005` / `ENGINE_50004` 明确可重试）。
4. 熔断：Feign 与网关对下游服务按「慢调用比例 > 50% 且 RT > 2s」打开熔断，10s 后半开；降级返回 `COMMON_50007` 或接口声明的兜底数据（如候选人解析降级用缓存）。
5. 背压：引擎命令队列满（`ENGINE_30012`）时，调用方必须按 `Retry-After` 退避，禁止忙等重试。

---

## 2. 服务接口清单

### 2.1 process-iam（身份与权限，8081，库 `pe_iam`）

**服务职责**：统一管理租户、用户、部门、角色、权限菜单、用户组、登录认证、在线会话与登录日志，并为其他服务提供数据权限与候选人解析所需的身份数据。

**接口总览（47 个）**

| # | 方法 | 路径 | 名称 | 权限码 | 幂等要求 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | POST | `/api/iam/v1/auth/login` | 登录 | 匿名（白名单） | RECOMMENDED | 支持账密/验证码；返回双 Token |
| 2 | POST | `/api/iam/v1/auth/refresh` | 刷新令牌 | 匿名（需 Refresh Token） | NONE | 旋转 Refresh Token |
| 3 | POST | `/api/iam/v1/auth/logout` | 登出 | 登录态 | RECOMMENDED | 令牌进黑名单 |
| 4 | GET | `/api/iam/v1/auth/me` | 当前用户信息 | 登录态 | NONE | 含权限码、数据权限范围 |
| 5 | GET | `/api/iam/v1/auth/menus` | 当前用户菜单树 | 登录态 | NONE | 按角色裁剪的菜单路由树 |
| 6 | POST | `/api/iam/v1/auth/captcha` | 生成图形验证码 | 匿名（白名单） | NONE | 返回 base64 图 + `captchaKey` |
| 7 | GET | `/api/iam/v1/users` | 用户分页列表 | `iam:user:list` | NONE | 支持部门/状态/关键字过滤 |
| 8 | GET | `/api/iam/v1/users/{userId}` | 用户详情 | `iam:user:query` | NONE | 回写 `ETag` |
| 9 | POST | `/api/iam/v1/users` | 新增用户 | `iam:user:create` | REQUIRED | 初始密码可指定或自动生成 |
| 10 | PUT | `/api/iam/v1/users/{userId}` | 修改用户 | `iam:user:update` | NONE | `If-Match: required` |
| 11 | DELETE | `/api/iam/v1/users/{userId}` | 删除用户 | `iam:user:delete` | NONE | 逻辑删除 |
| 12 | PUT | `/api/iam/v1/users/{userId}/password` | 重置密码 | `iam:user:reset-password` | RECOMMENDED | 生成临时密码并可选通知 |
| 13 | PATCH | `/api/iam/v1/users/{userId}/status` | 启用/停用用户 | `iam:user:update` | RECOMMENDED | 停用即踢下线 |
| 14 | POST | `/api/iam/v1/users/import` | 批量导入用户 | `iam:user:import` | REQUIRED | multipart Excel/CSV |
| 15 | GET | `/api/iam/v1/users/export` | 导出用户 | `iam:user:export` | NONE | 返回文件流（MinIO 临时链接） |
| 16 | PUT | `/api/iam/v1/users/me/password` | 修改本人密码 | 登录态 | RECOMMENDED | 需校验原密码 |
| 17 | GET | `/api/iam/v1/roles` | 角色分页列表 | `iam:role:list` | NONE | — |
| 18 | GET | `/api/iam/v1/roles/{roleId}` | 角色详情 | `iam:role:query` | NONE | — |
| 19 | POST | `/api/iam/v1/roles` | 新增角色 | `iam:role:create` | REQUIRED | — |
| 20 | PUT | `/api/iam/v1/roles/{roleId}` | 修改角色 | `iam:role:update` | NONE | `If-Match: required` |
| 21 | DELETE | `/api/iam/v1/roles/{roleId}` | 删除角色 | `iam:role:delete` | NONE | 有用户引用则拒绝 |
| 22 | POST | `/api/iam/v1/role-permissions` | 角色授权 | `iam:role:grant` | RECOMMENDED | Body 内指定 `roleId` + 权限集合 |
| 23 | GET | `/api/iam/v1/role-permissions` | 角色权限查询 | `iam:role:query` | NONE | Query `roleId`，返回权限 ID 列表 |
| 24 | POST | `/api/iam/v1/user-roles` | 角色绑定用户 | `iam:role:grant` | RECOMMENDED | 增量/全量模式 |
| 25 | DELETE | `/api/iam/v1/user-roles` | 角色解绑用户 | `iam:role:grant` | NONE | Query `roleId` + `userIds` |
| 26 | GET | `/api/iam/v1/permissions` | 权限/菜单树 | `iam:permission:list` | NONE | 树形返回（管理端完整树） |
| 27 | POST | `/api/iam/v1/permissions` | 新增权限/菜单 | `iam:permission:create` | REQUIRED | — |
| 28 | PUT | `/api/iam/v1/permissions/{permissionId}` | 修改权限/菜单 | `iam:permission:update` | NONE | `If-Match: required` |
| 29 | DELETE | `/api/iam/v1/permissions/{permissionId}` | 删除权限/菜单 | `iam:permission:delete` | NONE | 有子节点/被引用则拒绝 |
| 30 | GET | `/api/iam/v1/depts` | 部门树 | `iam:dept:list` | NONE | 树形返回，含负责人、人数 |
| 31 | POST | `/api/iam/v1/depts` | 新增部门 | `iam:dept:create` | REQUIRED | 自动维护 `ancestors` |
| 32 | PUT | `/api/iam/v1/depts/{deptId}` | 修改部门 | `iam:dept:update` | NONE | `If-Match: required` |
| 33 | DELETE | `/api/iam/v1/depts/{deptId}` | 删除部门 | `iam:dept:delete` | NONE | 有子部门/用户则拒绝 |
| 34 | GET | `/api/iam/v1/user-groups` | 用户组分页列表 | `iam:user-group:list` | NONE | 静态/动态用户组 |
| 35 | POST | `/api/iam/v1/user-groups` | 新增用户组 | `iam:user-group:create` | REQUIRED | 动态组需 `ruleJson` |
| 36 | PUT | `/api/iam/v1/user-groups/{groupId}` | 修改用户组 | `iam:user-group:update` | NONE | `If-Match: required` |
| 37 | DELETE | `/api/iam/v1/user-groups/{groupId}` | 删除用户组 | `iam:user-group:delete` | NONE | — |
| 38 | GET | `/api/iam/v1/user-groups/{groupId}/members` | 用户组成员列表 | `iam:user-group:query` | NONE | 动态组返回实时解析结果 |
| 39 | POST | `/api/iam/v1/user-groups/{groupId}/members` | 维护用户组成员 | `iam:user-group:update` | RECOMMENDED | 支持增量/全量两种模式 |
| 40 | GET | `/api/iam/v1/tenants` | 租户分页列表 | `iam:tenant:list` | NONE | 平台级，`@IgnoreTenant` |
| 41 | GET | `/api/iam/v1/tenants/{tenantId}` | 租户详情 | `iam:tenant:query` | NONE | — |
| 42 | POST | `/api/iam/v1/tenants` | 新增租户 | `iam:tenant:create` | REQUIRED | 平台级 |
| 43 | PUT | `/api/iam/v1/tenants/{tenantId}` | 修改租户 | `iam:tenant:update` | NONE | `If-Match: required` |
| 44 | PATCH | `/api/iam/v1/tenants/{tenantId}/status` | 启用/停用租户 | `iam:tenant:update` | RECOMMENDED | 停用后该租户全部请求 `COMMON_20004` |
| 45 | GET | `/api/iam/v1/sessions/online` | 在线会话列表 | `iam:session:list` | NONE | 来自 Redis 会话表 |
| 46 | DELETE | `/api/iam/v1/sessions/{sessionId}` | 强制下线 | `iam:session:kickout` | RECOMMENDED | 令牌入黑名单 |
| 47 | GET | `/api/iam/v1/login-logs` | 登录日志查询 | `iam:login-log:list` | NONE | 支持时间/结果/关键字过滤 |

> 说明：`#6`（图形验证码）、`#14/#15`（用户导入导出）属于登录与用户管理下的辅助能力，不单独成章，但**计入接口总数**（共 47 个，与上表行数严格一致）。全文各服务的「接口总览」计数口径统一为：**表格行数 = 章节接口数**（多角色域按 `2.x.y` 合并描述的接口，其行号连续且逐条计入）。

#### 2.1.1 POST `/api/iam/v1/auth/login` 登录

**请求头**：`Content-Type: application/json`、`X-Request-Id`；`X-Tenant-Id` 不传（由 `tenantCode` 解析）。

**请求体**

| 字段 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- |
| `tenantCode` | String | 否 | 租户编码 | 缺失时按登录域名推断，仍为空返回 `AUTH_10004` |
| `username` | String | 是 | 用户名 | 4–32 位，`^[a-zA-Z][a-zA-Z0-9_.-]*$`（`IAM_10001`） |
| `password` | String | 是 | 密码（明文，仅 HTTPS 传输） | 8–64 位；连续失败 5 次锁定 10 分钟 |
| `grantType` | Enum | 否 | 授权类型 | `PASSWORD`（默认）/ `SMS_CODE` |
| `captchaKey` | String | 条件 | 图形验证码键 | 开启验证码策略时必填 |
| `captchaCode` | String | 条件 | 图形验证码 | 5 分钟内有效（`AUTH_10002`） |
| `smsCode` | String | 条件 | 短信验证码 | `grantType=SMS_CODE` 时必填 |
| `rememberMe` | Boolean | 否 | 记住我 | `true` 时 Refresh Token 有效期 7d，否则 12h |
| `deviceInfo` | Object | 否 | 设备信息 | `{ "os": "Windows 11", "browser": "Chrome 126", "clientVersion": "web/1.4.0" }` |

**响应体（`data`）**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `accessToken` | String | JWT（RS256），有效期 2h |
| `refreshToken` | String | JWT，有效期 7d |
| `tokenType` | String | 固定 `Bearer` |
| `expiresIn` | Long | Access Token 剩余秒数（7200） |
| `userId` / `username` / `nickname` | String | 用户标识与展示名 |
| `tenantId` / `tenantCode` | String | 租户标识 |
| `deptId` / `deptName` | String | 主部门 |
| `avatar` | String | 头像 URL |
| `dataScope` | Enum | `ALL/DEPT_AND_CHILD/DEPT/SELF/CUSTOM` |
| `permissions` | Array\<String\> | 权限码列表（`iam:user:list` 形式） |
| `roles` | Array\<String\> | 角色编码列表 |
| `passwordExpired` | Boolean | 是否需强制改密（`AUTH_20008` 场景返回 `true`） |
| `mustChangePassword` | Boolean | 首次登录/重置密码后为 `true` |

**主要错误码**：`AUTH_10001`、`AUTH_10002`、`AUTH_10004`、`AUTH_20001`、`AUTH_20006`、`AUTH_20007`、`AUTH_20009`、`AUTH_40001`、`COMMON_30008`。

**业务规则**：登录成功写入 `sys_login_log`（`result=SUCCESS`）与 Redis 在线会话；失败写 `FAILED` 并递增失败计数器；`tenantCode` 对应的租户 `status=DISABLED` 或超过 `expire_time` 时返回 `COMMON_20004`。同一账号在 `SINGLE` 单点策略下登录会顶掉旧会话（`AUTH_30001` 由被顶掉方感知）。

#### 2.1.2 POST `/api/iam/v1/auth/refresh` 刷新令牌

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `refreshToken` | String | 是 | 刷新令牌 | 结构损坏 `AUTH_10005`，过期 `AUTH_20003` |
| Body | `withUserInfo` | Boolean | 否 | 是否同时返回用户信息与权限 | 默认 `true` |

**响应体**：同 2.1.1 的 `accessToken/refreshToken/expiresIn/tokenType`（`refreshToken` 采用**旋转**策略，旧 Token 立即失效），`withUserInfo=true` 时附带 `permissions/roles/dataScope`。

**主要错误码**：`AUTH_10005`、`AUTH_20003`、`AUTH_20004`、`AUTH_20006`、`COMMON_20004`、`AUTH_50001`。

**业务规则**：刷新时校验 Redis 中会话是否仍存在（被强制下线则拒绝）；旋转后的旧 Refresh Token 写入黑名单至其自然过期，防止重放。

#### 2.1.3 POST `/api/iam/v1/auth/logout` 登出

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Authorization` | String | 是 | Access Token | 解析 `jti` 入黑名单 |
| Body | `refreshToken` | String | 否 | 一并失效刷新令牌 | — |
| Body | `allDevices` | Boolean | 否 | 是否退出全部设备 | 默认 `false` |

**响应体**：`{ "logout": true, "kickedSessions": 1 }`。
**主要错误码**：`COMMON_20001`、`AUTH_20002`、`AUTH_50001`。

#### 2.1.4 GET `/api/iam/v1/auth/me` 当前用户信息

**请求参数**：无（从 JWT 与租户上下文解析）。

**响应体字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `userId` / `username` / `nickname` / `realName` | String | 用户基本信息 |
| `email` / `mobile` | String | 脱敏展示 |
| `tenantId` / `tenantName` | String | 租户 |
| `deptId` / `deptName` / `deptPath` | String | 部门与部门路径（`研发中心/平台组`） |
| `roles` | Array\<Object\> | `[{ "roleId": "...", "roleCode": "process_admin", "roleName": "流程管理员", "dataScope": "ALL" }]` |
| `permissions` | Array\<String\> | 权限码全集（含来自用户组的权限） |
| `dataScope` | Enum | 生效数据权限（多角色取最大范围） |
| `customDeptIds` | Array\<String\> | `dataScope=CUSTOM` 时的自定义部门集合 |
| `groupIds` | Array\<String\> | 所属用户组 |
| `passwordExpireDays` | Integer | 密码剩余有效天数，`-1` 表示不过期 |

**主要错误码**：`COMMON_20001`、`IAM_40001`、`IAM_50003`。

#### 2.1.5 GET `/api/iam/v1/auth/menus` 当前用户菜单树

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `type` | Enum | 否 | 节点类型过滤 | `MENU`（默认）/ `BUTTON` / `API` / `ALL` |
| Query | `appCode` | String | 否 | 应用编码 | 多应用前端路由隔离，默认 `process-engine` |

**响应体**：`data` 为树节点数组，节点字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `permissionId` | String | 权限 ID |
| `parentId` | String | 父节点（根为 `"0"`） |
| `permCode` | String | 权限码 `{服务}:{资源}:{动作}` |
| `permName` | String | 名称（按 `Accept-Language` 渲染） |
| `permType` | Enum | `MENU` / `BUTTON` / `API` |
| `routePath` | String | 前端路由路径，如 `/design/process-defs` |
| `component` | String | 前端组件路径，如 `design/process-def/index.vue` |
| `icon` | String | 图标名 |
| `sortNo` | Integer | 排序 |
| `hidden` | Boolean | 是否隐藏菜单 |
| `keepAlive` | Boolean | 前端是否缓存 |
| `children` | Array | 子节点（无子节点时返回 `[]`） |

**主要错误码**：`COMMON_20001`、`IAM_50003`。

#### 2.1.6 POST `/api/iam/v1/auth/captcha` 生成图形验证码

**响应体**：`{ "captchaKey": "cap-7f2c...", "imageBase64": "data:image/png;base64,iVBOR...", "expireSeconds": 300 }`。
**主要错误码**：`COMMON_30008`、`COMMON_50005`。

#### 2.1.7 GET `/api/iam/v1/users` 用户分页列表

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `keyword` | String | 否 | 用户名/昵称/手机号模糊 | ≤ 64 字符 |
| Query | `deptId` | String | 否 | 部门 ID | 配合 `includeChild` 生效 |
| Query | `includeChild` | Boolean | 否 | 是否含子部门 | 默认 `true` |
| Query | `status` | Enum | 否 | `ENABLED` / `DISABLED` | 见 1.7 枚举 |
| Query | `userType` | Enum | 否 | `INTERNAL` / `EXTERNAL` / `API` | — |
| Query | `roleId` | String | 否 | 按角色过滤 | — |
| Query | `groupId` | String | 否 | 按用户组过滤 | — |
| Query | `pageNum` / `pageSize` / `orderBy` / `orderDirection` | — | 否 | 分页与排序 | 见 1.6 |

**响应体（`records[]`）**：`userId`、`username`、`nickname`、`realName`、`email`（脱敏）、`mobile`（脱敏）、`deptId`、`deptName`、`userType`、`status`、`statusName`、`roleNames`、`lastLoginTime`、`createTime`、`version`。

**主要错误码**：`IAM_20001`、`IAM_20009`、`COMMON_10003`、`COMMON_10004`。
**业务规则**：受 `data_scope` 约束——`DEPT` 仅本部门、`DEPT_AND_CHILD` 含子部门、`SELF` 仅本人、`CUSTOM` 取 `customDeptIds`。

#### 2.1.8 GET `/api/iam/v1/users/{userId}` 用户详情

**路径参数**：`userId`（String，必填，雪花 ID）。
**响应体字段**：详情在列表字段基础上增加 `gender`、`birthday`、`avatar`、`remark`、`roleIds`、`groupIds`、`positionName`、`entryDate`、`updateTime`、`updateBy`、`version`。
**响应头**：`ETag: "7"`。
**主要错误码**：`IAM_20001`、`IAM_20009`、`IAM_40001`。

#### 2.1.9 POST `/api/iam/v1/users` 新增用户

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | 缺失 `COMMON_10001` |
| Body | `username` | String | 是 | 登录名 | 唯一（`IAM_30001`） |
| Body | `nickname` | String | 是 | 昵称 | ≤ 64 字符 |
| Body | `password` | String | 否 | 初始密码 | 不传则服务端生成 12 位随机密码；强度不足 `IAM_10002`/弱口令 `IAM_10010` |
| Body | `realName` / `email` / `mobile` | String | 否 | 联系方式 | 格式校验 `IAM_10003` |
| Body | `deptId` | String | 否 | 部门 | 必须存在（`IAM_40004`） |
| Body | `roleIds` | Array\<String\> | 否 | 角色集合 | 角色需存在 |
| Body | `groupIds` | Array\<String\> | 否 | 用户组集合 | — |
| Body | `userType` | Enum | 否 | 用户类型 | 默认 `INTERNAL` |
| Body | `sendNotify` | Boolean | 否 | 是否发送初始密码通知 | 默认 `true`，依赖 NOTIFY |
| Body | `remark` | String | 否 | 备注 | ≤ 500 字符 |

**响应体**：`{ "userId": "1836...", "username": "zhangsan", "initPassword": "Ab3#kL9mQ2x" }`（`initPassword` 仅在服务端生成时返回一次）。
**主要错误码**：`IAM_10001`、`IAM_10002`、`IAM_10003`、`IAM_10010`、`IAM_20001`、`IAM_30001`、`IAM_30007`、`IAM_40004`、`COMMON_30005`。
**业务规则**：新增用户占用租户 `max_user_num` 配额（`IAM_30007`）；`sendNotify=true` 时通过 NOTIFY 内部接口发送站内信/邮件。

#### 2.1.10 PUT `/api/iam/v1/users/{userId}` 修改用户

**请求头**：`If-Match: "当前version"`（必填）。
**请求体**：同 2.1.9，去掉 `password`/`sendNotify`，增加 `status`（`ENABLED/DISABLED`）、`version`（Integer，可省略，以 `If-Match` 为准）。
**响应体**：更新后的用户详情（同 2.1.8）。
**主要错误码**：`IAM_10003`、`IAM_20001`、`IAM_20007`、`IAM_20008`、`IAM_30009`、`IAM_40001`、`COMMON_30003`。
**业务规则**：`status=DISABLED` 时同步踢下线；内置 `admin` 用户的角色与状态不可修改（`IAM_20007`）。

#### 2.1.11 DELETE `/api/iam/v1/users/{userId}` 删除用户

**路径参数**：`userId`。**响应体**：`{ "deleted": true }`。
**主要错误码**：`IAM_20007`、`IAM_20008`、`IAM_40001`、`COMMON_30002`（存在未办结任务时拒绝）。
**业务规则**：逻辑删除（`deleted=1`）；存在运行中实例的待办任务或为流程发起人时返回 `COMMON_30002`，需先转办/结清。

#### 2.1.12 PUT `/api/iam/v1/users/{userId}/password` 重置密码

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `newPassword` | String | 否 | 指定新密码 | 不传则随机生成 |
| Body | `forceChange` | Boolean | 否 | 下次登录强制改密 | 默认 `true` |
| Body | `sendNotify` | Boolean | 否 | 是否通知用户 | 默认 `true` |
| Body | `channel` | Enum | 否 | 通知渠道 | `INBOX`（默认）/ `EMAIL` / `SMS` |

**响应体**：`{ "userId": "...", "initPassword": "Xy7#pQ2mN4k", "forceChange": true, "notified": true }`。
**主要错误码**：`IAM_10002`、`IAM_10010`、`IAM_20001`、`IAM_40001`、`IAM_50002`、`COMMON_30008`。
**业务规则**：重置后该用户全部在线会话失效，并写入操作审计。

#### 2.1.13 PATCH `/api/iam/v1/users/{userId}/status` 启用/停用用户

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `status` | Enum | 是 | `ENABLED` / `DISABLED` | 非法值 `COMMON_10009` |
| Body | `reason` | String | 条件 | 停用原因 | `DISABLED` 时必填，≤ 200 字符 |

**响应体**：`{ "userId": "...", "status": "DISABLED", "kickedSessions": 2 }`。
**主要错误码**：`IAM_20001`、`IAM_20007`、`IAM_20008`、`IAM_30009`、`IAM_40001`、`IAM_50003`。

#### 2.1.14 POST `/api/iam/v1/users/import` 批量导入用户

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Form | `file` | File | 是 | Excel(.xlsx)/CSV | ≤ 10 MB，≤ 5000 行 |
| Form | `mode` | Enum | 否 | `INSERT_ONLY` / `UPSERT` | 默认 `INSERT_ONLY` |
| Form | `defaultRoleIds` | String | 否 | 默认角色 ID，逗号分隔 | — |
| Form | `onError` | Enum | 否 | `ABORT` / `SKIP` | 默认 `ABORT`（全量回滚） |

**响应体**：`{ "total": 200, "successCount": 198, "failCount": 2, "errors": [{ "row": 37, "column": "username", "value": "1abc", "code": "IAM_10001", "message": "用户名格式非法" }] }`。
**主要错误码**：`IAM_10008`、`IAM_20001`、`IAM_30007`、`COMMON_10011`、`COMMON_50008`。

#### 2.1.15 GET `/api/iam/v1/users/export` 导出用户

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | 同 2.1.7 过滤条件 | — | 否 | 导出范围 | 单次 ≤ 5 万行 |
| Query | `async` | Boolean | 否 | 异步导出 | `true` 返回任务 ID，生成后站内信通知 |

**响应体**：`{ "fileUrl": "https://minio/...?expire=300", "fileName": "users_20250520.xlsx", "rowCount": 4213 }`（`async=true` 时为 `{ "taskId": "...", "status": "PROCESSING" }`）。
**主要错误码**：`IAM_20001`、`COMMON_10003`、`COMMON_50008`。

#### 2.1.16 PUT `/api/iam/v1/users/me/password` 修改本人密码

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `oldPassword` | String | 是 | 原密码 | 不匹配 `AUTH_20001` |
| Body | `newPassword` | String | 是 | 新密码 | 强度校验；不得与原密码相同 |
| Body | `confirmPassword` | String | 是 | 确认密码 | 必须一致（`COMMON_10001`） |

**响应体**：`{ "changed": true, "reLoginRequired": true }`。
**主要错误码**：`AUTH_20001`、`IAM_10002`、`IAM_10010`、`COMMON_10001`。

#### 2.1.17–2.1.25 角色与授权接口

**统一下拉：角色对象字段**（列表与详情通用）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `roleId` | String | 角色 ID |
| `roleCode` | String | 角色编码，唯一（`IAM_30002`） |
| `roleName` | String | 角色名称 |
| `roleType` | Enum | `SYSTEM`（内置，不可删）/ `CUSTOM` |
| `dataScope` | Enum | `ALL/DEPT_AND_CHILD/DEPT/SELF/CUSTOM` |
| `customDeptIds` | Array\<String\> | `dataScope=CUSTOM` 时的部门集合 |
| `sortNo` | Integer | 排序 |
| `status` | Enum | `ENABLED` / `DISABLED` |
| `remark` | String | 备注 |
| `userCount` | Integer | 关联用户数（列表返回） |
| `permissionCount` | Integer | 关联权限数（列表返回） |
| `createTime` / `updateTime` | LocalDateTime | 时间 |
| `version` | Integer | 乐观锁版本 |

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /roles` | Query：`keyword`、`roleType`、`status`、分页参数 | 分页 `records[]` 为角色对象 | `IAM_20002`、`COMMON_10003` |
| `GET /roles/{roleId}` | 路径 `roleId` | 角色对象 + `permissions`（权限码数组） | `IAM_20002`、`IAM_40002` |
| `POST /roles` | Header `Idempotency-Key`（REQUIRED）；Body：`roleCode`(必填)、`roleName`(必填)、`dataScope`、`customDeptIds`、`sortNo`、`remark` | `{ "roleId": "..." }` | `IAM_10005`、`IAM_20002`、`IAM_30002`、`COMMON_30005` |
| `PUT /roles/{roleId}` | `If-Match: required`；Body 同上 | 角色对象 | `IAM_10005`、`IAM_20002`、`IAM_40002`、`COMMON_30003` |
| `DELETE /roles/{roleId}` | — | `{ "deleted": true }` | `IAM_20002`、`IAM_30005`、`IAM_40002` |
| `POST /role-permissions` | Header `Idempotency-Key`（RECOMMENDED）；Body：`roleId`(必填)、`permissionIds`(Array\<String\>，必填)、`mode`(Enum `REPLACE`/`APPEND`/`REMOVE`，默认 `REPLACE`) | `{ "roleId": "...", "permissionCount": 42, "affectedUsers": 8 }` | `IAM_20002`、`IAM_20003`、`COMMON_30008` |
| `GET /role-permissions` | Query：`roleId`(必填) | `{ "roleId": "...", "permissionIds": [], "permissionCodes": [] }` | `IAM_20002`、`IAM_40002` |
| `POST /user-roles` | Header `Idempotency-Key`（RECOMMENDED）；Body：`roleId`(必填)、`userIds`(Array\<String\>，必填)；`mode`(`REPLACE`/`APPEND`/`REMOVE`，默认 `APPEND`) | `{ "bound": 5 }` | `IAM_20002`、`IAM_20009`、`COMMON_10010` |
| `DELETE /user-roles` | Query：`roleId`(必填)、`userIds`(必填，逗号分隔，≤ 100 个) | `{ "unbound": 3 }` | `IAM_20002`、`IAM_40001`、`IAM_40002` |

**业务规则**：授权变更后立即失效相关用户的权限缓存（Redis `iam:perm:{tenantId}:{userId}`）并广播到所有 IAM 实例；`roleType=SYSTEM` 的角色不允许删除或修改 `roleCode`；`dataScope=CUSTOM` 且 `customDeptIds` 为空时返回 `COMMON_10001`。

#### 2.1.26–2.1.29 权限/菜单接口

**权限对象字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `permissionId` | String | 权限 ID |
| `parentId` | String | 父节点（根 `"0"`） |
| `permCode` | String | `{服务}:{资源}:{动作}`（`IAM_10006`） |
| `permName` | String | 名称 |
| `permType` | Enum | `MENU` / `BUTTON` / `API` |
| `routePath` / `component` / `icon` | String | 前端路由与图标（`MENU` 必填 `routePath`） |
| `apiPath` / `httpMethod` | String | `API` 类型必填，如 `/api/design/v1/process-defs/**` + `POST` |
| `appCode` | String | 所属应用 |
| `sortNo` / `hidden` / `keepAlive` / `status` | Integer/Boolean/Enum | 展示控制 |
| `children` | Array | 树接口返回 |

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /permissions` | Query：`permType`、`appCode`、`keyword`、`includeDisabled`；返回树形结构 | 权限树数组（`children` 递归） | `IAM_20003` |
| `POST /permissions` | `Idempotency-Key`（REQUIRED）；Body：`parentId`(必填)、`permCode`(必填)、`permName`(必填)、`permType`(必填)、`routePath`、`component`、`apiPath`、`httpMethod`、`icon`、`sortNo` | `{ "permissionId": "..." }` | `IAM_10006`、`IAM_20003`、`COMMON_30001`、`COMMON_40002` |
| `PUT /permissions/{permissionId}` | `If-Match: required`；Body 同上 | 权限对象 | `IAM_10006`、`IAM_20003`、`IAM_40003`、`COMMON_30003` |
| `DELETE /permissions/{permissionId}` | Query `cascade`（Boolean，默认 `false`） | `{ "deleted": true, "cascaded": 3 }` | `IAM_20003`、`IAM_30008`、`IAM_40003` |

**业务规则**：删除非叶子节点且 `cascade=false` 返回 `COMMON_30004`；被角色引用时 `cascade=true` 会级联清理 `sys_role_permission` 并记录审计。`permCode` 在同一租户内唯一。

#### 2.1.30–2.1.33 部门接口

**部门对象字段**：`deptId`、`parentId`、`ancestors`（逗号串，如 `0,1001,1002`）、`deptName`、`deptCode`、`leaderUserId`、`leaderName`、`sortNo`、`status`、`userCount`、`children`、`version`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /depts` | Query：`keyword`、`status`、`withUserCount`（默认 `true`）、`dataScopeFilter`（默认 `true`，按当前用户数据权限裁剪）；返回树形结构 | 部门树数组 | `IAM_20004` |
| `POST /depts` | `Idempotency-Key`（REQUIRED）；Body：`parentId`(必填)、`deptName`(必填)、`deptCode`、`leaderUserId`、`sortNo`、`status` | `{ "deptId": "...", "ancestors": "0,1001" }` | `IAM_10004`、`IAM_20004`、`IAM_30003`、`COMMON_40002` |
| `PUT /depts/{deptId}` | `If-Match: required`；Body 同上（`parentId` 变更会重算子孙 `ancestors`） | 部门对象 | `IAM_10004`、`IAM_20004`、`IAM_40004`、`COMMON_30003` |
| `DELETE /depts/{deptId}` | — | `{ "deleted": true }` | `IAM_20004`、`IAM_30006`、`IAM_40004` |

**业务规则**：层级上限 10（`IAM_10004`）；移动部门时若新父节点在自身子树内返回 `IAM_10004`；部门变更后触发用户数据权限缓存失效。

#### 2.1.34–2.1.39 用户组接口

**用户组对象字段**：`groupId`、`groupCode`、`groupName`、`type`（`STATIC`/`DYNAMIC`）、`ruleJson`（动态组规则，Aviator JSON，如 `{"expression":"deptPath.startsWith('研发') && status=='ENABLED'"}`）、`memberCount`、`status`、`remark`、`version`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /user-groups` | Query：`keyword`、`type`、`status`、分页参数 | 分页 `records[]` | `IAM_20005`、`COMMON_10003` |
| `POST /user-groups` | `Idempotency-Key`（REQUIRED）；Body：`groupCode`(必填)、`groupName`(必填)、`type`(必填)、`ruleJson`（`DYNAMIC` 必填）、`remark` | `{ "groupId": "..." }` | `IAM_10007`、`IAM_20005`、`IAM_30004`、`COMMON_30005` |
| `PUT /user-groups/{groupId}` | `If-Match: required`；Body 同上 | 用户组对象 | `IAM_10007`、`IAM_20005`、`IAM_40005`、`COMMON_30003` |
| `DELETE /user-groups/{groupId}` | — | `{ "deleted": true }` | `IAM_20005`、`IAM_40005`、`COMMON_30002`（被流程节点候选策略引用） |
| `GET /user-groups/{groupId}/members` | Query：分页参数；`resolve`（Boolean，动态组是否实时解析，默认 `true`） | 分页成员列表（`userId/username/nickname/deptName`） | `IAM_20005`、`IAM_40005` |
| `POST /user-groups/{groupId}/members` | Body：`userIds`(Array\<String\>)、`mode`(`REPLACE`/`APPEND`/`REMOVE`，默认 `APPEND`)；动态组返回 `IAM_10011` | `{ "groupId": "...", "memberCount": 12 }` | `IAM_10011`、`IAM_20005`、`IAM_40005`、`COMMON_10010` |

**业务规则**：`DYNAMIC` 用户组不落成员表，每次按 `rule_json` 实时解析（供 ENGINE 候选人计算调用，结果缓存 60s）；`STATIC` 组被流程节点引用时禁止删除。

#### 2.1.40–2.1.44 租户接口

**租户对象字段**：`tenantId`、`tenantCode`、`tenantName`、`status`（`ENABLED`/`DISABLED`）、`isolationMode`（`LOGIC`/`SCHEMA`/`DATABASE`，v1.0 仅 `LOGIC`）、`expireTime`、`maxUserNum`、`userCount`、`contactName`、`contactMobile`、`quota`（对象：`maxRunningInstances`、`maxConcurrentTasks`、`maxDailyCalls`）、`version`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /tenants` | Query：`keyword`、`status`、分页参数；平台级权限 | 分页 `records[]` | `IAM_20006`、`COMMON_10003` |
| `GET /tenants/{tenantId}` | — | 租户对象 + 配额使用量 | `IAM_20006`、`IAM_40006` |
| `POST /tenants` | `Idempotency-Key`（REQUIRED）；Body：`tenantCode`(必填)、`tenantName`(必填)、`expireTime`、`maxUserNum`(必填)、`contactName`、`contactMobile`、`quota` | `{ "tenantId": "...", "tenantCode": "acme" }` | `IAM_10009`、`IAM_20006`、`COMMON_30001` |
| `PUT /tenants/{tenantId}` | `If-Match: required`；Body 同上（`tenantCode` 不可改） | 租户对象 | `IAM_10009`、`IAM_20006`、`IAM_40006`、`COMMON_30003` |
| `PATCH /tenants/{tenantId}/status` | Body：`status`(必填)、`reason`(条件必填) | `{ "tenantId": "...", "status": "DISABLED" }` | `IAM_20006`、`IAM_40006`、`COMMON_20004` |

**业务规则**：停用租户后该租户所有请求返回 `COMMON_20004`（网关校验 + 服务侧兜底）；`isolationMode` 在 v1.0 仅允许 `LOGIC`，其他值返回 `COMMON_10009`；新建租户自动初始化内置角色（`tenant_admin`、`process_admin`、`process_user`）与默认部门树。

#### 2.1.45–2.1.46 在线会话接口

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /sessions/online` | Query：`userId`、`username`、`ip`、`loginStartTime`/`loginEndTime`、分页参数 | 分页 `records[]`：`sessionId`、`userId`、`username`、`nickname`、`tenantId`、`ip`、`location`、`ua`、`os`、`browser`、`loginTime`、`lastActiveTime`、`expireTime`、`current`(Boolean) | `IAM_20001`、`IAM_20009`、`COMMON_10008` |
| `DELETE /sessions/{sessionId}` | Body：`reason`（可选） | `{ "kicked": true, "sessionId": "..." }` | `IAM_20001`、`IAM_30010`、`IAM_50003` |

**业务规则**：会话数据源为 Redis（`auth:session:{sessionId}`），非 DB，因此 `total` 为近似值；强制下线后对应 `jti` 入黑名单，网关在 1s 内生效（本地缓存 TTL 1s）。

#### 2.1.47 GET `/api/iam/v1/login-logs` 登录日志查询

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `username` / `ip` | String | 否 | 精确/模糊匹配 | — |
| Query | `result` | Enum | 否 | `SUCCESS` / `FAILED` / `LOCKED` | — |
| Query | `startTime` / `endTime` | LocalDateTime | 否 | 时间范围 | 格式见 1.7；跨度 ≤ 31 天（`COMMON_10008`） |
| Query | `pageNum` / `pageSize` / `orderBy` / `orderDirection` | — | 否 | 分页 | `orderBy` 默认 `login_time` |

**响应体（`records[]`）**：`logId`、`userId`、`username`、`nickname`、`tenantId`、`ip`、`location`、`ua`、`os`、`browser`、`result`、`msg`、`loginTime`、`sessionId`、`traceId`。
**主要错误码**：`IAM_20001`、`IAM_40007`、`COMMON_10008`。
**业务规则**：登录日志保留 180 天，超期归档至 ClickHouse（`process-trace`），查询可自动路由。

---

### 2.2 process-design（建模，8082，库 `pe_design`）

**服务职责**：流程分类、流程定义与不可变版本（`DRAFT`/`PUBLISHED`/`OFFLINE`）、模型校验、发布/下线/回滚、JSON 与 BPMN 2.0 XML 导入导出、流程模板，同时承载表单定义与表单版本。

**接口总览（45 个）**

| # | 方法 | 路径 | 名称 | 权限码 | 幂等要求 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | GET | `/api/design/v1/categories` | 分类树 | `design:category:list` | NONE | 树形返回 |
| 2 | POST | `/api/design/v1/categories` | 新增分类 | `design:category:create` | REQUIRED | 层级 ≤ 5 |
| 3 | PUT | `/api/design/v1/categories/{categoryId}` | 修改分类 | `design:category:update` | NONE | `If-Match: required` |
| 4 | DELETE | `/api/design/v1/categories/{categoryId}` | 删除分类 | `design:category:delete` | NONE | 非空分类拒绝 |
| 5 | GET | `/api/design/v1/process-defs` | 流程定义分页列表 | `design:process-def:list` | NONE | 支持分类/状态/关键字 |
| 6 | GET | `/api/design/v1/process-defs/{defId}` | 流程定义详情 | `design:process-def:query` | NONE | 含最新/已发布版本摘要 |
| 7 | POST | `/api/design/v1/process-defs` | 新建流程定义 | `design:process-def:create` | REQUIRED | 同时创建 v1 空草稿 |
| 8 | PUT | `/api/design/v1/process-defs/{defId}` | 修改流程定义 | `design:process-def:update` | NONE | `If-Match: required` |
| 9 | DELETE | `/api/design/v1/process-defs/{defId}` | 删除流程定义 | `design:process-def:delete` | NONE | 有运行实例拒绝 |
| 10 | PATCH | `/api/design/v1/process-defs/{defId}/status` | 启用/停用定义 | `design:process-def:update` | RECOMMENDED | 停用后不可新启动 |
| 11 | GET | `/api/design/v1/process-defs/{defId}/versions` | 版本列表 | `design:process-version:list` | NONE | 含状态与发布信息 |
| 12 | GET | `/api/design/v1/process-versions/{versionId}` | 版本详情 | `design:process-version:query` | NONE | Query `raw=true` 返回 `modelJson` 原文 |
| 13 | GET | `/api/design/v1/process-versions/{versionId}/model` | 获取版本模型 JSON | `design:process-version:query` | NONE | 设计器加载入口 |
| 14 | PUT | `/api/design/v1/process-versions/{versionId}/model` | 保存模型草稿 | `design:process-version:update` | RECOMMENDED | 仅 `DRAFT` 可改 |
| 15 | POST | `/api/design/v1/process-versions` | 新增草稿版本 | `design:process-version:create` | REQUIRED | 复制指定版本 |
| 16 | DELETE | `/api/design/v1/process-versions/{versionId}` | 删除草稿版本 | `design:process-version:delete` | NONE | 仅 `DRAFT` |
| 17 | POST | `/api/design/v1/process-versions/validate` | 模型校验 | `design:process-version:validate` | NONE | 返回结构化错误列表 |
| 18 | POST | `/api/design/v1/process-versions/{versionId}/publish` | 发布版本 | `design:process-version:publish` | RECOMMENDED | 发布前强制校验 |
| 19 | POST | `/api/design/v1/process-versions/{versionId}/offline` | 下线版本 | `design:process-version:offline` | RECOMMENDED | 有活跃实例需确认 |
| 20 | POST | `/api/design/v1/process-versions/{versionId}/rollback` | 回滚到该版本 | `design:process-version:publish` | REQUIRED | 生成新版本号 |
| 21 | POST | `/api/design/v1/process-versions/{versionId}/copy` | 复制版本 | `design:process-version:create` | REQUIRED | 生成新草稿 |
| 22 | POST | `/api/design/v1/process-versions/import-json` | 导入模型 JSON | `design:process-version:import` | REQUIRED | 支持 `defId` 或 `processKey` |
| 23 | POST | `/api/design/v1/process-versions/import-bpmn` | 导入 BPMN 2.0 XML | `design:process-version:import` | REQUIRED | 转换为内部 JSON |
| 24 | GET | `/api/design/v1/process-versions/{versionId}/export-bpmn` | 导出 BPMN 2.0 XML | `design:process-version:export` | NONE | 返回 XML 文本 |
| 25 | GET | `/api/design/v1/process-versions/{versionId}/export-json` | 导出模型 JSON | `design:process-version:export` | NONE | 可下载或内联 |
| 26 | GET | `/api/design/v1/process-versions/diff` | 版本差异对比 | `design:process-version:query` | NONE | Query `leftVersionId`+`rightVersionId` |
| 27 | GET | `/api/design/v1/process-defs/{defId}/statistics` | 定义统计 | `design:process-def:query` | NONE | 版本数/实例数/成功率 |
| 28 | GET | `/api/design/v1/process-versions/{versionId}/deploy-logs` | 发布记录 | `design:process-version:query` | NONE | 发布/下线/回滚历史 |
| 29 | GET | `/api/design/v1/templates` | 模板分页列表 | `design:template:list` | NONE | 流程/表单模板 |
| 30 | GET | `/api/design/v1/templates/{templateId}` | 模板详情 | `design:template:query` | NONE | 含 `contentJson` |
| 31 | POST | `/api/design/v1/templates` | 新增模板 | `design:template:create` | REQUIRED | — |
| 32 | PUT | `/api/design/v1/templates/{templateId}` | 修改模板 | `design:template:update` | NONE | `If-Match: required` |
| 33 | DELETE | `/api/design/v1/templates/{templateId}` | 删除模板 | `design:template:delete` | NONE | 内置模板拒绝 |
| 34 | POST | `/api/design/v1/templates/apply` | 应用模板 | `design:process-def:create` | REQUIRED | 由模板生成定义与草稿 |
| 35 | POST | `/api/design/v1/templates/from-version` | 由版本存为模板 | `design:template:create` | REQUIRED | 快照 `model_json` |
| 36 | GET | `/api/design/v1/form-defs` | 表单定义分页列表 | `form:def:list` | NONE | 表单主体管理 |
| 37 | GET | `/api/design/v1/form-defs/{formId}` | 表单定义详情 | `form:def:query` | NONE | — |
| 38 | POST | `/api/design/v1/form-defs` | 新增表单定义 | `form:def:create` | REQUIRED | 自动创建 v1 草稿 |
| 39 | PUT | `/api/design/v1/form-defs/{formId}` | 修改表单定义 | `form:def:update` | NONE | `If-Match: required` |
| 40 | DELETE | `/api/design/v1/form-defs/{formId}` | 删除表单定义 | `form:def:delete` | NONE | 被流程引用拒绝 |
| 41 | GET | `/api/design/v1/form-defs/{formId}/versions` | 表单版本列表 | `form:version:list` | NONE | — |
| 42 | GET | `/api/design/v1/form-versions/{formVersionId}` | 表单版本详情 | `form:version:query` | NONE | 含 `schemaJson` |
| 43 | PUT | `/api/design/v1/form-versions/{formVersionId}` | 保存表单版本（草稿） | `form:version:update` | RECOMMENDED | 仅 `DRAFT` |
| 44 | POST | `/api/design/v1/form-versions/{formVersionId}/publish` | 发布表单版本 | `form:version:publish` | RECOMMENDED | 版本号递增 |
| 45 | POST | `/api/design/v1/form-versions` | 新增表单版本 | `form:version:create` | REQUIRED | 复制指定版本 |

#### 2.2.1 流程分类接口

**分类对象字段**：`categoryId`、`parentId`（根 `"0"`）、`categoryName`、`categoryCode`、`sortNo`、`icon`、`processDefCount`、`children`、`version`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /categories` | Query：`keyword`、`withCount`（默认 `true`） | 分类树数组 | `DESIGN_20001` |
| `POST /categories` | `Idempotency-Key`（REQUIRED）；Body：`parentId`(必填)、`categoryName`(必填，≤ 64)、`categoryCode`、`sortNo`、`icon` | `{ "categoryId": "..." }` | `DESIGN_10012`、`DESIGN_20001`、`COMMON_30001` |
| `PUT /categories/{categoryId}` | `If-Match: required`；Body 同上 | 分类对象 | `DESIGN_10012`、`DESIGN_20001`、`DESIGN_40003`、`COMMON_30003` |
| `DELETE /categories/{categoryId}` | — | `{ "deleted": true }` | `DESIGN_20001`、`DESIGN_30009`、`DESIGN_40003` |

#### 2.2.2 流程定义接口

**流程定义对象字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `defId` | String | 定义 ID（`pf_process_def.id`） |
| `processKey` | String | 唯一键，`^[a-z][a-z0-9_]{2,63}$`（`DESIGN_10004`） |
| `processName` | String | 名称，≤ 128 |
| `categoryId` / `categoryName` | String | 分类 |
| `icon` | String | 图标 |
| `description` | String | 描述，≤ 500 |
| `status` | Enum | `ENABLED` / `DISABLED` |
| `latestVersion` | Integer | 最新版本号（含草稿） |
| `publishedVersion` | Integer | 当前发布版本号，未发布为 `0` |
| `latestVersionId` / `publishedVersionId` | String | 对应版本 ID |
| `maintainerIds` / `maintainerNames` | Array\<String\> | 负责人（非负责人不可编辑，`DESIGN_20005`） |
| `instanceCount` | Integer | 历史实例数（列表返回，来自 ENGINE 冗余统计） |
| `createTime` / `updateTime` / `createBy` / `updateBy` | LocalDateTime / String | 审计字段 |
| `version` | Integer | 乐观锁版本 |

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /process-defs` | Query：`keyword`、`categoryId`（可多值）、`status`、`published`(Boolean)、`maintainerId`、分页参数 | 分页 `records[]` 为定义对象 | `DESIGN_20002`、`COMMON_10003/10004` |
| `GET /process-defs/{defId}` | 路径 `defId`（或 Query `processKey`） | 定义对象 + `latestVersionInfo`/`publishedVersionInfo` 摘要 | `DESIGN_20002`、`DESIGN_40001` |
| `POST /process-defs` | `Idempotency-Key`（REQUIRED）；Body：`processKey`(必填)、`processName`(必填)、`categoryId`、`icon`、`description`、`maintainerIds`、`initModel`(Object，可选) | `{ "defId": "...", "processKey": "...", "versionId": "...", "version": 1 }` | `DESIGN_10004`、`DESIGN_10012`、`DESIGN_30001`、`COMMON_30005` |
| `PUT /process-defs/{defId}` | `If-Match: required`；Body：`processName`、`categoryId`、`icon`、`description`、`maintainerIds`（`processKey` 不可改） | 定义对象 | `DESIGN_20002`、`DESIGN_20005`、`DESIGN_40001`、`COMMON_30003` |
| `DELETE /process-defs/{defId}` | Query `force`（Boolean，默认 `false`） | `{ "deleted": true }` | `DESIGN_20002`、`DESIGN_30005`、`DESIGN_40001` |
| `PATCH /process-defs/{defId}/status` | Body：`status`(必填，`ENABLED`/`DISABLED`)、`reason` | `{ "defId": "...", "status": "DISABLED" }` | `DESIGN_20002`、`DESIGN_40001` |

**业务规则**：`processKey` 在租户内唯一且创建后不可变（对外契约锚点）；`status=DISABLED` 时按 key 启动实例返回 `ENGINE_40006`（无可用发布版本）；删除定义前必须无 `RUNNING/SUSPENDED` 实例（`force=true` 仅平台管理员可绕过并留审计）。

#### 2.2.3 流程版本与模型接口

**版本对象字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `versionId` | String | 版本 ID |
| `defId` / `processKey` | String | 所属定义 |
| `version` | Integer | 版本号（定义内递增，不复用） |
| `status` | Enum | `DRAFT` / `PUBLISHED` / `OFFLINE` |
| `modelHash` | String | 模型内容 SHA-256，用于导入校验与去重（`DESIGN_10010`） |
| `nodeCount` / `edgeCount` | Integer | 节点与连线数量 |
| `settings` | Object | 对应基线 5 `settings`（`allowWithdraw`/`withdrawScope`/`autoSkipSameApprover`/`notifyOnStart`/`timeoutPolicy`/`vars`） |
| `publishTime` / `publishBy` / `publishByName` | LocalDateTime / String | 发布信息 |
| `offlineTime` / `offlineReason` | LocalDateTime / String | 下线信息 |
| `remark` | String | 版本说明（发布时必填，≤ 500） |
| `basedOnVersionId` | String | 复制/回滚来源版本 |
| `version` | Integer | 乐观锁版本（字段名与业务版本号区分，JSON 中为 `version`，业务号另有 `version` 同名字段 → **约定：业务版本号字段名统一为 `version`，乐观锁字段名统一为 `lockVersion`**） |

> **字段命名约定（避免歧义）**：所有含业务版本号的资源（流程版本、表单版本），JSON 中 `version` 表示**业务版本号**，乐观锁字段一律命名为 `lockVersion`（`If-Match` 对应 `lockVersion`）。其他资源（定义、表单定义、连接器等）无业务版本号，`version` 即乐观锁字段。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /process-defs/{defId}/versions` | Query：`status`、`includeModel`（默认 `false`） | `{ "defId": "...", "records": [版本对象] }`（不分页，版本数上限 200） | `DESIGN_20002`、`DESIGN_40001` |
| `GET /process-versions/{versionId}` | Query `raw`（默认 `false`） | `raw=false`：版本对象 + `model`(Object)；`raw=true`：`modelJson`(String) | `DESIGN_20002`、`DESIGN_40002` |
| `GET /process-versions/{versionId}/model` | — | `{ "versionId": "...", "version": 3, "status": "DRAFT", "model": { key, name, version, settings, nodes, edges } }`（结构见基线 5） | `DESIGN_20002`、`DESIGN_40002` |
| `PUT /process-versions/{versionId}/model` | Body：`model`(Object，必填，完整模型)、`modelHash`(String，可选，乐观校验)、`remark`(可选) | `{ "versionId": "...", "modelHash": "sha256:...", "nodeCount": 9, "edgeCount": 10, "updateTime": "..." }` | `DESIGN_10001`、`DESIGN_10002`、`DESIGN_30002`、`COMMON_30003`（`modelHash` 不一致） |
| `POST /process-versions` | `Idempotency-Key`（REQUIRED）；Body：`defId`(必填)、`basedOnVersionId`(可选)、`remark` | `{ "versionId": "...", "version": 4, "status": "DRAFT" }` | `DESIGN_30006`、`DESIGN_40002`、`COMMON_30005` |
| `DELETE /process-versions/{versionId}` | — | `{ "deleted": true }` | `DESIGN_30003`、`DESIGN_40002`、`COMMON_30004` |

**保存模型草稿的业务规则**

1. 仅 `status=DRAFT` 可保存，否则 `DESIGN_30002`。
2. 服务端做**轻度结构校验**（JSON 结构、节点类型、连线引用），全量语义校验由 `/validate` 完成。
3. 保存成功后刷新 Redis 缓存 `design:model:{tenantId}:{versionId}` 并记录 `model_hash`。
4. 并发保存以 `modelHash` 做 CAS：请求体带 `modelHash` 时与库中不一致返回 `COMMON_30003`。

#### 2.2.4 模型校验、发布、下线、回滚

**（1）`POST /api/design/v1/process-versions/validate` 模型校验**

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `model` | Object | 条件 | 待校验模型 | 与 `versionId` 二选一 |
| Body | `versionId` | String | 条件 | 校验库中版本 | 与 `model` 二选一 |
| Body | `level` | Enum | 否 | 返回级别阈值 | `ERROR`（默认）/ `WARN` / `INFO` |
| Body | `checkForms` | Boolean | 否 | 是否校验表单引用存在 | 默认 `true` |

**响应体（`data`）**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `valid` | Boolean | 是否存在 `ERROR` 级问题 |
| `errorCount` / `warnCount` | Integer | 各级问题数量 |
| `errors` | Array\<Object\> | 结构化错误列表 |
| `errors[].level` | Enum | `ERROR` / `WARN` / `INFO` |
| `errors[].code` | String | 校验项编码，见下表 |
| `errors[].nodeId` / `edgeId` | String | 定位信息（可能为 `null`） |
| `errors[].field` | String | 字段路径，如 `properties.formRef.formKey` |
| `errors[].message` | String | 可读描述（按 `Accept-Language`） |
| `errors[].hint` | String | 修复建议 |
| `summary` | Object | `{ "nodeTypeCount": {"USER_TASK": 3}, "unreachableNodes": [], "deadEndNodes": [] }` |

**校验项编码（`code`）**

| code | 级别 | 含义 |
| --- | --- | --- |
| `MODEL_NO_START` | ERROR | 缺少 `START_EVENT` |
| `MODEL_MULTI_START` | ERROR | 存在多个 `START_EVENT` |
| `MODEL_NO_END` | ERROR | 缺少 `END_EVENT`/`TERMINATE_END_EVENT` |
| `MODEL_NODE_ID_DUP` | ERROR | 节点 ID 重复 |
| `MODEL_EDGE_DANGLING` | ERROR | 连线 `source`/`target` 指向不存在的节点 |
| `MODEL_NODE_UNREACHABLE` | ERROR | 从开始节点不可达 |
| `MODEL_NODE_NO_OUTGOING` | ERROR | 非结束节点无出线 |
| `MODEL_GATEWAY_CONDITION_MISSING` | ERROR | 排他/包容网关出口缺少条件且无默认流 |
| `MODEL_GATEWAY_DEFAULT_MULTI` | ERROR | 默认流多于一条 |
| `MODEL_USER_TASK_NO_ASSIGNEE` | ERROR | 人任务未配置候选人策略且无 `emptyStrategy` |
| `MODEL_BOUNDARY_NO_HOST` | ERROR | 边界事件未挂载活动 |
| `MODEL_COMPENSATION_NO_HANDLER` | ERROR | 存在补偿边界但无 `COMPENSATION_HANDLER` |
| `MODEL_SUB_PROCESS_EMPTY` | ERROR | 子流程为空 |
| `MODEL_CALL_ACTIVITY_TARGET_MISSING` | ERROR | 调用目标流程定义不存在 |
| `MODEL_EXPRESSION_INVALID` | ERROR | 条件/映射表达式语法错误 |
| `MODEL_VAR_DUP` | ERROR | `settings.vars` 变量名重复 |
| `MODEL_FORM_REF_MISSING` | ERROR | 表单引用不存在或未发布 |
| `MODEL_CONNECTOR_REF_MISSING` | WARN | 连接器操作引用不存在或被禁用 |
| `MODEL_TIMER_NO_DUE` | ERROR | 定时器节点未配置到期时间 |
| `MODEL_MESSAGE_NAME_EMPTY` | ERROR | 消息节点未配置 `messageName` |
| `MODEL_LOOP_NO_COLLECTION` | ERROR | 循环节点未配置集合或次数 |
| `MODEL_UNUSED_NODE` | WARN | 存在孤立节点 |
| `MODEL_LONG_CHAIN` | INFO | 主干节点数 > 50，建议拆分 |

**主要错误码**：`DESIGN_10001`、`DESIGN_10002`、`DESIGN_10003`、`DESIGN_40002`。

**（2）`POST /api/design/v1/process-versions/{versionId}/publish` 发布版本**

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 否（RECOMMENDED） | 幂等键 | — |
| Body | `remark` | String | 是 | 版本说明 | 1–500 字符 |
| Body | `force` | Boolean | 否 | 忽略 `WARN` 级问题 | 默认 `false`；`ERROR` 级不可忽略 |
| Body | `autoOfflinePrevious` | Boolean | 否 | 是否自动下线上一发布版本 | 默认 `false` |
| Body | `effectiveTime` | LocalDateTime | 否 | 定时生效时间 | 不传立即生效；未来时间写入定时任务 |

**响应体**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `versionId` | String | 版本 ID |
| `defId` / `processKey` | String | 定义信息 |
| `version` | Integer | 版本号 |
| `status` | Enum | `PUBLISHED` |
| `modelHash` | String | 模型哈希 |
| `publishTime` | LocalDateTime | 发布时间 |
| `publishBy` / `publishByName` | String | 发布人 |
| `offlineVersionId` | String | 若自动下线上一版本，返回其 ID |
| `warnings` | Array\<Object\> | 被忽略的 `WARN` 列表（同校验结构） |

**主要错误码**：`DESIGN_10003`、`DESIGN_30003`、`DESIGN_30004`、`DESIGN_20003`、`DESIGN_20005`、`DESIGN_40002`、`DESIGN_50002`。
**业务规则**：发布前强制跑校验（`force=true` 仅跳过 `WARN`）；发布后版本不可改（只读）；写 `pf_deploy_log`（`action=PUBLISH`）；广播 Redis 缓存刷新事件，ENGINE 在 1s 内可见新版本；`autoOfflinePrevious=true` 且旧版本存在活跃实例时仅标记离线不终止实例。

**（3）`POST /api/design/v1/process-versions/{versionId}/offline` 下线版本**

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `reason` | String | 是 | 下线原因 | 1–500 字符 |
| Body | `terminateRunningInstances` | Boolean | 否 | 是否终止该版本运行中实例 | 默认 `false`（仅禁止新启动） |

**响应体**：`{ "versionId": "...", "status": "OFFLINE", "runningInstanceCount": 12, "terminatedCount": 0, "offlineTime": "2025-05-20 10:00:00.000" }`。
**主要错误码**：`DESIGN_20003`、`DESIGN_40002`、`COMMON_30004`（已是 `OFFLINE`）。

**（4）`POST /api/design/v1/process-versions/{versionId}/rollback` 回滚**

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Body | `targetVersionId` | String | 是 | 回滚目标（历史 `PUBLISHED` 版本） | 非历史发布版本 `DESIGN_30007` |
| Body | `remark` | String | 是 | 回滚说明 | — |
| Body | `affectRunning` | Enum | 否 | `NONE`（默认）/ `NEW_ONLY` / `ALL` | `ALL` 表示同步迁移运行中实例（见 `engine/migrate`） |

**响应体**：`{ "newVersionId": "...", "version": 9, "basedOnVersionId": "...", "status": "PUBLISHED", "migratedInstances": 0 }`。
**主要错误码**：`DESIGN_30007`、`DESIGN_30008`、`DESIGN_20003`、`DESIGN_40002`。
**业务规则**：回滚**不**修改历史版本，而是以目标版本模型内容生成一个**新版本号**的发布版本；`affectRunning=ALL` 时校验新旧版本节点集合兼容性，不兼容返回 `DESIGN_30008`。

**（5）`POST /api/design/v1/process-versions/{versionId}/copy` 复制版本**

**请求体**：`{ "targetDefId": "可选，跨定义复制", "remark": "从 v3 复制", "asDraft": true }`。
**响应体**：`{ "versionId": "...", "defId": "...", "version": 5, "status": "DRAFT", "basedOnVersionId": "..." }`。
**主要错误码**：`DESIGN_20002`、`DESIGN_40001`、`DESIGN_40002`、`DESIGN_30006`。

#### 2.2.5 导入导出与差异对比

**（1）`POST /api/design/v1/process-versions/import-json` 导入模型 JSON**

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Form | `file` | File | 条件 | `.json` 文件 | ≤ 10 MB，与 `model` 二选一 |
| Form | `model` | Object | 条件 | 内联模型 | 与 `file` 二选一 |
| Form | `defId` | String | 条件 | 导入到已有定义 | 与 `processKey` 二选一 |
| Form | `processKey` | String | 条件 | 按 key 匹配或新建定义 | 不存在则自动创建定义 |
| Form | `mode` | Enum | 否 | `NEW_VERSION`（默认）/ `OVERWRITE_DRAFT` | — |
| Form | `modelHash` | String | 否 | 期望哈希 | 不匹配 `DESIGN_10010` |
| Form | `validateOnly` | Boolean | 否 | 仅校验不落库 | 默认 `false` |

**响应体**：`{ "defId": "...", "versionId": "...", "version": 4, "status": "DRAFT", "valid": true, "errors": [], "createdDef": false }`。
**主要错误码**：`DESIGN_10001`、`DESIGN_10002`、`DESIGN_10003`、`DESIGN_10010`、`DESIGN_40001`、`COMMON_10011`。

**（2）`POST /api/design/v1/process-versions/import-bpmn` 导入 BPMN 2.0 XML**

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Form | `file` | File | 条件 | `.bpmn`/`.xml` | ≤ 10 MB |
| Form | `xml` | String | 条件 | 内联 XML | 与 `file` 二选一 |
| Form | `defId` / `processKey` | String | 条件 | 目标定义 | 规则同上 |
| Form | `strict` | Boolean | 否 | 严格模式（未支持元素报错） | 默认 `false`（转为 `WARN` 并忽略） |

**响应体**：`{ "defId": "...", "versionId": "...", "version": 2, "convertedNodeCount": 14, "convertedEdgeCount": 15, "unsupportedElements": [{ "element": "bpmn:sendTask", "action": "CONVERTED_TO_SERVICE_TASK" }], "warnings": [] }`。
**主要错误码**：`DESIGN_10006`、`DESIGN_10003`、`COMMON_10011`。
**BPMN 元素映射（v1.0 支持范围）**

| BPMN 元素 | 内部 `NodeType` | 说明 |
| --- | --- | --- |
| `startEvent` / `endEvent` | `START_EVENT` / `END_EVENT` | 含 `terminateEventDefinition` → `TERMINATE_END_EVENT` |
| `userTask` | `USER_TASK` | 读取 `candidateUsers`/`candidateGroups`/`assignee` 映射为 `assigneeStrategy` |
| `serviceTask` / `sendTask` / `receiveTask` | `SERVICE_TASK` / `MESSAGE_CATCH_EVENT` | `sendTask` 转 `SERVICE_TASK` + MQ 连接器 |
| `scriptTask` | `SCRIPT_TASK` | 脚本语言必须为 JavaScript（GraalVM） |
| `manualTask` | `USER_TASK` | 提示为人工节点 |
| `exclusiveGateway` / `parallelGateway` / `inclusiveGateway` / `eventBasedGateway` | 对应网关 | 条件读 `conditionExpression` |
| `intermediateCatchEvent`（timer/message/signal） | `TIMER_CATCH_EVENT` / `MESSAGE_CATCH_EVENT` / `SIGNAL_CATCH_EVENT` | — |
| `boundaryEvent`（timer/error/message） | 对应边界事件 | `cancelActivity` → 打断/非打断 |
| `subProcess` / `callActivity` | `SUB_PROCESS` / `CALL_ACTIVITY` | — |
| `sequenceFlow` | `edges[]` | — |
| 其他（`businessRuleTask`、`complexGateway`、`choreography` 等） | — | `strict=true` 报错，否则忽略并返回 `unsupportedElements` |

**（3）`GET /api/design/v1/process-versions/{versionId}/export-bpmn` 导出 BPMN**

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `download` | Boolean | 否 | `true` 返回文件流，`false` 返回文本 | 默认 `false` |
| Query | `includeDiagram` | Boolean | 否 | 是否输出 `BPMNDiagram` 坐标 | 默认 `true` |
| Query | `fileName` | String | 否 | 下载文件名 | 默认 `{processKey}_v{version}.bpmn` |

**响应体**：`download=false` → `{ "fileName": "leave_approval_v3.bpmn", "contentType": "application/xml", "xml": "<?xml version=\"1.0\" encoding=\"UTF-8\"?>..." }`；`download=true` → 文件流（`Content-Disposition: attachment`）。
**主要错误码**：`DESIGN_20006`、`DESIGN_40002`、`DESIGN_50003`。

**（4）`GET /api/design/v1/process-versions/{versionId}/export-json` 导出模型 JSON**：响应体 `{ "fileName": "leave_approval_v3.json", "model": {...}, "modelHash": "sha256:...", "fileUrl": "可选 MinIO 链接" }`；错误码 `DESIGN_20006`、`DESIGN_40002`。

**（5）`GET /api/design/v1/process-versions/diff` 版本差异对比**

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `leftVersionId` | String | 是 | 基线版本 | 必须同 `defId` |
| Query | `rightVersionId` | String | 是 | 对比版本 | 同上 |
| Query | `scope` | Enum | 否 | `ALL`（默认）/ `NODES` / `EDGES` / `SETTINGS` | 对比范围 |
| Query | `includeFields` | Boolean | 否 | 是否输出字段级差异 | 默认 `true` |

**响应体**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `left` / `right` | Object | `{ versionId, version, status, modelHash }` |
| `compatible` | Boolean | 是否结构兼容（可安全迁移实例） |
| `nodeChanges` | Array\<Object\> | `{ changeType: ADDED/REMOVED/MODIFIED, nodeId, nodeType, nodeName, fieldDiffs: [{ field, leftValue, rightValue }] }` |
| `edgeChanges` | Array\<Object\> | 同上（`edgeId`） |
| `settingsChanges` | Array\<Object\> | `{ field, leftValue, rightValue }` |
| `varChanges` | Array\<Object\> | 变量定义增删改 |
| `summary` | Object | `{ addedNodes, removedNodes, modifiedNodes, addedEdges, removedEdges, breakingChanges }` |
| `breakingChanges` | Array\<Object\> | 破坏性变更说明（用于迁移前提示） |

**主要错误码**：`DESIGN_10003`、`DESIGN_40002`、`COMMON_40002`。

#### 2.2.6 定义统计与发布记录

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /process-defs/{defId}/statistics` | Query：`startTime`/`endTime`（默认近 30 天） | `{ "defId": "...", "versionCount": 5, "publishedVersion": 4, "instanceTotal": 12043, "runningCount": 37, "completedCount": 11800, "terminatedCount": 206, "avgDurationMs": "1843200", "successRate": 0.9832 }` | `DESIGN_20002`、`DESIGN_40001`、`COMMON_10008` |
| `GET /process-versions/{versionId}/deploy-logs` | Query：`action`、`operatorId`、`startTime`/`endTime`、分页参数 | 分页 `records[]`：`logId`、`defId`、`versionId`、`version`、`action`(`PUBLISH`/`OFFLINE`/`ROLLBACK`)、`operator`、`operatorName`、`remark`、`operTime`、`traceId` | `DESIGN_20002`、`DESIGN_40005`、`COMMON_10003` |

**业务规则**：统计口径以 ENGINE 的 `tr_instance_stat`（`process-trace` 物化）为准，DESIGN 只做透传聚合，**不查 `pe_engine` 业务库**；数据延迟 ≤ 5 分钟，响应体带 `dataTime` 字段标识统计时间。

#### 2.2.7 模板与表单定义/版本接口（含 2.2 总表中的 `#29`–`#45`）

**模板对象字段**：`templateId`、`templateKey`、`templateName`、`type`（`PROCESS`/`FORM`）、`category`、`contentJson`（流程模板为模型 JSON，表单模板为 schema）、`tags`（Array\<String\>）、`icon`、`description`、`builtin`、`useCount`、`status`、`version`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /templates` | Query：`keyword`、`type`、`category`、`tag`、`builtin`、分页参数 | 分页 `records[]` | `DESIGN_20004`、`COMMON_10003` |
| `GET /templates/{templateId}` | Query `withContent`（默认 `true`） | 模板对象 | `DESIGN_20004`、`DESIGN_40004` |
| `POST /templates` | `Idempotency-Key`（REQUIRED）；Body：`templateKey`(必填)、`templateName`(必填)、`type`(必填)、`category`、`contentJson`(必填)、`tags`、`icon`、`description` | `{ "templateId": "..." }` | `DESIGN_10001`、`DESIGN_20004`、`DESIGN_30010` |
| `PUT /templates/{templateId}` | `If-Match: required`；Body 同上（`templateKey` 不可改） | 模板对象 | `DESIGN_20004`、`DESIGN_40004`、`COMMON_30003` |
| `DELETE /templates/{templateId}` | — | `{ "deleted": true }` | `DESIGN_20004`、`DESIGN_30004`、`DESIGN_40004` |
| `POST /templates/apply` | `Idempotency-Key`（REQUIRED）；Body：`templateId`(必填)、`processKey`(条件必填)、`processName`(条件必填)、`categoryId`、`variableOverrides`(Object，变量默认值覆盖) | `{ "defId": "...", "versionId": "...", "version": 1, "processKey": "..." }` | `DESIGN_10001`、`DESIGN_30001`、`DESIGN_40004` |
| `POST /templates/from-version` | `Idempotency-Key`（REQUIRED）；Body：`versionId`(必填)、`templateName`(必填)、`templateKey`(必填)、`category`、`tags` | `{ "templateId": "..." }` | `DESIGN_10001`、`DESIGN_30010`、`DESIGN_40002` |

**业务规则**：应用模板时若 `processKey` 已存在返回 `DESIGN_30001`；模板 `useCount` 采用异步累加（不阻塞主流程）；`builtin=true` 模板不可删改（`DESIGN_30004` 等价语义，返回 `DESIGN_20004` + `COMMON_30004`）。

---

### 2.3 process-form（表单，8082，库 `pe_design`）

**服务职责**：提供表单 schema 的按需获取（供设计器与任务办理页渲染）与表单数据服务端校验，物理上由 `process-design` 承载，路径前缀独立为 `/api/form/v1`。

**接口总览（10 个）**

| # | 方法 | 路径 | 名称 | 权限码 | 幂等要求 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | GET | `/api/form/v1/forms/{formKey}` | 按 key 获取表单 schema | 登录态 / `form:schema:read` | NONE | 默认取 `LATEST` 已发布版本 |
| 2 | GET | `/api/form/v1/forms/{formKey}/versions/{version}` | 按版本获取 schema | 登录态 / `form:schema:read` | NONE | `version` 支持数字或 `LATEST` |
| 3 | GET | `/api/form/v1/form-schemas/{schemaId}` | 按 schema ID 获取 | `form:schema:read` | NONE | 任务详情内嵌 schema 引用 |
| 4 | POST | `/api/form/v1/form-data/validate` | 表单数据校验 | 登录态 | NONE | 不落库的纯校验 |
| 5 | POST | `/api/form/v1/form-data/normalize` | 表单数据规范化 | 登录态 | NONE | 类型转换、默认值、富文本清洗 |
| 6 | GET | `/api/form/v1/form-defs/resolve` | 按 key+版本解析元数据 | 登录态 | NONE | 轻量（不含 schema 全量） |
| 7 | GET | `/api/form/v1/form-defs/{formKey}/dictionaries` | 表单字典/选项解析 | 登录态 | NONE | 下拉选项数据源 |
| 8 | POST | `/api/form/v1/form-defs/{formKey}/render-meta` | 渲染元数据批量获取 | 登录态 | NONE | 批量 key + 版本 |
| 9 | GET | `/api/form/v1/form-schemas/{schemaId}/field-tree` | 字段树 | `form:schema:read` | NONE | 供变量映射选择器 |
| 10 | POST | `/api/form/v1/form-data/export` | 表单数据导出 | `form:data:export` | RECOMMENDED | 按 schema 生成 Excel |

#### 2.3.1 GET `/api/form/v1/forms/{formKey}` 获取表单 schema（LATEST）

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Path | `formKey` | String | 是 | 表单键 | `^[a-z][a-z0-9_]{2,63}$`（`FORM_10002`） |
| Query | `mode` | Enum | 否 | 使用场景 | `CREATE` / `APPROVE` / `VIEW`，默认 `APPROVE` |
| Query | `withDictionaries` | Boolean | 否 | 是否解析下拉选项 | 默认 `false`（性能考虑） |
| Query | `taskId` | String | 否 | 关联任务 ID | 提供时按任务数据权限过滤字段（隐藏无权限字段） |
| Query | `instanceId` | String | 否 | 关联实例 | 用于表达式型默认值求值 |

**响应体**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `formKey` / `formName` | String | 表单标识 |
| `formId` | String | 表单定义 ID |
| `formType` | Enum | `CREATE` / `APPROVE` / `VIEW` |
| `version` | Integer | 命中的表单版本号 |
| `versionId` | String | 版本 ID |
| `schema` | Object | JSON Schema + 扩展 UI 配置 |
| `schema.fields` | Array\<Object\> | 字段定义数组 |
| `schema.fields[].fieldKey` | String | 字段键（变量名） |
| `schema.fields[].label` | String | 标签（按 `Accept-Language`） |
| `schema.fields[].type` | Enum | `input` / `textarea` / `number` / `amount` / `date` / `datetime` / `select` / `multiSelect` / `radio` / `checkbox` / `switch` / `upload` / `user` / `dept` / `table` / `richText` / `divider` |
| `schema.fields[].required` | Boolean | 是否必填 |
| `schema.fields[].readonly` / `hidden` / `disabled` | Boolean | 展示与编辑控制 |
| `schema.fields[].defaultValue` | Object | 默认值（可为表达式 `{"expression":"${initiatorName}"}`） |
| `schema.fields[].props` | Object | 类型相关属性（`placeholder`/`min`/`max`/`precision`/`maxLength`/`options`/`accept`/`multiple`…） |
| `schema.fields[].rules` | Array\<Object\> | 校验规则 `[{ rule: "regex|range|expression|unique", value: "...", message: "..." }]` |
| `schema.fields[].visibleWhen` | Object | 联动显隐表达式 |
| `schema.layout` | Object | 布局（`columns`/`groups`） |
| `dictionaries` | Object | `withDictionaries=true` 时返回 `{ fieldKey: [{label,value}] }` |
| `dataPermissions` | Object | `{ editableFields: [], hiddenFields: [] }`（按 `taskId` 计算） |

**主要错误码**：`FORM_10002`、`FORM_40001`、`FORM_40002`、`FORM_40003`、`FORM_50001`。
**业务规则**：`LATEST` 指该 `formKey` 下**已发布**的最大版本；无已发布版本返回 `FORM_40002`；响应 `Cache-Control: max-age=60` 并回写 `ETag: "{formKey}-{version}"`；`mode=VIEW` 时所有字段 `readonly=true`。

#### 2.3.2 GET `/api/form/v1/forms/{formKey}/versions/{version}` 按版本获取 schema

**路径参数**：`formKey`、`version`（Integer 或字面量 `LATEST`）。
**Query**：同 2.3.1。
**响应体**：同 2.3.1，`version` 回显实际命中版本。
**主要错误码**：`FORM_10008`、`FORM_40001`、`FORM_40002`、`FORM_40003`。

#### 2.3.3 GET `/api/form/v1/form-schemas/{schemaId}` 按 schema ID 获取

**响应体**：`{ "schemaId": "...", "formKey": "...", "version": 2, "schema": {...}, "checksum": "sha256:..." }`。
**主要错误码**：`FORM_40002`、`FORM_40003`、`FORM_20001`。

#### 2.3.4 POST `/api/form/v1/form-data/validate` 表单数据校验

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `formKey` | String | 条件 | 表单键 | 与 `schemaId` 二选一 |
| Body | `version` | Integer/String | 否 | 版本或 `LATEST` | 默认 `LATEST` |
| Body | `schemaId` | String | 条件 | 直接指定 schema | 与 `formKey` 二选一 |
| Body | `data` | Object | 是 | 待校验数据，键为 `fieldKey` | 未知字段默认忽略（`strict=true` 时报错） |
| Body | `strict` | Boolean | 否 | 严格模式 | 默认 `false` |
| Body | `partial` | Boolean | 否 | 局部校验（仅校验提供的字段） | 默认 `false` |

**响应体**：`{ "valid": false, "errors": [{ "fieldKey": "days", "fieldLabel": "请假天数", "code": "REQUIRED", "message": "请假天数为必填项", "value": null }], "normalizedData": {...} }`（`code` ∈ `REQUIRED`/`TYPE_MISMATCH`/`RANGE`/`REGEX`/`LENGTH`/`UNIQUE`/`EXPRESSION`/`OPTION_INVALID`/`FILE_INVALID`）。
**主要错误码**：`FORM_10006`、`FORM_10001`、`FORM_40001`、`FORM_40002`、`FORM_50002`。

#### 2.3.5 POST `/api/form/v1/form-data/normalize` 表单数据规范化

**请求体**：同 2.3.4（`strict` 无意义）。
**响应体**：`{ "normalizedData": { "days": 3, "startDate": "2025-05-20" }, "changedFields": ["days", "startDate"], "warnings": [{ "fieldKey": "attachment", "code": "TRUNCATED", "message": "富文本已按 20000 字截断" }] }`。
**主要错误码**：`FORM_10006`、`FORM_40002`、`FORM_50002`。

#### 2.3.6–2.3.10 表单辅助接口

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /form-defs/resolve` | Query：`formKey`(必填)、`version`(默认 `LATEST`)、`mode` | `{ "formId": "...", "formKey": "...", "formName": "...", "formType": "...", "version": 3, "versionId": "...", "status": "PUBLISHED", "fieldCount": 12 }` | `FORM_10002`、`FORM_40001`、`FORM_40002` |
| `GET /form-defs/{formKey}/dictionaries` | Query：`fieldKeys`（逗号分隔）、`version`、`keyword`、`limit`（默认 200） | `{ "dictionaries": { "deptId": [{ "label": "研发中心", "value": "1002", "extra": {} }] } }` | `FORM_40001`、`FORM_50001`、`COMMON_30008` |
| `POST /form-defs/{formKey}/render-meta` | Body：`items`: `[{ "formKey": "leave_form", "version": "LATEST", "mode": "APPROVE" }]`（≤ 20 项） | `{ "items": [{ "formKey": "...", "version": 3, "schemaId": "...", "fieldCount": 12, "checksum": "..." }] }` | `FORM_40002`、`COMMON_10010` |
| `GET /form-schemas/{schemaId}/field-tree` | Query：`onlyEditable`（默认 `false`） | `{ "fields": [{ "fieldKey": "days", "label": "请假天数", "type": "number", "path": "days", "children": [] }] }` | `FORM_40002`、`FORM_20001` |
| `POST /form-data/export` | Header `Idempotency-Key`；Body：`formKey`(必填)、`version`、`instanceIds`(Array\<String\>，≤ 500)、`fields`(Array\<String\>，可选)、`format`(`XLSX`/`CSV`) | `{ "fileUrl": "...", "rowCount": 120, "expireSeconds": 300 }` | `FORM_20001`、`COMMON_10010`、`COMMON_50008` |

**业务规则**：`render-meta` 用于列表页批量渲染（仅返回元数据，前端按需再拉单个 schema）；字典解析结果缓存 5 分钟；导出的实例表单数据来自 ENGINE（通过内部接口按 `instanceId` 批量取 `form_data_json`）。

---

### 2.4 process-engine（执行内核，8083，库 `pe_engine`）

**服务职责**：自研令牌驱动引擎的运行时能力——流程实例生命周期、令牌调度与网关闭环、节点实例与变量、人任务产生与推进、定时器与事件订阅、故障（incident）处理、Saga 补偿、连接器执行，并对外提供外部消息/信号投递与轨迹查询。

**接口总览（31 个）**

| # | 方法 | 路径 | 名称 | 权限码 | 幂等要求 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | POST | `/api/engine/v1/instances` | 启动实例（同步） | `engine:instance:start` | REQUIRED | 支持 `processKey`+`LATEST` 或 `versionId` |
| 2 | POST | `/api/engine/v1/instances/async` | 启动实例（异步） | `engine:instance:start` | REQUIRED | 立即返回 `instanceId`，MQ 推进 |
| 3 | GET | `/api/engine/v1/instances` | 实例分页列表 | `engine:instance:list` | NONE | `CURSOR` 支持 |
| 4 | GET | `/api/engine/v1/instances/{instanceId}` | 实例详情 | `engine:instance:query` | NONE | 含当前节点与阻塞原因 |
| 5 | GET | `/api/engine/v1/instances/by-business-key` | 按 businessKey 查询 | `engine:instance:query` | NONE | 返回活跃实例（0/1 条） |
| 6 | GET | `/api/engine/v1/instances/{instanceId}/variables` | 变量查询 | `engine:instance:query` | NONE | 支持按名精确/前缀 |
| 7 | PUT | `/api/engine/v1/instances/{instanceId}/variables` | 修改变量 | `engine:variable:update` | RECOMMENDED | 支持 upsert 批量 |
| 8 | DELETE | `/api/engine/v1/instances/{instanceId}/variables/{varName}` | 删除变量 | `engine:variable:update` | NONE | Query `scopeId` 可选 |
| 9 | GET | `/api/engine/v1/variable-objects` | 大变量对象获取 | `engine:instance:query` | NONE | Query `varRef`，返回内容或下载链接 |
| 10 | POST | `/api/engine/v1/instances/{instanceId}/suspend` | 挂起实例 | `engine:instance:suspend` | RECOMMENDED | 挂起后不推进令牌 |
| 11 | POST | `/api/engine/v1/instances/{instanceId}/activate` | 恢复实例 | `engine:instance:suspend` | RECOMMENDED | 恢复并继续推进 |
| 12 | POST | `/api/engine/v1/instances/{instanceId}/terminate` | 终止实例 | `engine:instance:terminate` | REQUIRED | 级联取消待办/定时器 |
| 13 | DELETE | `/api/engine/v1/instances/{instanceId}` | 删除实例 | `engine:instance:delete` | NONE | 仅已结束实例 |
| 14 | GET | `/api/engine/v1/instances/{instanceId}/diagram` | 流程图高亮数据 | `engine:instance:query` | NONE | 一次性拉取；增量由 TRACE 的 SSE 推送 |
| 15 | GET | `/api/engine/v1/instances/{instanceId}/node-instances` | 节点实例列表 | `engine:instance:query` | NONE | `CURSOR` 支持 |
| 16 | GET | `/api/engine/v1/node-instances/{nodeInstanceId}` | 节点实例详情 | `engine:instance:query` | NONE | 含输入输出快照 |
| 17 | GET | `/api/engine/v1/instances/{instanceId}/tokens` | 令牌列表 | `engine:instance:query` | NONE | 含作用域与网关汇聚键 |
| 18 | GET | `/api/engine/v1/instances/{instanceId}/incidents` | 故障列表（按实例） | `engine:incident:list` | NONE | 按实例维度 |
| 19 | GET | `/api/engine/v1/incidents` | 故障分页列表（管理视角） | `engine:incident:list` | NONE | 跨实例多条件 |
| 20 | POST | `/api/engine/v1/incidents/{incidentId}/retry` | 故障重试 | `engine:incident:resolve` | RECOMMENDED | 重投节点执行 |
| 21 | POST | `/api/engine/v1/incidents/{incidentId}/skip` | 故障跳过 | `engine:incident:resolve` | RECOMMENDED | 跳过节点继续推进 |
| 22 | POST | `/api/engine/v1/incidents/{incidentId}/cancel` | 故障关闭 | `engine:incident:resolve` | RECOMMENDED | 标记 `CANCELED` 不改流程 |
| 23 | POST | `/api/engine/v1/instances/{instanceId}/compensations` | 触发补偿 | `engine:compensation:trigger` | REQUIRED | Saga 反向执行 |
| 24 | GET | `/api/engine/v1/instances/{instanceId}/compensations` | 补偿记录查询 | `engine:instance:query` | NONE | — |
| 25 | POST | `/api/engine/v1/messages` | 外部消息投递 | `engine:message:send` | REQUIRED | 触发 `MESSAGE_CATCH_EVENT` |
| 26 | POST | `/api/engine/v1/signals` | 信号广播 | `engine:signal:broadcast` | REQUIRED | 全局/按实例广播 |
| 27 | GET | `/api/engine/v1/instances/{instanceId}/timers` | 定时器列表 | `engine:instance:query` | NONE | — |
| 28 | DELETE | `/api/engine/v1/timers/{timerId}` | 取消定时器 | `engine:instance:manage` | NONE | 仅 `PENDING` |
| 29 | GET | `/api/engine/v1/instances/{instanceId}/traces` | 实例轨迹分页 | `engine:instance:query` | NONE | `CURSOR`；读 TRACE 轨迹数据 |
| 30 | POST | `/api/engine/v1/instances/migrate` | 实例版本迁移 | `engine:instance:migrate` | REQUIRED | 批量迁移到目标版本 |
| 31 | GET | `/api/engine/v1/instances/statistics` | 实例运行统计 | `engine:instance:list` | NONE | 实时概览（缓存 30s） |

#### 2.4.1 POST `/api/engine/v1/instances` 启动实例（同步）

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | 建议使用业务单号，如 `LEAVE-2025-0001` |
| Body | `processKey` | String | 条件 | 流程 key | 与 `versionId` 二选一；未指定版本时取 `LATEST` 已发布版本 |
| Body | `versionId` | String | 条件 | 指定版本 ID | 指定后按该版本运行，运行期不再变版本 |
| Body | `version` | Integer | 否 | 指定版本号 | 与 `versionId` 互斥，`versionId` 优先 |
| Body | `businessKey` | String | 否 | 业务键 | ≤ 128 字符（`ENGINE_10003`）；同 `processKey` 下活跃实例唯一 |
| Body | `title` | String | 否 | 实例标题 | 默认取 `processName`，≤ 256 字符 |
| Body | `variables` | Object | 否 | 启动变量 | 键为变量名，值遵循 1.7 序列化约定；需满足 `settings.vars` 定义（`ENGINE_10002`） |
| Body | `initiatorId` | String | 否 | 发起人 | 默认取当前登录用户；系统发起需 `engine:instance:start-system` |
| Body | `initiatorDeptId` | String | 否 | 发起人部门 | 默认取当前用户部门 |
| Body | `parentInstanceId` | String | 否 | 父实例（子流程） | 一般由引擎内部填写 |
| Body | `startNodeId` | String | 否 | 指定起始节点 | 默认取 `START_EVENT`，用于「从中间节点测试启动」 |
| Body | `syncWaitAfterStart` | Boolean | 否 | 同步推进到第一个等待态 | 默认 `true`；`false` 则只落启动记录 |
| Body | `callbackUrl` | String | 否 | 结束回调地址 | 实例结束时 HTTP 回调（见第 4 节补充说明） |

**响应体**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `instanceId` | String | 实例 ID |
| `instanceNo` | String | 业务编号 `PI{yyyyMMdd}{8位序列}` |
| `defId` / `versionId` / `processKey` / `version` | String/Integer | 绑定的定义与版本 |
| `businessKey` / `title` | String | 业务键与标题 |
| `status` | Enum | `RUNNING` / `SUSPENDED` / `COMPLETED` / `TERMINATED` |
| `startTime` | LocalDateTime | 启动时间 |
| `endTime` | LocalDateTime | 结束时间（同步跑到结束时返回） |
| `durationMs` | Long | 耗时（字符串） |
| `currentNodes` | Array\<Object\> | 当前活动节点 `[{ nodeId, nodeName, nodeType, nodeInstanceId, status }]` |
| `activeTokens` | Integer | 活跃令牌数 |
| `pendingTaskCount` | Integer | 生成待办数 |
| `idempotent` | Boolean | 是否命中了 businessKey 幂等（复用已有实例时为 `true`） |
| `existingInstance` | Object | 幂等命中时的已有实例摘要 |

**主要错误码**：`ENGINE_10001`、`ENGINE_10002`、`ENGINE_10003`、`ENGINE_10004`、`ENGINE_20001`、`ENGINE_30003`、`ENGINE_30005`、`ENGINE_30012`、`ENGINE_40006`、`ENGINE_50004`、`ENGINE_50006`。

**业务规则**

1. **版本解析**：`versionId` > `version` > `LATEST`（按 key 取最大 `PUBLISHED` 版本）。无可用发布版本返回 `ENGINE_40006`；定义 `status=DISABLED` 同样返回 `ENGINE_40006`。
2. **businessKey 幂等**：`tenantId + processKey + businessKey` 维度下若存在 `RUNNING/SUSPENDED` 实例，**不新建**，返回 `ENGINE_30003`，并在 `data.existingInstance` 中返回已有实例摘要，便于调用方复用（配合 `Idempotency-Key` 形成双重幂等）。
3. **同步语义**：`syncWaitAfterStart=true` 时引擎在当前请求线程内推进令牌直到第一个等待态（人任务/定时器/消息/异步节点）或实例结束，受 `engine.sync-start-timeout`（默认 5s）保护，超时返回 `ENGINE_50006`（实例已创建，调用方可按 `businessKey` 查询）。
4. **启动变量**：写入根作用域（`scope_id=0`）；超过 `engine.var.inline-max-size`（默认 64 KB）自动外置到 MinIO 并返回 `valueRef`（失败返回 `ENGINE_50005`）。
5. 启动写 `rt_process_instance` + `rt_node_instance`（`START_EVENT`）+ 首个人任务/令牌，同一本地事务内写 `rt_outbox` 投递 `PE_ENGINE_INSTANCE_STARTED`。

#### 2.4.2 POST `/api/engine/v1/instances/async` 启动实例（异步）

**请求体**：同 2.4.1，另加：

| 字段 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- |
| `priority` | Integer | 否 | 命令优先级 | 0–9，默认 5，越大越先调度 |
| `notifyOnComplete` | Boolean | 否 | 结束时发通知事件 | 默认 `false` |
| `resultCallback` | Object | 否 | 结果回调配置 | `{ "url": "...", "headers": {}, "timeoutMs": 3000 }` |

**响应体**：`{ "accepted": true, "instanceId": "...", "instanceNo": "...", "commandId": "cmd-9f8e7d6c", "queuePosition": 12, "estimatedWaitMs": 300 }`。
**主要错误码**：`ENGINE_10007`、`ENGINE_20001`、`ENGINE_30012`、`ENGINE_40006`。
**业务规则**：实例记录与首个命令在**同一事务**内落库（保证不丢），命令通过 `rt_outbox` 投递到 `PE_ENGINE_COMMAND`，由 `engine-worker` 消费推进；`queuePosition` 为队列积压估算值，仅作提示。

#### 2.4.3 GET `/api/engine/v1/instances` 实例分页列表

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `processKey` | String | 否 | 流程 key（可多值） | — |
| Query | `defId` / `versionId` | String | 否 | 定义/版本过滤 | — |
| Query | `instanceNo` / `businessKey` / `title` | String | 否 | 精确/模糊匹配 | `title` 为模糊，其余精确 |
| Query | `status` | Enum | 否 | 实例状态（可多值） | `RUNNING`/`SUSPENDED`/`COMPLETED`/`TERMINATED` |
| Query | `initiatorId` | String | 否 | 发起人 | 默认按数据权限过滤 |
| Query | `currentNodeId` | String | 否 | 当前停留节点 | 走 `current_node_ids` 冗余列 |
| Query | `startTimeFrom` / `startTimeTo` | LocalDateTime | 否 | 启动时间范围 | ≤ 31 天（`COMMON_10008`） |
| Query | `endTimeFrom` / `endTimeTo` | LocalDateTime | 否 | 结束时间范围 | 同上 |
| Query | `onlyMine` | Boolean | 否 | 仅我发起 | 默认 `false` |
| Query | `archived` | Boolean | 否 | 是否查归档表 | 默认 `false`（> 180 天数据） |
| Query | `includeVariables` | Boolean | 否 | 是否附带变量摘要 | 默认 `false`（避免大响应） |
| Query | `pageNum` / `pageSize` / `orderBy` / `orderDirection` / `lastId` / `searchAfter` | — | 否 | 分页（`CURSOR`） | 见 1.6 |

**响应体（`records[]`）**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `instanceId` / `instanceNo` | String | 实例标识 |
| `processKey` / `processName` / `version` | String/Integer | 定义与版本 |
| `businessKey` / `title` | String | 业务信息 |
| `status` / `statusName` | Enum/String | 状态 |
| `currentNodes` | Array\<Object\> | 当前节点摘要（`nodeId`/`nodeName`/`nodeType`） |
| `initiatorId` / `initiatorName` / `initiatorDeptName` | String | 发起人信息 |
| `pendingTaskCount` / `activeTokenCount` | Integer | 待办与令牌数 |
| `startTime` / `endTime` / `durationMs` | LocalDateTime/Long | 时间与耗时 |
| `parentInstanceId` / `rootInstanceId` | String | 父子实例关系 |
| `suspended` | Boolean | 是否挂起 |

**主要错误码**：`ENGINE_10008`、`ENGINE_20001`、`COMMON_10003`、`COMMON_10004`、`COMMON_10008`。
**业务规则**：默认按 `start_time DESC, id DESC`；`countTotal` 默认 `true`，前端滚动加载时传 `countTotal=false`；游标分页不跨归档边界（`archived` 为独立查询域）。

#### 2.4.4 GET `/api/engine/v1/instances/{instanceId}` 实例详情

**响应体在列表字段基础上增加**：`variables`（根作用域变量对象）、`activeTokens`（Array，同 2.4.17 摘要）、`blockingReason`（Enum：`USER_TASK`/`TIMER`/`MESSAGE`/`SIGNAL`/`ASYNC_NODE`/`INCIDENT`/`SUSPENDED`/`NONE`）、`incidentCount`、`compensationStatus`、`formData`（发起表单数据，若为表单发起）、`tags`、`updateTime`、`lockVersion`。
**主要错误码**：`ENGINE_20001`、`ENGINE_40001`、`COMMON_20005`。

#### 2.4.5 GET `/api/engine/v1/instances/by-business-key` 按 businessKey 查询

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `processKey` | String | 是 | 流程 key | — |
| Query | `businessKey` | String | 是 | 业务键 | ≤ 128 字符 |
| Query | `includeEnded` | Boolean | 否 | 是否包含已结束实例 | 默认 `false` |

**响应体**：`{ "found": true, "instance": { ...实例详情摘要... } }`；未找到时 `{ "found": false, "instance": null }`（HTTP 200，**不**返回 404，便于业务侧判空）。
**主要错误码**：`ENGINE_10003`、`ENGINE_20001`、`COMMON_10001`。

#### 2.4.6 GET `/api/engine/v1/instances/{instanceId}/variables` 变量查询

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `names` | String | 否 | 变量名列表，逗号分隔 | ≤ 100 个 |
| Query | `namePrefix` | String | 否 | 前缀模糊 | 与 `names` 互斥 |
| Query | `scopeId` | String | 否 | 作用域（子流程/多实例） | 默认根作用域 `0` |
| Query | `includeLarge` | Boolean | 否 | 是否内联返回大变量内容 | 默认 `false`（只返回 `valueRef`） |
| Query | `withMetadata` | Boolean | 否 | 是否返回变量元数据 | 默认 `true` |

**响应体**：`{ "instanceId": "...", "scopeId": "0", "variables": [{ "name": "days", "type": "NUMBER", "value": 3, "textValue": "3", "valueRef": null, "scopeId": "0", "nodeInstanceId": null, "isLocal": false, "updateTime": "2025-05-20 09:31:02.114" }], "total": 12 }`。
**主要错误码**：`ENGINE_20001`、`ENGINE_40001`、`ENGINE_40007`。
**业务规则**：`value` 为按 `var_type` 反序列化后的 JSON 值；大变量返回 `{ "valueRef": "minio://...", "sizeBytes": "1048576" }`，内容通过 2.4.9 或 MinIO 预签名 URL 获取。

#### 2.4.7 PUT `/api/engine/v1/instances/{instanceId}/variables` 修改变量

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `variables` | Array\<Object\> | 是 | 变量数组 | ≤ 100 项 |
| Body | `variables[].name` | String | 是 | 变量名 | 标识符，≤ 64（`ENGINE_10004`） |
| Body | `variables[].type` | Enum | 是 | 变量类型 | `STRING`/`NUMBER`/`BOOLEAN`/`DATE`/`DATETIME`/`JSON`/`ARRAY`/`FILE` |
| Body | `variables[].value` | Object | 条件 | 变量值 | `type=FILE` 时用 `valueRef` |
| Body | `variables[].valueRef` | String | 条件 | 大对象引用 | — |
| Body | `variables[].scopeId` | String | 否 | 作用域 | 默认 `0`（根） |
| Body | `variables[].isLocal` | Boolean | 否 | 是否局部变量 | 默认 `false` |
| Body | `mode` | Enum | 否 | `UPSERT`（默认）/ `UPDATE_ONLY` | `UPDATE_ONLY` 时目标不存在返回 `COMMON_40001` |
| Body | `triggerRefresh` | Boolean | 否 | 是否触发条件重评估 | 默认 `true`（重评估事件网关/条件边界） |

**响应体**：`{ "updated": 3, "created": 1, "skipped": 0, "refreshTriggered": true, "lockVersion": 14 }`。
**主要错误码**：`ENGINE_10002`、`ENGINE_10004`、`ENGINE_20003`、`ENGINE_30001`（实例已结束）、`ENGINE_40001`、`ENGINE_50005`。
**业务规则**：修改变量会触发 `CONDITION` 类事件订阅重评估（仅 `triggerRefresh=true`）；变量变更写轨迹事件 `VARIABLE_UPDATED`；并发修改以实例锁串行化。

#### 2.4.8 DELETE `/api/engine/v1/instances/{instanceId}/variables/{varName}` 删除变量

**Query**：`scopeId`（可选，默认 `0`）。
**响应体**：`{ "deleted": true, "name": "days" }`。
**主要错误码**：`ENGINE_20003`、`ENGINE_40001`、`COMMON_40001`。

#### 2.4.9 GET `/api/engine/v1/variable-objects` 大变量对象获取

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `varRef` | String | 是 | 对象引用（`minio://...`） | 必须属于当前租户 |
| Query | `mode` | Enum | 否 | `PRESIGN`（默认，返回 5 分钟预签名 URL）/ `INLINE`（≤ 5 MB 直接返回内容） | — |

**响应体**：`PRESIGN` → `{ "mode": "PRESIGN", "url": "https://minio/...", "expireSeconds": 300, "sizeBytes": "1048576", "contentType": "application/json" }`；`INLINE` → 同结构 + `content`。
**主要错误码**：`ENGINE_40007`、`COMMON_20006`、`ENGINE_10005`。

#### 2.4.10–2.4.13 实例状态控制与删除

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `POST /instances/{instanceId}/suspend` | `Idempotency-Key`（RECOMMENDED）；Body：`reason`(可选，≤ 200)、`suspendTasks`(Boolean，默认 `true`，是否同时挂起待办) | `{ "instanceId": "...", "status": "SUSPENDED", "suspendedTasks": 3 }` | `ENGINE_20002`、`ENGINE_30001`、`ENGINE_30002`、`ENGINE_40001` |
| `POST /instances/{instanceId}/activate` | 同上；Body：`reason`(可选)、`resumeFromBlocking`(Boolean，默认 `true`，是否立即推进) | `{ "instanceId": "...", "status": "RUNNING", "resumedTasks": 3, "advanced": true }` | `ENGINE_20002`、`ENGINE_30001`、`ENGINE_30005`、`ENGINE_40001` |
| `POST /instances/{instanceId}/terminate` | `Idempotency-Key`（REQUIRED）；Body：`reason`(必填，≤ 500)、`cancelTasks`(默认 `true`)、`cancelTimers`(默认 `true`)、`triggerCompensation`(默认 `false`，终止前先补偿) | `{ "instanceId": "...", "status": "TERMINATED", "terminatedNodeInstances": 2, "canceledTasks": 3, "canceledTimers": 1, "endTime": "..." }` | `ENGINE_20002`、`ENGINE_20005`、`ENGINE_30001`、`ENGINE_40001`、`ENGINE_50001` |
| `DELETE /instances/{instanceId}` | Query `force`(默认 `false`，仅平台管理员，且须为已结束实例) | `{ "deleted": true }` | `ENGINE_20002`、`ENGINE_30004`、`ENGINE_40001` |

**业务规则**：挂起实例后所有令牌停止调度，待办任务状态不变但不可办理（办理返回 `TASK_30002`）；终止会投递 `PE_ENGINE_INSTANCE_TERMINATED` 并向业务侧 `callbackUrl` 回调（若配置）；`triggerCompensation=true` 时先按补偿序列逆序执行再终止。

#### 2.4.14 GET `/api/engine/v1/instances/{instanceId}/diagram` 流程图高亮数据

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `includeModel` | Boolean | 否 | 是否内联模型 JSON | 默认 `true` |
| Query | `onlyActive` | Boolean | 否 | 仅返回活动节点 | 默认 `false` |
| Query | `withEdgeStats` | Boolean | 否 | 是否统计每条连线通过次数 | 默认 `true` |

**响应体**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `instanceId` / `versionId` / `version` | String/Integer | 实例与版本 |
| `model` | Object | 流程模型（`includeModel=true`） |
| `nodeStates` | Array\<Object\> | `[{ nodeId, nodeType, status, activeCount, totalCount, lastNodeInstanceId, assigneeNames: [], enterTime, durationMs, failCount, loopIndexes: [0,1] }]` |
| `nodeStates[].status` | Enum | `NOT_EXECUTED` / `ACTIVE` / `COMPLETED` / `FAILED` / `SKIPPED` / `COMPENSATED` / `TERMINATED` |
| `edgeStates` | Array\<Object\> | `[{ edgeId, status, passCount, lastPassTime, activeTokens }]` |
| `summary` | Object | `{ activeNodes, completedNodes, failedNodes, skippedNodes, compensatedNodes, progressPercent }` |
| `legend` | Array\<Object\> | 前端色板映射 `[{ status, color, label }]` |
| `dataTime` | LocalDateTime | 数据时间（用于与 SSE 增量对齐） |

**主要错误码**：`ENGINE_20001`、`ENGINE_40001`、`ENGINE_40008`。
**业务规则**：数据源为 `rt_node_instance` 聚合（实时），非 ClickHouse；同一节点多次执行（循环/多实例）通过 `loopIndexes` 区分；本接口为**一次性全量拉取**，实例运行中的增量变更由 `process-trace` 的 `GET /sse/instances/{instanceId}` 推送（见第 3 节），前端先用本接口初始化，再订阅增量并本地合并。

#### 2.4.15–2.4.16 节点实例接口

**节点实例对象字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `nodeInstanceId` | String | 节点实例 ID |
| `instanceId` / `tokenId` | String | 所属实例与令牌 |
| `nodeId` / `nodeName` / `nodeType` | String/Enum | 节点定义与类型（`NodeType` 全集见基线 5.1） |
| `status` | Enum | `ACTIVE` / `COMPLETED` / `FAILED` / `SKIPPED` / `COMPENSATED` |
| `loopIndex` | Integer | 循环/多实例序号 |
| `scopeId` | String | 作用域（子流程/多实例） |
| `assigneeId` / `assigneeName` | String | 办理人（人任务） |
| `retryCount` | Integer | 重试次数 |
| `errorCode` / `errorMsg` | String | 失败信息 |
| `input` / `output` | Object | 输入/输出快照（列表默认省略） |
| `startTime` / `endTime` / `durationMs` | LocalDateTime/Long | 时间 |
| `connectorRef` | Object | 连接器节点：`{ connectorCode, operationCode, callLogId }` |
| `compensated` | Boolean | 是否已被补偿 |

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /instances/{instanceId}/node-instances` | Query：`nodeId`、`nodeType`、`status`、`loopIndex`、`scopeId`、`onlyFailed`、`includeSnapshot`(默认 `false`)、`startTimeFrom/To`、分页（`CURSOR`） | 分页 `records[]` 为节点实例对象 | `ENGINE_20001`、`ENGINE_40001`、`COMMON_10003` |
| `GET /node-instances/{nodeInstanceId}` | Query `includeSnapshot`（默认 `true`）、`includeIncident`（默认 `true`） | 节点实例对象 + `incident`(Object) + `variables`（该节点局部变量） | `ENGINE_20001`、`ENGINE_40002` |

**业务规则**：`input/output` 快照列表默认不返回（响应体积考虑），详情默认返回；快照中的敏感变量按 `engine.snapshot.mask-keys` 配置脱敏。

#### 2.4.17 GET `/api/engine/v1/instances/{instanceId}/tokens` 令牌列表

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `status` | Enum | 否 | `ACTIVE` / `WAITING` / `CONSUMED` / `DEAD` | 默认全部 |
| Query | `scopeId` | String | 否 | 作用域过滤 | — |
| Query | `includeConsumed` | Boolean | 否 | 是否包含已消费令牌 | 默认 `false` |

**响应体**：`{ "instanceId": "...", "records": [{ "tokenId": "...", "nodeId": "n_approve", "nodeInstanceId": "...", "status": "WAITING", "parentTokenId": "...", "scopeId": "0", "gatewayJoinKey": "join_1", "priority": 5, "createTime": "...", "consumeTime": null }], "activeCount": 1, "waitingCount": 1 }`。
**主要错误码**：`ENGINE_20001`、`ENGINE_40001`、`ENGINE_40003`。

#### 2.4.18–2.4.22 故障（incident）接口

**故障对象字段**：`incidentId`、`instanceId`、`instanceNo`、`nodeInstanceId`、`nodeId`、`nodeName`、`nodeType`、`incidentType`（`RETRY_EXHAUSTED`/`EXPRESSION_ERROR`/`CONNECTOR_ERROR`/`UNKNOWN`）、`errorCode`、`errorMsg`、`stackDigest`、`retryCount`、`maxRetryCount`、`status`（`OPEN`/`RESOLVED`/`CANCELED`）、`resolveRemark`、`resolvedBy`、`resolvedTime`、`createTime`、`processKey`、`version`、`title`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /instances/{instanceId}/incidents` | Query：`status`、`incidentType`、分页参数 | 分页 `records[]` | `ENGINE_20004`、`ENGINE_40001` |
| `GET /incidents` | Query：`processKey`、`status`、`incidentType`、`errorCode`、`nodeType`、`startTimeFrom/To`、分页（`CURSOR`） | 分页 `records[]`（跨实例管理视角） | `ENGINE_20004`、`COMMON_10003` |
| `POST /incidents/{incidentId}/retry` | `Idempotency-Key`(RECOMMENDED)；Body：`remark`(≤ 500)、`resetRetryCount`(默认 `true`)、`variables`(可选，重试前覆盖变量) | `{ "incidentId": "...", "status": "RESOLVED", "retryAccepted": true, "newNodeInstanceId": "...", "commandId": "..." }` | `ENGINE_20004`、`ENGINE_30008`、`ENGINE_40004`、`ENGINE_50004` |
| `POST /incidents/{incidentId}/skip` | `Idempotency-Key`(RECOMMENDED)；Body：`remark`(必填)、`skipStrategy`(Enum `CONTINUE`(默认)/`TO_END`/`TO_NODE`)、`targetNodeId`(条件必填) | `{ "incidentId": "...", "status": "RESOLVED", "nodeInstanceStatus": "SKIPPED", "advanced": true }` | `ENGINE_20004`、`ENGINE_30009`、`ENGINE_40004` |
| `POST /incidents/{incidentId}/cancel` | Body：`remark`(必填) | `{ "incidentId": "...", "status": "CANCELED" }` | `ENGINE_20004`、`ENGINE_30008`、`ENGINE_40004` |

**业务规则**：重试创建**新的节点实例**（保留原失败记录，`retryCount` 递增）并投递 `PE_ENGINE_INCIDENT_RESOLVED`；跳过按 `skipStrategy` 决定后续令牌走向，`TO_NODE` 会校验目标节点是当前节点的下游可达节点；故障解决后实例若因 `INCIDENT` 阻塞则自动恢复推进。

#### 2.4.23–2.4.24 补偿接口

**补偿记录字段**：`compensationId`、`instanceId`、`nodeInstanceId`、`nodeId`、`compensateNodeId`、`seqNo`、`status`（`PENDING`/`DONE`/`FAILED`）、`payload`、`errorMsg`、`startTime`、`endTime`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `POST /instances/{instanceId}/compensations` | `Idempotency-Key`（REQUIRED）；Body：`scope`(Enum `ALL`(默认)/`NODE`/`FROM_NODE`)、`nodeId`(条件必填)、`reason`(必填)、`sync`(默认 `false`)、`continueOnError`(默认 `false`) | `{ "instanceId": "...", "triggered": 3, "compensationIds": [], "status": "PENDING", "commandId": "..." }` | `ENGINE_10009`、`ENGINE_20002`、`ENGINE_30010`、`ENGINE_40001`、`ENGINE_50007` |
| `GET /instances/{instanceId}/compensations` | Query：`status`、分页参数 | 分页 `records[]` | `ENGINE_20002`、`ENGINE_40001` |

**业务规则**：补偿按 `seq_no` **逆序**执行已成功完成的自动化节点上的 `COMPENSATION_HANDLER`；`continueOnError=false` 时首个补偿失败即停止并生成 incident；补偿完成后实例状态不变（除非与 terminate 配合调用）。

#### 2.4.25 POST `/api/engine/v1/messages` 外部消息投递

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | 建议业务单号 |
| Body | `messageName` | String | 是 | 消息名 | 对应 `MESSAGE_CATCH_EVENT.properties.messageName`（`ENGINE_10006`） |
| Body | `instanceId` | String | 条件 | 目标实例 | 与 `businessKey` 至少一个 |
| Body | `businessKey` | String | 条件 | 目标业务键 | 需配合 `processKey` |
| Body | `processKey` | String | 否 | 流程 key | 限定匹配范围 |
| Body | `correlationKey` | String | 否 | 关联键 | 用于多实例/子流程精确定位 |
| Body | `variables` | Object | 否 | 投递携带变量 | 合并进实例变量 |
| Body | `timeToLiveMs` | Long | 否 | 消息有效期 | 默认 0（立即投递，不缓冲）；> 0 时无匹配订阅则缓冲等待 |
| Body | `consumeOnce` | Boolean | 否 | 是否只消费一次 | 默认 `true`（`once` 语义） |

**响应体**：`{ "delivered": true, "messageName": "payment_done", "matchedSubscriptions": 1, "triggeredNodeInstanceIds": ["..."], "buffered": false, "instanceId": "..." }`。
**主要错误码**：`ENGINE_10006`、`ENGINE_20001`、`ENGINE_30005`、`ENGINE_30006`、`ENGINE_30007`、`ENGINE_40001`。
**业务规则**：投递前校验实例存在且未结束；`timeToLiveMs=0` 且无匹配订阅返回 `ENGINE_30006`（HTTP 409）；`timeToLiveMs>0` 时消息入缓冲表并返回 `{ "buffered": true }`（HTTP 202）；命中后取消 `EVENT_GATEWAY` 上的其余等待分支。

#### 2.4.26 POST `/api/engine/v1/signals` 信号广播

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Body | `signalName` | String | 是 | 信号名 | `SIGNAL_CATCH_EVENT.properties.signalName` |
| Body | `scope` | Enum | 否 | 广播范围 | `GLOBAL`（默认，当前租户全部订阅）/ `INSTANCE` / `PROCESS` |
| Body | `instanceId` / `processKey` | String | 条件 | 限定范围 | 与 `scope` 对应必填 |
| Body | `variables` | Object | 否 | 携带变量 | — |
| Body | `async` | Boolean | 否 | 是否异步广播 | 默认 `true`（订阅多时避免长事务） |

**响应体**：`{ "broadcast": true, "signalName": "refresh_config", "scope": "GLOBAL", "matchedInstances": 42, "matchedSubscriptions": 45, "commandId": "..." }`。
**主要错误码**：`ENGINE_10006`、`ENGINE_20001`、`ENGINE_30007`、`ENGINE_30012`。

#### 2.4.27–2.4.28 定时器接口

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /instances/{instanceId}/timers` | Query：`status`(`PENDING`/`FIRED`/`CANCELED`)、`timerType`(`DATE`/`DURATION`/`CYCLE`/`CRON`)、分页参数 | 分页 `records[]`：`timerId`、`instanceId`、`nodeInstanceId`、`nodeId`、`nodeName`、`scopeId`、`timerType`、`dueTime`、`repeatInterval`、`repeatLimit`、`repeatCount`、`status`、`payload`、`createTime` | `ENGINE_20001`、`ENGINE_40001`、`ENGINE_40005` |
| `DELETE /timers/{timerId}` | Body：`reason`(可选) | `{ "timerId": "...", "status": "CANCELED" }` | `ENGINE_20002`、`ENGINE_30011`、`ENGINE_40005` |

**业务规则**：`CYCLE` 定时器在 `repeatCount` 达到 `repeatLimit` 后自动转为 `FIRED`；取消定时器同时结束对应的 `TIMER_CATCH_EVENT` 等待（节点实例转为跳过或按边界事件配置处理）。

#### 2.4.29 GET `/api/engine/v1/instances/{instanceId}/traces` 实例轨迹分页

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `eventType` | Enum | 否 | 事件类型（可多值） | 见下表 |
| Query | `nodeInstanceId` / `nodeId` | String | 否 | 节点过滤 | — |
| Query | `startTimeFrom` / `startTimeTo` | LocalDateTime | 否 | 时间范围 | ≤ 31 天 |
| Query | `orderDirection` | Enum | 否 | 排序 | 默认 `ASC`（轨迹按时间正序） |
| Query | `pageNum` / `pageSize` / `lastId` | — | 否 | 分页（`CURSOR`） | 见 1.6 |

**轨迹事件类型（`eventType`）**：`INSTANCE_STARTED`、`NODE_ENTERED`、`NODE_COMPLETED`、`NODE_FAILED`、`TOKEN_CREATED`、`TOKEN_CONSUMED`、`GATEWAY_EVALUATED`、`GATEWAY_JOINED`、`VARIABLE_CREATED`、`VARIABLE_UPDATED`、`TASK_CREATED`、`TASK_CLAIMED`、`TASK_COMPLETED`、`TASK_TRANSFERRED`、`TASK_DELEGATED`、`TASK_WITHDRAWN`、`TIMER_SCHEDULED`、`TIMER_FIRED`、`MESSAGE_DELIVERED`、`SIGNAL_BROADCAST`、`INCIDENT_CREATED`、`INCIDENT_RESOLVED`、`COMPENSATION_EXECUTED`、`INSTANCE_SUSPENDED`、`INSTANCE_ACTIVATED`、`INSTANCE_MIGRATED`、`INSTANCE_ENDED`。

**响应体（`records[]`）**：`execTraceId`、`instanceId`、`nodeInstanceId`、`tokenId`、`eventType`、`nodeId`、`nodeType`、`ts`（毫秒时间戳）、`occurredAt`（LocalDateTime）、`payload`（Object）、`operatorId`、`operatorName`、`traceId`（链路）、`seq`（实例内单调序号）；响应体顶层含 `dataSyncedAt`（已同步水位）。
**主要错误码**：`ENGINE_20001`、`ENGINE_40001`、`ENGINE_40008`、`TRACE_50001`。
**业务规则**：轨迹数据物理存放于 ClickHouse（`tr_exec_trace`，由 `process-trace` 写入），ENGINE 通过 `/api/trace/v1/internal/exec-traces` 内部接口读取，**不直连 ClickHouse**；写入为异步，最近约 5s 的数据可能尚未落库（`dataSyncedAt` 标识）。

#### 2.4.30 POST `/api/engine/v1/instances/migrate` 实例版本迁移

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Body | `instanceIds` | Array\<String\> | 条件 | 待迁移实例 | 与 `filter` 二选一，≤ 1000 |
| Body | `filter` | Object | 条件 | 按条件批量迁移 | `{ processKey, version, status, categoryId, startedBefore }` |
| Body | `targetVersionId` | String | 是 | 目标版本 | 必须为 `PUBLISHED` 且同 `processKey` |
| Body | `migrateActiveNodes` | Boolean | 否 | 是否迁移当前活动节点 | 默认 `true`；`false` 时仅影响后续节点 |
| Body | `nodeMapping` | Object | 否 | 节点映射 `{ "旧nodeId": "新nodeId" }` | 未提供时按 `nodeId` 同名匹配 |
| Body | `variableMapping` | Object | 否 | 变量重命名映射 | — |
| Body | `async` | Boolean | 否 | 异步执行 | 默认 `true`（> 100 实例强制异步） |
| Body | `dryRun` | Boolean | 否 | 预检不执行 | 默认 `false`；`true` 时返回可迁移/不可迁移明细 |

**响应体**：`{ "accepted": true, "taskId": "mig-20250520-001", "total": 42, "migratable": 40, "blocked": 2, "blockedDetails": [{ "instanceId": "...", "reason": "TASK_NODE_MISSING", "nodeId": "n_approve" }] }`。
**主要错误码**：`ENGINE_10008`、`ENGINE_20002`、`ENGINE_30001`、`ENGINE_30005`、`DESIGN_30008`。
**业务规则**：迁移在实例锁内逐实例执行；当前活动节点为人任务时要求目标版本存在对应人任务节点，否则计入 `blocked`；迁移生成轨迹事件 `INSTANCE_MIGRATED` 并写审计。

#### 2.4.31 GET `/api/engine/v1/instances/statistics` 实例运行统计

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `processKey` | String | 否 | 流程 key | 默认全部 |
| Query | `timeRange` | Enum | 否 | `TODAY`(默认)/`LAST_7D`/`LAST_30D`/`CUSTOM` | `CUSTOM` 需传时间 |
| Query | `startTime` / `endTime` | LocalDateTime | 条件 | 自定义范围 | ≤ 31 天 |

**响应体**：`{ "timeRange": "TODAY", "startedCount": 128, "completedCount": 96, "terminatedCount": 3, "runningCount": 29, "suspendedCount": 0, "incidentOpenCount": 4, "avgDurationMs": "1834200", "p95DurationMs": "5400000", "pendingTaskCount": 61, "overdueTaskCount": 5, "dataTime": "2025-05-20 10:15:00.000", "cached": true }`。
**主要错误码**：`ENGINE_20001`、`COMMON_10008`、`TRACE_50001`。

---

### 2.5 process-task（人任务，8083，库 `pe_engine`）

**服务职责**：人任务的查询与办理——待办/已办/抄送、任务详情（表单 + 流程图上下文）、候选人预览、认领/释放/完成/驳回/加签/转办/委托/撤回/催办，以及任务操作记录。物理上由 `process-engine` 承载，路径前缀独立为 `/api/task/v1`（与设计文档时序图一致）。

**接口总览（27 个）**

| # | 方法 | 路径 | 名称 | 权限码 | 幂等要求 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | GET | `/api/task/v1/tasks/todo` | 我的待办 | `task:todo:list` | NONE | `CURSOR` 支持 |
| 2 | GET | `/api/task/v1/tasks/done` | 我的已办 | `task:done:list` | NONE | `CURSOR` 支持 |
| 3 | GET | `/api/task/v1/tasks/copy` | 我的抄送 | `task:copy:list` | NONE | `CURSOR` 支持 |
| 4 | GET | `/api/task/v1/tasks` | 任务列表（管理视角） | `task:task:manage` | NONE | 多条件、跨用户 |
| 5 | GET | `/api/task/v1/tasks/{taskId}` | 任务详情 | `task:task:query` | NONE | 含表单 schema + 数据 + 流程图上下文 |
| 6 | GET | `/api/task/v1/tasks/{taskId}/candidates` | 候选人预览 | `task:task:query` | NONE | 解析后候选人与来源 |
| 7 | POST | `/api/task/v1/tasks/{taskId}/claim` | 认领任务 | `task:task:claim` | REQUIRED | 候选人之一 |
| 8 | POST | `/api/task/v1/tasks/{taskId}/release` | 释放任务 | `task:task:claim` | RECOMMENDED | 回到未认领 |
| 9 | POST | `/api/task/v1/tasks/{taskId}/complete` | 完成任务 | `task:task:complete` | REQUIRED | 同意/驳回 + 表单 + 意见 + 附件 |
| 10 | POST | `/api/task/v1/tasks/batch-approve` | 批量审批 | `task:task:batch-approve` | REQUIRED | ≤ 100 条 |
| 11 | POST | `/api/task/v1/tasks/{taskId}/reject` | 驳回（到节点/发起人） | `task:task:complete` | REQUIRED | 指定目标节点 |
| 12 | POST | `/api/task/v1/tasks/{taskId}/add-sign-before` | 前加签 | `task:task:add-sign` | REQUIRED | 加签链 ≤ 5 层 |
| 13 | POST | `/api/task/v1/tasks/{taskId}/add-sign-after` | 后加签 | `task:task:add-sign` | REQUIRED | 同上 |
| 14 | POST | `/api/task/v1/tasks/{taskId}/transfer` | 转办 | `task:task:transfer` | REQUIRED | 彻底移交 |
| 15 | POST | `/api/task/v1/tasks/{taskId}/delegate` | 委托 | `task:task:delegate` | REQUIRED | 办理后回退确认 |
| 16 | POST | `/api/task/v1/tasks/{taskId}/delegate-confirm` | 委托确认 | `task:task:delegate` | RECOMMENDED | 委托人确认结果 |
| 17 | POST | `/api/task/v1/tasks/{taskId}/withdraw` | 撤回 | `task:task:withdraw` | REQUIRED | 按 `withdrawScope` 校验 |
| 18 | POST | `/api/task/v1/tasks/{taskId}/urge` | 催办 | `task:task:urge` | RECOMMENDED | 24h ≤ 3 次 |
| 19 | GET | `/api/task/v1/tasks/{taskId}/operations` | 任务操作记录 | `task:task:query` | NONE | 全生命周期日志 |
| 20 | POST | `/api/task/v1/tasks/{taskId}/copies` | 追加抄送 | `task:task:copy` | RECOMMENDED | 补发抄送 |
| 21 | PUT | `/api/task/v1/copies/{copyId}/read` | 标记抄送已读 | 登录态 | RECOMMENDED | — |
| 22 | GET | `/api/task/v1/tasks/{taskId}/attachments` | 任务附件列表 | `task:task:query` | NONE | 来自 MinIO 元数据 |
| 23 | POST | `/api/task/v1/tasks/{taskId}/attachments` | 上传任务附件 | `task:task:complete` | RECOMMENDED | ≤ 50 MB/个，≤ 10 个 |
| 24 | DELETE | `/api/task/v1/attachments/{attachmentId}` | 删除附件 | `task:task:complete` | NONE | 仅本人上传且任务未完结 |
| 25 | GET | `/api/task/v1/tasks/{taskId}/form` | 任务表单（schema + 数据） | `task:task:query` | NONE | 轻量入口 |
| 26 | PUT | `/api/task/v1/tasks/{taskId}/form` | 保存任务表单草稿 | 登录态 | RECOMMENDED | 不推进流程 |
| 27 | GET | `/api/task/v1/tasks/statistics` | 任务统计（我的/全局） | `task:todo:list` | NONE | 待办数、超期数、按流程分布 |

#### 2.5.1–2.5.4 任务列表类接口

**任务列表对象字段（`records[]`）**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `taskId` | String | 任务 ID |
| `taskDefKey` / `taskName` | String | 任务定义键与名称 |
| `taskType` / `multiMode` | Enum | `SINGLE`/`MULTI`；`ALL`/`ANY`/`PERCENT` |
| `instanceId` / `instanceNo` / `businessKey` / `title` | String | 所属实例信息 |
| `processKey` / `processName` / `version` | String/Integer | 流程信息 |
| `nodeId` / `nodeName` | String | 节点信息 |
| `assigneeId` / `assigneeName` | String | 当前办理人（未认领为 `null`） |
| `candidateSummary` | Array\<String\> | 候选人摘要（前 5 个名字 + `+N`） |
| `status` | Enum | `CREATED` / `CLAIMED` / `COMPLETED` / `DELEGATED` / `TRANSFERRED` / `CANCELED` / `TIMEOUT` |
| `result` | Enum | `APPROVE` / `REJECT`（已办返回） |
| `opinion` | String | 办理意见（已办返回，可截断） |
| `priority` | Integer | 优先级 0–9 |
| `dueTime` | LocalDateTime | 到期时间 |
| `overdue` | Boolean | 是否超期 |
| `createTime` / `claimTime` / `completeTime` | LocalDateTime | 时间线 |
| `initiatorName` | String | 发起人 |
| `delegatorId` / `delegatorName` | String | 委托人（委托场景） |
| `attachmentCount` / `copyCount` | Integer | 附件与抄送数 |
| `durationMs` | Long | 任务从创建到完成的耗时（已办） |

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /tasks/todo` | Query：`keyword`(标题/发起人/业务键)、`processKey`(多值)、`taskName`、`priority`、`overdue`、`createTimeFrom/To`、`dueTimeFrom/To`、`categoryId`、`includeClaimed`(默认 `true`)、`includeCandidate`(默认 `true`，含我可认领的未认领任务)、分页（`CURSOR`） | 分页 `records[]`；`data.counts = { all, overdue, urgent }` | `TASK_20001`、`TASK_20006`、`COMMON_10003/10008` |
| `GET /tasks/done` | Query：`keyword`、`processKey`、`result`、`operType`(`COMPLETE`/`TRANSFER`/`DELEGATE`/`ADD_SIGN`/`WITHDRAW`)、`completeTimeFrom/To`、分页（`CURSOR`） | 分页 `records[]` | `TASK_20001`、`COMMON_10003` |
| `GET /tasks/copy` | Query：`keyword`、`processKey`、`readStatus`(`READ`/`UNREAD`)、`createTimeFrom/To`、分页（`CURSOR`） | 分页 `records[]` + `unreadCount` | `TASK_20001`、`COMMON_10003` |
| `GET /tasks` | Query：`assigneeId`、`candidateUserId`、`status`(多值)、`processKey`、`instanceId`、`nodeId`、`deptId`、`tenantId`(平台级)、`overdue`、`priority`、`createTimeFrom/To`、`completeTimeFrom/To`、`orderBy`(默认 `create_time`)、分页（`CURSOR`） | 分页 `records[]`（含 `tenantId`、`assigneeName`） | `TASK_20003`、`COMMON_10003/10004/10008` |

**业务规则**：`/tasks/todo` 返回「已认领给我」∪「未认领且我具备候选资格」；`includeCandidate=false` 时仅返回前者。数据权限 `SELF` 强制 `assigneeId=当前用户`；`/tasks` 管理视角受 `data_scope` 约束，`ALL` 才可跨部门，跨租户仅平台管理员可用。

#### 2.5.5 GET `/api/task/v1/tasks/{taskId}` 任务详情

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `includeForm` | Boolean | 否 | 是否内联表单 schema 与数据 | 默认 `true` |
| Query | `includeDiagram` | Boolean | 否 | 是否内嵌流程图高亮数据 | 默认 `false` |
| Query | `includeOperations` | Boolean | 否 | 是否内联操作记录 | 默认 `false` |
| Query | `includeAttachments` | Boolean | 否 | 是否内联附件元数据 | 默认 `true` |

**响应体**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `task` | Object | 任务对象（字段同 2.5.1，含 `formDefId`/`formVersion`/`assigneeExpr`） |
| `instance` | Object | 实例摘要（`instanceId`/`instanceNo`/`processKey`/`processName`/`version`/`status`/`businessKey`/`title`/`initiatorName`/`startTime`） |
| `form` | Object | `{ formKey, formName, version, mode, schema: {...}, data: {...}, dataPermission: { editableFields, hiddenFields } }` |
| `diagram` | Object | 流程图高亮数据（同 2.4.14 结构） |
| `candidates` | Object | `{ users, roles, depts, groups, resolvedUsers, emptyStrategy }` |
| `operations` | Array\<Object\> | 操作记录（同 2.5.19） |
| `attachments` | Array\<Object\> | `[{ attachmentId, fileName, fileSize, contentType, uploaderId, uploaderName, uploadTime, url }]` |
| `permissions` | Object | 当前用户可用动作：`{ canClaim, canComplete, canReject, canAddSign, canTransfer, canDelegate, canWithdraw, canUrge, reason }` |
| `countersign` | Object | 会签进度：`{ multiMode, total, completed, percent, pendingUsers }` |
| `timeout` | Object | `{ dueTime, overdue, remindCount, timeoutPolicy }` |

**主要错误码**：`TASK_20002`、`TASK_20006`、`TASK_40001`、`TASK_50002`、`FORM_40002`。

#### 2.5.6 GET `/api/task/v1/tasks/{taskId}/candidates` 候选人预览

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `resolveUsers` | Boolean | 否 | 是否解析为具体用户列表 | 默认 `true` |
| Query | `keyword` | String | 否 | 用户关键字过滤 | ≤ 64 字符 |
| Query | `limit` | Integer | 否 | 返回上限 | 默认 200，最大 1000 |

**响应体**：`{ "taskId": "...", "assigneeStrategy": "ROLE", "assigneeExpr": null, "candidateRoles": ["dept_manager"], "candidateDepts": [], "candidateGroups": [], "candidateUsers": [], "resolvedUsers": [{ "userId": "...", "username": "lisi", "nickname": "李四", "deptName": "研发中心", "source": "ROLE:dept_manager", "skipped": false, "skipReason": null }], "total": 3, "emptyStrategy": "ADMIN", "deduped": true, "autoSkipApplied": true }`。
**主要错误码**：`TASK_20002`、`TASK_40001`、`TASK_40002`、`TASK_50002`。
**业务规则**：解析顺序为「策略解析 → 去重（`dedupe`）→ 自动跳过（`autoSkipSameApprover`）→ 空结果兜底（`emptyStrategy`：`ADMIN`/`SKIP`/`EXCEPTION`）」；`skipped=true` 的候选人会在派发时自动通过，此处提前预览便于排障。

#### 2.5.7 POST `/api/task/v1/tasks/{taskId}/claim` 认领任务

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Body | `userId` | String | 否 | 指定认领人 | 默认当前用户；管理员代领需 `task:task:manage` |
| Body | `remark` | String | 否 | 备注 | ≤ 200 字符 |

**响应体**：`{ "taskId": "...", "status": "CLAIMED", "assigneeId": "...", "assigneeName": "李四", "claimTime": "2025-05-20 10:20:00.000", "lockVersion": 4 }`。
**主要错误码**：`TASK_20002`、`TASK_30001`、`TASK_30002`、`TASK_40001`、`COMMON_30003`。
**业务规则**：仅候选人可认领；已有人认领时返回 `TASK_30001`；认领写 `rt_task_oper_log`（`CLAIM`）并投递 `PE_ENGINE_TASK_CLAIMED`（可选订阅）。

#### 2.5.8 POST `/api/task/v1/tasks/{taskId}/release` 释放任务

**请求体**：`{ "reason": "临时出差", "clearFormDraft": true }`。
**响应体**：`{ "taskId": "...", "status": "CREATED", "assigneeId": null, "releaseTime": "..." }`。
**主要错误码**：`TASK_20002`、`TASK_30002`、`TASK_40001`。

#### 2.5.9 POST `/api/task/v1/tasks/{taskId}/complete` 完成任务

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | 重复提交返回首次结果 |
| Body | `result` | Enum | 是 | `APPROVE` / `REJECT` | — |
| Body | `opinion` | String | 条件 | 办理意见 | `REJECT` 必填；≤ 1000 字符（`TASK_10001`） |
| Body | `formData` | Object | 否 | 表单数据 | 服务端按表单 schema 校验（`TASK_10007`） |
| Body | `variables` | Object | 否 | 额外流程变量 | 与表单数据合并（同名以 `formData` 为准） |
| Body | `attachmentIds` | Array\<String\> | 否 | 已上传附件 ID | ≤ 10 个 |
| Body | `rejectTo` | Object | 条件 | 驳回目标 | `result=REJECT` 时：`{ "strategy": "TO_NODE"/"TO_START"/"TO_PREV", "targetNodeId": "n_start", "targetTaskId": "可选" }` |
| Body | `nextAssigneeOverride` | Object | 否 | 指定下一节点办理人 | `{ "nodeId": "n_next", "userIds": [] }` |
| Body | `signatureData` | String | 否 | 电子签名数据（base64） | ≤ 200 KB，需租户开启 |
| Body | `lockVersion` | Integer | 否 | 任务版本（乐观锁） | 与 `If-Match` 等效 |

**响应体**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `taskId` | String | 任务 ID |
| `status` | Enum | `COMPLETED` |
| `result` | Enum | `APPROVE` / `REJECT` |
| `completeTime` | LocalDateTime | 完成时间 |
| `instanceId` / `instanceStatus` | String/Enum | 实例状态（可能已 `COMPLETED`） |
| `advanced` | Boolean | 是否推进了下游 |
| `nextNodes` | Array\<Object\> | 下游生成节点 `[{ nodeId, nodeName, nodeType, nodeInstanceId }]` |
| `nextTasks` | Array\<Object\> | 下游生成任务 `[{ taskId, taskName, assigneeId, assigneeName, candidateSummary }]` |
| `ended` | Boolean | 实例是否已结束 |
| `durationMs` | Long | 本次办理耗时 |
| `countersign` | Object | 会签进度（会签节点未终结时返回） |
| `warnings` | Array\<String\> | 提示（如「下一节点候选人为空，已按 ADMIN 兜底」） |

**主要错误码**：`TASK_10001`、`TASK_10007`、`TASK_20002`、`TASK_30002`、`TASK_30004`、`TASK_30005`、`TASK_30006`、`TASK_30010`、`TASK_40001`、`TASK_50001`、`ENGINE_30005`。

**业务规则**

1. **幂等**：同 `Idempotency-Key` 重复提交返回首次 `data`，并回写响应头 `X-Idempotent-Replay: true`。
2. **会签/或签**：`ALL` 模式下本次完成仅记录，最后一个办理人完成时才推进；`ANY` 模式下首个完成即推进，其余任务置 `CANCELED`（后到者感知 `TASK_30006`）；`PERCENT` 模式达到阈值比例即推进。
3. **表单校验**：`formData` 必须满足任务绑定的 `formDefId + formVersion` schema；校验失败返回 `TASK_10007` 且**不**推进流程。
4. **驳回**：`TO_START` 回到发起人（重新生成发起任务）；`TO_NODE` 回到指定历史节点，其间已办任务置为「作废重办」并记录操作日志；驳回触发引擎回退令牌。
5. **事务边界**：任务完成与引擎推进在同一事务（`TaskCompleteCommand`），失败整体回滚并返回 `TASK_50001`。
6. 完成后写 `rt_task_oper_log`（`COMPLETE`/`REJECT`），投递 `PE_ENGINE_TASK_COMPLETED`，并触发下一节点通知（由 NOTIFY 订阅规则消费）。

#### 2.5.10 POST `/api/task/v1/tasks/batch-approve` 批量审批

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Body | `taskIds` | Array\<String\> | 是 | 待办理任务 | ≤ 100（`TASK_10002`） |
| Body | `result` | Enum | 是 | `APPROVE` / `REJECT` | 批量驳回仅支持 `TO_START` |
| Body | `opinion` | String | 条件 | 统一意见 | `REJECT` 必填；可被 `opinionMap` 覆盖 |
| Body | `opinionMap` | Object | 否 | 按任务覆盖意见 | `{ "taskId": "意见" }` |
| Body | `formDataMap` | Object | 否 | 按任务覆盖表单数据 | `{ "taskId": { ... } }` |
| Body | `atomic` | Boolean | 否 | 是否原子（全成功或全回滚） | 默认 `true` |
| Body | `skipInvalid` | Boolean | 否 | 跳过无权限/已办任务 | 默认 `false` |

**响应体**：`{ "total": 5, "successCount": 4, "failCount": 1, "atomic": true, "rolledBack": false, "results": [{ "taskId": "...", "success": true, "instanceId": "...", "nextTaskCount": 1 }, { "taskId": "...", "success": false, "errorCode": "TASK_30002", "errorMessage": "任务状态不允许该操作" }] }`。
**主要错误码**：`TASK_10002`、`TASK_20004`、`TASK_30011`、`COMMON_10010`。
**业务规则**：`atomic=true` 时任一失败整体回滚并返回 `TASK_30011`；单批最多跨 20 个实例（超限需拆批）；批量审批要求任务属于当前用户或当前用户具备 `task:task:manage`。

#### 2.5.11 POST `/api/task/v1/tasks/{taskId}/reject` 驳回（独立入口）

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Body | `strategy` | Enum | 是 | `TO_START` / `TO_NODE` / `TO_PREV` | — |
| Body | `targetNodeId` | String | 条件 | `TO_NODE` 必填 | 必须是当前节点可达的**历史**节点（`TASK_10004`） |
| Body | `opinion` | String | 是 | 驳回意见 | ≤ 1000 字符 |
| Body | `variables` | Object | 否 | 覆盖变量 | — |
| Body | `rejectToInitiatorMode` | Enum | 否 | `RESTART`（默认，回到开始前重提）/ `BACK_TO_START_TASK` | 到发起人的两种语义 |

**响应体**：`{ "taskId": "...", "status": "COMPLETED", "result": "REJECT", "rejectedTo": { "strategy": "TO_NODE", "nodeId": "n_start", "nodeName": "开始", "newTaskIds": [] }, "canceledTaskCount": 2, "instanceStatus": "RUNNING" }`。
**主要错误码**：`TASK_10001`、`TASK_10004`、`TASK_20002`、`TASK_30002`、`TASK_40001`、`ENGINE_30005`。

#### 2.5.12–2.5.13 加签接口

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `POST /tasks/{taskId}/add-sign-before` | `Idempotency-Key`（REQUIRED）；Body：`userIds`(Array\<String\>，必填，≤ 20)、`opinion`(可选)、`addSignMode`(Enum `SERIAL`(默认)/`PARALLEL`)、`requiredCount`(条件，`PARALLEL` 时的会签数) | `{ "parentTaskId": "...", "addedTaskIds": ["..."], "addSignType": "ADD_SIGN_BEFORE", "signChainDepth": 2, "newTasks": [{ "taskId": "...", "assigneeName": "王五" }] }` | `TASK_10005`、`TASK_20002`、`TASK_30009`、`TASK_40001` |
| `POST /tasks/{taskId}/add-sign-after` | 同上 | 同上（`addSignType=ADD_SIGN_AFTER`） | 同上 |

**业务规则**：前加签在**当前办理人之前**插入节点（当前任务转为等待），后加签在**当前办理人之后**插入；加签链深度上限 5（`TASK_30009`）；`SERIAL` 按 `userIds` 顺序串行生成任务，`PARALLEL` 并行生成且在满足 `requiredCount` 后推进；加签任务以 `parent_task_id`/`root_task_id` 组织树。

#### 2.5.14 POST `/api/task/v1/tasks/{taskId}/transfer` 转办

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Body | `targetUserId` | String | 是 | 受让人 | 必须存在且启用（`TASK_10005`） |
| Body | `reason` | String | 是 | 转办原因 | ≤ 500 字符（`TASK_10008`） |
| Body | `keepCandidate` | Boolean | 否 | 是否保留原候选人 | 默认 `false` |
| Body | `notify` | Boolean | 否 | 是否通知受让人 | 默认 `true` |

**响应体**：`{ "taskId": "...", "status": "CLAIMED", "assigneeId": "...", "assigneeName": "...", "transferTime": "...", "previousAssigneeId": "..." }`。
**主要错误码**：`TASK_10005`、`TASK_10008`、`TASK_20002`、`TASK_30002`、`TASK_40001`。
**业务规则**：转办为**彻底移交**，原办理人不再拥有该任务（`assignee` 直接变更，操作日志记 `TRANSFERRED`，任务状态随即回到 `CLAIMED` 给新办理人）；不产生回退确认。

#### 2.5.15 POST `/api/task/v1/tasks/{taskId}/delegate` 委托

**请求体**：`targetUserId`(必填)、`reason`(必填，≤ 500)、`notify`(默认 `true`)、`deadlineTime`(可选)。
**响应体**：`{ "taskId": "...", "status": "DELEGATED", "assigneeId": "被委托人ID", "delegatorId": "原办理人ID", "delegateTime": "...", "expectConfirm": true }`。
**主要错误码**：`TASK_10006`、`TASK_10008`、`TASK_20002`、`TASK_30002`、`TASK_40001`、`COMMON_30004`。
**业务规则**：委托**不**移交所有权——被委托人办理后任务回到委托人处等待**确认**；委托链只允许一层（被委托人不可再委托，返回 `COMMON_30004`）；不允许委托给自己（`TASK_10006`）。

#### 2.5.16 POST `/api/task/v1/tasks/{taskId}/delegate-confirm` 委托确认

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `decision` | Enum | 是 | `ACCEPT`（确认，推进流程）/ `REJECT`（不认可，回到自己重办） | — |
| Body | `opinion` | String | 条件 | 意见 | `REJECT` 必填 |
| Body | `formData` | Object | 否 | 覆盖表单数据 | 校验同 2.5.9 |

**响应体**：`{ "taskId": "...", "status": "COMPLETED", "decision": "ACCEPT", "advanced": true, "nextTasks": [] }`。
**主要错误码**：`TASK_20007`、`TASK_30002`、`TASK_30008`、`TASK_40001`、`TASK_50001`。

#### 2.5.17 POST `/api/task/v1/tasks/{taskId}/withdraw` 撤回

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Body | `reason` | String | 是 | 撤回原因 | ≤ 500 字符 |
| Body | `scope` | Enum | 否 | `FIRST_TASK`（默认，撤回到首个人工节点）/ `PREV_TASK` / `TO_NODE` | 不得超出流程 `settings.withdrawScope` |
| Body | `targetNodeId` | String | 条件 | `TO_NODE` 必填 | — |
| Body | `force` | Boolean | 否 | 强制作废下游已办任务 | 默认 `false`，需 `task:task:withdraw-force` |

**响应体**：`{ "instanceId": "...", "withdrawn": true, "withdrawnTaskIds": ["..."], "canceledTaskIds": ["..."], "restoredTaskId": "...", "currentNodes": [] }`。
**主要错误码**：`TASK_20002`、`TASK_30007`、`TASK_40001`、`ENGINE_30005`、`COMMON_30004`。
**业务规则**：仅发起人或当前在办人可撤回；下游任务已办结时默认拒绝（`TASK_30007`），`force=true` 时下游已办记录标记为「撤回作废」并保留审计；撤回受 `settings.allowWithdraw` 与 `settings.withdrawScope` 双重约束。

#### 2.5.18 POST `/api/task/v1/tasks/{taskId}/urge` 催办

**请求体**：`channel`(Enum `INBOX`(默认)/`EMAIL`/`SMS`/`DINGTALK`/`WECOM`/`FEISHU`)、`content`(可选，≤ 500)、`urgeAll`(Boolean，默认 `false`，是否催办全部办理人)。
**响应体**：`{ "taskId": "...", "urged": true, "urgedUsers": [{ "userId": "...", "channel": "INBOX" }], "urgeCountIn24h": 1, "messageIds": ["..."] }`。
**主要错误码**：`TASK_10010`、`TASK_20005`、`TASK_30002`、`TASK_40001`、`NOTIFY_50001`。
**业务规则**：同一任务 24h 内催办 ≤ 3 次（`TASK_10010`）；催办通过 NOTIFY 内部接口发送，写操作日志 `URGE`。

#### 2.5.19 GET `/api/task/v1/tasks/{taskId}/operations` 任务操作记录

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `operType` | Enum | 否 | 操作类型过滤 | `CLAIM`/`COMPLETE`/`REJECT`/`DELEGATE`/`TRANSFER`/`ADD_SIGN_BEFORE`/`ADD_SIGN_AFTER`/`WITHDRAW`/`URGE`/`CANCEL`/`TIMEOUT_REMIND` |
| Query | `pageNum` / `pageSize` | — | 否 | 分页 | 默认按 `oper_time DESC` |

**响应体（`records[]`）**：`operLogId`、`taskId`、`operType`、`operTypeName`、`operatorId`、`operatorName`、`operatorDeptName`、`targetUserId`、`targetUserName`、`opinion`、`formDataDigest`、`attachments`、`operTime`、`durationMs`（与上一操作的间隔）、`traceId`。
**主要错误码**：`TASK_20002`、`TASK_40001`、`TASK_40003`。

#### 2.5.20–2.5.24 抄送与附件接口

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `POST /tasks/{taskId}/copies` | `Idempotency-Key`（RECOMMENDED）；Body：`userIds`(Array\<String\>，必填，≤ 50)、`remark`(可选)、`notify`(默认 `true`) | `{ "copyIds": ["..."], "copiedCount": 3 }` | `TASK_10009`、`TASK_20002`、`TASK_40001` |
| `PUT /copies/{copyId}/read` | Body：`read`(Boolean，默认 `true`) | `{ "copyId": "...", "read": true, "readTime": "..." }` | `TASK_20001`、`TASK_40004` |
| `GET /tasks/{taskId}/attachments` | Query：`uploaderId`、`fileType` | `{ "attachments": [{ "attachmentId": "...", "fileName": "发票.pdf", "fileSize": "204800", "contentType": "application/pdf", "uploaderId": "...", "uploaderName": "...", "uploadTime": "...", "url": "预签名URL", "source": "UPLOAD" }] }` | `TASK_20002`、`TASK_40001`、`COMMON_50008` |
| `POST /tasks/{taskId}/attachments` | Form：`file`(必填，单个 ≤ 50 MB)、`bizType`(Enum `OPINION`(默认)/`FORM`/`SIGNATURE`)；支持多文件（≤ 10） | `{ "attachmentIds": ["..."], "attachments": [附件元数据] }` | `TASK_10003`、`TASK_20002`、`TASK_30002`、`COMMON_50008` |
| `DELETE /attachments/{attachmentId}` | — | `{ "deleted": true }` | `TASK_20002`、`TASK_30002`、`COMMON_40001` |

#### 2.5.25–2.5.26 任务表单接口

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /tasks/{taskId}/form` | Query：`withData`(默认 `true`)、`withDraft`(默认 `true`) | `{ "taskId": "...", "formKey": "...", "version": 3, "mode": "APPROVE", "schema": {...}, "data": {...}, "draft": {...}, "dataPermission": {...} }` | `TASK_20002`、`TASK_40001`、`FORM_40002` |
| `PUT /tasks/{taskId}/form` | Body：`formData`(Object，必填)、`partial`(Boolean，默认 `true`，局部保存) | `{ "taskId": "...", "saved": true, "saveTime": "..." }`（**不推进流程**） | `TASK_10007`、`TASK_20002`、`TASK_30002`、`TASK_50003` |

**业务规则**：草稿存于 `rt_task.form_data_json` 的 `__draft` 分区（不对流程变量生效），完成任务时以提交的 `formData` 覆盖并写入流程变量；草稿仅本人可见。

#### 2.5.27 GET `/api/task/v1/tasks/statistics` 任务统计

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `scope` | Enum | 否 | `MINE`（默认）/ `GLOBAL` | `GLOBAL` 需 `task:task:manage` |
| Query | `processKey` | String | 否 | 流程过滤 | — |
| Query | `timeRange` | Enum | 否 | `TODAY`（默认）/ `LAST_7D` / `LAST_30D` | — |

**响应体**：`{ "scope": "MINE", "todoCount": 12, "candidateCount": 5, "overdueCount": 2, "urgentCount": 1, "doneTodayCount": 8, "doneTotalCount": 341, "avgHandleDurationMs": "7200000", "byProcess": [{ "processKey": "leave_approval", "processName": "请假审批", "todoCount": 7, "doneCount": 120 }], "byPriority": [{ "priority": 9, "count": 1 }] }`。
**主要错误码**：`TASK_20001`、`TASK_20003`、`COMMON_10008`。

---

### 2.6 process-scheduler（调度，8084，库 `pe_scheduler`）

**服务职责**：cron / 固定频率 / 一次性作业的注册与触发，作业目标可为流程实例、连接器或 Spring Bean；维护 misfire 策略、并发策略、超时与重试，提供执行记录、手动补跑与执行日志。定时器底座与引擎定时器共用（Redis ZSet 时间轮 + DB 持久化）。

**接口总览（15 个）**

| # | 方法 | 路径 | 名称 | 权限码 | 幂等要求 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | GET | `/api/sched/v1/jobs` | 作业分页列表 | `sched:job:list` | NONE | 支持分组/状态/关键字 |
| 2 | GET | `/api/sched/v1/jobs/{jobId}` | 作业详情 | `sched:job:query` | NONE | 含下次触发时间 |
| 3 | POST | `/api/sched/v1/jobs` | 新增作业 | `sched:job:create` | REQUIRED | 校验 cron 与目标引用 |
| 4 | PUT | `/api/sched/v1/jobs/{jobId}` | 修改作业 | `sched:job:update` | NONE | `If-Match: required` |
| 5 | DELETE | `/api/sched/v1/jobs/{jobId}` | 删除作业 | `sched:job:delete` | NONE | 运行中需先暂停 |
| 6 | POST | `/api/sched/v1/jobs/{jobId}/pause` | 暂停作业 | `sched:job:pause` | RECOMMENDED | — |
| 7 | POST | `/api/sched/v1/jobs/{jobId}/resume` | 恢复作业 | `sched:job:pause` | RECOMMENDED | 重算 `next_fire_time` |
| 8 | POST | `/api/sched/v1/jobs/{jobId}/trigger` | 立即执行一次 | `sched:job:trigger` | REQUIRED | 不改变调度计划 |
| 9 | POST | `/api/sched/v1/jobs/{jobId}/catch-up` | 手动补跑 | `sched:job:trigger` | REQUIRED | 指定时间段补跑 |
| 10 | GET | `/api/sched/v1/job-executions` | 执行记录列表 | `sched:execution:list` | NONE | `CURSOR` 支持 |
| 11 | GET | `/api/sched/v1/job-executions/{executionId}` | 执行记录详情 | `sched:execution:query` | NONE | 含结果与错误 |
| 12 | POST | `/api/sched/v1/job-executions/{executionId}/rerun` | 重跑失败记录 | `sched:job:trigger` | REQUIRED | 仅 `FAILED`/`TIMEOUT` |
| 13 | GET | `/api/sched/v1/job-executions/{executionId}/logs` | 执行日志 | `sched:execution:query` | NONE | 分片日志，支持游标 |
| 14 | POST | `/api/sched/v1/jobs/{jobId}/validate-cron` | cron 表达式校验 | `sched:job:query` | NONE | 返回未来 5 次触发时间 |
| 15 | GET | `/api/sched/v1/jobs/statistics` | 作业运行统计 | `sched:job:list` | NONE | 成功率、延迟、积压 |

#### 2.6.1 作业对象与列表接口

**作业对象字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `jobId` / `jobCode` / `jobName` | String | 作业标识（`jobCode` 租户内唯一） |
| `jobGroup` | String | 分组，如 `PROCESS_TRIGGER`、`DATA_SYNC` |
| `targetType` | Enum | `PROCESS` / `CONNECTOR` / `BEAN` |
| `targetRef` | String | 目标引用：流程 key / `{connectorCode}:{operationCode}` / Bean 名 |
| `cronExpr` | String | cron 表达式（与 `fixedRate` 互斥） |
| `fixedRate` | Long | 固定频率（秒），与 `cronExpr` 互斥，最小 10s |
| `startTime` / `endTime` | LocalDateTime | 生效时间窗口 |
| `params` | Object | 触发参数（流程启动变量 / 连接器入参） |
| `businessKeyExpr` | String | businessKey 表达式（如 `"JOB-${yyyyMMdd}"`），保证按业务幂等启动 |
| `misfirePolicy` | Enum | `SKIP` / `FIRE_ONCE` / `CATCH_UP` |
| `concurrencyPolicy` | Enum | `ALLOW` / `FORBID` / `REPLACE` |
| `timeoutSec` | Integer | 单次执行超时，1–86400 |
| `retryPolicy` | Object | `{ maxRetries: 3, intervalSec: 60, backoff: "FIXED"|"EXPONENTIAL" }` |
| `status` | Enum | `RUNNING` / `PAUSED` |
| `nextFireTime` / `lastFireTime` | LocalDateTime | 触发时间 |
| `lastExecutionStatus` | Enum | `SUCCESS` / `FAILED` / `RUNNING` / `TIMEOUT` |
| `owner` | String | 抢占节点标识（分布式执行） |
| `createTime` / `updateTime` / `version` | LocalDateTime/Integer | 审计与乐观锁 |

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /jobs` | Query：`keyword`(编码/名称)、`jobGroup`、`status`、`targetType`、`lastExecutionStatus`、`nextFireTimeFrom/To`、分页参数 | 分页 `records[]` 为作业对象 | `SCHED_20001`、`COMMON_10003/10004` |
| `GET /jobs/{jobId}` | Query `withRecentExecutions`（默认 `false`，附带最近 5 次） | 作业对象 + `recentExecutions[]` | `SCHED_20001`、`SCHED_40001` |

#### 2.6.2 作业 CRUD 与状态控制

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `POST /jobs` | `Idempotency-Key`（REQUIRED）；Body：`jobCode`(必填)、`jobName`(必填)、`jobGroup`、`targetType`(必填)、`targetRef`(必填)、`cronExpr`/`fixedRate`（二选一必填）、`startTime`/`endTime`、`params`、`businessKeyExpr`、`misfirePolicy`(默认 `SKIP`)、`concurrencyPolicy`(默认 `FORBID`)、`timeoutSec`(默认 300)、`retryPolicy`、`status`(默认 `RUNNING`)、`remark` | `{ "jobId": "...", "jobCode": "...", "nextFireTime": "...", "status": "RUNNING" }` | `SCHED_10001`~`SCHED_10009`、`SCHED_20001`、`SCHED_30003`、`COMMON_30005` |
| `PUT /jobs/{jobId}` | `If-Match: required`；Body 同上（`jobCode` 不可改）；修改 cron 会重算 `nextFireTime` | 作业对象 | `SCHED_10001`~`SCHED_10009`、`SCHED_20001`、`SCHED_40001`、`COMMON_30003` |
| `DELETE /jobs/{jobId}` | Query `force`（默认 `false`，仅平台管理员；`RUNNING` 状态需 `force` 且会先暂停） | `{ "deleted": true }` | `SCHED_20001`、`SCHED_30001`、`SCHED_40001` |
| `POST /jobs/{jobId}/pause` | `Idempotency-Key`(RECOMMENDED)；Body：`reason`(可选) | `{ "jobId": "...", "status": "PAUSED", "pausedAt": "..." }` | `SCHED_20001`、`SCHED_30001`、`SCHED_40001` |
| `POST /jobs/{jobId}/resume` | `Idempotency-Key`(RECOMMENDED)；Body：`resumeFrom`(Enum `NOW`(默认)/`NEXT_CRON`) | `{ "jobId": "...", "status": "RUNNING", "nextFireTime": "..." }` | `SCHED_20001`、`SCHED_30002`、`SCHED_40001` |
| `POST /jobs/{jobId}/validate-cron` | Body：`cronExpr`(必填)；返回未来 5 次触发时间 | `{ "valid": true, "nextFireTimes": ["2025-05-21 00:00:00.000"], "description": "每天 00:00 执行" }` | `SCHED_10001`、`SCHED_20001` |

#### 2.6.3 POST `/api/sched/v1/jobs/{jobId}/trigger` 立即执行一次

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | 防止重复点击 |
| Body | `params` | Object | 否 | 覆盖作业默认参数 | — |
| Body | `sync` | Boolean | 否 | 是否同步等待结果 | 默认 `false`；`true` 且超过 10s 返回 `SCHED_50003` |
| Body | `fireTime` | LocalDateTime | 否 | 指定逻辑触发时间 | 默认当前时间，用于补数据场景 |
| Body | `businessKey` | String | 否 | 覆盖 businessKey | 流程目标时生效 |

**响应体**：`{ "executionId": "...", "jobId": "...", "status": "RUNNING"|"SUCCESS", "triggerType": "MANUAL", "fireTime": "...", "instanceId": "流程目标时返回", "costMs": "120" }`。
**主要错误码**：`SCHED_20002`、`SCHED_30004`、`SCHED_40001`、`SCHED_50003`、`COMMON_30006`。
**业务规则**：手动触发**不**改变 cron 调度计划与 `next_fire_time`；`concurrencyPolicy=FORBID` 且上一实例仍在运行时返回 `SCHED_30004`；`REPLACE` 策略会中断上一实例后再启动；作业执行通过 `sch_lock` 抢占，避免多节点重复触发。

#### 2.6.4 POST `/api/sched/v1/jobs/{jobId}/catch-up` 手动补跑

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Header | `Idempotency-Key` | String | 是 | 幂等键 | — |
| Body | `ranges` | Array\<Object\> | 是 | 补跑时间段 | `[{ "startTime": "...", "endTime": "..." }]`，单段 ≤ 31 天，最多 10 段且不重叠（`SCHED_10008`） |
| Body | `fireIntervalSec` | Long | 否 | 补跑触发间隔 | 默认按原 cron 频率推算；最小 10s |
| Body | `maxCount` | Integer | 否 | 最大补跑次数 | 默认 1000，上限 10000 |
| Body | `skipExisting` | Boolean | 否 | 跳过已有成功记录的时间点 | 默认 `true` |
| Body | `params` | Object | 否 | 覆盖参数 | — |
| Body | `dryRun` | Boolean | 否 | 仅预览补跑时间点 | 默认 `false` |

**响应体**：`{ "accepted": true, "taskId": "catchup-20250520-001", "plannedCount": 48, "skippedCount": 12, "totalCount": 60, "fireTimes": ["2025-05-01 00:00:00.000"] }`（`fireTimes` 仅在 `dryRun=true` 或 `plannedCount ≤ 50` 时返回）。
**主要错误码**：`SCHED_10008`、`SCHED_20002`、`SCHED_30006`、`SCHED_40001`、`COMMON_10010`。
**业务规则**：补跑按时间正序、串行执行（避免对下游造成并发放大）；`skipExisting=true` 时跳过 `sch_job_execution` 中同 `fire_time` 已 `SUCCESS` 的记录，否则返回 `SCHED_30006`。

#### 2.6.5 执行记录与日志接口

**执行记录字段**：`executionId`、`jobId`、`jobCode`、`jobName`、`fireTime`、`startTime`、`endTime`、`status`（`SUCCESS`/`FAILED`/`RUNNING`/`TIMEOUT`）、`triggerType`（`CRON`/`MANUAL`/`RETRY`/`CATCH_UP`）、`costMs`、`instanceId`、`result`（Object）、`errorCode`、`errorMsg`、`retryCount`、`owner`（执行节点）、`traceId`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /job-executions` | Query：`jobId`、`jobCode`、`status`(多值，**主口径**：`RUNNING`/`SUCCESS`/`FAILED`/`TIMEOUT`/`SKIPPED`)、`triggerType`、`fireTimeFrom/To`、分页（`CURSOR`） | 分页 `records[]` + `summary: { successRate, avgCostMs }` | `SCHED_20003`、`COMMON_10003/10008` |
| `GET /job-executions/{executionId}` | Query `withLogs`（默认 `false`） | 执行记录对象 + `logs[]`（`withLogs=true`，最多 200 行） | `SCHED_20003`、`SCHED_40002` |
| `POST /job-executions/{executionId}/rerun` | `Idempotency-Key`（REQUIRED）；Body：`params`(可选覆盖)、`sync`(默认 `false`)、`resetRetryCount`(默认 `true`) | `{ "newExecutionId": "...", "status": "RUNNING", "triggerType": "RETRY" }` | `SCHED_20002`、`SCHED_30007`、`SCHED_40002` |
| `GET /job-executions/{executionId}/logs` | Query：`level`(`DEBUG`/`INFO`/`WARN`/`ERROR`)、`keyword`、`cursor`（日志游标）、`limit`（默认 200） | `{ "executionId": "...", "logs": [{ "logId": "...", "ts": 1747704662114, "level": "ERROR", "logger": "com.pe.sched.JobRunner", "message": "...", "stackDigest": "...", "traceId": "..." }], "nextCursor": "...", "hasMore": false }` | `SCHED_20003`、`SCHED_40002`、`SCHED_40003` |

**业务规则**：执行记录保留 90 天（`SUCCESS`）/180 天（`FAILED`），超期归档到 ClickHouse；日志仅对 `FAILED`/`TIMEOUT` 记录完整内容，成功记录只留摘要（减少存储）。

> **筛选口径说明**：执行记录筛选**以 `status` 枚举为主口径**（`RUNNING`/`SUCCESS`/`FAILED`/`TIMEOUT`/`SKIPPED`），与 `sch_job_execution.status` 列一一对应。若管理端需要「成功/失败」两态快捷筛选，由 `status` **派生**（`SUCCESS` → 成功；`FAILED`/`TIMEOUT` → 失败；`RUNNING`/`SKIPPED` → 不计入），派生值**不落库、不作为独立字段返回**，避免与枚举语义重叠。`SKIPPED` 表示因 misfire 策略（`SKIP`）或并发策略（`FORBID`）跳过的本次触发。

#### 2.6.6 GET `/api/sched/v1/jobs/statistics` 作业运行统计

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `jobGroup` | String | 否 | 分组过滤 | — |
| Query | `timeRange` | Enum | 否 | `TODAY`（默认）/ `LAST_7D` / `LAST_30D` / `CUSTOM` | `CUSTOM` 需传时间 |
| Query | `startTime` / `endTime` | LocalDateTime | 条件 | 自定义范围 | ≤ 31 天 |

**响应体**：`{ "jobTotal": 24, "runningCount": 20, "pausedCount": 4, "executionTotal": 15320, "successCount": 15180, "failedCount": 140, "successRate": 0.9909, "avgCostMs": "820", "p95CostMs": "4200", "misfireCount": 3, "backlogCount": 0, "topFailedJobs": [{ "jobId": "...", "jobCode": "...", "failedCount": 12, "lastErrorMsg": "..." }], "dataTime": "..." }`。
**主要错误码**：`SCHED_20001`、`COMMON_10008`。

---

### 2.7 process-integration（集成，8085，库 `pe_integration`）

**服务职责**：连接器与连接器操作的元数据定义、数据源与凭证（AES-256-GCM 加密）管理、连通性测试、连接器在线调试执行与调用日志统计。**注意**：连接器**执行**在 `process-engine` 内嵌 SDK 中完成（基线 3 归属裁决），本服务只负责定义、测试与调试。

**接口总览（23 个）**

| # | 方法 | 路径 | 名称 | 权限码 | 幂等要求 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | GET | `/api/integ/v1/connectors` | 连接器分页列表 | `integ:connector:list` | NONE | 支持类型/状态/内置过滤 |
| 2 | GET | `/api/integ/v1/connectors/{connectorId}` | 连接器详情 | `integ:connector:query` | NONE | 含 `configSchema` |
| 3 | POST | `/api/integ/v1/connectors` | 新增连接器 | `integ:connector:create` | REQUIRED | — |
| 4 | PUT | `/api/integ/v1/connectors/{connectorId}` | 修改连接器 | `integ:connector:update` | NONE | `If-Match: required` |
| 5 | DELETE | `/api/integ/v1/connectors/{connectorId}` | 删除连接器 | `integ:connector:delete` | NONE | 被引用/内置拒绝 |
| 6 | PATCH | `/api/integ/v1/connectors/{connectorId}/status` | 启用/禁用连接器 | `integ:connector:update` | RECOMMENDED | — |
| 7 | GET | `/api/integ/v1/connectors/{connectorId}/operations` | 连接器操作列表 | `integ:connector:query` | NONE | — |
| 8 | GET | `/api/integ/v1/connector-operations/{operationId}` | 操作详情 | `integ:connector:query` | NONE | 含入参/出参 schema |
| 9 | POST | `/api/integ/v1/connector-operations` | 新增操作 | `integ:connector:update` | REQUIRED | — |
| 10 | PUT | `/api/integ/v1/connector-operations/{operationId}` | 修改操作 | `integ:connector:update` | NONE | `If-Match: required` |
| 11 | DELETE | `/api/integ/v1/connector-operations/{operationId}` | 删除操作 | `integ:connector:update` | NONE | 被引用拒绝 |
| 12 | POST | `/api/integ/v1/connectors/{connectorId}/debug` | 连接器在线调试执行 | `integ:connector:debug` | RECOMMENDED | 同步执行并返回入参/出参 |
| 13 | GET | `/api/integ/v1/datasources` | 数据源分页列表 | `integ:datasource:list` | NONE | — |
| 14 | GET | `/api/integ/v1/datasources/{dsId}` | 数据源详情 | `integ:datasource:query` | NONE | 密码不返回 |
| 15 | POST | `/api/integ/v1/datasources` | 新增数据源 | `integ:datasource:create` | REQUIRED | 加密落库 |
| 16 | PUT | `/api/integ/v1/datasources/{dsId}` | 修改数据源 | `integ:datasource:update` | NONE | `If-Match: required` |
| 17 | DELETE | `/api/integ/v1/datasources/{dsId}` | 删除数据源 | `integ:datasource:delete` | NONE | 被引用拒绝 |
| 18 | POST | `/api/integ/v1/datasources/{dsId}/test` | 数据源连通性测试 | `integ:datasource:query` | NONE | 返回延迟与版本 |
| 19 | GET | `/api/integ/v1/credentials` | 凭证分页列表 | `integ:credential:list` | NONE | 只返回脱敏元数据 |
| 20 | POST | `/api/integ/v1/credentials` | 新增凭证 | `integ:credential:create` | REQUIRED | 加密落库 |
| 21 | PUT | `/api/integ/v1/credentials/{credId}` | 修改凭证 | `integ:credential:update` | NONE | `If-Match: required` |
| 22 | DELETE | `/api/integ/v1/credentials/{credId}` | 删除凭证 | `integ:credential:delete` | NONE | 被引用拒绝 |
| 23 | GET | `/api/integ/v1/call-logs` | 调用日志查询 | `integ:call-log:list` | NONE | `CURSOR` 支持 |

#### 2.7.1 连接器与操作接口

**连接器对象字段**：`connectorId`、`connectorCode`、`connectorName`、`connectorType`（`HTTP`/`SQL`/`MQ`/`SCRIPT`/`GRPC`/`BEAN`）、`icon`、`description`、`configSchema`（Object，JSON Schema）、`config`（Object，非敏感配置；敏感项以 `credentialId` 引用）、`credentialId`、`allowedHosts`（Array\<String\>，SSRF 白名单）、`version`、`status`（`ENABLED`/`DISABLED`）、`builtin`（Boolean）、`operationCount`、`refCount`（被流程节点引用数）、`createTime`/`updateTime`、`lockVersion`。

**连接器操作对象字段**：`operationId`、`connectorId`、`operationCode`、`operationName`、`httpMethod`（`GET`/`POST`/`PUT`/`PATCH`/`DELETE`）、`pathTemplate`、`headersTemplate`、`queryTemplate`、`bodyTemplate`、`inputSchema`、`outputSchema`、`timeoutMs`、`retryPolicy`（`{ maxRetries, intervalMs, retryOn: ["TIMEOUT","5XX"] }`）、`transform`（入参/出参映射 `{ input: {}, output: {} }`）、`sampleRequest`、`sampleResponse`、`status`、`lockVersion`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /connectors` | Query：`keyword`、`connectorType`、`status`、`builtin`、分页参数 | 分页 `records[]` | `INTEG_20001`、`COMMON_10003` |
| `GET /connectors/{connectorId}` | Query `withOperations`（默认 `false`） | 连接器对象 + `operations[]` | `INTEG_20001`、`INTEG_40001` |
| `POST /connectors` | `Idempotency-Key`（REQUIRED）；Body：`connectorCode`(必填)、`connectorName`(必填)、`connectorType`(必填)、`icon`、`description`、`configSchema`、`config`、`credentialId`、`allowedHosts`、`status` | `{ "connectorId": "...", "connectorCode": "..." }` | `INTEG_10001`、`INTEG_10002`、`INTEG_10007`、`INTEG_20001`、`INTEG_30001` |
| `PUT /connectors/{connectorId}` | `If-Match: required`；Body 同上（`connectorCode`/`connectorType` 不可改，`builtin` 仅允许改 `status`/`config`） | 连接器对象 | `INTEG_10007`、`INTEG_20001`、`INTEG_30004`、`INTEG_40001`、`COMMON_30003` |
| `DELETE /connectors/{connectorId}` | — | `{ "deleted": true }` | `INTEG_20001`、`INTEG_30003`、`INTEG_30004`、`INTEG_40001` |
| `PATCH /connectors/{connectorId}/status` | Body：`status`(必填)、`reason`(可选) | `{ "connectorId": "...", "status": "DISABLED" }` | `INTEG_20001`、`INTEG_40001`、`COMMON_10009` |
| `GET /connectors/{connectorId}/operations` | Query：`keyword`、`httpMethod`、`status` | `{ "records": [操作对象] }`（单连接器操作数 ≤ 200，不分页） | `INTEG_20001`、`INTEG_40001` |
| `GET /connector-operations/{operationId}` | Query `withSample`（默认 `true`） | 操作对象 | `INTEG_20001`、`INTEG_40002` |
| `POST /connector-operations` | `Idempotency-Key`（REQUIRED）；Body：`connectorId`(必填)、`operationCode`(必填)、`operationName`(必填)、`httpMethod`、`pathTemplate`、`headersTemplate`、`queryTemplate`、`bodyTemplate`、`inputSchema`、`outputSchema`、`timeoutMs`(默认 30000)、`retryPolicy`、`transform`、`sampleRequest`、`sampleResponse` | `{ "operationId": "..." }` | `INTEG_10003`、`INTEG_10008`、`INTEG_10009`、`INTEG_20001`、`INTEG_30005`、`INTEG_40001` |
| `PUT /connector-operations/{operationId}` | `If-Match: required`；Body 同上（`connectorId` 不可改） | 操作对象 | `INTEG_10003`、`INTEG_10008`、`INTEG_20001`、`INTEG_40002`、`COMMON_30003` |
| `DELETE /connector-operations/{operationId}` | — | `{ "deleted": true }` | `INTEG_20001`、`INTEG_30003`、`INTEG_40002` |

**业务规则**：`connectorCode` 租户内唯一且创建后不可变；`builtin=true` 的连接器不可删除（`INTEG_30004`），仅可启停与改配置；HTTP 连接器必须配置 `allowedHosts`（SSRF 白名单，运行时校验失败返回 `INTEG_50004`）；`config` 中声明的敏感字段（`configSchema` 里 `x-sensitive: true`）必须走 `credentialId` 引用，**禁止**明文写入 `config`。

#### 2.7.2 POST `/api/integ/v1/connectors/{connectorId}/debug` 在线调试执行

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Body | `operationId` | String | 条件 | 目标操作 | 与 `operationCode` 二选一 |
| Body | `operationCode` | String | 条件 | 操作编码 | — |
| Body | `input` | Object | 否 | 入参（按 `inputSchema`） | 校验失败 `INTEG_10010` |
| Body | `pathParams` / `queryParams` / `headers` / `body` | Object | 否 | 覆盖模板参数 | 与 `input` 合并，优先级更高 |
| Body | `timeoutMs` | Integer | 否 | 本次超时覆盖 | 100–600000 |
| Body | `dryRun` | Boolean | 否 | 仅渲染请求不发送 | 默认 `false`，返回最终 URL/头/体 |
| Body | `saveSample` | Boolean | 否 | 是否把请求/响应存为示例 | 默认 `false` |
| Body | `maskSensitive` | Boolean | 否 | 响应中是否脱敏 | 默认 `true` |

**响应体**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `callLogId` | String | 本次调用日志 ID（可在 2.7.5 查询；`callStatus` 取值见 2.7.5） |
| `callStatus` | Enum | `RUNNING` / `SUCCESS` / `FAILED` / `UNKNOWN`（调试为同步执行，正常只会返回 `SUCCESS`/`FAILED`） |
| `httpStatus` | Integer | HTTP 状态码（HTTP 连接器） |
| `costMs` | Long | 耗时 |
| `request` | Object | `{ method, url, headers, query, body }`（敏感头脱敏） |
| `response` | Object | 响应体（JSON 对象或字符串，超 64 KB 截断并给 `responseRef`） |
| `responseHeaders` | Object | 响应头 |
| `errorCode` / `errorMsg` | String | 失败信息（`INTEG_50005`/`INTEG_50006`/`INTEG_50004` 等） |
| `retried` | Integer | 实际重试次数 |
| `renderedSql` | String | SQL 连接器渲染后的 SQL（参数已绑定占位） |
| `truncated` | Boolean | 响应是否被截断 |

**主要错误码**：`INTEG_10010`、`INTEG_20004`、`INTEG_30002`、`INTEG_30008`、`INTEG_40002`、`INTEG_50004`、`INTEG_50005`、`INTEG_50006`、`INTEG_50007`、`COMMON_30008`。
**业务规则**：调试只读缓存连接器元数据（不经过 ENGINE）；同一用户调试并发 ≤ 5、频率 ≤ 30 次/分钟（`INTEG_30008`）；调试请求同样受 SSRF 白名单与超时约束；每次调试都写 `int_call_log`（`biz_type=DEBUG`）与操作审计。

#### 2.7.3 数据源接口

**数据源对象字段**：`dsId`、`dsCode`、`dsName`、`dsType`（`MYSQL`/`POSTGRES`/`ORACLE`/`CLICKHOUSE`）、`jdbcUrl`、`username`、`passwordSet`（Boolean，是否已配置密码）、`poolConfig`（Object：`maxPoolSize`/`minIdle`/`connectionTimeoutMs`/`idleTimeoutMs`）、`status`、`lastTestTime`、`lastTestSuccess`、`lastTestMsg`、`refCount`、`lockVersion`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /datasources` | Query：`keyword`、`dsType`、`status`、分页参数 | 分页 `records[]` | `INTEG_20002`、`COMMON_10003` |
| `GET /datasources/{dsId}` | — | 数据源对象（**不返回** `password`，仅 `passwordSet`） | `INTEG_20002`、`INTEG_40003` |
| `POST /datasources` | `Idempotency-Key`（REQUIRED）；Body：`dsCode`(必填)、`dsName`(必填)、`dsType`(必填)、`jdbcUrl`(必填)、`username`(必填)、`password`(必填，加密落库)、`poolConfig`、`status`、`testOnCreate`(默认 `true`，创建后立即测试连通性) | `{ "dsId": "...", "testResult": { "success": true, "costMs": "42", "dbVersion": "MySQL 8.0.36" } }` | `INTEG_10004`、`INTEG_20002`、`INTEG_30001`、`INTEG_30006`、`COMMON_30005` |
| `PUT /datasources/{dsId}` | `If-Match: required`；Body 同上（`dsCode`/`dsType` 不可改；`password` 不传表示保持原值） | 数据源对象 | `INTEG_10004`、`INTEG_20002`、`INTEG_30006`、`INTEG_40003`、`COMMON_30003` |
| `DELETE /datasources/{dsId}` | — | `{ "deleted": true }` | `INTEG_20002`、`INTEG_30003`、`INTEG_40003` |
| `POST /datasources/{dsId}/test` | Body：`timeoutMs`(默认 5000)、`testSql`(可选，默认连接即测) | `{ "success": true, "costMs": "38", "dbVersion": "MySQL 8.0.36", "driverVersion": "8.0.33", "message": "连接成功" }` | `INTEG_20002`、`INTEG_30006`、`INTEG_40003`、`INTEG_50002`、`INTEG_50003` |

**业务规则**：`password_cipher` 使用 AES-256-GCM，密钥由 KMS/环境变量注入（基线 6.8），**禁止**明文落库与日志输出；连通性测试使用独立短连接池（不占用业务池），超时 5s。

#### 2.7.4 凭证接口

**凭证对象字段**：`credId`、`credName`、`credType`（`BASIC`/`BEARER`/`AK_SK`/`OAUTH2`）、`contentMasked`（Object，脱敏摘要，如 `{ username: "api***", passwordSet: true }`）、`expireTime`、`status`、`refCount`、`lastRefreshTime`、`lockVersion`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /credentials` | Query：`keyword`、`credType`、`status`、`expired`(Boolean)、分页参数 | 分页 `records[]`（仅脱敏元数据） | `INTEG_20003`、`COMMON_10003` |
| `POST /credentials` | `Idempotency-Key`（REQUIRED）；Body：`credName`(必填)、`credType`(必填)、`content`(Object，必填：`BASIC`→`{username,password}`；`BEARER`→`{token}`；`AK_SK`→`{accessKey,secretKey}`；`OAUTH2`→`{clientId,clientSecret,tokenUrl,scope}`)、`expireTime`、`status` | `{ "credId": "...", "contentMasked": {...} }` | `INTEG_10005`、`INTEG_10006`、`INTEG_20003`、`INTEG_50002` |
| `PUT /credentials/{credId}` | `If-Match: required`；Body：`credName`、`content`（不传则保持）、`expireTime`、`status` | 凭证对象 | `INTEG_10005`、`INTEG_20003`、`INTEG_30007`、`INTEG_40004`、`COMMON_30003` |
| `DELETE /credentials/{credId}` | — | `{ "deleted": true }` | `INTEG_20003`、`INTEG_30003`、`INTEG_40004` |

**业务规则**：`content` 仅在写入时接受明文（HTTPS 传输），落库前加密；任何读取接口都**不返回**明文；凭证过期（`expire_time` 已过）在连接器执行时返回 `INTEG_30007`；被连接器引用的凭证不可删除（`INTEG_30003`）。

#### 2.7.5 GET `/api/integ/v1/call-logs` 调用日志查询

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `connectorId` / `operationCode` | String | 否 | 连接器/操作过滤 | — |
| Query | `instanceId` / `nodeInstanceId` | String | 否 | 引擎调用来源 | 由 ENGINE 内部接口写入 |
| Query | `callStatus` | Enum | 否 | 调用状态（可多值） | `RUNNING` / `SUCCESS` / `FAILED` / `UNKNOWN`；**`UNKNOWN` = 失联执行**，见下方说明 |
| Query | `httpStatus` | Integer | 否 | HTTP 状态码 | — |
| Query | `bizType` | Enum | 否 | `ENGINE_NODE`（默认全部）/ `DEBUG` / `TEST` | — |
| Query | `traceId` | String | 否 | 链路 ID | — |
| Query | `startTimeFrom` / `startTimeTo` | LocalDateTime | 否 | 时间范围 | ≤ 31 天 |
| Query | `minCostMs` / `maxCostMs` | Long | 否 | 耗时区间 | 慢调用排查 |
| Query | `pageNum` / `pageSize` / `lastId` | — | 否 | 分页（`CURSOR`） | 见 1.6 |

**响应体（`records[]`）**：`callLogId`、`connectorId`、`connectorCode`、`operationCode`、`instanceId`、`nodeInstanceId`、`bizType`、`requestDigest`、`responseDigest`、`httpStatus`、`costMs`、`callStatus`、`errorCode`、`errorMsg`、`retryCount`、`traceId`、`startTime`、`endTime`、`callerIp`、`requestRef`（完整请求体对象存储引用）、`responseRef`、`reconcileFlag`（Boolean，是否由对账巡检置为 `UNKNOWN`）。
**主要错误码**：`INTEG_20001`、`INTEG_40005`、`COMMON_10003/10008`。

**`callStatus` 取值语义（基线 v1.0.1 起，字段名由 `success` 布尔修正为 `call_status`）**

| 取值 | 含义 | 说明 |
| --- | --- | --- |
| `RUNNING` | 执行中 | 请求已发出、结果未回写（正常情况下仅存续数百毫秒） |
| `SUCCESS` | 调用成功 | HTTP 2xx 或 SQL 执行成功且出参转换成功 |
| `FAILED` | 调用失败 | HTTP 非 2xx、超时、SQL 错误、出参转换失败等已知失败 |
| `UNKNOWN` | **失联执行（黑洞调用）** | worker 执行中被强制终止（OOM、节点重启、kill -9），导致「**请求已发出但结果丢失**」；由引擎对账巡检在周期内把遗留超过阈值的 `RUNNING` 记录置为 `UNKNOWN`，并触发故障事件（`incidentType=CONNECTOR_ERROR`，`errorCode=INTEG_50006`）与告警，需人工核对下游是否已产生实际效果 |

> 能力说明：该字段使调用日志从「只能区分成功/失败」升级为「**能发现黑洞调用**」——对 `INSERT`/`转账`/`下单` 等非幂等外部操作，`UNKNOWN` 是运维必须人工核对的最高优先级信号。默认巡检周期 60s，`RUNNING` 超过 `integ.call-log.running-timeout`（默认 5 分钟）即判定为 `UNKNOWN`；建议对 `UNKNOWN` 建立 P1 告警规则（见 2.9.8 告警规则），并在 `requestDigest` 中保留幂等键以便对账。

**业务规则**：`int_call_log` 只保留近期热数据（默认 15 天），全量明细落 ClickHouse 由 `process-trace` 检索；`requestDigest`/`responseDigest` 为脱敏摘要（截断 2 KB + SHA-256），完整报文在 MinIO（`requestRef`/`responseRef`，保留 7 天）；列表默认按 `start_time DESC`，`callStatus=RUNNING` 与 `UNKNOWN` 的记录建议在管理端提供独立「待核对」视图（前端按 `callStatus in (RUNNING, UNKNOWN)` 过滤）。

#### 2.7.6 扩展接口

| 接口 | 方法 | 路径 | 说明 | 权限码 | 错误码要点 |
| --- | --- | --- | --- | --- | --- |
| 调用统计 | GET | `/api/integ/v1/call-logs/statistics` | 按连接器/操作/时间聚合成功率与耗时分布 | `integ:call-log:list` | `COMMON_10008` |

**调用统计响应体示例**：`{ "totalCount": 128340, "callStatusCount": { "SUCCESS": 127980, "FAILED": 340, "RUNNING": 4, "UNKNOWN": 16 }, "successRate": 0.9972, "unknownCount": 16, "avgCostMs": "142", "p95CostMs": "860", "p99CostMs": "2100", "byConnector": [{ "connectorCode": "crm_http", "count": 82100, "successRate": 0.9991, "avgCostMs": "118", "unknownCount": 2 }], "errorDistribution": [{ "errorCode": "INTEG_50005", "count": 210 }] }`。

> 说明：统计口径以 `callStatus` 为准（`successRate` 由 `SUCCESS / (SUCCESS + FAILED)` 派生，`unknownCount` 单列并建议纳入 P1 告警），**不再使用布尔 `success`**；`successRate` 仅为便于看板展示的派生素率，不落库。

---

### 2.8 process-notify（通知，8086，库 `pe_notify`）

**服务职责**：消息模板管理、渠道配置（站内信/邮件/短信/Webhook/钉钉/企微/飞书）与测试发送、事件订阅规则、消息记录查询与重发、未读消息与已读标记；消费引擎与各服务的事件 Topic 按订阅规则投递通知。

**接口总览（22 个）**

| # | 方法 | 路径 | 名称 | 权限码 | 幂等要求 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | GET | `/api/notify/v1/templates` | 模板分页列表 | `notify:template:list` | NONE | 支持渠道/关键字 |
| 2 | GET | `/api/notify/v1/templates/{templateId}` | 模板详情 | `notify:template:query` | NONE | 含变量声明 |
| 3 | POST | `/api/notify/v1/templates` | 新增模板 | `notify:template:create` | REQUIRED | — |
| 4 | PUT | `/api/notify/v1/templates/{templateId}` | 修改模板 | `notify:template:update` | NONE | `If-Match: required` |
| 5 | DELETE | `/api/notify/v1/templates/{templateId}` | 删除模板 | `notify:template:delete` | NONE | 被订阅引用拒绝 |
| 6 | POST | `/api/notify/v1/templates/{templateId}/preview` | 模板预览渲染 | `notify:template:query` | NONE | 传入变量预览标题与正文 |
| 7 | GET | `/api/notify/v1/channels` | 渠道配置列表 | `notify:channel:list` | NONE | 按渠道类型 |
| 8 | POST | `/api/notify/v1/channels` | 新增渠道配置 | `notify:channel:create` | REQUIRED | 敏感配置加密 |
| 9 | PUT | `/api/notify/v1/channels/{channelId}` | 修改渠道配置 | `notify:channel:update` | NONE | `If-Match: required` |
| 10 | DELETE | `/api/notify/v1/channels/{channelId}` | 删除渠道配置 | `notify:channel:delete` | NONE | 被订阅引用拒绝 |
| 11 | POST | `/api/notify/v1/channels/{channelId}/test` | 渠道测试发送 | `notify:channel:test` | RECOMMENDED | 指定接收人发一条测试消息 |
| 12 | GET | `/api/notify/v1/subscriptions` | 订阅规则分页列表 | `notify:subscription:list` | NONE | — |
| 13 | POST | `/api/notify/v1/subscriptions` | 新增订阅规则 | `notify:subscription:create` | REQUIRED | — |
| 14 | PUT | `/api/notify/v1/subscriptions/{subscriptionId}` | 修改订阅规则 | `notify:subscription:update` | NONE | `If-Match: required` |
| 15 | DELETE | `/api/notify/v1/subscriptions/{subscriptionId}` | 删除订阅规则 | `notify:subscription:delete` | NONE | — |
| 16 | GET | `/api/notify/v1/messages` | 消息分页列表 | `notify:message:list` | NONE | `CURSOR` 支持 |
| 17 | GET | `/api/notify/v1/messages/{messageId}` | 消息详情 | `notify:message:query` | NONE | 含发送报文 |
| 18 | POST | `/api/notify/v1/messages/{messageId}/resend` | 消息重发 | `notify:message:resend` | REQUIRED | 仅 `FAILED` |
| 19 | GET | `/api/notify/v1/messages/inbox` | 我的未读消息/已读列表 | 登录态 | NONE | Query `readStatus` 区分 |
| 20 | PUT | `/api/notify/v1/messages/{messageId}/read` | 标记消息已读 | 登录态 | RECOMMENDED | 支持批量 body |
| 21 | PUT | `/api/notify/v1/messages/read-all` | 全部标记已读 | 登录态 | RECOMMENDED | 可按渠道过滤 |
| 22 | GET | `/api/notify/v1/messages/unread-count` | 未读消息数 | 登录态 | NONE | 前端角标轮询 |

#### 2.8.1 模板接口

**模板对象字段**：`templateId`、`templateCode`、`templateName`、`channel`（`INBOX`/`EMAIL`/`SMS`/`WEBHOOK`/`DINGTALK`/`WECOM`/`FEISHU`）、`titleTpl`、`contentTpl`、`vars`（Array\<Object\>：`[{ name, label, type, required, defaultValue }]`）、`contentType`（`TEXT`/`HTML`/`MARKDOWN`）、`status`（`ENABLED`/`DISABLED`）、`remark`、`refCount`、`createTime`/`updateTime`、`version`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /templates` | Query：`keyword`(编码/名称)、`channel`、`status`、分页参数 | 分页 `records[]` | `NOTIFY_20001`、`COMMON_10003` |
| `GET /templates/{templateId}` | Query `withVars`（默认 `true`） | 模板对象 | `NOTIFY_20001`、`NOTIFY_40001` |
| `POST /templates` | `Idempotency-Key`（REQUIRED）；Body：`templateCode`(必填)、`templateName`(必填)、`channel`(必填)、`titleTpl`、`contentTpl`(必填)、`vars`、`contentType`、`status`、`remark` | `{ "templateId": "...", "templateCode": "..." }` | `NOTIFY_10001`、`NOTIFY_10002`、`NOTIFY_10008`、`NOTIFY_10009`、`NOTIFY_20001`、`NOTIFY_30003` |
| `PUT /templates/{templateId}` | `If-Match: required`；Body 同上（`templateCode` 不可改；已发布模板改内容需新建版本，`NOTIFY_30001`） | 模板对象 | `NOTIFY_10002`、`NOTIFY_10009`、`NOTIFY_20001`、`NOTIFY_30001`、`NOTIFY_40001`、`COMMON_30003` |
| `DELETE /templates/{templateId}` | — | `{ "deleted": true }` | `NOTIFY_20001`、`NOTIFY_40001`、`COMMON_30002` |
| `POST /templates/{templateId}/preview` | Body：`vars`(Object，必填)、`receiver`(可选，用于渠道差异渲染) | `{ "title": "您有新的待办：请假审批", "content": "...", "channel": "INBOX", "truncated": false, "renderedFields": { "userName": "张三" } }` | `NOTIFY_10003`、`NOTIFY_20001`、`NOTIFY_40001`、`NOTIFY_50005` |

**业务规则**：模板语法为 `${varName}` 占位（由 NOTIFY 内部轻量渲染器处理，**不**使用 Aviator 求值以防注入）；`contentTpl` 声明的变量必须存在于 `vars`，否则渲染返回 `NOTIFY_10003`；短信渠道内容长度 ≤ 500 字（`NOTIFY_10009`）。

#### 2.8.2 渠道配置接口

**渠道对象字段**：`channelId`、`channel`、`channelName`、`configMasked`（Object，脱敏配置，如 `{ "smtpHost": "smtp.exmail.qq.com", "smtpPort": 465, "username": "no***@corp.com", "passwordSet": true }`）、`rateLimit`（Integer，每分钟上限）、`status`、`defaultFlag`（Boolean，同渠道唯一默认）、`lastTestTime`、`lastTestSuccess`、`lastTestMsg`、`version`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /channels` | Query：`channel`、`status`、分页参数 | 分页 `records[]`（仅脱敏配置） | `NOTIFY_20001`、`COMMON_10003` |
| `POST /channels` | `Idempotency-Key`（REQUIRED）；Body：`channel`(必填)、`channelName`(必填)、`config`(Object，必填，敏感字段加密)、`rateLimit`(默认 1000)、`status`、`defaultFlag` | `{ "channelId": "...", "channel": "EMAIL" }` | `NOTIFY_10004`、`NOTIFY_10007`、`NOTIFY_10008`、`NOTIFY_10010`、`NOTIFY_20001`、`NOTIFY_30004` |
| `PUT /channels/{channelId}` | `If-Match: required`；Body 同上（`channel` 不可改；`config` 不传表示保持） | 渠道对象 | `NOTIFY_10004`、`NOTIFY_10010`、`NOTIFY_20001`、`NOTIFY_40002`、`COMMON_30003` |
| `DELETE /channels/{channelId}` | — | `{ "deleted": true }` | `NOTIFY_20001`、`NOTIFY_40002`、`COMMON_30002` |
| `POST /channels/{channelId}/test` | `Idempotency-Key`(RECOMMENDED)；Body：`receiver`(必填，邮箱/手机号/Webhook URL/用户 ID)、`templateCode`(可选，默认内置测试模板)、`vars`(可选) | `{ "success": true, "messageId": "...", "channel": "EMAIL", "receiver": "z***@corp.com", "costMs": "820", "providerMsgId": "...", "errorMsg": null }` | `NOTIFY_20001`、`NOTIFY_30002`、`NOTIFY_30006`、`NOTIFY_40002`、`NOTIFY_50001`~`NOTIFY_50004` |

**业务规则**：`config` 中的敏感字段（密码、密钥、Token、Webhook 签名密钥）以 AES-256-GCM 加密存 `config_cipher`，**不返回**明文；Webhook/钉钉/企微/飞书地址必须为 http(s) 且通过 SSRF 白名单校验（`NOTIFY_10007`）；渠道 `rate_limit` 命中返回 `NOTIFY_30006`（HTTP 429，带 `Retry-After`）。

#### 2.8.3 订阅规则接口

**订阅对象字段**：`subscriptionId`、`eventType`（如 `TASK_CREATED`、`PROCESS_INSTANCE_ENDED`、`INCIDENT_CREATED`、`JOB_FAILED`）、`bizScope`（Object，业务范围过滤 `{ processKeys: [], categoryIds: [], tenantId }`）、`channel`、`templateCode`、`receiverExpr`（Aviator 表达式，返回用户 ID 列表）、`receiverFallback`（`INITIATOR`/`ADMIN`/`NONE`）、`dedupeWindowSec`（去重窗口，默认 0）、`enabled`、`remark`、`version`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /subscriptions` | Query：`eventType`、`channel`、`enabled`、`keyword`、分页参数 | 分页 `records[]` | `NOTIFY_20001`、`COMMON_10003` |
| `POST /subscriptions` | `Idempotency-Key`（REQUIRED）；Body：`eventType`(必填)、`bizScope`、`channel`(必填)、`templateCode`(必填)、`receiverExpr`、`receiverFallback`、`dedupeWindowSec`、`enabled`(默认 `true`)、`remark` | `{ "subscriptionId": "..." }` | `NOTIFY_10005`、`NOTIFY_10006`、`NOTIFY_20001`、`NOTIFY_30007`、`NOTIFY_40001` |
| `PUT /subscriptions/{subscriptionId}` | `If-Match: required`；Body 同上 | 订阅对象 | `NOTIFY_10005`、`NOTIFY_10006`、`NOTIFY_20001`、`NOTIFY_40003`、`COMMON_30003` |
| `DELETE /subscriptions/{subscriptionId}` | — | `{ "deleted": true }` | `NOTIFY_20001`、`NOTIFY_40003` |

**业务规则**：同 `event_type + biz_scope + channel` 组合唯一（`NOTIFY_30007`）；`receiverExpr` 求值结果为空的兜底顺序：`receiverFallback` → 事件 `payload.initiatorId` → 丢弃并记 `WARN` 日志；`dedupe_window_sec > 0` 时同接收人同事件在窗口内只发一条。

#### 2.8.4 消息接口

**消息对象字段**：`messageId`、`msgNo`（`NT{yyyyMMdd}{8位序列}`）、`channel`、`templateCode`、`receiver`（脱敏）、`receiverUserId`、`title`、`content`、`bizType`、`bizId`、`instanceId`、`taskId`、`status`（`PENDING`/`SENT`/`FAILED`/`READ`）、`retryCount`、`sendTime`、`readTime`、`errorCode`、`errorMsg`、`providerMsgId`、`traceId`、`createTime`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /messages` | Query：`keyword`、`channel`、`status`(多值)、`templateCode`、`receiverUserId`、`bizType`、`bizId`、`instanceId`、`sendTimeFrom/To`、`onlyFailed`、分页（`CURSOR`） | 分页 `records[]` + `summary: { total, failedCount, successRate }` | `NOTIFY_20002`、`COMMON_10003/10008` |
| `GET /messages/{messageId}` | Query `withPayload`（默认 `false`，是否返回完整发送报文） | 消息对象 + `payload`（请求/响应摘要） | `NOTIFY_20002`、`NOTIFY_40004` |
| `POST /messages/{messageId}/resend` | `Idempotency-Key`（REQUIRED）；Body：`receiver`(可选覆盖)、`channel`(可选切换渠道)、`vars`(可选)、`resetRetryCount`(默认 `true`) | `{ "messageId": "...", "newMessageId": "...", "status": "PENDING", "channel": "EMAIL" }` | `NOTIFY_20002`、`NOTIFY_30002`、`NOTIFY_30005`、`NOTIFY_30006`、`NOTIFY_40004` |
| `GET /messages/inbox` | Query：`readStatus`(`UNREAD`(默认)/`READ`/`ALL`)、`channel`、`keyword`、分页（`CURSOR`） | 分页 `records[]`（仅当前用户消息） | `COMMON_20001`、`COMMON_10003` |
| `PUT /messages/{messageId}/read` | Body：`read`(Boolean，默认 `true`)、`messageIds`(Array\<String\>，可选批量，≤ 200) | `{ "updated": 3, "readTime": "..." }` | `NOTIFY_40004`、`COMMON_10010` |
| `PUT /messages/read-all` | Body：`channel`(可选)、`beforeTime`(可选) | `{ "updated": 42 }` | `COMMON_20001` |
| `GET /messages/unread-count` | Query：`channel`(可选) | `{ "total": 12, "byChannel": { "INBOX": 12, "EMAIL": 0 } }`（结果缓存 10s，前端角标轮询用） | `COMMON_20001`、`COMMON_30008` |

**业务规则**：消息状态流转 `PENDING → SENT → READ` 或 `PENDING → FAILED →（重发）PENDING`；发送失败自动重试 3 次（指数退避 10s/60s/300s），超过后置 `FAILED` 并投递 `PE_NOTIFY_MESSAGE_FAILED`（触发 `NOTIFY_MESSAGE_FAILED` 告警规则）；重发使用**新的** `messageId` 并保留原记录（链路可追溯）。

---

### 2.9 process-trace（轨迹与审计，8087，库 `pe_trace` + ClickHouse）

**服务职责**：操作审计日志检索、执行轨迹检索（按实例/节点/事件类型/时间范围）、实例多维统计聚合、流程耗时热力分析、告警规则管理，并**统一提供实时推送**（WebSocket 工作台 + SSE 实例监控，见第 3 节）。本服务**只读检索与推送，不写业务库**。

**接口总览（15 个）**

| # | 方法 | 路径 | 名称 | 权限码 | 幂等要求 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | GET | `/api/trace/v1/oper-logs` | 操作审计日志查询 | `trace:oper-log:list` | NONE | `CURSOR` 支持 |
| 2 | GET | `/api/trace/v1/oper-logs/{logId}` | 审计日志详情 | `trace:oper-log:query` | NONE | 含参数摘要 |
| 3 | GET | `/api/trace/v1/exec-traces` | 执行轨迹检索 | `trace:exec-trace:list` | NONE | 按实例/节点/事件类型/时间 |
| 4 | GET | `/api/trace/v1/exec-traces/timeline` | 实例轨迹时间线 | `trace:exec-trace:list` | NONE | 聚合为可视化时间线 |
| 5 | GET | `/api/trace/v1/instance-statistics` | 实例统计（多维聚合） | `trace:stat:query` | NONE | 日粒度物化 + 实时补充 |
| 6 | GET | `/api/trace/v1/instance-statistics/duration-heatmap` | 流程耗时热力 | `trace:stat:query` | NONE | 按流程/时间段聚合 |
| 7 | GET | `/api/trace/v1/instance-statistics/node-duration` | 节点耗时排行 | `trace:stat:query` | NONE | TopN 慢节点 |
| 8 | GET | `/api/trace/v1/alert-rules` | 告警规则分页列表 | `trace:alert-rule:list` | NONE | — |
| 9 | GET | `/api/trace/v1/alert-rules/{ruleId}` | 告警规则详情 | `trace:alert-rule:query` | NONE | — |
| 10 | POST | `/api/trace/v1/alert-rules` | 新增告警规则 | `trace:alert-rule:create` | REQUIRED | — |
| 11 | PUT | `/api/trace/v1/alert-rules/{ruleId}` | 修改告警规则 | `trace:alert-rule:update` | NONE | `If-Match: required` |
| 12 | DELETE | `/api/trace/v1/alert-rules/{ruleId}` | 删除告警规则 | `trace:alert-rule:delete` | NONE | — |
| 13 | PUT | `/api/trace/v1/alert-rules/{ruleId}/enabled` | 启用/停用告警规则 | `trace:alert-rule:update` | RECOMMENDED | — |
| 14 | GET | `/api/trace/v1/alert-records` | 告警触发记录 | `trace:alert-rule:list` | NONE | 含确认状态 |
| 15 | PUT | `/api/trace/v1/alert-records/{recordId}/ack` | 告警确认/静默 | `trace:alert-rule:update` | RECOMMENDED | 含静默时长 |

#### 2.9.1 GET `/api/trace/v1/oper-logs` 操作审计日志查询

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `operatorId` / `operatorName` | String | 否 | 操作人 | — |
| Query | `module` | String | 否 | 模块（`IAM`/`DESIGN`/`ENGINE`/`TASK`/`SCHED`/`INTEG`/`NOTIFY`） | — |
| Query | `action` | String | 否 | 动作（`CREATE`/`UPDATE`/`DELETE`/`PUBLISH`/`TERMINATE`/`LOGIN`…） | — |
| Query | `targetType` / `targetId` | String | 否 | 目标对象类型与 ID | — |
| Query | `success` | Boolean | 否 | 是否成功 | — |
| Query | `ip` | String | 否 | 客户端 IP | 支持网段（`10.0.0.0/8`） |
| Query | `traceId` | String | 否 | 链路 ID | — |
| Query | `tenantId` | String | 否 | 租户（跨租户） | 需 `TRACE` 平台权限（`TRACE_20002`） |
| Query | `startTimeFrom` / `startTimeTo` | LocalDateTime | 是 | 时间范围 | 必填；跨度 ≤ 31 天（`TRACE_10001`/`TRACE_10002`） |
| Query | `pageNum` / `pageSize` / `lastId` / `orderDirection` | — | 否 | 分页（`CURSOR`） | 默认 `start_time DESC` |

**响应体（`records[]`）**：`logId`、`tenantId`、`tenantName`、`operatorId`、`operatorName`、`module`、`action`、`targetType`、`targetId`、`targetName`、`requestUri`、`httpMethod`、`paramsDigest`、`resultCode`、`success`、`errorMsg`、`costMs`、`ip`、`location`、`ua`、`operTime`、`traceId`。
**主要错误码**：`TRACE_10001`、`TRACE_10002`、`TRACE_20001`、`TRACE_20002`、`TRACE_40001`、`TRACE_50001`、`TRACE_50002`。
**业务规则**：数据源为 ClickHouse（`tr_oper_log` 由各服务通过 MQ `PE_TRACE_OPER_LOG` 投递，保留 ≥ 180 天，基线 6.8）；`paramsDigest` 为脱敏摘要（密码、Token、凭证字段永久剔除）；单次查询 `pageSize` 上限 200，超过 10000 条命中时强制游标模式。

#### 2.9.2 GET `/api/trace/v1/oper-logs/{logId}` 审计日志详情

**响应体**：审计日志对象 + `requestBodyDigest`（脱敏后报文，≤ 4 KB）、`responseDigest`、`userAgentDetail`（`{ os, browser, device }`）、`relatedLogs`（同一 `traceId` 的前后 5 条摘要）。
**主要错误码**：`TRACE_20001`、`TRACE_40001`。

#### 2.9.3 GET `/api/trace/v1/exec-traces` 执行轨迹检索

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `instanceId` | String | 条件 | 实例 ID | 与 `processKey` 至少一个 |
| Query | `processKey` | String | 条件 | 流程 key | — |
| Query | `nodeId` / `nodeInstanceId` | String | 否 | 节点过滤 | — |
| Query | `eventType` | Enum | 否 | 事件类型（可多值） | 取值见 2.4.29 |
| Query | `tenantId` | String | 否 | 租户 | 跨租户需平台权限（`TRACE_20002`） |
| Query | `startTimeFrom` / `startTimeTo` | LocalDateTime | 是 | 时间范围 | 必填；≤ 31 天（`TRACE_10001`/`TRACE_10002`） |
| Query | `orderDirection` | Enum | 否 | `ASC`（默认）/ `DESC` | — |
| Query | `pageNum` / `pageSize` / `lastId` | — | 否 | 分页（`CURSOR`） | — |

**响应体（`records[]`）**：`execTraceId`、`tenantId`、`instanceId`、`instanceNo`、`processKey`、`version`、`nodeInstanceId`、`tokenId`、`eventType`、`nodeId`、`nodeType`、`payload`（Object）、`ts`（毫秒时间戳）、`occurredAt`、`traceId`、`seq`；顶层含 `dataSyncedAt`、`total`（`countTotal=false` 时为 `null`）。
**主要错误码**：`TRACE_10001`、`TRACE_10002`、`TRACE_10003`、`TRACE_20001`、`TRACE_20002`、`TRACE_30002`、`TRACE_50001`、`TRACE_50002`。

#### 2.9.4 GET `/api/trace/v1/exec-traces/timeline` 实例轨迹时间线

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `instanceId` | String | 是 | 实例 ID | — |
| Query | `groupByNode` | Boolean | 否 | 是否按节点聚合（节点维度时间线） | 默认 `true` |
| Query | `includeVariables` | Boolean | 否 | 是否含变量变更明细 | 默认 `false` |

**响应体**：`{ "instanceId": "...", "processKey": "...", "startTime": "...", "endTime": "...", "durationMs": "1834200", "items": [{ "nodeId": "n_approve", "nodeName": "主管审批", "nodeType": "USER_TASK", "nodeInstanceId": "...", "segments": [{ "eventType": "NODE_ENTERED", "ts": 1747704662114, "occurredAt": "...", "operatorName": "李四", "durationMs": "3600000", "payload": {} }], "totalDurationMs": "3600000", "loopCount": 1 }], "tokens": [{ "tokenId": "...", "lifecycle": [{ "eventType": "TOKEN_CREATED", "ts": 1747704662114 }] }] }`。
**主要错误码**：`TRACE_10001`、`TRACE_20001`、`TRACE_40003`、`TRACE_50001`。

#### 2.9.5 GET `/api/trace/v1/instance-statistics` 实例统计（多维聚合）

| 位置 | 名称 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- | --- |
| Query | `groupBy` | Enum | 否 | 聚合维度 | `DATE`（默认）/ `PROCESS_KEY` / `NODE_TYPE` / `INITIATOR` / `DEPT` / `TENANT` |
| Query | `granularity` | Enum | 否 | 时间粒度 | `DAY`（默认）/ `HOUR` / `WEEK` / `MONTH` |
| Query | `metrics` | String | 否 | 指标列表 | 默认 `started,completed,terminated,avgDuration`；可选 `p95Duration,incidentCount,overdueTaskCount` |
| Query | `processKey` / `categoryId` | String | 否 | 过滤条件 | — |
| Query | `startDate` / `endDate` | LocalDate | 是 | 日期范围 | 必填；≤ 180 天（按日粒度） |
| Query | `tenantId` | String | 否 | 租户 | 跨租户需平台权限 |

**响应体**：`{ "groupBy": "PROCESS_KEY", "granularity": "DAY", "items": [{ "key": "leave_approval", "keyName": "请假审批", "statDate": "2025-05-19", "startedCnt": 128, "completedCnt": 96, "terminatedCnt": 3, "avgDurationMs": "1834200", "p95DurationMs": "5400000", "incidentCount": 4, "overdueTaskCount": 5 }], "totals": { "startedCnt": 1024, "completedCnt": 968, "avgDurationMs": "1622400" }, "dataTime": "2025-05-20 10:20:00.000", "realtimeSupplement": true }`。
**主要错误码**：`TRACE_10004`、`TRACE_10001`、`TRACE_20001`、`TRACE_20002`、`TRACE_40003`、`TRACE_50001`、`TRACE_50002`。
**业务规则**：主数据源为 `tr_instance_stat` 物化表（日粒度，T+1 精确）；`realtimeSupplement=true` 表示当日数据由实时聚合补充（可能有 ≤ 5 分钟延迟）。

#### 2.9.6–2.9.7 耗时分析接口

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /instance-statistics/duration-heatmap` | Query：`processKey`(可选)、`dimension`(Enum `WEEKDAY_HOUR`(默认)/`DATE`/`NODE`)、`startDate`/`endDate`(必填，≤ 90 天)、`metric`(Enum `AVG`(默认)/`P95`/`COUNT`) | `{ "dimension": "WEEKDAY_HOUR", "cells": [{ "x": "周一", "y": "09", "value": 1820000, "count": 42 }], "maxValue": 5400000, "minValue": 120000, "unit": "ms", "legend": { "slow": 3600000, "normal": 600000 } }` | `TRACE_10004`、`TRACE_10001`、`TRACE_10002`、`TRACE_50001` |
| `GET /instance-statistics/node-duration` | Query：`processKey`(必填)、`nodeType`(可选)、`topN`(默认 20，最大 100)、`startDate`/`endDate`、`metric`(`AVG`/`P95`/`MAX`)、`includeIncident`(默认 `false`) | `{ "processKey": "...", "items": [{ "nodeId": "n_approve", "nodeName": "主管审批", "nodeType": "USER_TASK", "avgDurationMs": "4200000", "p95DurationMs": "10800000", "maxDurationMs": "86400000", "instanceCount": 120, "overdueCount": 8, "incidentCount": 1, "bottleneck": true }] }` | `TRACE_10004`、`TRACE_10001`、`TRACE_20001`、`TRACE_50001` |

**业务规则**：`bottleneck=true` 由服务端按 P95 超阈值（默认 4 小时）自动标记；热力图数据缓存 5 分钟（`Cache-Control: private, max-age=300`）。

#### 2.9.8–2.9.13 告警规则接口

**告警规则对象字段**：`ruleId`、`ruleCode`、`ruleName`、`ruleType`（`METRIC`（阈值）/`EVENT`（事件计数）/`ABSENCE`（缺失检测））、`target`（Object：`{ metric: "incidentOpenCount"|"runningCount"|"pendingTaskCount"|"jobFailedCount"|"callErrorRate", eventType, processKey }`）、`condition`（Object：`{ operator: "GT"|"GTE"|"LT"|"LTE"|"EQ", threshold: 10, windowSec: 300, consecutiveTimes: 2 }`）、`level`（`P0`/`P1`/`P2`）、`channels`（Array\<String\>）、`templateCode`、`receivers`（Array\<String\>）、`silenceSec`（静默期）、`enabled`、`lastTriggerTime`、`triggerCount`、`status`（`NORMAL`/`FIRING`/`SILENCED`）、`version`。

| 接口 | 请求要点 | 响应要点 | 主要错误码 |
| --- | --- | --- | --- |
| `GET /alert-rules` | Query：`keyword`、`ruleType`、`level`、`enabled`、`status`、分页参数 | 分页 `records[]` | `TRACE_20003`、`COMMON_10003` |
| `GET /alert-rules/{ruleId}` | — | 告警规则对象 | `TRACE_20003`、`TRACE_40002` |
| `POST /alert-rules` | `Idempotency-Key`（REQUIRED）；Body：`ruleCode`(必填)、`ruleName`(必填)、`ruleType`(必填)、`target`(必填)、`condition`(必填)、`level`(必填)、`channels`(必填)、`templateCode`、`receivers`、`silenceSec`、`enabled` | `{ "ruleId": "...", "ruleCode": "..." }` | `TRACE_10005`、`TRACE_10006`、`TRACE_10007`、`TRACE_20003`、`COMMON_30001` |
| `PUT /alert-rules/{ruleId}` | `If-Match: required`；Body 同上（`ruleCode` 不可改） | 告警规则对象 | `TRACE_10005`、`TRACE_10006`、`TRACE_20003`、`TRACE_40002`、`TRACE_30003`、`COMMON_30003` |
| `DELETE /alert-rules/{ruleId}` | — | `{ "deleted": true }` | `TRACE_20003`、`TRACE_40002` |
| `PUT /alert-rules/{ruleId}/enabled` | Body：`enabled`(Boolean，必填)、`reason`(可选) | `{ "ruleId": "...", "enabled": false, "status": "NORMAL" }` | `TRACE_20003`、`TRACE_30001`、`TRACE_40002` |
| `GET /alert-records` | Query：`ruleId`、`level`、`ackStatus`(`PENDING`/`ACKED`/`SILENCED`)、`startTimeFrom/To`、分页（`CURSOR`） | 分页 `records[]`：`recordId`、`ruleId`、`ruleName`、`level`、`triggerValue`、`threshold`、`targetSummary`、`firedTime`、`ackStatus`、`ackedBy`、`ackedTime`、`silenceUntil`、`notifyMessageIds`、`traceId` | `TRACE_20003`、`COMMON_10003/10008` |
| `PUT /alert-records/{recordId}/ack` | Body：`action`(Enum `ACK`(默认)/`SILENCE`)、`silenceSec`(条件必填，`SILENCE` 时)、`remark`(可选) | `{ "recordId": "...", "ackStatus": "ACKED", "silenceUntil": null }` | `TRACE_20003`、`TRACE_30001`、`TRACE_40002` |

**业务规则**：告警由 `process-trace` 内部定时器按 `windowSec` 周期评估（数据源为自身聚合 + 消费的轨迹事件流）；命中后按 `channels`/`templateCode` 调用 NOTIFY 内部接口发送并写 `alert_records`，同时通过 WebSocket `/ws/notify` 推送 `ALERT_FIRED` 消息给在线管理员；`silenceSec` 内的重复命中合并为一条（`triggerCount` 累加）；`status=FIRING` 时重复确认返回 `TRACE_30001`。

---

## 3. 实时推送契约

**归属与分工（强制约定）**

| 项 | 约定 |
| --- | --- |
| 提供方 | **全部实时推送统一由 `process-trace` 提供**（它已消费全量轨迹事件，无需额外跨服务订阅） |
| 网关路由 | `/ws/**`、`/sse/**` → `process-trace`（8087），鉴权在握手/JWT 校验阶段完成 |
| 一次性拉取 | 与增量推送**互补**：实例流程图高亮全量数据由 `GET /api/engine/v1/instances/{instanceId}/diagram`（`process-engine`）提供，前端先全量初始化，再订阅 SSE 增量本地合并；**不存在**由 ENGINE 提供的流式接口 |
| 传输选择 | 工作台类（双向、按用户/租户维度、多资源订阅）用 **WebSocket**；实例监控类（单向、单实例、可自动重连）用 **SSE** |
| 消息可靠性 | 至少一次投递 + 客户端按 `seq` 幂等去重；断线后按 `lastSeq` 增量补拉（补拉走 REST 轨迹接口） |
| 背压 | 单连接发送队列上限 1000 条，超限丢弃 `P2` 级消息并下发 `BACKPRESSURE` 通知（`TRACE_50005`） |

### 3.1 连接地址与鉴权

**WebSocket（工作台，双向）**

| 项 | 值 |
| --- | --- |
| 地址 | `wss://{domain}/ws/notify?token={accessToken}`（本地 `ws://127.0.0.1:8080/ws/notify?token=...`） |
| 鉴权 | 握手阶段校验 `token`（同一套 JWT，不额外签发）；校验失败返回 HTTP 401 并**不升级**协议；服务端从 Token 解析 `tenantId`/`userId`，租户上下文由服务端推导（客户端不可指定） |
| 协议 | 原生 WebSocket（`Sec-WebSocket-Protocol: pe-notify-v1`，服务端择一确认） |
| 心跳 | 客户端每 25s 发 `{"type":"PING"}`，服务端回 `{"type":"PONG","ts":...}`；服务端 3 次未收到 PING（75s）则关闭连接（close code `4000`） |
| 连接数 | 单用户 ≤ 5 连接，超出返回握手失败并附带 `TRACE_50004` 语义（close code `4029`） |
| 生命周期 | 心跳保活 + 客户端指数退避重连（1s/2s/4s/8s，上限 30s，带 0–500ms 抖动） |

**SSE（实例监控，单向）**

| 项 | 值 |
| --- | --- |
| 地址 | `https://{domain}/sse/instances/{instanceId}?token={accessToken}&types={事件类型逗号分隔}&lastSeq={上次序号}` |
| 鉴权 | 与 WebSocket 相同（握手期校验 Token，并校验该实例的数据权限，越权返回 403 + `COMMON_20005`） |
| 响应头 | `Content-Type: text/event-stream;charset=UTF-8`、`Cache-Control: no-cache`、`Connection: keep-alive`、`X-Accel-Buffering: no` |
| 心跳 | 服务端每 15s 发送注释帧 `: heartbeat`（不产生事件） |
| 断线续传 | 支持 `Last-Event-ID` 请求头或 `lastSeq` 查询参数；服务端只补发 `seq > lastSeq` 且距今 ≤ 10 分钟的事件，超出范围响应 `event: RESYNC_REQUIRED` 提示前端回退到 REST 全量拉取 |
| 连接上限 | 单用户 ≤ 5 条 SSE；超限返回 `TRACE_50004`（HTTP 429） |

### 3.2 订阅/退订消息格式（WebSocket）

**建立连接后客户端必须发送 `SUBSCRIBE`（否则不推送任何业务消息，只响应心跳）**

| 消息类型 | 方向 | 说明 |
| --- | --- | --- |
| `SUBSCRIBE` | C→S | 订阅资源变更 |
| `UNSUBSCRIBE` | C→S | 退订 |
| `ACK` | C→S | 确认收到（用于服务端清理重发队列） |
| `PING` / `PONG` | C↔S | 心跳 |
| `EVENT` | S→C | 业务事件推送 |
| `SNAPSHOT` | S→C | 订阅成功后的首帧快照（避免客户端空白等待） |
| `ERROR` | S→C | 订阅错误/权限不足 |
| `BACKPRESSURE` | S→C | 背压通知（丢弃了低优先级消息） |

**`SUBSCRIBE` 请求体**

| 字段 | 类型 | 必填 | 说明 | 约束 |
| --- | --- | --- | --- | --- |
| `type` | String | 是 | 固定 `SUBSCRIBE` | — |
| `requestId` | String | 是 | 客户端请求标识，服务端在响应中原样回显 | ≤ 64 字符 |
| `topics` | Array\<String\> | 是 | 订阅主题列表 | 见下表；单连接 ≤ 20 个 |
| `filters` | Object | 否 | 过滤条件 | 如 `{ "processKeys": ["leave_approval"], "onlyMine": true }` |
| `lastSeq` | Long | 否 | 上次收到的最大序号 | 用于断线补发；不传则只推新事件 |
| `snapshot` | Boolean | 否 | 是否先推 `SNAPSHOT` | 默认 `true` |

**可订阅主题（`topics`）**

| 主题 | 说明 | 主要事件类型 | 权限码 |
| --- | --- | --- | --- |
| `INSTANCE_STATE` | 我可见实例的状态变更 | `INSTANCE_STARTED`/`INSTANCE_SUSPENDED`/`INSTANCE_ACTIVATED`/`INSTANCE_ENDED` | `engine:instance:list` |
| `TASK_ARRIVED` | 待办到达/变化 | `TASK_CREATED`/`TASK_CLAIMED`/`TASK_TRANSFERRED`/`TASK_DELEGATED`/`TASK_CANCELED`/`TASK_TIMEOUT` | `task:todo:list` |
| `NODE_TRACE` | 节点执行轨迹（实例监控用） | `NODE_ENTERED`/`NODE_COMPLETED`/`NODE_FAILED`/`TOKEN_CREATED`/`TOKEN_CONSUMED`/`GATEWAY_JOINED` | `engine:instance:query` |
| `INCIDENT_ALERT` | 故障与告警 | `INCIDENT_CREATED`/`INCIDENT_RESOLVED`/`ALERT_FIRED`/`ALERT_ACKED` | `engine:incident:list` |
| `MESSAGE_INBOX` | 我的站内信 | `MESSAGE_RECEIVED`/`MESSAGE_READ` | 登录态 |
| `NOTIFY_STATUS` | 通知发送状态（管理视角） | `MESSAGE_SENT`/`MESSAGE_FAILED` | `notify:message:list` |
| `JOB_STATUS` | 调度作业执行状态 | `JOB_STARTED`/`JOB_SUCCEEDED`/`JOB_FAILED` | `sched:job:list` |
| `CALL_STATUS` | 连接器调用异常 | `CALL_FAILED`/`CALL_RATE_ABNORMAL` | `integ:call-log:list` |

**`SUBSCRIBE` / `UNSUBSCRIBE` / `ACK` / `ERROR` 消息示例（JSON 文本帧）**

```json
{ "type": "SUBSCRIBE", "requestId": "req-1", "topics": ["TASK_ARRIVED", "INSTANCE_STATE"], "filters": { "processKeys": ["leave_approval"], "onlyMine": true }, "lastSeq": 1024, "snapshot": true }
```

```json
{ "type": "UNSUBSCRIBE", "requestId": "req-2", "topics": ["INSTANCE_STATE"] }
```

```json
{ "type": "ACK", "seq": 1031 }
```

```json
{ "type": "ERROR", "requestId": "req-1", "code": "COMMON_20002", "message": "无接口访问权限", "detail": { "topic": "JOB_STATUS", "requiredPerm": "sched:job:list" } }
```

### 3.3 服务端推送消息体

**统一消息信封（WebSocket `EVENT`、SSE `data:` 同构）**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `type` | String | `EVENT`（WebSocket）/ SSE 中由 `event:` 行承载 |
| `eventId` | String | 事件唯一 ID（UUID），客户端幂等去重键 |
| `eventType` | Enum | 见下表 |
| `topic` | String | 所属订阅主题 |
| `tenantId` | String | 租户（服务端推导） |
| `instanceId` | String | 相关实例（可为 `null`） |
| `taskId` | String | 相关任务（可为 `null`） |
| `nodeInstanceId` | String | 相关节点实例（可为 `null`） |
| `seq` | Long | **连接内单调递增序号**，从 1 开始；客户端按此去重与补拉 |
| `occurredAt` | Long | 事件发生时间（毫秒时间戳） |
| `occurredAtText` | String | `yyyy-MM-dd HH:mm:ss.SSS` 格式，便于直接展示 |
| `priority` | Enum | `P0` / `P1` / `P2`（背压时优先丢弃 `P2`） |
| `payload` | Object | 业务载荷，按 `eventType` 定义 |
| `traceId` | String | 链路 ID，便于跳转轨迹检索 |
| `version` | Integer | 事件结构版本（当前 `1`），向后兼容新增字段 |

**核心事件推送体字段表**

| eventType | payload 字段 | 说明 |
| --- | --- | --- |
| `INSTANCE_STARTED`（实例状态变更） | `instanceNo`、`processKey`、`processName`、`version`、`businessKey`、`title`、`status`、`initiatorId`、`initiatorName`、`startTime`、`currentNodes[]` | 实例启动 |
| `INSTANCE_SUSPENDED` / `INSTANCE_ACTIVATED` | `instanceId`、`status`、`reason`、`operatorName`、`operTime` | 挂起/恢复 |
| `INSTANCE_ENDED` | `instanceId`、`instanceNo`、`status`（`COMPLETED`/`TERMINATED`）、`endTime`、`durationMs`、`reason` | 实例结束（含终止原因） |
| `TASK_CREATED`（任务到达） | `taskId`、`taskName`、`taskDefKey`、`instanceId`、`instanceNo`、`title`、`processKey`、`nodeId`、`nodeName`、`assigneeId`、`candidateSummary`、`priority`、`dueTime`、`multiMode`、`formKey`、`initiatorName` | 待办到达；`assigneeId` 为空表示候选待认领 |
| `TASK_CLAIMED` / `TASK_TRANSFERRED` / `TASK_DELEGATED` | `taskId`、`status`、`assigneeId`、`assigneeName`、`previousAssigneeId`、`opinion`、`operTime` | 任务流转 |
| `TASK_COMPLETED` | `taskId`、`result`、`opinion`、`operatorName`、`completeTime`、`instanceStatus`、`nextTasks[]` | 任务办结 |
| `TASK_CANCELED` / `TASK_TIMEOUT` | `taskId`、`reason`、`dueTime`、`operTime` | 待办失效 |
| `NODE_ENTERED`（节点执行轨迹） | `nodeInstanceId`、`nodeId`、`nodeName`、`nodeType`、`status`、`loopIndex`、`tokenId`、`scopeId`、`enterTime` | 节点进入（SSE 高亮用） |
| `NODE_COMPLETED` | `nodeInstanceId`、`nodeId`、`status`、`durationMs`、`endTime`、`outputDigest` | 节点完成 |
| `NODE_FAILED` | `nodeInstanceId`、`nodeId`、`status`、`errorCode`、`errorMsg`、`retryCount`、`incidentId` | 节点失败 |
| `TOKEN_CREATED` / `TOKEN_CONSUMED` | `tokenId`、`nodeId`、`status`、`scopeId`、`gatewayJoinKey` | 令牌推进 |
| `GATEWAY_JOINED` | `nodeId`、`joinKey`、`arrivedCount`、`expectedCount`、`arrivedTokens[]` | 网关闭环进度 |
| `INCIDENT_CREATED`（故障告警） | `incidentId`、`instanceId`、`instanceNo`、`nodeId`、`nodeName`、`incidentType`、`errorCode`、`errorMsg`、`retryCount` | 故障产生 |
| `INCIDENT_RESOLVED` | `incidentId`、`status`、`resolveType`（`RETRY`/`SKIP`/`CANCEL`）、`operatorName`、`remark` | 故障处理 |
| `ALERT_FIRED` | `recordId`、`ruleId`、`ruleName`、`level`、`triggerValue`、`threshold`、`targetSummary`、`firedTime` | 告警触发（P0/P1 建议同时走通知渠道） |
| `ALERT_ACKED` | `recordId`、`ackStatus`、`ackedBy`、`silenceUntil` | 告警确认/静默 |
| `MESSAGE_RECEIVED` | `messageId`、`msgNo`、`channel`、`title`、`content`、`bizType`、`bizId` | 站内信到达 |
| `JOB_STARTED` / `JOB_SUCCEEDED` / `JOB_FAILED` | `jobId`、`jobCode`、`executionId`、`status`、`fireTime`、`costMs`、`errorMsg` | 调度状态 |
| `CALL_FAILED` | `callLogId`、`connectorCode`、`operationCode`、`httpStatus`、`errorCode`、`costMs`、`instanceId` | 连接器调用失败 |

**SSE 帧格式示例（实例监控）**

```text
event: NODE_ENTERED
id: 1032
data: {"eventId":"6b1f0f2e-...","eventType":"NODE_ENTERED","topic":"NODE_TRACE","tenantId":"1001","instanceId":"1836427591029384756","seq":1032,"occurredAt":1747704662114,"occurredAtText":"2025-05-20 10:31:02.114","priority":"P1","version":1,"traceId":"4f2a9c1b...","payload":{"nodeInstanceId":"1836...","nodeId":"n_approve","nodeName":"主管审批","nodeType":"USER_TASK","status":"ACTIVE","loopIndex":0,"tokenId":"...","enterTime":"2025-05-20 10:31:02.114"}}

: heartbeat
```

**WebSocket 帧示例（任务到达）**

```json
{ "type": "EVENT", "eventId": "3c9a2b71-...", "eventType": "TASK_CREATED", "topic": "TASK_ARRIVED", "tenantId": "1001", "instanceId": "1836427591029384756", "taskId": "1836427591029384999", "nodeInstanceId": "1836427591029384888", "seq": 1033, "occurredAt": 1747704663114, "occurredAtText": "2025-05-20 10:31:03.114", "priority": "P1", "version": 1, "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718", "payload": { "taskId": "1836427591029384999", "taskName": "主管审批", "instanceId": "1836427591029384756", "instanceNo": "PI2025052000000123", "title": "张三的请假申请", "processKey": "leave_approval", "nodeId": "n_approve", "nodeName": "主管审批", "assigneeId": null, "candidateSummary": ["李四", "+2"], "priority": 5, "dueTime": "2025-05-21 10:31:02.114", "multiMode": "ANY", "formKey": "leave_form", "initiatorName": "张三" } }
```

### 3.4 心跳、重连与消息幂等

| 机制 | WebSocket | SSE |
| --- | --- | --- |
| 心跳 | 客户端 `PING`（25s）→ 服务端 `PONG`；服务端 75s 无 `PING` 关闭（code `4000`） | 服务端每 15s `: heartbeat` 注释帧；客户端 45s 未收到任何帧则主动重连 |
| 重连策略 | 指数退避 1s→2s→4s→8s→16s→30s（上限），抖动 0–500ms；重连成功后重新 `SUBSCRIBE` 并携带 `lastSeq` | 浏览器 `EventSource` 自动重连（`retry: 3000`），携带 `Last-Event-ID` |
| 补发窗口 | 服务端为每连接保留最近 1000 条事件（内存环形缓冲）+ 5 分钟 Redis 缓冲 | 同左；超出窗口返回 `RESYNC_REQUIRED` |
| 幂等键 | `eventId`（UUID）+ `seq`。客户端**必须**按 `eventId` 去重；`seq` 用于检测丢包（`seq` 跳变即触发 REST 补拉） | 同左 |
| 乱序处理 | 服务端保证单连接内按 `seq` 有序投递；客户端合并到本地模型时同时校验 `occurredAt`，旧事件丢弃 | 同左 |
| 补拉接口 | `GET /api/trace/v1/exec-traces?instanceId=...&startTimeFrom=...`（轨迹）/ `GET /api/task/v1/tasks/todo`（待办） | 同左 |
| 重连后一致性 | 建议客户端重连后先调 `INSTANCE_STATE`/`TASK_ARRIVED` 对应的 REST 列表接口做一次全量对账，再应用增量 | 实例图直接重拉 `GET /api/engine/v1/instances/{instanceId}/diagram` |

### 3.5 推送相关的错误与关闭码

| 场景 | WebSocket close code | SSE | 错误码 |
| --- | --- | --- | --- |
| Token 无效/过期 | 握手 HTTP 401（不升级） | HTTP 401 | `AUTH_20002` |
| Token 在连接中被踢下线 | `4001` | 服务端主动 `event: ERROR` + 关闭 | `COMMON_20007` |
| 单用户连接数超限 | `4029` | HTTP 429 | `TRACE_50004` |
| 实例数据权限不足（SSE） | — | HTTP 403 | `COMMON_20005` |
| 订阅主题无权限 | 连接保持，回 `ERROR` 单主题失败 | — | `COMMON_20002` |
| 会话已不存在（服务端清理） | `4004` | 连接关闭 | `TRACE_50003` |
| 发送队列积压丢帧 | 连接保持，发 `BACKPRESSURE` | 服务端丢弃 `P2` 事件并记日志 | `TRACE_50005` |
| 服务端重启/发布 | `1001`（going away） | 连接断开，由客户端重连 | — |

---

## 4. 消息（MQ）契约

消息中间件为 **Apache RocketMQ 5.3.x**（基线 2.1）。所有跨服务异步协作、引擎异步指令、领域事件与轨迹投递均以本文为准。

### 4.1 Topic 命名规范

| 项 | 规范 |
| --- | --- |
| Topic 格式 | `PE_{DOMAIN}_{EVENT}`，全大写下划线；`DOMAIN ∈ {ENGINE, DESIGN, SCHED, INTEG, NOTIFY, TRACE, DLQ}`（基线 6.10） |
| 示例 | `PE_ENGINE_NODE_COMPLETED`、`PE_ENGINE_TASK_COMPLETED` |
| 死信 Topic | `PE_DLQ_{原Topic}`，如 `PE_DLQ_PE_ENGINE_NODE_COMPLETED` |
| Tag | 同一 Topic 内按子类型打 Tag（如 `PE_ENGINE_NODE_COMPLETED` 的 Tag = `nodeType`），消费端可用 `*` 或指定 Tag 订阅 |
| Key | 消息 Key 固定为 `{tenantId}:{instanceId}:{nodeInstanceId}` 或业务主键，用于消息轨迹查询与顺序消费 |
| 顺序性 | 引擎指令 Topic `PE_ENGINE_COMMAND` 使用**顺序消息**（ShardingKey = `instanceId`），保证同实例命令串行；其余 Topic 为普通消息，依赖消费端幂等 |
| 分区/队列 | 默认 4 队列；`PE_ENGINE_COMMAND` 按 `instanceId` 哈希选队列；`PE_TRACE_EXEC_TRACE` 8 队列（高吞吐） |
| 消费模式 | 集群消费（Clustering）；轨迹类可配广播用于本地推送节点 |
| 保留期 | 业务事件 3 天；轨迹类 1 天；DLQ 14 天 |
| 生产者约束 | 仅允许**发件箱中继**（Outbox Relay）投递业务事件，禁止业务代码直接 `send`（保证与本地事务一致，基线 6.10） |
| 版本兼容 | 消息信封 `version` 字段标识结构版本；新增字段向后兼容，删除/改语义需升版本并在消费端双读（见第 7 节） |

### 4.2 Topic 清单

| # | Topic | 生产者 | 消费者 | 消息体（关键载荷） | 幂等键 | 重试与死信策略 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `PE_ENGINE_COMMAND` | engine（Outbox 中继） | engine-worker | `{ commandType, instanceId, nodeInstanceId, payload, attempt }` | `{instanceId}:{commandType}:{nodeInstanceId}:{attempt}` | 顺序消息；消费失败重试 5 次（退避 1s/5s/30s/60s/300s），仍失败入 DLQ 并生成 incident |
| 2 | `PE_ENGINE_INSTANCE_STARTED` | engine | trace、notify、integration（订阅方） | 见 4.3.1 | `eventId` | 重试 3 次（10s/60s/300s）→ DLQ |
| 3 | `PE_ENGINE_INSTANCE_SUSPENDED` | engine | trace、notify | `{ instanceId, instanceNo, reason, operatorId, suspendedTasks }` | `eventId` | 同 2 |
| 4 | `PE_ENGINE_INSTANCE_ENDED` | engine | trace、notify、sched（回调）、business（外部订阅） | 见 4.3.7 | `eventId` | 同 2 |
| 5 | `PE_ENGINE_INSTANCE_TERMINATED` | engine | trace、notify | `{ instanceId, instanceNo, reason, operatorId, canceledTasks, canceledTimers }` | `eventId` | 同 2 |
| 6 | `PE_ENGINE_NODE_COMPLETED` | engine | trace | 见 4.3.2 | `eventId` | 重试 3 次 → DLQ（轨迹允许最终丢弃但必须先入 DLQ） |
| 7 | `PE_ENGINE_NODE_FAILED` | engine | trace、notify | 见 4.3.3 | `eventId` | 同 6 |
| 8 | `PE_ENGINE_TASK_CREATED` | engine | trace、notify | 见 4.3.4 | `eventId` | 重试 3 次 → DLQ |
| 9 | `PE_ENGINE_TASK_COMPLETED` | engine | trace、notify、integration | 见 4.3.5 | `eventId` | 同 8 |
| 10 | `PE_ENGINE_TIMER_FIRED` | engine | trace、notify | 见 4.3.6 | `{instanceId}:{timerId}:{fireTime}` | 同 8 |
| 11 | `PE_ENGINE_MESSAGE_CORRELATED` | engine | trace | `{ instanceId, messageName, subscriptionId, variables }` | `{instanceId}:{messageName}:{messageId}` | 同 2 |
| 12 | `PE_ENGINE_SIGNAL_BROADCAST` | engine | trace | `{ signalName, scope, matchedInstances, variables }` | `eventId` | 同 2 |
| 13 | `PE_ENGINE_INCIDENT_CREATED` | engine | trace、notify | `{ incidentId, instanceId, nodeInstanceId, incidentType, errorCode, errorMsg, retryCount }` | `incidentId` | 重试 5 次 → DLQ（P0 类事件，禁止静默丢弃） |
| 14 | `PE_ENGINE_INCIDENT_RESOLVED` | engine | trace、notify | `{ incidentId, resolveType, operatorId, remark }` | `incidentId` | 同 13 |
| 15 | `PE_ENGINE_COMPENSATION_EXECUTED` | engine | trace | `{ instanceId, compensationId, nodeId, status, seqNo }` | `compensationId` | 同 6 |
| 16 | `PE_DESIGN_VERSION_PUBLISHED` | design | engine（刷模型缓存）、trace | 见 4.3.8 | `{defId}:{versionId}` | 重试 10 次（1s 起指数退避），失败告警并人工触发缓存刷新 |
| 17 | `PE_DESIGN_VERSION_OFFLINE` | design | engine、trace | `{ defId, versionId, version, reason }` | `{defId}:{versionId}` | 同 16 |
| 18 | `PE_DESIGN_VERSION_ROLLBACK` | design | engine、trace | `{ defId, newVersionId, targetVersionId, operatorId }` | `{defId}:{newVersionId}` | 同 16 |
| 19 | `PE_DESIGN_FORM_PUBLISHED` | design | engine（刷表单缓存）、trace | `{ formId, formKey, version, versionId }` | `{formKey}:{version}` | 同 16 |
| 20 | `PE_SCHED_JOB_FIRED` | sched | trace | `{ jobId, jobCode, executionId, fireTime, triggerType }` | `executionId` | 重试 3 次 → DLQ |
| 21 | `PE_SCHED_JOB_SUCCEEDED` | sched | trace、notify | `{ jobId, jobCode, executionId, costMs, instanceId }` | `executionId` | 同 20 |
| 22 | `PE_SCHED_JOB_FAILED` | sched | trace、notify | `{ jobId, jobCode, executionId, errorCode, errorMsg, retryCount }` | `executionId` | 重试 5 次 → DLQ |
| 23 | `PE_INTEG_CONNECTOR_CHANGED` | integration | engine（刷连接器缓存） | `{ connectorId, connectorCode, action: "CREATED"/"UPDATED"/"DISABLED"/"DELETED", operationIds }` | `{connectorId}:{action}:{updateTime}` | 重试 10 次，失败告警（引擎缓存 TTL 5 分钟兜底） |
| 24 | `PE_INTEG_CREDENTIAL_EXPIRED` | integration | notify、trace | `{ credId, credName, expireTime, refCount }` | `{credId}:{expireTime}` | 重试 3 次 → DLQ |
| 25 | `PE_INTEG_CALL_FAILED` | engine / integration（调试） | trace、notify | `{ callLogId, connectorCode, operationCode, instanceId, nodeInstanceId, httpStatus, errorCode, errorMsg, costMs }` | `callLogId` | 重试 3 次 → DLQ |
| 26 | `PE_NOTIFY_MESSAGE_SENT` | notify | trace | `{ messageId, msgNo, channel, receiverUserId, templateCode, providerMsgId, costMs }` | `messageId` | 重试 3 次 → DLQ |
| 27 | `PE_NOTIFY_MESSAGE_FAILED` | notify | trace、notify（触发告警） | `{ messageId, msgNo, channel, errorCode, errorMsg, retryCount, receiverUserId }` | `messageId` | 重试 5 次 → DLQ |
| 28 | `PE_TRACE_OPER_LOG` | 全部服务 | trace（写 ClickHouse） | `{ logId, tenantId, operatorId, module, action, targetType, targetId, requestUri, httpMethod, paramsDigest, success, costMs, ip, ua, operTime, traceId }` | `logId` | 重试 3 次 → DLQ；DLQ 满时降级为本地日志文件 |
| 29 | `PE_TRACE_EXEC_TRACE` | engine | trace（写 ClickHouse + 实时推送） | `{ execTraceId, instanceId, nodeInstanceId, tokenId, eventType, nodeId, nodeType, payload, ts, traceId, seq }` | `execTraceId` | 重试 3 次 → DLQ（允许丢弃低价值事件，丢弃计数入 Prometheus） |
| 30 | `PE_TRACE_ALERT_FIRED` | trace | notify | `{ recordId, ruleId, ruleName, level, triggerValue, threshold, targetSummary, receivers }` | `recordId` | 重试 5 次 → DLQ（P0 告警不允许丢失） |
| 31 | `PE_DLQ_{原Topic}` | 各服务 | 运维/管理端（人工或自动重放） | 原消息体 + `{ originTopic, consumerGroup, errorStack, retryCount, firstFailTime, lastFailTime }` | 原幂等键 | 不自动重试；支持管理端按 Topic/时间/关键字重放（见 4.6） |

> 统计：业务 Topic 共 **30** 个（#1–#30）+ DLQ Topic 命名模板 1 个（`PE_DLQ_{原Topic}`，实际实例数按源 Topic 数量动态生成，当前 30 个）。

### 4.3 消息信封与关键事件消息体

#### 4.3.0 统一信封结构

所有业务事件（含 DLQ 中的原始消息）统一信封（基线 6.10）：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `eventId` | String | 是 | 事件唯一 ID（UUID），**消费幂等主键** |
| `eventType` | String | 是 | 事件类型，与 Topic 语义一致，如 `NODE_COMPLETED` |
| `tenantId` | String | 是 | 租户 ID（Long 字符串） |
| `occurredAt` | Long | 是 | 事件发生时间（毫秒时间戳） |
| `traceId` | String | 是 | 链路 ID，与 HTTP 请求 `traceId` 贯通 |
| `version` | Integer | 是 | 信封 + 载荷结构版本，当前 `1` |
| `payload` | Object | 是 | 业务载荷，按 `eventType` 定义 |
| `producer` | String | 否 | 生产者服务名（`process-engine` 等） |
| `aggregateType` / `aggregateId` | String | 否 | 聚合类型与 ID（对应 `rt_outbox`） |
| `shardingKey` | String | 否 | 顺序消息分片键（默认 `instanceId`） |
| `retryCount` | Integer | 否 | 由 DLQ/消费端回填的重试次数 |

```json
{
  "eventId": "9f2c7a10-5b3d-4e21-9c88-31ab0f7e6d52",
  "eventType": "NODE_COMPLETED",
  "tenantId": "1001",
  "occurredAt": 1747704663114,
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718",
  "version": 1,
  "producer": "process-engine",
  "aggregateType": "NODE_INSTANCE",
  "aggregateId": "1836427591029384888",
  "shardingKey": "1836427591029384756",
  "payload": { "instanceId": "1836427591029384756", "nodeInstanceId": "1836427591029384888", "nodeId": "n_http_call", "nodeType": "HTTP_TASK", "status": "COMPLETED", "durationMs": "812", "attempt": 1 }
}
```

#### 4.3.1 实例已启动（`PE_ENGINE_INSTANCE_STARTED`）

| payload 字段 | 类型 | 说明 |
| --- | --- | --- |
| `instanceId` / `instanceNo` | String | 实例标识 |
| `defId` / `versionId` / `processKey` / `processName` / `version` | String/Integer | 定义与版本 |
| `businessKey` / `title` | String | 业务键与标题 |
| `initiatorId` / `initiatorName` / `initiatorDeptId` | String | 发起人 |
| `variables` | Object | 启动变量（大对象以 `valueRef` 呈现） |
| `startNodeId` / `startNodeInstanceId` | String | 起始节点 |
| `startTime` | Long | 启动毫秒时间戳 |
| `parentInstanceId` / `rootInstanceId` | String | 父子实例 |
| `source` | Enum | `API` / `FORM` / `SCHEDULE` / `MESSAGE` / `SUB_PROCESS` |

#### 4.3.2 节点已完成（`PE_ENGINE_NODE_COMPLETED`）

| payload 字段 | 类型 | 说明 |
| --- | --- | --- |
| `instanceId` / `nodeInstanceId` / `tokenId` | String | 实例、节点实例、令牌 |
| `nodeId` / `nodeName` / `nodeType` | String | 节点信息（`NodeType` 见基线 5.1） |
| `status` | Enum | `COMPLETED` / `SKIPPED` / `COMPENSATED` |
| `loopIndex` / `scopeId` | Integer/String | 多实例序号与作用域 |
| `assigneeId` / `operatorId` | String | 办理人（人任务） |
| `durationMs` | Long | 节点耗时（字符串） |
| `startTime` / `endTime` | Long | 起止毫秒时间戳 |
| `attempt` / `retryCount` | Integer | 第几次尝试 |
| `output` / `outputDigest` | Object | 输出快照（大对象转 `outputRef`） |
| `edgeTakenId` / `nextNodeIds` | String/Array | 走出的连线与下游节点（网关决策结果） |
| `connectorRef` | Object | 连接器节点：`{ connectorCode, operationCode, callLogId }`（`HTTP_TASK`/`SQL_TASK`/`MQ_PRODUCE_TASK`/`SERVICE_TASK` 等） |

#### 4.3.3 节点执行失败（`PE_ENGINE_NODE_FAILED`）

| payload 字段 | 类型 | 说明 |
| --- | --- | --- |
| `instanceId` / `nodeInstanceId` / `tokenId` | String | 定位信息 |
| `nodeId` / `nodeName` / `nodeType` | String | 节点信息 |
| `status` | Enum | `FAILED` |
| `errorCode` / `errorMsg` | String | 错误码（可透传 `INTEG_*`）与信息 |
| `errorType` | Enum | `BUSINESS` / `TECHNICAL` / `TIMEOUT` / `EXPRESSION` |
| `retryCount` / `maxRetryCount` / `nextRetryTime` | Integer/Long | 重试信息；`nextRetryTime=null` 表示不再重试 |
| `incidentId` | String | 重试耗尽后生成的故障 ID（未耗尽为 `null`） |
| `stackDigest` | String | 异常栈摘要（脱敏，≤ 2 KB） |
| `failedTime` | Long | 失败毫秒时间戳 |

#### 4.3.4 任务已创建（`PE_ENGINE_TASK_CREATED`）

| payload 字段 | 类型 | 说明 |
| --- | --- | --- |
| `taskId` / `taskDefKey` / `taskName` | String | 任务标识 |
| `instanceId` / `instanceNo` / `processKey` / `title` | String | 实例信息 |
| `nodeId` / `nodeName` / `nodeInstanceId` | String | 节点信息 |
| `taskType` / `multiMode` | Enum | `SINGLE`/`MULTI`；`ALL`/`ANY`/`PERCENT` |
| `assigneeId` / `assigneeName` | String | 直接指派时返回；候选模式为 `null` |
| `candidateUsers` / `candidateRoles` / `candidateDepts` / `candidateGroups` | Array\<String\> | 候选人配置 |
| `resolvedUserIds` | Array\<String\> | 解析后的候选人（去重后，≤ 200） |
| `emptyStrategy` | Enum | `ADMIN` / `SKIP` / `EXCEPTION` |
| `formDefId` / `formKey` / `formVersion` | String/Integer | 表单引用 |
| `priority` / `dueTime` | Integer/Long | 优先级与到期时间 |
| `initiatorId` / `initiatorName` | String | 发起人（通知模板常用） |
| `createTime` | Long | 创建毫秒时间戳 |

#### 4.3.5 任务已完成（`PE_ENGINE_TASK_COMPLETED`）

| payload 字段 | 类型 | 说明 |
| --- | --- | --- |
| `taskId` / `instanceId` / `nodeInstanceId` | String | 定位信息 |
| `taskName` / `nodeId` / `nodeName` | String | 节点信息 |
| `result` | Enum | `APPROVE` / `REJECT` |
| `decidedBy` / `decidedByName` | String | 办理人 |
| `operType` | Enum | `COMPLETE` / `REJECT` / `DELEGATE_CONFIRM` / `ADD_SIGN` |
| `opinion` | String | 办理意见（≤ 1000） |
| `formDataDigest` | Object | 表单数据摘要（敏感字段脱敏；全量在 `rt_task.form_data_json`） |
| `variables` | Object | 本次提交写入的流程变量 |
| `attachmentIds` | Array\<String\> | 附件 ID |
| `multiMode` / `completedCount` / `totalCount` | Enum/Integer | 会签进度 |
| `instanceStatus` | Enum | 办理后实例状态 |
| `nextTaskIds` / `nextNodeIds` | Array\<String\> | 下游生成内容 |
| `completeTime` / `durationMs` | Long | 完成时间与耗时 |

#### 4.3.6 定时器到期（`PE_ENGINE_TIMER_FIRED`）

| payload 字段 | 类型 | 说明 |
| --- | --- | --- |
| `timerId` / `instanceId` / `nodeInstanceId` | String | 定位信息 |
| `nodeId` / `nodeName` | String | 节点信息 |
| `timerType` | Enum | `DATE` / `DURATION` / `CYCLE` / `CRON` |
| `dueTime` / `fireTime` | Long | 应触发时间与实际触发时间（毫秒时间戳），偏差 = `fireTime - dueTime` |
| `repeatCount` / `repeatLimit` | Integer | 周期定时器进度 |
| `delayMs` | Long | 触发偏差（用于监控定时器精度，基线 7 要求 P99 < 2s） |
| `payload` | Object | 定时器携带数据 |
| `boundary` | Boolean | 是否为边界定时器（打断/非打断） |
| `targetNodeId` | String | 到期后进入的节点 |

#### 4.3.7 流程实例已结束（`PE_ENGINE_INSTANCE_ENDED`）

| payload 字段 | 类型 | 说明 |
| --- | --- | --- |
| `instanceId` / `instanceNo` | String | 实例标识 |
| `processKey` / `processName` / `version` / `versionId` | String/Integer | 定义与版本 |
| `businessKey` / `title` | String | 业务键与标题 |
| `status` | Enum | `COMPLETED` / `TERMINATED` |
| `endNodeId` / `endType` | String/Enum | 结束节点与结束方式（`END_EVENT`/`TERMINATE_END_EVENT`/`TERMINATED_BY_API`） |
| `initiatorId` / `initiatorName` | String | 发起人 |
| `startTime` / `endTime` / `durationMs` | Long | 时间与总耗时 |
| `variables` | Object | 结束时的根作用域变量快照（大对象转 `valueRef`） |
| `finalResult` | Object | 业务结果摘要（如审批结论 `{ approved: true, approver: "李四" }`） |
| `completedNodeCount` / `totalNodeCount` | Integer | 节点执行统计 |
| `incidentCount` / `compensationCount` | Integer | 故障与补偿次数 |
| `terminateReason` | String | 终止原因（`status=TERMINATED` 时） |

#### 4.3.8 领域事件发布（`PE_ENGINE_DOMAIN_EVENT` 通用通道）

对于流程模型通过 `MQ_PRODUCE_TASK` 节点或「领域事件发布」能力对外投递的业务事件，统一使用领域事件通道：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `domainEventType` | String | 业务自定义事件类型，如 `LEAVE_APPROVED`（由节点 `properties.eventType` 配置） |
| `instanceId` / `instanceNo` / `processKey` / `businessKey` | String | 来源实例 |
| `nodeId` / `nodeInstanceId` | String | 发布节点 |
| `eventPayload` | Object | 节点输出映射后的业务载荷（经 `transform_json` 映射，遵循 `output_schema`） |
| `publishedBy` | String | 触发者（用户 ID 或 `SYSTEM`） |
| `publishedAt` | Long | 发布毫秒时间戳 |
| `deliveryMode` | Enum | `BROADCAST`（Topic 订阅）/ `TARGETED`（指定业务系统回调） |
| `targetRef` | String | `TARGETED` 模式下的目标引用（连接器操作或回调 URL） |

### 4.4 消费幂等实现要求

| 要求 | 说明 |
| --- | --- |
| 幂等主键 | 统一使用信封 `eventId`；引擎指令类使用 `{instanceId}:{commandType}:{nodeInstanceId}:{attempt}`（基线 6.10） |
| 落库去重 | 消费端在**同一本地事务**内：① 写幂等表（`rt_idempotent` 或各服务 `*_consumed` 表，`biz_key` 唯一索引）；② 执行业务变更；③ 写自身 Outbox。唯一键冲突即视为已消费，直接 ACK |
| 幂等表清理 | 幂等记录保留 ≥ 7 天（与 MQ 保留期 + 最大重试窗口匹配），由定时任务批量清理 |
| 事务边界 | 禁止「先执行业务再写幂等表」；禁止在消费逻辑内调用外部 HTTP（应转为 Outbox 二次投递） |
| 顺序性 | 引擎命令按 `shardingKey=instanceId` 顺序消费；其他事件不保证顺序，消费端必须容忍乱序（例如用 `occurredAt` 与新值比较后再更新） |
| 失败语义 | 业务校验类失败（数据非法、状态冲突）**直接 ACK 并记录 DLQ 副本**，避免无意义重试；技术类失败（DB/网络）抛出异常触发重试 |
| 重复消费可观测 | 每次去重命中打点 `mq_consume_duplicate_total{topic}`，用于识别生产端重复投递 |
| 消费位点 | 依赖 RocketMQ 位点；不使用「手动跳过位点」；不可恢复消息必须入 DLQ 后再 ACK |

### 4.5 死信处理与重放

**死信产生流程**

1. 消费失败 → 按 Topic 策略重试（见 4.2 表；默认 3 次，指数退避 10s/60s/300s）。
2. 重试耗尽 → 原消息投递至 `PE_DLQ_{原Topic}`，并补充头部字段：`originTopic`、`consumerGroup`、`errorStack`（脱敏摘要）、`retryCount`、`firstFailTime`、`lastFailTime`、`dlqReason`（`RETRY_EXHAUSTED` / `DESERIALIZE_ERROR` / `BIZ_REJECTED`）。
3. 同时：① 打点 `mq_dlq_total{topic}`；② 若为引擎关键事件，生成 `rt_incident`（`incidentType=UNKNOWN`）并对账任务告警（基线 7：挂起态巡检 5 分钟一次）；③ 向 `PE_TRACE_OPER_LOG` 投递一条 DLQ 审计记录。

**DLQ 管理契约（运维接口，不计入服务接口总数）**

| 方法 | 路径 | 名称 | 权限码 | 说明 |
| --- | --- | --- | --- | --- |
| GET | `/api/trace/v1/dlq/messages` | 死信列表查询 | `trace:dlq:list` | Query：`topic`、`originTopic`、`consumerGroup`、`dlqReason`、`timeFrom`/`timeTo`、`keyword`、分页（`CURSOR`） |
| GET | `/api/trace/v1/dlq/messages/{dlqId}` | 死信详情 | `trace:dlq:query` | 返回原始消息体、错误栈、重试轨迹 |
| POST | `/api/trace/v1/dlq/messages/{dlqId}/replay` | 单条重放 | `trace:dlq:replay` | Body：`{ "targetTopic": "原 Topic 或修正后 Topic", "overridePayload": {}, "dryRun": false }`；重放使用**新的** `eventId` 但保留 `originEventId`，避免幂等表直接吞掉 |
| POST | `/api/trace/v1/dlq/messages/batch-replay` | 批量重放 | `trace:dlq:replay` | Body：`{ "query": {同列表过滤条件}, "maxCount": 1000, "targetTopic": "...", "resetIdempotent": false }`；返回受理任务 ID |
| POST | `/api/trace/v1/dlq/messages/{dlqId}/discard` | 丢弃死信 | `trace:dlq:discard` | Body：`{ "reason": "必填", "approver": "二级审批人" }`；丢弃必须填原因，写审计 |
| GET | `/api/trace/v1/dlq/statistics` | 死信统计 | `trace:dlq:list` | 按 Topic/原因/时间聚合，用于大盘与告警 |

**重放操作要求**

1. **权限与审批**：重放需 `trace:dlq:replay`；批量重放 > 100 条或 `resetIdempotent=true` 需二级审批（审批留痕在 `tr_oper_log`）。
2. **幂等处理**：默认重放消息带 `originEventId`，消费端幂等表若已存在则直接 ACK（安全但可能无效）；需要真正重跑时置 `resetIdempotent=true`，服务端会先清理对应幂等记录（仅允许单条或 ≤ 100 条批量）。
3. **顺序性**：批量重放按原 `occurredAt` 升序串行投递，且同一 `shardingKey` 落入同一队列。
4. **失败再入队**：重放后仍失败的消息重新回到 DLQ，`retryCount` 累加并标记 `replayed=true`，避免无限循环（`replayCount > 3` 时禁止再次重放，需人工修数）。
5. **可观测**：重放过程打点 `mq_dlq_replay_total{topic, result}`，并在 `tr_oper_log` 记录操作人、条数、目标 Topic。
6. **兜底**：对账任务每 5 分钟巡检「有活跃令牌但无待办/无定时器」的悬挂实例（基线 7），发现后按实例维度重投 `PE_ENGINE_COMMAND`，此路径**不**依赖 DLQ。

### 4.6 消息与 HTTP 的边界

| 场景 | 走 HTTP（同步） | 走 MQ（异步） |
| --- | --- | --- |
| 客户端查询/配置类操作 | ✅ | ❌ |
| 启动实例（同步推进到第一个等待态） | ✅（`/instances`） | ❌ |
| 启动实例（高吞吐、结果可后置） | ❌ | ✅（`/instances/async` → `PE_ENGINE_COMMAND`） |
| 任务办理 | ✅（必须同步返回推进结果） | ❌ |
| 自动化节点执行完成回调 | ✅（内部接口，见第 6 节） | 可选（MQ 指令恢复） |
| 轨迹/审计落库 | ❌ | ✅（`PE_TRACE_EXEC_TRACE` / `PE_TRACE_OPER_LOG`） |
| 通知发送 | ❌（除测试发送） | ✅（订阅触发，内部调用 NOTIFY） |
| 领域事件对外发布 | ❌ | ✅（`PE_ENGINE_DOMAIN_EVENT`） |

---

## 5. 完整交互示例

以下 4 个示例均为端到端可复现的最佳路径（Happy Path），并给出对应的错误场景。示例中的 `X-Tenant-Id` 一律为网关注入值（客户端不传），`Authorization` 为 `Bearer {accessToken}` 的省略写法。

### 5.1 示例 A：设计器发布流程版本

**场景说明**：设计器完成「请假审批 v3」建模后先做模型校验，再保存草稿、发布版本、导出 BPMN 归档。ENGINE 通过 `PE_DESIGN_VERSION_PUBLISHED` 刷新模型缓存，1s 内新版本可被启动使用。

**步骤 1：模型校验（保存前自检）**

```http
POST /api/design/v1/process-versions/validate HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
X-Request-Id: web-7f3c1a
Accept-Language: zh-CN
Content-Type: application/json; charset=UTF-8

{
  "versionId": "1836427591029384700",
  "level": "WARN",
  "checkForms": true
}
```

```json
HTTP/1.1 200 OK
X-Request-Id: web-7f3c1a
X-Trace-Id: 4f2a9c1b7e8d4306a1c2b3d4e5f60718
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "valid": true,
    "errorCount": 0,
    "warnCount": 1,
    "errors": [
      { "level": "WARN", "code": "MODEL_UNUSED_NODE", "nodeId": "n_old_review", "edgeId": null, "field": null, "message": "存在孤立节点 n_old_review", "hint": "删除该节点或补充连线" }
    ],
    "summary": { "nodeTypeCount": { "USER_TASK": 3, "EXCLUSIVE_GATEWAY": 1, "SERVICE_TASK": 1 }, "unreachableNodes": [], "deadEndNodes": [] }
  },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

**步骤 2：保存模型草稿**

```http
PUT /api/design/v1/process-versions/1836427591029384700/model HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
Idempotency-Key: design-save-1836427591029384700-9
Content-Type: application/json; charset=UTF-8

{
  "model": {
    "key": "leave_approval",
    "name": "请假审批",
    "version": 3,
    "settings": {
      "allowWithdraw": true,
      "withdrawScope": "FIRST_TASK",
      "autoSkipSameApprover": true,
      "notifyOnStart": true,
      "timeoutPolicy": { "type": "REMIND", "intervalMin": 60, "maxTimes": 3 },
      "vars": [{ "name": "days", "type": "NUMBER", "required": true }]
    },
    "nodes": [
      { "id": "n_start", "type": "START_EVENT", "name": "开始", "position": { "x": 120, "y": 200 }, "properties": { "initiator": "APPLICANT" } },
      { "id": "n_approve", "type": "USER_TASK", "name": "主管审批", "position": { "x": 400, "y": 200 },
        "properties": { "multiMode": "ANY", "assigneeStrategy": "ROLE", "assigneeValue": ["dept_manager"],
          "formRef": { "formKey": "leave_form", "version": "LATEST", "mode": "APPROVE" }, "dueMinutes": 1440 } },
      { "id": "n_end", "type": "END_EVENT", "name": "结束", "position": { "x": 700, "y": 200 }, "properties": {} }
    ],
    "edges": [
      { "id": "e1", "source": "n_start", "target": "n_approve", "name": "", "condition": { "expression": "", "isDefault": true } },
      { "id": "e2", "source": "n_approve", "target": "n_end", "name": "", "condition": { "expression": "", "isDefault": true } }
    ]
  },
  "modelHash": "sha256:9b1f2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f9",
  "remark": "新增主管审批节点"
}
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": { "versionId": "1836427591029384700", "modelHash": "sha256:9b1f2c3d4e5f6071...", "nodeCount": 3, "edgeCount": 2, "updateTime": "2025-05-20 09:20:11.238" },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

**步骤 3：发布版本**

```http
POST /api/design/v1/process-versions/1836427591029384700/publish HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
Idempotency-Key: publish-leave_approval-v3
Content-Type: application/json; charset=UTF-8

{ "remark": "v3：新增主管审批节点，支持 24h 超时提醒", "force": false, "autoOfflinePrevious": true }
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "versionId": "1836427591029384700",
    "defId": "1836427591029384000",
    "processKey": "leave_approval",
    "version": 3,
    "status": "PUBLISHED",
    "modelHash": "sha256:9b1f2c3d4e5f6071...",
    "publishTime": "2025-05-20 09:21:02.114",
    "publishBy": "1836427591029384001",
    "publishByName": "张设计",
    "offlineVersionId": "1836427591029384600",
    "warnings": [{ "level": "WARN", "code": "MODEL_UNUSED_NODE", "nodeId": "n_old_review", "message": "存在孤立节点 n_old_review" }]
  },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

**步骤 4：导出 BPMN 归档（可选）**

```http
GET /api/design/v1/process-versions/1836427591029384700/export-bpmn?download=false&includeDiagram=true HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": { "fileName": "leave_approval_v3.bpmn", "contentType": "application/xml", "xml": "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<bpmn:definitions xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" ...>...</bpmn:definitions>" },
  "traceId": "4f2a9c1b7e8d4306a1c2b3d4e5f60718"
}
```

**异步副作用**：DESIGN 在同一事务内写 `pf_deploy_log` 与 `rt_outbox`，中继投递 `PE_DESIGN_VERSION_PUBLISHED`（`payload { defId, versionId, version, modelHash }`）；ENGINE 消费后刷新 Redis 缓存 `design:model:{tenantId}:{versionId}`，缓存 TTL 5 分钟作为兜底。

**错误场景**

| 场景 | 请求变化 | 响应 |
| --- | --- | --- |
| 模型有 `ERROR` 级问题 | `force: true` 也无法绕过 | `HTTP 422` + `DESIGN_10003`，`data.errors[]` 给出 `MODEL_GATEWAY_CONDITION_MISSING` 等明细 |
| 保存已发布版本 | `PUT .../model` 到 `PUBLISHED` 版本 | `HTTP 409` + `DESIGN_30002`（提示需先复制为新草稿） |
| 并发保存（他人已改） | `modelHash` 与库中不一致 | `HTTP 412` + `COMMON_30003`，前端提示刷新后合并 |
| 表单引用不存在 | 节点 `formRef.formKey=leave_form_v9` 未发布 | `HTTP 422` + `DESIGN_10007`（`errors[].field=properties.formRef.formKey`） |
| 无发布权限 | 用户缺少 `design:process-version:publish` | `HTTP 403` + `DESIGN_20003` |
| 重复点击发布 | 同 `Idempotency-Key` 重放 | `HTTP 200` + `X-Idempotent-Replay: true`，返回首次结果 |

### 5.2 示例 B：业务系统按 businessKey 幂等启动流程实例

**场景说明**：外部 HR 系统在员工提交请假单后调用平台启动流程，以业务单号 `LEAVE-2025-0001` 作为 `businessKey` 与 `Idempotency-Key`。因网络重试，同一请求可能被提交两次——平台必须**只创建一个实例**并把已有实例返回给调用方。

**步骤 1：首次启动（同步推进到第一个等待态）**

```http
POST /api/engine/v1/instances HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
X-Request-Id: hr-sync-20250520-0001
Idempotency-Key: LEAVE-2025-0001
Accept-Language: zh-CN
Content-Type: application/json; charset=UTF-8

{
  "processKey": "leave_approval",
  "version": "LATEST",
  "businessKey": "LEAVE-2025-0001",
  "title": "张三的请假申请（3 天）",
  "initiatorId": "1836427591029384100",
  "variables": {
    "days": 3,
    "startDate": "2025-05-21",
    "endDate": "2025-05-23",
    "reason": "家中有事"
  },
  "syncWaitAfterStart": true
}
```

```json
HTTP/1.1 201 Created
X-Request-Id: hr-sync-20250520-0001
X-Trace-Id: 7c1d9e2f8a3b4c5d6e7f8091a2b3c4d5
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "instanceId": "1836427591029385000",
    "instanceNo": "PI2025052000000123",
    "defId": "1836427591029384000",
    "versionId": "1836427591029384700",
    "processKey": "leave_approval",
    "version": 3,
    "businessKey": "LEAVE-2025-0001",
    "title": "张三的请假申请（3 天）",
    "status": "RUNNING",
    "startTime": "2025-05-20 09:31:02.114",
    "endTime": null,
    "durationMs": "182",
    "currentNodes": [
      { "nodeId": "n_approve", "nodeName": "主管审批", "nodeType": "USER_TASK", "nodeInstanceId": "1836427591029385100", "status": "ACTIVE" }
    ],
    "activeTokens": 1,
    "pendingTaskCount": 1,
    "idempotent": false
  },
  "traceId": "7c1d9e2f8a3b4c5d6e7f8091a2b3c4d5"
}
```

**步骤 2：业务系统超时重试（同一 `Idempotency-Key` + 同一 `businessKey`）**

```http
POST /api/engine/v1/instances HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
Idempotency-Key: LEAVE-2025-0001
Content-Type: application/json; charset=UTF-8

{ "processKey": "leave_approval", "businessKey": "LEAVE-2025-0001", "variables": { "days": 3 }, "syncWaitAfterStart": true }
```

```json
HTTP/1.1 200 OK
X-Idempotent-Replay: true
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "instanceId": "1836427591029385000",
    "instanceNo": "PI2025052000000123",
    "status": "RUNNING",
    "businessKey": "LEAVE-2025-0001",
    "idempotent": true,
    "currentNodes": [{ "nodeId": "n_approve", "nodeName": "主管审批", "nodeType": "USER_TASK", "nodeInstanceId": "1836427591029385100", "status": "ACTIVE" }],
    "pendingTaskCount": 1
  },
  "traceId": "7c1d9e2f8a3b4c5d6e7f8091a2b3c4d5"
}
```

**步骤 3：若换用新的 `Idempotency-Key` 但 `businessKey` 不变（防御性去重生效）**

```json
HTTP/1.1 409 Conflict
Content-Type: application/json; charset=UTF-8

{
  "code": "ENGINE_30003",
  "message": "businessKey 重复启动：该业务键已存在运行中的流程实例",
  "data": {
    "processKey": "leave_approval",
    "businessKey": "LEAVE-2025-0001",
    "existingInstance": { "instanceId": "1836427591029385000", "instanceNo": "PI2025052000000123", "status": "RUNNING", "startTime": "2025-05-20 09:31:02.114" }
  },
  "traceId": "7c1d9e2f8a3b4c5d6e7f8091a2b3c4d5"
}
```

> 调用方处理建议：`409 + ENGINE_30003` 时**不是错误**，应读取 `data.existingInstance.instanceId` 复用；该设计保证「网络重试」「并发双写」都不会产生重复实例。

**步骤 4：查询实例状态（业务系统定时对账）**

```http
GET /api/engine/v1/instances/by-business-key?processKey=leave_approval&businessKey=LEAVE-2025-0001&includeEnded=false HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "found": true,
    "instance": {
      "instanceId": "1836427591029385000",
      "instanceNo": "PI2025052000000123",
      "processKey": "leave_approval",
      "version": 3,
      "businessKey": "LEAVE-2025-0001",
      "status": "RUNNING",
      "blockingReason": "USER_TASK",
      "currentNodes": [{ "nodeId": "n_approve", "nodeName": "主管审批", "nodeType": "USER_TASK" }],
      "startTime": "2025-05-20 09:31:02.114"
    }
  },
  "traceId": "7c1d9e2f8a3b4c5d6e7f8091a2b3c4d5"
}
```

**异步副作用**：启动事务内写 `rt_outbox` → 投递 `PE_ENGINE_INSTANCE_STARTED`（TRACE 落轨迹、NOTIFY 按订阅规则给发起人发站内信）与 `PE_ENGINE_TASK_CREATED`（推送给候选人）。

**错误场景**

| 场景 | 请求变化 | 响应 |
| --- | --- | --- |
| 未带 `Idempotency-Key` | 缺头 | `HTTP 400` + `COMMON_10001` |
| 流程无已发布版本 / 定义停用 | `processKey` 未发布 | `HTTP 404` + `ENGINE_40006` |
| 变量不满足 `settings.vars` | `days: "三天"`（类型不符） | `HTTP 400` + `ENGINE_10002`（`data` 内指出变量名与期望类型） |
| `businessKey` 超长 | 长度 200 | `HTTP 400` + `ENGINE_10003` |
| 同步推进超时 | 首节点为慢 SQL/HTTP，超过 5s | `HTTP 504` + `ENGINE_50006`（**实例已创建**，调用方按 `businessKey` 查询后续） |
| 租户实例配额超限 | 租户并发实例超限 | `HTTP 429` + `COMMON_30008`（带 `Retry-After`） |
| 命令队列背压 | 引擎积压 | `HTTP 409` + `ENGINE_30012`，按 `Retry-After` 退避重试 |
| 并发同实例推进 | 同实例命令冲突 | `HTTP 409` + `ENGINE_30005`（可重试） |

### 5.3 示例 C：审批人认领并完成任务（含表单数据与意见）

**场景说明**：李四在待办列表看到「主管审批」，先预览候选人，认领任务，读取任务详情（含表单 schema、表单数据、流程图上下文、会签进度），上传附件后提交同意意见并完成。完成后引擎在同一事务内推进令牌到结束节点。

**步骤 1：查询我的待办**

```http
GET /api/task/v1/tasks/todo?includeCandidate=true&pageNum=1&pageSize=20&orderBy=create_time&orderDirection=DESC HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "pageNum": 1,
    "pageSize": 20,
    "total": 2,
    "records": [
      {
        "taskId": "1836427591029385100",
        "taskDefKey": "n_approve",
        "taskName": "主管审批",
        "taskType": "SINGLE",
        "multiMode": "ANY",
        "instanceId": "1836427591029385000",
        "instanceNo": "PI2025052000000123",
        "businessKey": "LEAVE-2025-0001",
        "title": "张三的请假申请（3 天）",
        "processKey": "leave_approval",
        "processName": "请假审批",
        "version": 3,
        "nodeId": "n_approve",
        "nodeName": "主管审批",
        "assigneeId": null,
        "candidateSummary": ["李四", "+1"],
        "status": "CREATED",
        "priority": 5,
        "dueTime": "2025-05-21 09:31:02.114",
        "overdue": false,
        "createTime": "2025-05-20 09:31:02.114",
        "initiatorName": "张三",
        "attachmentCount": 0,
        "copyCount": 0
      }
    ],
    "counts": { "all": 2, "overdue": 0, "urgent": 0 }
  },
  "traceId": "a1b2c3d4e5f60718293a4b5c6d7e8f90"
}
```

**步骤 2：候选人预览**

```http
GET /api/task/v1/tasks/1836427591029385100/candidates?resolveUsers=true HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "taskId": "1836427591029385100",
    "assigneeStrategy": "ROLE",
    "candidateRoles": ["dept_manager"],
    "candidateDepts": [],
    "candidateGroups": [],
    "candidateUsers": [],
    "resolvedUsers": [
      { "userId": "1836427591029384100", "username": "lisi", "nickname": "李四", "deptName": "研发中心", "source": "ROLE:dept_manager", "skipped": false, "skipReason": null }
    ],
    "total": 1,
    "emptyStrategy": "ADMIN",
    "deduped": true,
    "autoSkipApplied": true
  },
  "traceId": "a1b2c3d4e5f60718293a4b5c6d7e8f90"
}
```

**步骤 3：认领任务**

```http
POST /api/task/v1/tasks/1836427591029385100/claim HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
Idempotency-Key: claim-1836427591029385100-lisi
Content-Type: application/json; charset=UTF-8

{ "remark": "开始处理" }
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": { "taskId": "1836427591029385100", "status": "CLAIMED", "assigneeId": "1836427591029384100", "assigneeName": "李四", "claimTime": "2025-05-20 10:02:11.004", "lockVersion": 4 },
  "traceId": "a1b2c3d4e5f60718293a4b5c6d7e8f90"
}
```

**步骤 4：读取任务详情（表单 schema + 数据 + 流程图上下文）**

```http
GET /api/task/v1/tasks/1836427591029385100?includeForm=true&includeDiagram=true&includeAttachments=true HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "task": {
      "taskId": "1836427591029385100", "taskName": "主管审批", "status": "CLAIMED",
      "assigneeId": "1836427591029384100", "assigneeName": "李四",
      "formDefId": "1836427591029384200", "formVersion": 2,
      "priority": 5, "dueTime": "2025-05-21 09:31:02.114", "lockVersion": 4
    },
    "instance": {
      "instanceId": "1836427591029385000", "instanceNo": "PI2025052000000123",
      "processKey": "leave_approval", "processName": "请假审批", "version": 3,
      "status": "RUNNING", "businessKey": "LEAVE-2025-0001", "title": "张三的请假申请（3 天）",
      "initiatorName": "张三", "startTime": "2025-05-20 09:31:02.114"
    },
    "form": {
      "formKey": "leave_form", "formName": "请假申请单", "version": 2, "mode": "APPROVE",
      "schema": {
        "fields": [
          { "fieldKey": "days", "label": "请假天数", "type": "number", "required": true, "readonly": true, "props": { "min": 0.5, "precision": 1 } },
          { "fieldKey": "startDate", "label": "开始日期", "type": "date", "required": true, "readonly": true },
          { "fieldKey": "endDate", "label": "结束日期", "type": "date", "required": true, "readonly": true },
          { "fieldKey": "reason", "label": "请假事由", "type": "textarea", "required": true, "readonly": true, "props": { "maxLength": 500 } },
          { "fieldKey": "approveRemark", "label": "审批意见", "type": "textarea", "required": false, "readonly": false },
          { "fieldKey": "attachment", "label": "附件", "type": "upload", "required": false, "props": { "accept": ".pdf,.jpg,.png", "multiple": true } }
        ],
        "layout": { "columns": 2, "groups": [{ "title": "申请信息", "fields": ["days", "startDate", "endDate", "reason"] }, { "title": "审批信息", "fields": ["approveRemark", "attachment"] }] }
      },
      "data": { "days": 3, "startDate": "2025-05-21", "endDate": "2025-05-23", "reason": "家中有事" },
      "dataPermission": { "editableFields": ["approveRemark", "attachment"], "hiddenFields": [] }
    },
    "diagram": {
      "instanceId": "1836427591029385000",
      "nodeStates": [
        { "nodeId": "n_start", "nodeType": "START_EVENT", "status": "COMPLETED", "activeCount": 0, "totalCount": 1, "enterTime": "2025-05-20 09:31:02.114", "durationMs": "12" },
        { "nodeId": "n_approve", "nodeType": "USER_TASK", "status": "ACTIVE", "activeCount": 1, "totalCount": 1, "assigneeNames": ["李四"], "enterTime": "2025-05-20 09:31:02.126", "durationMs": "0", "failCount": 0, "loopIndexes": [0] },
        { "nodeId": "n_end", "nodeType": "END_EVENT", "status": "NOT_EXECUTED", "activeCount": 0, "totalCount": 0 }
      ],
      "summary": { "activeNodes": 1, "completedNodes": 1, "failedNodes": 0, "skippedNodes": 0, "compensatedNodes": 0, "progressPercent": 50.0 },
      "dataTime": "2025-05-20 10:02:15.220"
    },
    "candidates": { "users": [], "roles": ["dept_manager"], "depts": [], "groups": [], "resolvedUsers": [{ "userId": "1836427591029384100", "nickname": "李四" }], "emptyStrategy": "ADMIN" },
    "attachments": [],
    "permissions": { "canClaim": false, "canComplete": true, "canReject": true, "canAddSign": true, "canTransfer": true, "canDelegate": true, "canWithdraw": false, "canUrge": true, "reason": null },
    "countersign": { "multiMode": "ANY", "total": 1, "completed": 0, "percent": 0.0, "pendingUsers": ["李四"] },
    "timeout": { "dueTime": "2025-05-21 09:31:02.114", "overdue": false, "remindCount": 0, "timeoutPolicy": { "type": "REMIND", "intervalMin": 60, "maxTimes": 3 } }
  },
  "traceId": "a1b2c3d4e5f60718293a4b5c6d7e8f90"
}
```

**步骤 5：上传附件（可选）**

```http
POST /api/task/v1/tasks/1836427591029385100/attachments HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
Content-Type: multipart/form-data; boundary=----pe

------pe
Content-Disposition: form-data; name="bizType"

OPINION
------pe
Content-Disposition: form-data; name="file"; filename="handover.pdf"
Content-Type: application/pdf

<二进制内容>
------pe--
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": { "attachmentIds": ["1836427591029385200"], "attachments": [{ "attachmentId": "1836427591029385200", "fileName": "handover.pdf", "fileSize": "204800", "contentType": "application/pdf", "uploaderId": "1836427591029384100", "uploaderName": "李四", "uploadTime": "2025-05-20 10:05:30.881", "url": "https://minio/...", "source": "UPLOAD" }] },
  "traceId": "a1b2c3d4e5f60718293a4b5c6d7e8f90"
}
```

**步骤 6：完成任务（同意）**

```http
POST /api/task/v1/tasks/1836427591029385100/complete HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
Idempotency-Key: complete-1836427591029385100-lisi
If-Match: "4"
Content-Type: application/json; charset=UTF-8

{
  "result": "APPROVE",
  "opinion": "请假事由合理，同意 3 天，注意做好工作交接。",
  "formData": { "approveRemark": "同意" },
  "variables": { "approved": true, "approveDays": 3 },
  "attachmentIds": ["1836427591029385200"],
  "lockVersion": 4
}
```

```json
HTTP/1.1 200 OK
X-Idempotent-Replay: false
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "taskId": "1836427591029385100",
    "status": "COMPLETED",
    "result": "APPROVE",
    "completeTime": "2025-05-20 10:06:02.114",
    "instanceId": "1836427591029385000",
    "instanceStatus": "COMPLETED",
    "advanced": true,
    "nextNodes": [{ "nodeId": "n_end", "nodeName": "结束", "nodeType": "END_EVENT", "nodeInstanceId": "1836427591029385300" }],
    "nextTasks": [],
    "ended": true,
    "durationMs": "300114",
    "countersign": { "multiMode": "ANY", "total": 1, "completed": 1, "percent": 100.0, "pendingUsers": [] },
    "warnings": []
  },
  "traceId": "a1b2c3d4e5f60718293a4b5c6d7e8f90"
}
```

**步骤 7：查询任务操作记录（审计追溯）**

```http
GET /api/task/v1/tasks/1836427591029385100/operations HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "pageNum": 1,
    "pageSize": 20,
    "total": 3,
    "records": [
      { "operLogId": "1836427591029385400", "operType": "CLAIM", "operTypeName": "认领", "operatorId": "1836427591029384100", "operatorName": "李四", "targetUserId": "1836427591029384100", "opinion": "开始处理", "operTime": "2025-05-20 10:02:11.004", "durationMs": null, "traceId": "a1b2c3d4..." },
      { "operLogId": "1836427591029385401", "operType": "COMPLETE", "operTypeName": "同意", "operatorId": "1836427591029384100", "operatorName": "李四", "opinion": "请假事由合理，同意 3 天，注意做好工作交接。", "formDataDigest": { "approveRemark": "同意" }, "attachments": [{ "attachmentId": "1836427591029385200", "fileName": "handover.pdf" }], "operTime": "2025-05-20 10:06:02.114", "durationMs": "231110", "traceId": "a1b2c3d4..." }
    ]
  },
  "traceId": "a1b2c3d4e5f60718293a4b5c6d7e8f90"
}
```

**异步副作用**：① `PE_ENGINE_TASK_COMPLETED`（TRACE 落轨迹、NOTIFY 通知发起人「审批通过」）；② `PE_ENGINE_INSTANCE_ENDED`（业务系统可订阅或通过 `callbackUrl` 接收结果）；③ 若租户订阅了领域事件，节点若配置 `MQ_PRODUCE_TASK` 还会投递 `PE_ENGINE_DOMAIN_EVENT`。

**错误场景**

| 场景 | 请求变化 | 响应 |
| --- | --- | --- |
| 任务已被他人认领 | 李四认领前王五已认领 | `HTTP 409` + `TASK_30001` |
| 重复提交（网络重试） | 同 `Idempotency-Key` | `HTTP 200` + `X-Idempotent-Replay: true`（不重复推进流程） |
| 表单校验失败 | `formData.approveRemark` 超长或缺失必填字段 | `HTTP 422` + `TASK_10007`，`data.errors[]` 字段级明细，**流程不推进** |
| 并发完成（双端同时点） | `If-Match` 版本落后 | `HTTP 412` + `COMMON_30003`（前端提示刷新任务） |
| 或签已被他人办结 | `ANY` 模式下他人先完成 | `HTTP 409` + `TASK_30006` |
| 会签强制推进 | `ALL` 模式下未集齐 | `HTTP 409` + `TASK_30005` |
| 零候选人且策略为 `EXCEPTION` | 角色无人 | `HTTP 409` + `TASK_30003`，需管理员补配 |
| 下游节点异常 | 下一节点为失败连接器 | `HTTP 500` + `TASK_50001`，任务与流程**整体回滚**为待办，且生成 incident |
| 超期禁办 | 任务超 `due_time` 且策略禁止 | `HTTP 409` + `TASK_30010` |
| 无办理权限 | 非办理人调用 | `HTTP 403` + `TASK_20002` |

### 5.4 示例 D：自动化节点异步执行完成后的引擎回调与令牌推进

**场景说明**：流程走到 `HTTP_TASK` 节点 `n_notify_hr`（调用外部 HR 系统的回调连接器），语义为「调用第三方系统并等待其处理结果」。引擎按连接器定义发起请求后**释放线程**（节点实例置 `ACTIVE`，令牌置 `WAITING`），由连接器 SDK 异步 worker 执行；worker 拿到结果后通过**内部接口**回调引擎，引擎在实例锁内推进令牌。

**步骤 1：引擎发起节点执行（内部动作，非 HTTP 接口，仅说明）**

引擎在节点进入时写 `rt_node_instance`（`status=ACTIVE`）、`int_call_log`（`call_status=RUNNING`），并通过连接器 SDK 向外发起请求：

```http
POST https://hr.example.com/api/leave/notify HTTP/1.1
X-Request-Id: pe-1836427591029385000-1836427591029386100
X-Trace-Id: e5f60718293a4b5c6d7e8f90a1b2c3d4
X-Pe-Callback: https://{domain}/api/engine/v1/internal/nodes/callback
Content-Type: application/json; charset=UTF-8

{ "instanceId": "1836427591029385000", "businessKey": "LEAVE-2025-0001", "days": 3, "notifyTo": "hr@example.com" }
```

**步骤 2：连接器 worker 执行完成，回调引擎（内部接口）**

```http
POST /api/engine/v1/internal/nodes/callback HTTP/1.1
Host: {domain}
X-Internal-Token: eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.internal.eyJzdWIiOiJlbmdpbmUtd29ya2VyIiwidGFyZ2V0IjoicHJvY2Vzcy1lbmdpbmUiLCJleHAiOjE3NDc3MDQ5NjJ9
X-Request-Id: wk-1836427591029386100
Idempotency-Key: 1836427591029385000:1836427591029386100:1
Accept-Language: zh-CN
Content-Type: application/json; charset=UTF-8

{
  "instanceId": "1836427591029385000",
  "nodeInstanceId": "1836427591029386100",
  "tokenId": "1836427591029386200",
  "nodeId": "n_notify_hr",
  "nodeType": "HTTP_TASK",
  "status": "COMPLETED",
  "attempt": 1,
  "callLogId": "1836427591029386300",
  "callStatus": "SUCCESS",
  "httpStatus": 200,
  "costMs": 812,
  "output": { "notified": true, "hrTicketNo": "HR-2025-88213" },
  "variables": { "hrTicketNo": "HR-2025-88213" },
  "occurredAt": 1747704902114,
  "traceId": "e5f60718293a4b5c6d7e8f90a1b2c3d4"
}
```

```json
HTTP/1.1 200 OK
X-Request-Id: wk-1836427591029386100
X-Trace-Id: e5f60718293a4b5c6d7e8f90a1b2c3d4
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "accepted": true,
    "instanceId": "1836427591029385000",
    "nodeInstanceId": "1836427591029386100",
    "nodeInstanceStatus": "COMPLETED",
    "idempotent": false,
    "tokenAdvanced": true,
    "consumedTokenId": "1836427591029386200",
    "createdTokens": [{ "tokenId": "1836427591029386400", "nodeId": "n_end", "status": "ACTIVE" }],
    "instanceStatus": "RUNNING",
    "nextNodes": [{ "nodeId": "n_end", "nodeName": "结束", "nodeType": "END_EVENT", "nodeInstanceId": "1836427591029386500" }],
    "durationMs": "812",
    "commandId": "cmd-7a1b2c3d"
  },
  "traceId": "e5f60718293a4b5c6d7e8f90a1b2c3d4"
}
```

**步骤 3：worker 重复回调（至少一次投递语义）**

```json
HTTP/1.1 200 OK
X-Idempotent-Replay: true
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": { "accepted": true, "instanceId": "1836427591029385000", "nodeInstanceId": "1836427591029386100", "nodeInstanceStatus": "COMPLETED", "idempotent": true, "tokenAdvanced": false, "instanceStatus": "RUNNING" },
  "traceId": "e5f60718293a4b5c6d7e8f90a1b2c3d4"
}
```

> **幂等键契约（关键）**：内部回调的幂等键必须为 `{instanceId}:{nodeInstanceId}:{attempt}`（与基线 6.10 的 MQ 消费幂等键一致）。`idempotent=true` 时引擎**不**再次推进令牌，只回放首次结果——这是「至少一次投递 + 最多一次业务效果」的落点。

**步骤 4：实例轨迹核对（确认令牌推进链路）**

```http
GET /api/engine/v1/instances/1836427591029385000/traces?eventType=NODE_COMPLETED,TOKEN_CONSUMED,TOKEN_CREATED&orderDirection=ASC&pageSize=50 HTTP/1.1
Host: {domain}
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

```json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8

{
  "code": "0",
  "message": "ok",
  "data": {
    "pageNum": 0,
    "pageSize": 50,
    "total": null,
    "records": [
      { "execTraceId": "1836427591029386600", "instanceId": "1836427591029385000", "nodeInstanceId": "1836427591029386100", "tokenId": "1836427591029386200", "eventType": "NODE_COMPLETED", "nodeId": "n_notify_hr", "nodeType": "HTTP_TASK", "ts": 1747704902114, "occurredAt": "2025-05-20 10:15:02.114", "seq": 18, "payload": { "status": "COMPLETED", "durationMs": "812", "connectorRef": { "connectorCode": "hr_http", "operationCode": "notifyLeave", "callLogId": "1836427591029386300" } }, "traceId": "e5f60718293a4b5c6d7e8f90a1b2c3d4" },
      { "execTraceId": "1836427591029386601", "eventType": "TOKEN_CONSUMED", "tokenId": "1836427591029386200", "nodeId": "n_notify_hr", "ts": 1747704902126, "seq": 19, "payload": { "status": "CONSUMED" } },
      { "execTraceId": "1836427591029386602", "eventType": "TOKEN_CREATED", "tokenId": "1836427591029386400", "nodeId": "n_end", "ts": 1747704902128, "seq": 20, "payload": { "status": "ACTIVE", "scopeId": "0" } }
    ],
    "hasNext": false,
    "nextLastId": null,
    "searchAfter": true,
    "dataSyncedAt": "2025-05-20 10:15:07.500"
  },
  "traceId": "e5f60718293a4b5c6d7e8f90a1b2c3d4"
}
```

**回调失败与重试处理（引擎 + worker 协同）**

| 情况 | 引擎侧动作 | worker 侧动作 |
| --- | --- | --- |
| 回调返回 5xx / 超时 | 不推进令牌，节点保持 `ACTIVE` | 按 `{instanceId}:{nodeInstanceId}:{attempt}` 递增 `attempt` 重试（默认 3 次，退避 5s/30s/120s） |
| 回调返回 `ENGINE_30008`（故障已处理） | 忽略并 ACK | 停止重试 |
| 回调返回 `COMMON_30003`（版本冲突） | 提示客户端刷新（`lockVersion`） | 拉取最新节点实例版本后重试 |
| worker 重试耗尽 | 生成 incident（`RETRY_EXHAUSTED` / `CONNECTOR_ERROR`），实例进入 `INCIDENT` 阻塞 | 停止并投递 `PE_ENGINE_INCIDENT_CREATED` |
| 外部系统返回非 2xx | 由 worker 上报 `callStatus=FAILED` + `httpStatus`，引擎走节点失败语义 | 按 `retryPolicy` 重试 |
| worker 被强杀（OOM/重启） | 对账巡检将 `int_call_log.call_status` 由 `RUNNING` 置为 `UNKNOWN` 并生成故障事件，人工核对 | 重启后不再续跑该次调用（结果未知，禁止盲目重放非幂等操作） |

**错误场景**

| 场景 | 请求变化 | 响应 |
| --- | --- | --- |
| 缺少 `X-Internal-Token` | 直连服务调用 | `HTTP 401` + `COMMON_20003` |
| 内部令牌过期/受众不符 | `exp` 过期或 `target` 非 `process-engine` | `HTTP 401` + `COMMON_20003` |
| 从公网访问内部路径 | 经网关访问 `/api/engine/v1/internal/**` | `HTTP 403`（网关直接拒绝，不进服务） |
| 节点实例已终态再次回调 | 对 `SKIPPED` 节点回调 | `HTTP 409` + `ENGINE_30009` |
| 实例已终止 | 终止后 worker 才回调 | `HTTP 409` + `ENGINE_30001`（worker 应停止重试并丢弃） |
| 实例级锁竞争 | 同时有其他命令推进同实例 | `HTTP 409` + `ENGINE_50004`（按 `Retry-After` 退避重试） |
| 回调携带的 `tokenId` 已被消费 | 乱序/重复回调 | `HTTP 409` + `ENGINE_30007`；引擎按幂等键判定后回放成功响应 |

---

## 6. 内部服务接口（Feign 契约）

### 6.1 服务间依赖关系

```mermaid
graph TD
  GW[process-gateway :8080]
  IAM[process-iam :8081]
  DESIGN[process-design :8082]
  ENGINE[process-engine :8083]
  SCHED[process-scheduler :8084]
  INTEG[process-integration :8085]
  NOTIFY[process-notify :8086]
  TRACE[process-trace :8087]
  MQ[(RocketMQ)]
  CH[(ClickHouse)]

  GW --> IAM
  GW --> DESIGN
  GW --> ENGINE
  GW --> SCHED
  GW --> INTEG
  GW --> NOTIFY
  GW --> TRACE

  DESIGN -.->|表单 schema / 模板| ENGINE
  ENGINE -.->|用户/部门/角色/用户组| IAM
  ENGINE -.->|发布版本模型| DESIGN
  ENGINE -->|连接器元数据| INTEG
  ENGINE -->|催办/通知| NOTIFY
  ENGINE -->|轨迹写入| TRACE
  ENGINE -.->|异步节点回调 / 作业触发实例| MQ
  SCHED -.->|启动实例| ENGINE
  SCHED -->|连接器目标| INTEG
  INTEG -.->|凭证到期| NOTIFY
  TRACE --> CH
  NOTIFY -.->|消息状态| TRACE

  IAM --> MQ
  DESIGN --> MQ
  ENGINE --> MQ
  SCHED --> MQ
  INTEG --> MQ
  NOTIFY --> MQ
  TRACE --> MQ

  classDef sync fill:#e8f4ff,stroke:#2b7bd6,color:#000
  classDef async fill:#fff4e5,stroke:#d68a2b,color:#000
  class DESIGN,ENGINE,SCHED,INTEG,NOTIFY,TRACE,IAM sync
  class MQ,CH async
```

| 图例 | 含义 |
| --- | --- |
| 实线箭头（`-->`） | 同步 Feign 调用（带 `X-Internal-Token`） |
| 虚线箭头（`-.->`） | 异步 MQ 消息（见第 4 节） |
| 双向依赖禁止 | 仅允许「上层调用下层」；发现环立即重构（当前无环） |

**依赖矩阵（行=调用方，列=被调用方；`S`=同步 Feign，`M`=MQ，空=无依赖）**

| 调用方 \ 被调用方 | IAM | DESIGN | ENGINE | SCHED | INTEG | NOTIFY | TRACE |
| --- | --- | --- | --- | --- | --- | --- | --- |
| DESIGN | S | — | S | | | S | M |
| ENGINE | S | S | — | | S | S | S + M |
| SCHED | | | S | — | S | S | M |
| INTEG | | | | | — | S | M |
| NOTIFY | S | | | | | — | M |
| TRACE | | | | | | S | — |
| IAM | — | | | | | S | M |

> 规则：**引擎不依赖 SCHED 与 TRACE 的写接口**（TRACE 只通过 MQ 消费引擎轨迹）；**TRACE 不被任何服务用作同步强依赖**（检索失败可降级）；SCHED 单向调用 ENGINE，ENGINE 从不回调 SCHED。

### 6.2 内部接口清单

内部接口统一前缀 `/api/{服务短名}/v1/internal/**`，**网关不路由该前缀**（直接 403），仅供集群内服务间调用；调用方必须携带 `X-Internal-Token`。

| # | 提供方 | 方法 + 路径 | 用途 | 调用方 | 超时 | 重试 | 降级策略 | 幂等 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | IAM | GET `/api/iam/v1/internal/users/{userId}` | 用户详情（含部门路径、状态、上级） | DESIGN、ENGINE、NOTIFY | 500ms | 2 次 | 读本地冗余快照 / Redis 缓存（TTL 10 min） | 天然幂等 |
| 2 | IAM | GET `/api/iam/v1/internal/users/by-ids` | 批量用户（≤ 200） | DESIGN、ENGINE、NOTIFY | 800ms | 2 次 | 返回已有缓存，缺失用户占位「未知用户」 | 天然幂等 |
| 3 | IAM | POST `/api/iam/v1/internal/users/query` | 按角色/部门/用户组解析候选人 | ENGINE | 2000ms | 1 次 | 使用最近一次成功结果的缓存（TTL 60s），并告警 | 天然幂等 |
| 4 | IAM | GET `/api/iam/v1/internal/depts/{deptId}` | 部门详情与祖先链 | DESIGN、ENGINE | 500ms | 2 次 | 本地缓存 | 天然幂等 |
| 5 | IAM | GET `/api/iam/v1/internal/user-groups/{groupId}/members` | 用户组成员解析（支持 `DYNAMIC` 动态组） | ENGINE | 2000ms | 1 次 | 缓存结果（TTL 60s）；失败时按 `emptyStrategy` 兜底 | 天然幂等 |
| 6 | IAM | GET `/api/iam/v1/internal/leaders` | 取直属上级（`INITIATOR_LEADER` 策略） | ENGINE | 800ms | 2 次 | 缓存；无上级返回 `null` 走 `emptyStrategy` | 天然幂等 |
| 7 | IAM | GET `/api/iam/v1/internal/tenants/{tenantId}` | 租户状态与配额校验 | 全部服务 | 500ms | 2 次 | 本地缓存（TTL 60s）；缓存不可用则**放行**（可用性优先） | 天然幂等 |
| 8 | DESIGN | GET `/api/design/v1/internal/process-models/{versionId}` | 获取版本模型 JSON（引擎启动时装载） | ENGINE | 1000ms | 2 次 | Redis 缓存 `design:model:{tenantId}:{versionId}`（TTL 5 min，发布事件主动刷新） | 天然幂等 |
| 9 | DESIGN | GET `/api/design/v1/internal/process-defs/versions` | 按 `processKey` 解析 `LATEST`/指定版本号对应的 `versionId` | ENGINE | 1000ms | 2 次 | 缓存（TTL 60s） | 天然幂等 |
| 10 | DESIGN | GET `/api/design/v1/internal/form-schemas` | 表单 schema（按 `formKey` + `version`/`LATEST`） | ENGINE、TASK | 1000ms | 2 次 | 缓存（TTL 5 min，发布事件刷新） | 天然幂等 |
| 11 | DESIGN | POST `/api/design/v1/internal/form-data/validate` | 表单数据服务端校验 | ENGINE、TASK | 1000ms | 1 次 | **不降级**：校验失败即拒绝办理（宁可失败不可脏数据） | 天然幂等 |
| 12 | DESIGN | GET `/api/design/v1/internal/templates/{templateId}` | 应用模板时读取内容 | DESIGN 内部 / ENGINE（测试启动） | 1000ms | 2 次 | 直接报错 | 天然幂等 |
| 13 | ENGINE | POST `/api/engine/v1/internal/nodes/callback` | 自动化节点异步执行完成回调（推进令牌） | ENGINE worker、engine-connector-sdk | 3000ms | 3 次（5s/30s/120s） | 重试耗尽 → incident + DLQ；**禁止**降级为「忽略」 | `{instanceId}:{nodeInstanceId}:{attempt}` |
| 14 | ENGINE | POST `/api/engine/v1/internal/instances/{instanceId}/commands` | 投递引擎命令（Scheduler 触发、外部系统高级用法） | SCHED、运维工具 | 2000ms | 3 次 | 失败转 MQ `PE_ENGINE_COMMAND` 异步重投 | `{instanceId}:{commandType}:{idempotencyKey}` |
| 15 | ENGINE | GET `/api/engine/v1/internal/instances/{instanceId}/summary` | 实例摘要（供 SCHED 记录关联、NOTIFY 渲染） | SCHED、NOTIFY | 800ms | 2 次 | 返回缓存摘要或跳过通知（记 WARN） | 天然幂等 |
| 16 | ENGINE | GET `/api/engine/v1/internal/tasks/{taskId}/summary` | 任务摘要（催办、抄送、通知渲染） | NOTIFY | 800ms | 2 次 | 同上 | 天然幂等 |
| 17 | ENGINE | POST `/api/engine/v1/internal/tasks/{taskId}/notify-context` | 批量取通知上下文（接收人表达式求值所需变量） | NOTIFY | 1500ms | 2 次 | 跳过该条通知 | 天然幂等 |
| 18 | ENGINE | POST `/api/engine/v1/internal/instances/batch-summary` | 批量实例摘要（看板、导出） | TRACE、SCHED | 2000ms | 1 次 | 返回部分结果 + `partial=true` | 天然幂等 |
| 19 | INTEG | GET `/api/integ/v1/internal/connectors/operations` | 连接器操作元数据（批量/增量，按 `updateTime`） | ENGINE | 2000ms | 3 次 | 本地缓存（TTL 5 min）+ `PE_INTEG_CONNECTOR_CHANGED` 主动刷新 | 天然幂等 |
| 20 | INTEG | POST `/api/integ/v1/internal/credential-resolve` | 凭证解密获取（仅引擎 SDK 在集群内调用） | ENGINE | 1000ms | 2 次 | 失败即节点失败（生成 incident），**不返回明文缓存** | 天然幂等 |
| 21 | INTEG | POST `/api/integ/v1/internal/call-logs` | 上报调用日志（引擎节点执行结果） | ENGINE | 1000ms | 3 次 | 转 MQ `PE_INTEG_CALL_FAILED`；再失败写本地文件补偿 | `callLogId` |
| 22 | NOTIFY | POST `/api/notify/v1/internal/messages` | 发送通知（按事件类型 + 模板 + 接收人） | ENGINE、SCHED、INTEG、IAM、TRACE | 3000ms | 3 次（10s/60s/300s） | 转 MQ 重投；仍失败置 `FAILED` 并入 DLQ（**不影响主流程**） | `{eventType}:{bizId}:{receiverUserId}` |
| 23 | TRACE | POST `/api/trace/v1/internal/exec-traces` | 查询执行轨迹（引擎的 `/instances/{id}/traces` 代理） | ENGINE | 3000ms | 1 次 | 返回 `ENGINE_40008` 并提示轨迹延迟 | 天然幂等 |
| 24 | TRACE | POST `/api/trace/v1/internal/push` | 向指定用户/实例推送实时事件（供其他服务触发推送） | ENGINE、NOTIFY、SCHED | 1000ms | 1 次 | 丢弃（推送非关键路径），记 `TRACE_50005` 指标 | `eventId` |
| 25 | TRACE | GET `/api/trace/v1/internal/statistics` | 实例统计聚合（DESIGN/看板复用） | DESIGN、SCHED | 3000ms | 1 次 | 返回最近一次物化结果（`stale=true`） | 天然幂等 |

> **内部接口计数**：共 **25** 个内部接口（不计入第 2 节对外接口总数，另见文末统计）。内部接口同样遵循 1.4 统一响应信封与 1.5 错误码。

### 6.3 `X-Internal-Token` 校验规范

| 项 | 约定 |
| --- | --- |
| 签发方 | `process-gateway`（集群内统一签发）与各服务启动时向网关换取的长效令牌（用于网关不参与的场景，如 worker 直连） |
| 令牌格式 | JWT（HS256，专用内部密钥，与用户侧 RS256 密钥**分离**） |
| 载荷 | `{ sub: "process-engine", target: "process-engine", scope: ["internal"], iat, exp }`；`exp` 默认 5 分钟 |
| 传递方式 | 请求头 `X-Internal-Token`；**不**使用 `Authorization`（避免与用户身份混淆） |
| 校验点 | 各服务 `InternalTokenFilter`（仅对 `/api/*/v1/internal/**` 生效），校验签名、`exp`、`target` 是否等于自身服务名、`scope` 含 `internal` |
| 失败响应 | `HTTP 401` + `COMMON_20003`（`data` 内含 `reason`：`MISSING`/`EXPIRED`/`SIGNATURE_INVALID`/`TARGET_MISMATCH`） |
| 审计 | 每次内部调用记录调用方 `sub`、目标路径、耗时；对目标不匹配的请求立即告警（疑似横向移动） |
| 密钥管理 | 内部密钥经 KMS/环境变量注入，支持双密钥轮换（`kid` 头标识，旧密钥保留 24h） |
| 网关防护 | 网关对 `/api/*/v1/internal/**` 直接返回 403 且不转发（防公网穿透是第一道防线，服务侧校验是第二道） |

`X-Internal-Token` 示例载荷与请求头：

```text
X-Internal-Token: eyJhbGciOiJIUzI1NiIsImtpZCI6ImludGVybmFsLTIwMjVRMiJ9.eyJzdWIiOiJwcm9jZXNzLWVuZ2luZSIsInRhcmdldCI6InByb2Nlc3MtaWFtIiwic2NvcGUiOlsiaW50ZXJuYWwiXSwiaWF0IjoxNzQ3NzA0NjYyLCJleHAiOjE3NDc3MDQ5NjJ9.9xQ7... 
```

### 6.4 超时、重试与降级策略

**统一 Feign 配置（`pe-feign-defaults`）**

| 配置 | 值 | 说明 |
| --- | --- | --- |
| `connectTimeout` | 1000ms | 建连超时（集群内网） |
| `readTimeout` | 3000ms（默认）/ 按 6.2 表逐接口覆盖 | 读超时 |
| 重试器 | 自定义 `PeRetryer`：仅重试 `IOException`、`SocketTimeoutException`、`HTTP 502/503/504`；**不重试** 4xx | 重试次数按 6.2 表 |
| 重试退避 | 固定间隔（默认 200ms），幂等接口可配指数退避 | 内部接口绝大多数幂等 |
| 熔断 | Sentinel Feign 适配：慢调用比例 > 50%（RT > 1s，窗口 10s，最小请求数 20）触发熔断，熔断 10s 后半开 | 与网关独立，双层保护 |
| 降级 | `fallbackFactory` 返回 `COMMON_50007`（HTTP 503）或接口声明的兜底值 | 见 6.2 降级列 |
| 连接池 | Apache HttpClient 5：`maxConnections=500`、`maxPerRoute=200` | 按服务设置 |
| 请求头透传 | 自动透传 `X-Request-Id`、`X-Trace-Id`、`tenantId`（`TenantContext` → `X-Tenant-Id`），并注入 `X-Internal-Token` | 由 `PeFeignRequestInterceptor` 完成 |
| 日志 | 仅记录 `BASIC` 级别（方法、URL、状态、耗时）；`FULL` 仅在排障时开启且**脱敏** | 防止凭证/表单数据入日志 |

**分级降级原则（重要）**

| 依赖类型 | 降级策略 | 理由 |
| --- | --- | --- |
| **身份数据**（IAM 用户/部门/角色/用户组） | 使用本地冗余快照 + Redis 缓存，允许**过期数据**继续服务（TTL 60s–10min）；缓存与远端同时不可用时：任务审批走 `emptyStrategy` 兜底（默认 `ADMIN`，即派给管理员） | 可用性优先，避免 IAM 抖动阻塞在途流程 |
| **模型/表单定义**（DESIGN） | Redis 缓存 + 发布事件刷新；缓存不可用则**拒绝新实例启动**（`ENGINE_40006`）但**不影响运行中实例推进**（已在内存/会话中的模型继续可用） | 数据正确性优先（错模型会放大错误），但已启动实例不应被牵连 |
| **表单数据校验**（DESIGN） | **不降级**，校验不可用即拒绝办理（`TASK_10007` / `COMMON_50002`） | 宁可失败，不可脏数据 |
| **连接器元数据**（INTEG） | 本地缓存（TTL 5 min）+ 变更事件刷新；缓存过期且远端不可用时节点失败并生成 incident（可人工重试） | 明确失败优于静默错误执行 |
| **凭证解密**（INTEG） | **不缓存明文、不降级**；失败即节点失败 | 安全优先 |
| **通知**（NOTIFY） | 转 MQ 重投，最终失败置 `FAILED` 并进 DLQ；**绝不阻塞**流程推进 | 通知是旁路能力 |
| **轨迹/审计**（TRACE 写） | 走 MQ；MQ 不可用时降级写本地文件（`filebeat` 补投），不阻塞业务 | 可观测性不能反噬核心链路 |
| **轨迹检索**（TRACE 读） | 返回缓存/物化数据的 `stale=true` 结果或明确报错 | 查询类可容忍陈旧 |
| **统计聚合**（TRACE） | 返回最近一次物化结果 | 看板可容忍 T+1 |
| **引擎命令**（ENGINE 写） | 同步失败转 MQ 异步重投（`PE_ENGINE_COMMAND`） | 不允许丢命令 |

**超时预算（自顶向下）**

| 链路 | 总预算 | 拆分 |
| --- | --- | --- |
| 启动实例（同步） | 5s（`engine.sync-start-timeout`） | IAM 校验 0.5s + DESIGN 模型 1s + 引擎推进 3s + 余量 0.5s |
| 任务完成 | 5s | 表单校验 1s + 引擎推进 3s + 通知投递（异步，不占预算） |
| 列表查询（大列表） | 1s | 单库查询（无跨库 JOIN） |
| 轨迹检索 | 10s（`trace.query-timeout`） | ClickHouse 查询 8s + 序列化 2s |
| 网关整体 | 10s（`gateway.httpclient.response-timeout`） | 覆盖上述所有同步链路 |

---

## 7. 接口版本演进与兼容策略

### 7.1 版本策略

| 项 | 规则 |
| --- | --- |
| 版本载体 | 路径段 `/v{n}`；`v1` 为当前基线，`v2` 仅在**破坏性变更**时启用 |
| `v1` 内允许的变更（**不算破坏性**） | 新增接口；新增可选请求字段；新增响应字段；新增错误码；放宽校验（如长度上限变大）；新增枚举值（客户端必须容忍未知枚举，见 7.3）；提高分页上限 |
| 需要升 `v2` 的变更 | 删除/重命名字段或接口；字段类型变更；必填性由可选变必填；路径语义变更；响应结构层级调整；错误码语义反转；默认值变更导致行为差异 |
| 灰度发布 | 新版本接口先以网关灰度规则放量（`X-Gray-Tag` 或用户白名单，见 1.2），观察 7 天无异常后全量 |
| 双版本并存 | `v1` 与 `v2` 并存期 ≥ 6 个月；并存期内两版共用同一底层服务与数据模型，`v2` 通过 `DTOConvert` 适配 |
| 版本下线 | 下线前 3 个月在 `/v3/api-docs` 标记废弃并通知已知调用方（依据 `X-Client-Info` 统计），下线后 `v1` 返回 `COMMON_40003` |

### 7.2 废弃标注

| 标记位置 | 形式 | 说明 |
| --- | --- | --- |
| OpenAPI 文档 | `@Operation(deprecated = true, description = "【已废弃】请使用 GET /v2/xxx")` | springdoc 输出 `deprecated: true` |
| 响应头 | `Deprecation: true`、`Sunset: Wed, 31 Dec 2025 23:59:59 GMT`、`Link: </api/xxx/v2/yyy>; rel="successor-version"` | 遵循 RFC 8594/RFC 8288 |
| 响应体扩展 | `data._deprecated: { "deprecatedSince": "1.1.0", "removeAfter": "2026-06-30", "replacement": "/api/xxx/v2/yyy" }` | 仅在 `?withDeprecationInfo=true` 时返回，避免污染常规响应 |
| 日志 | 命中废弃接口记录 `WARN` 并打点 `api_deprecated_calls_total{path,client}` | 用于识别存量调用方并定向通知 |
| 文档标注 | 本文档接口行加「⚠️ 已废弃」并给出替代路径 | 与代码同步维护 |

**当前 `v1` 内的废弃清单（示例，随迭代更新）**

| 废弃接口 | 替代 | 废弃版本 | 计划移除 |
| --- | --- | --- | --- |
| `GET /api/notify/v1/messages/inbox?readStatus=ALL` | `GET /api/notify/v1/messages/inbox` + `readStatus` 多值 | v1.1 | v1.3 |
| `GET /api/engine/v1/incidents?success=false` | 改用 `status` 枚举（见 2.6 说明） | v1.1 | v1.3 |
| `int_call_log.success`（布尔字段） | `callStatus` 枚举（见 2.7.5） | v1.0.1 | **已移除**（基线 v1.0.1 直接修正，无兼容期，字段从未对外发布） |

### 7.3 向后兼容规则（客户端与服务端各自义务）

**服务端义务（写侧）**

1. 响应**只增不减**：删除字段必须走 `v2`。
2. 新增字段必须可选且提供默认值，不得改变既有字段语义。
3. 枚举新增值时，`*Name` 字段必须同步返回可读名称；不得复用已废弃的枚举字面量。
4. 不得在 `v1` 内改变字段类型（含 `Long`→`String` 的序列化形态，见 1.7）。
5. 校验放宽可直接上线；校验**收紧**属于破坏性变更（存量合法请求会失败），必须经 7.1 的灰度流程并提前公告。
6. 错误码只新增不修改语义；同一 `code` 的 HTTP 状态码不得变更。
7. 分页出参字段保持稳定；游标模式下 `total` 语义（可能为 `null`）已在 1.6 冻结。
8. 幂等语义不得弱化：已承诺 `REQUIRED` 的接口不得改为可选。

**客户端义务（读侧）**

1. 必须容忍未知字段（Jackson `FAIL_ON_UNKNOWN_PROPERTIES=false`；前端不得因多余字段报错）。
2. 必须容忍未知枚举值：默认分支按「其他/未知」渲染，**禁止**抛异常或丢弃整条记录。
3. `Long` 一律按字符串处理，**禁止** `parseInt`/`Number()` 后参与运算（精度丢失）；需要计算时在服务端做或使用大整数库。
4. 时间字段按 `yyyy-MM-dd HH:mm:ss.SSS` 解析，缺失时区按 UTC+8 处理；不得假设 ISO8601。
5. 不得依赖字段顺序、`records` 内的稳定排序（除接口显式承诺的 `orderBy`）。
6. 收到 `Deprecation: true` 时必须排期迁移；`Sunset` 前完成。
7. 对 `429`/`503`/`504` 按 1.9 的重试策略处理，禁止无限重试。
8. 长连接客户端必须实现 `seq`/`eventId` 去重与断线补拉（第 3 节）。

### 7.4 契约变更流程

| 步骤 | 动作 | 产出 |
| --- | --- | --- |
| 1 | 提出变更（PR 修改本文档 + 代码注解） | 变更说明（接口、字段、影响面、是否破坏性） |
| 2 | 影响评估：扫描 `X-Client-Info` 与访问日志，列出存量调用方 | 影响清单与通知列表 |
| 3 | 评审：接口 Owner + 架构组 + 前端代表 | 评审结论（通过/退回/改走 `v2`） |
| 4 | 实现与联调：`v1` 新增字段灰度 / `v2` 双版本 | 通过的契约测试（`springdoc` diff + Pact 风格契约用例） |
| 5 | 文档与发布：更新本文档、变更日志（第 9 节）、OpenAPI 描述 | 发布公告 |
| 6 | 下线：`Sunset` 到期后移除代码与文档 | 归档记录 |

**契约回归测试要求**：每个服务必须维护 `openapi.json` 快照，CI 中 diff 上一次基线，出现「删除字段/类型变更/必填收紧」时**构建失败**，必须人工确认后更新基线快照。

## 8. 附录：权限码清单

权限码格式 `{服务}:{资源}:{动作}`（基线 6.1）；`资源` 用单数名词，`动作` ∈ `list`/`query`/`create`/`update`/`delete`/`import`/`export`/`publish`/`offline`/`validate`/`grant`/`reset-password`/`trigger`/`pause`/`debug`/`test`/`resend`/`batch-approve`/`add-sign`/`transfer`/`delegate`/`withdraw`/`urge`/`copy`/`claim`/`complete`/`manage`/`migrate`/`start`/`suspend`/`terminate`/`resolve`/`compensation`/`send`/`broadcast`/`read`/`kickout`/`replay`/`discard`。

**IAM（身份与权限）**

| 权限码 | 说明 | 默认授予角色 |
| --- | --- | --- |
| `iam:user:list` | 用户列表 | `tenant_admin`、`user_admin` |
| `iam:user:query` | 用户详情 | `tenant_admin`、`user_admin` |
| `iam:user:create` | 新增用户 | `tenant_admin`、`user_admin` |
| `iam:user:update` | 修改/启停用用户 | `tenant_admin`、`user_admin` |
| `iam:user:delete` | 删除用户 | `tenant_admin` |
| `iam:user:import` | 批量导入用户 | `tenant_admin` |
| `iam:user:export` | 导出用户 | `tenant_admin`、`user_admin` |
| `iam:user:reset-password` | 重置密码 | `tenant_admin`、`user_admin` |
| `iam:role:list` / `query` / `create` / `update` / `delete` | 角色 CRUD | `tenant_admin` |
| `iam:role:grant` | 角色授权（权限/用户绑定） | `tenant_admin` |
| `iam:permission:list` / `create` / `update` / `delete` | 权限/菜单 CRUD | `tenant_admin`、`platform_admin` |
| `iam:dept:list` / `create` / `update` / `delete` | 部门树 CRUD | `tenant_admin`、`user_admin` |
| `iam:user-group:list` / `query` / `create` / `update` / `delete` | 用户组 CRUD 与成员维护 | `tenant_admin`、`process_admin` |
| `iam:tenant:list` / `query` / `create` / `update` | 租户 CRUD（平台级） | `platform_admin` |
| `iam:session:list` | 在线会话查询 | `tenant_admin` |
| `iam:session:kickout` | 强制下线 | `tenant_admin` |
| `iam:login-log:list` | 登录日志查询 | `tenant_admin`、`auditor` |

**DESIGN / FORM（建模与表单）**

| 权限码 | 说明 | 默认授予角色 |
| --- | --- | --- |
| `design:category:list` / `create` / `update` / `delete` | 流程分类树 CRUD | `process_admin` |
| `design:process-def:list` / `query` / `create` / `update` / `delete` | 流程定义 CRUD | `process_admin`、`process_designer` |
| `design:process-version:list` / `query` / `create` / `update` / `delete` | 版本与草稿管理 | `process_designer` |
| `design:process-version:validate` | 模型校验 | `process_designer` |
| `design:process-version:publish` | 发布/回滚 | `process_admin` |
| `design:process-version:offline` | 下线 | `process_admin` |
| `design:process-version:import` / `export` | 导入导出（JSON/BPMN） | `process_designer` |
| `design:template:list` / `query` / `create` / `update` / `delete` | 模板 CRUD | `process_admin`、`process_designer` |
| `form:def:list` / `query` / `create` / `update` / `delete` | 表单定义 CRUD | `form_designer` |
| `form:version:list` / `query` / `create` / `update` | 表单版本管理 | `form_designer` |
| `form:version:publish` | 表单发布 | `form_admin` |
| `form:schema:read` | schema 读取 | 全体登录用户 |
| `form:data:export` | 表单数据导出 | `form_admin` |

**ENGINE（执行内核）**

| 权限码 | 说明 | 默认授予角色 |
| --- | --- | --- |
| `engine:instance:list` / `query` | 实例列表/详情 | `process_user`、`process_admin` |
| `engine:instance:start` | 启动实例 | `process_user` |
| `engine:instance:start-system` | 以系统身份启动（指定发起人） | 服务账号 |
| `engine:instance:suspend` | 挂起/恢复 | `process_admin` |
| `engine:instance:terminate` | 终止实例 | `process_admin` |
| `engine:instance:delete` | 删除已结束实例 | `platform_admin` |
| `engine:instance:manage` | 定时器等运行期资源管理 | `process_admin` |
| `engine:instance:migrate` | 实例版本迁移 | `platform_admin` |
| `engine:variable:update` | 修改变量 | `process_admin` |
| `engine:incident:list` | 故障查询 | `process_admin`、`ops` |
| `engine:incident:resolve` | 故障重试/跳过/关闭 | `process_admin`、`ops` |
| `engine:compensation:trigger` | 触发补偿 | `process_admin` |
| `engine:message:send` | 外部消息投递 | 服务账号、`process_admin` |
| `engine:signal:broadcast` | 信号广播 | `process_admin`、`platform_admin` |

**TASK（人任务）**

| 权限码 | 说明 | 默认授予角色 |
| --- | --- | --- |
| `task:todo:list` | 我的待办 | 全体登录用户 |
| `task:done:list` | 我的已办 | 全体登录用户 |
| `task:copy:list` | 我的抄送 | 全体登录用户 |
| `task:task:query` | 任务详情/候选人/操作记录 | 全体登录用户（受数据权限） |
| `task:task:manage` | 任务管理视角列表/代领 | `process_admin` |
| `task:task:claim` | 认领/释放 | 全体登录用户 |
| `task:task:complete` | 完成/办理 | 全体登录用户 |
| `task:task:batch-approve` | 批量审批 | `process_admin`、`dept_manager` |
| `task:task:add-sign` | 加签 | 全体登录用户 |
| `task:task:transfer` | 转办 | 全体登录用户 |
| `task:task:delegate` | 委托/委托确认 | 全体登录用户 |
| `task:task:withdraw` | 撤回 | 全体登录用户（受流程 `withdrawScope`） |
| `task:task:withdraw-force` | 强制撤回（作废下游） | `process_admin` |
| `task:task:urge` | 催办 | 全体登录用户、`process_admin` |
| `task:task:copy` | 追加抄送 | 全体登录用户 |

**SCHED（调度）**

| 权限码 | 说明 | 默认授予角色 |
| --- | --- | --- |
| `sched:job:list` / `query` | 作业查询 | `sched_admin`、`ops` |
| `sched:job:create` / `update` / `delete` | 作业 CRUD | `sched_admin` |
| `sched:job:pause` | 暂停/恢复 | `sched_admin`、`ops` |
| `sched:job:trigger` | 立即执行/补跑/重跑 | `sched_admin` |
| `sched:execution:list` / `query` | 执行记录与日志 | `sched_admin`、`ops` |

**INTEG（集成）**

| 权限码 | 说明 | 默认授予角色 |
| --- | --- | --- |
| `integ:connector:list` / `query` / `create` / `update` / `delete` | 连接器与操作 CRUD | `integ_admin` |
| `integ:connector:debug` | 在线调试执行 | `integ_admin`、`process_designer` |
| `integ:datasource:list` / `query` / `create` / `update` / `delete` | 数据源 CRUD | `integ_admin` |
| `integ:credential:list` / `create` / `update` / `delete` | 凭证 CRUD | `integ_admin`（`platform_admin` 审批） |
| `integ:call-log:list` | 调用日志与统计 | `integ_admin`、`ops` |

**NOTIFY（通知）**

| 权限码 | 说明 | 默认授予角色 |
| --- | --- | --- |
| `notify:template:list` / `query` / `create` / `update` / `delete` | 模板 CRUD | `notify_admin` |
| `notify:channel:list` / `create` / `update` / `delete` | 渠道配置 CRUD | `notify_admin` |
| `notify:channel:test` | 渠道测试发送 | `notify_admin` |
| `notify:subscription:list` / `create` / `update` / `delete` | 订阅规则 CRUD | `notify_admin`、`process_admin` |
| `notify:message:list` / `query` | 消息查询 | `notify_admin`、`ops` |
| `notify:message:resend` | 消息重发 | `notify_admin`、`ops` |

**TRACE（轨迹与审计）**

| 权限码 | 说明 | 默认授予角色 |
| --- | --- | --- |
| `trace:oper-log:list` / `query` | 审计日志查询 | `auditor`、`platform_admin` |
| `trace:exec-trace:list` | 执行轨迹检索 | `auditor`、`process_admin`、`ops` |
| `trace:stat:query` | 统计聚合/热力/耗时排行 | `auditor`、`process_admin`、`ops` |
| `trace:alert-rule:list` / `query` / `create` / `update` / `delete` | 告警规则 CRUD | `ops`、`platform_admin` |
| `trace:dlq:list` / `query` | 死信查询 | `ops` |
| `trace:dlq:replay` | 死信重放 | `ops`（批量需二级审批） |
| `trace:dlq:discard` | 死信丢弃 | `platform_admin`（必须填原因） |

> **权限码总量：142 个**（统计口径：附录各行中每个独立的 `{服务}:{资源}:{动作}` 记一条；表格中以 `/` 并列的动作按数量展开，如 `iam:role:list` / `query` / `create` / `update` / `delete` 计 5 条）。按服务分布：IAM 40、DESIGN 31、FORM 11、ENGINE 16、TASK 15、SCHED 8、INTEG 11、NOTIFY 14、TRACE 12，合计 **142** 条。权限码与接口的绑定关系见各服务「接口总览」表的「权限码」列；未列出的（如「登录态」）表示仅需有效身份，无需额外权限码。

---

## 附：接口清单统计

### A.1 对外接口分布

| 服务 | 章节 | 接口数 | 说明 |
| --- | --- | --- | --- |
| `process-iam` | 2.1 | 47 | 认证 6 + 用户 10 + 角色 9 + 权限 4 + 部门 4 + 用户组 6 + 租户 5 + 会话 2 + 登录日志 1 |
| `process-design` | 2.2 | 45 | 分类 4 + 流程定义 6 + 版本与模型 6 + 校验/发布/下线/回滚/复制 5 + 导入导出与差异对比 5 + 统计与发布记录 2 + 模板 7 + 表单定义与版本 10 |
| `process-form` | 2.3 | 10 | schema 获取 3 + 校验/规范化 2 + 辅助查询 3 + 字段树 1 + 导出 1 |
| `process-engine` | 2.4 | 31 | 实例 13 + 变量 4 + 节点/令牌 3 + 故障 5 + 补偿 2 + 消息/信号 2 + 定时器 2 + 轨迹 1 + 迁移 1 + 统计 1 |
| `process-task` | 2.5 | 27 | 列表 4 + 详情 2 + 认领/释放 2 + 完成/批量/驳回 3 + 加签 2 + 转办/委托/确认 3 + 撤回/催办 2 + 操作记录 1 + 抄送与附件 5 + 表单 2 + 统计 1 |
| `process-scheduler` | 2.6 | 15 | 作业 9 + 执行记录 4 + cron 校验 1 + 统计 1 |
| `process-integration` | 2.7 | 23 | 连接器 6 + 操作 4 + 调试 1 + 数据源 6 + 凭证 4 + 调用日志 1（+1 扩展：调用统计） |
| `process-notify` | 2.8 | 22 | 模板 6 + 渠道 5 + 订阅 4 + 消息 7 |
| `process-trace` | 2.9 | 15 | 审计 2 + 轨迹 2 + 统计 3 + 告警规则 6 + 告警记录 2 |
| **对外接口合计** | 2.1–2.9 | **235** | 含扩展接口 `GET /api/integ/v1/call-logs/statistics` 与 `GET /api/notify/v1/messages/unread-count` |
| 实时推送协议 | 3 | 2 个通道 | `WS /ws/notify`、`SSE /sse/instances/{instanceId}`（非 REST 接口，单列） |
| 死信运维接口 | 4.5 | 6 | `/api/trace/v1/dlq/**`（运维专用，单列） |
| **内部接口（Feign）** | 6.2 | **25** | `/api/*/v1/internal/**`，网关不暴露 |
| **接口总数（对外 + 内部）** | — | **260** | 对外 235 + 内部 25 |

### A.2 其他统计

| 项 | 数量 | 位置 |
| --- | --- | --- |
| 错误码 | **339** | 1.5（COMMON 39 / AUTH 18 / IAM 41 / DESIGN 36 / FORM 20 / ENGINE 41 / TASK 36 / SCHED 25 / INTEG 34 / NOTIFY 28 / TRACE 21） |
| MQ Topic（业务） | **30** | 4.2（#1–#30），另有 `PE_DLQ_{原Topic}` 命名模板 |
| 关键事件消息体明细 | **8** | 4.3.1–4.3.8 |
| 权限码 | **142** | 第 8 节 |
| 端到端交互示例 | **4** | 5.1–5.4（含请求、响应与错误场景） |
| 网关路由规则 | **15** | 1.2 |
| 实时订阅主题 | **8** | 3.2 |
| 实时推送事件类型 | **20** | 3.3 |

### A.3 变更日志

| 版本 | 日期 | 变更 | 说明 |
| --- | --- | --- | --- |
| v1.0 | — | 初版契约 | 与设计基线 v1.0 同步冻结：统一约定、9 个服务 235 个对外接口、25 个内部接口、30 个 Topic、实时推送与 MQ 契约 |
| v1.0.1 | — | 字段修正 | `int_call_log.success`（布尔）→ `callStatus`（`RUNNING`/`SUCCESS`/`FAILED`/`UNKNOWN`），新增 `UNKNOWN` 失联执行语义与对账巡检说明；执行记录筛选统一以 `status` 为主口径（`success` 标注为派生便捷筛选）；`sch_job_execution.status` 增加 `SKIPPED` |
| v1.0.2 | — | 统计修正 + 补码 | ①**修正错误码统计**：原「183 条」及其分域明细（COMMON 32 / AUTH 19 / IAM 30 / DESIGN 43 / TASK 25 / SCHED 19 / NOTIFY 24）与 1.5 表实际行数不符，逐域重核后更正为 **339 条**（COMMON 39 / AUTH 18 / IAM 41 / DESIGN 36 / FORM 20 / ENGINE 41 / TASK 36 / SCHED 25 / INTEG 34 / NOTIFY 28 / TRACE 21）；数字段分布同步更正为 1xxxx 103 / 2xxxx 57 / 3xxxx 85 / 4xxxx 46 / 5xxxx 48。②**补充 `IAM_30011`**（租户编码已存在，409），补齐 `tenant_code` 唯一冲突缺失的错误码（原 FR-IAM-001 误引用 `IAM_30001`，语义为「用户名已存在」）。③**补充 `TASK_30012`**（候选人数量超过单任务上限，409），供 `rt_task_candidate` 的 5000 人上限校验使用（原 03 文档引用了一个不存在的错误码） |





