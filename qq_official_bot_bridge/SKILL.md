# QQ 官方机器人文件桥

- 用这份技能把 QQ 官方机器人接到 Muse 本人身上。参考实现打包在本技能的 `references/implementation/` 里（daemon、两个 hook 脚本、扫码绑定、离线测试、运维手册），照着复制就能重建。桥的目录下文统一记作 `~/workspace/qq-bridge/`。

## 架构与原则

- 走文件桥。守护进程只负责收发消息，回复内容由被唤醒的 agent 回合按人格与记忆写出，中间不接任何第三方模型。
- 只走官方路线。用 QQ 开放平台 Bot API v2 加腾讯官方 `qqbot-agent-sdk`，不用 NapCat、Lagrange 等非官方协议端。
- 按被动回复窗口设计整条链路。官方文档对窗口自相矛盾（接口页一处写 60 分钟、msg_id 字段写 5 分钟内有效），默认按 5 分钟保守值守：`config.json` 的 `reply_window_seconds` 默认 300，守护进程以自身收到消息的时刻计时，超窗的回复归档 EXPIRED 不发。自己实测过更晚也能发成的，可以调大这个数，但别拿文档的一句话当实测。

## 平台侧准备

- 在 QQ 开放平台创建机器人，拿到 AppID 与 AppSecret。
- 先确认主体限制。个人主体可以用自有群（上限 500 人）和开发期体验用户私聊，公开投放需要企业主体。
- 开通事件订阅 `C2C_MESSAGE_CREATE` 与 `GROUP_AT_MESSAGE_CREATE`，intent 为 `GROUP_MESSAGES`，SDK 默认已带。
- 要接收不 @ 机器人的群消息时，另行给机器人开群全量消息权限，并在 `config.json` 里把 `group_full_messages` 打开，否则 `GROUP_MESSAGE_CREATE` 到达了也会被守护进程丢弃。

## 装 SDK 与目录

- 在桥目录建 Python venv，装 `qqbot-agent-sdk` 1.2.2（见 implementation 的 requirements.txt）。升级版本前先跑离线测试确认缺口补丁还成立。
- 建目录 `inbox`、`outbox`、`proactive`、`events`、`done`、`logs`、`state`（daemon 启动时会自动建）。
- 从 `references/implementation/` 复制参考实现作为起点：`daemon.py`、`push.py`、`run.sh`、离线测试、扫码绑定脚本、两个 hook 脚本和 systemd 单元，运维细节看同目录的 `README.md`。

## 绑定凭据与主人白名单

- 首选扫码绑定。在 `qr-connect/` 里装 `@tencent-connect/qqbot-connector` 1.2.0 与 `qrcode`，跑 `connect.mjs` 生成 `qr.png` 给用户扫，成功回调把凭据写进 `credentials.env` 并设权限 0600。密钥全程不经过聊天。
- 备选手动绑定。把 `credentials.env.example` 复制为 `credentials.env`，填 AppID 与 AppSecret，同样设 0600。
- 永远不把 AppSecret 写进聊天或日志。汇报绑定结果只报 AppID 的掩码形式。
- 主人白名单在 `config.json` 的 `allow_openids`。守护进程在入箱前就验。名单为空时是绑定模式：第一位发信人的 openid 记入 `state/pending.json` 并回一句未绑定提示，他的内容不入箱；由用户在主聊天确认后把该 openid 填进 `allow_openids` 并重启 daemon。群消息的发信人是群内 member_openid（与私聊 openid 不是一套），用 `allow_group_member_openids` 单独管，留空表示群内成员都可达。
- 私聊发信人不在白名单时的去向由 `access_mode` 决定（见下节「框架 v3」）：`private`（默认）直接丢弃；`public` 时成为访客，进 `guest_inbox/` 给受限的访客 worker。`block_openids` 在任何模式下都最先丢弃。

## 守护进程的职责与 SDK 缺口

- `daemon.py` 用 SDK 的网关与 OpenAPI 收消息，一条消息一个文件原子写入 `inbox/<msg_id>.json`；每 3 秒轮询 `outbox/` 发被动回复、轮询 `proactive/` 发主动消息；处理完把文件归档到 `done/` 并打 SENT、PARTIAL、EXPIRED、INVALID、ORPHAN 或 FAILED 标记（标记文件内含 JSON，记录时间和失败原因）。
- 启动时先清洗 `NO_PROXY`，剔除方括号形式的 IPv6 条目，否则 httpx 会以 InvalidURL 报错让守护进程直接崩溃。
- 补 `GROUP_MESSAGE_CREATE`。SDK 1.2.2 不路由这个事件，向 `EventParser._EVENT_HANDLERS` 注册解析函数并子类化 `QQWebSocket` 覆写分发方法，生命周期事件走同一条通道记入 `events/events.jsonl`。
- 补频道私信发送。SDK 没有现成方法，用它的通用 `request()` 直调官方 `POST /dms/{guild_id}/messages`。
- 附件只存元数据与 `att.resolved_url` 原文。纯附件消息也要入箱并标 `attachment_only`，不要丢弃。
- 语音走 SDK 的 STTPipeline。未配置 STT 引擎时回退平台自带的 `asr_refer_text`，转写不到就如实标 `asr_text` 为 null 并在 `asr_note` 写原因，不伪造转写。
- 按钮交互用 `acknowledge_interaction` 确认。C2C 消息入箱时发 `send_typing` 给用户即时反馈（`config.json` 的 `typing` 可关）——即时反馈用输入状态，不要发预设文案占回复额度。
- 网关存活自检。SDK 的重连在连接掉线后可能假死（进程还在、心跳 ACK 不再来），daemon 记录最后一次心跳 ACK 时刻，静默超过 `liveness_timeout_seconds`（默认 300）就硬退出，交给监督层拉起新进程。另有 `state/heartbeat.json` 每 20 秒刷新，供看门狗判活。

## 框架 v3：权限分层、访客、动作与管理接口

- **权限三层**。owner（`allow_openids`）走 `inbox/`，完整 agent 处理；guest（public 模式下的其他私聊发信人）走 `guest_inbox/`，由访客 worker 处理；blocked（`block_openids`）静默丢弃。记录里带 `tier` 字段，owner 的 inbox 出现 guest 记录就是路由 bug。
- **访客 worker 是隔离的**：用 `references/guest-hook-prompt.txt` 那份提示词唤醒——它的人格只有公开人设卡，手里没有主人的任何私人信息（不是叮嘱保密，是根本不给），不能用工具、不能主动发消息，只能写 outbox 回复和维护这个访客自己的记忆。给访客 worker 喂任何主人记忆都算事故。
- **访客记忆**在 `guests/` 目录：`<openid>.history.jsonl` 是完整对话流水（daemon 自动记双方，回复成功时把发出的每条记为 assistant）；`<openid>.notes.md` 由访客 worker 维护，只记这个访客本人的事；`guests/persona.md` 存在时是主人定的人设补充，访客 worker 先读它。`guests/state.json` 是限流与消息索引的内部状态，不要手改。
- **访客限流在 daemon 层**：`guest_daily_limit`（默认 30，0=不限）与 `guest_min_interval_seconds`（默认 3）。超限的消息不入箱、不唤醒 worker；撞到每日上限时 daemon 直接回一句额度用完的提示，每人每天最多提示一次。
- **动作目录 `actions/`**：owner agent 除了被动回复，还能驱动 QQ API——放 `{"action": "send_group", "args": {...}}` 这类文件，daemon 按 `daemon.py` 的 `ACTION_NAMES` 白名单执行 `qqapi.py` 里的方法，结果写 `done/<文件名>.result.json`（含 ok 与 result/error）。覆盖发送/撤回（四场景）、富媒体文件上传、私信会话、频道与成员与角色管理、公告、精华、日程、表态、论坛、音频等 api-v2 接口；签名以 `qqapi.py` 为准，不要凭记忆编参数。频道管理类接口还需要机器人在频道里有对应权限，平台会拒，没权限就如实报错误。
- **配置热加载**：daemon 每 5 秒看一次 `config.json` 的 mtime，变了就校验并整体换入（非法值整份拒绝、保留旧配置并记日志）。改配置不用重启；凭据不在热加载范围。
- **本地管理接口**：`admin_port` > 0 时 daemon 在 127.0.0.1 起一个 HTTP 管理口（`admin.py`，令牌在 `state/admin.token`，请求头 `X-Admin-Token`）：`GET /api/status`、`/api/config`、`/api/guests`、`/api/events?n=`，`PUT /api/config`（走同一套校验），`POST /api/push`（排一条主动消息）。只绑回环，不要往外暴露。
- **访客 hook**：`hooks/qq-bridge-guest-inbox.sh` 与主人 inbox hook 同款两段式投递（pending 先提交再 peek），指向 `guest_inbox/`，唤醒访客 worker。主人 hook 与访客 hook 必须用各自的提示词，不要混用。

## 回信契约（agent 写 outbox）

- 一条源消息一个文件：`outbox/<msg_id>.json`，先写到 outbox 之外的临时文件再改名进来，避免 daemon 读到半截。
- 单条：`{"reply": "..."}`。拆条：`{"replies": ["...", "..."]}`，每条作为一则独立被动回复按序发出——长答复在私聊里拆 2~3 条短消息更像聊天。平台对单条源消息的被动回复有条数上限（默认按 4 计，`max_replies_per_message`），daemon 有持久化的计数账本，超额部分不发、归档 PARTIAL。
- 每条回复的 msg_seq 由 daemon 统一分配并按 msg_id 去重登记：SDK 的序号生成是 0–65535 伪随机，撞了平台会按重复消息静默丢弃；发送失败的那条序号永不复用（平台可能已经记下）。
- 可加 `"markdown": true`（C2C/群/频道有效，频道私信仍是纯文本），可加 `"media": [{"kind": "image|video|voice|file", "path": "本地绝对路径"}]` 发富媒体（msg_type 7，仅 C2C/群；有文字时文字随第一条媒体一起发）。媒体路径不存在，整份 payload 归档 INVALID。
- 发送错误分级：平台致命错误（SDK 的 `is_fatal_send_error` 判定）立即归档 FAILED 不重试；瞬时错误在窗口内持续重试，窗口关了按已发条数归档 EXPIRED 或 PARTIAL。

## 主动消息

- 往 `proactive/` 放 `{"scene": "c2c|group", "target": openid, "content": "...", "markdown": false, "media": [...]}`，或用 `push.py "文本"`（默认发给 `allow_openids` 第一位）。daemon 以主动消息发出（不引用 msg_id），失败自动重试（默认 3 次、间隔 5 秒），最终失败归档 FAILED 并把平台错误原文记进归档文件。
- 普通主动消息不要凭空假设配额数字：各类流传的说法（每月 4 条、每天 2 条）与实测对不上，以自己机器人实测到的平台返回为准，配额类错误会原样落在日志和 FAILED 归档里。
- 另一种是互动召回：payload 加 `"wakeup": true`，走 `is_wakeup` 通道。它的账是按与该用户最后一次互动起算的四个周期（当天、1–3 天、3–7 天、7–30 天）各一次，daemon 在 `state/wakeup_usage.json` 里记账，发送前领取、最终失败才释放。普通推送不要用这个通道。

## 钩子与保活

- 两个 hook 分工，别合并：`qq-bridge-inbox` 只管新消息唤醒，`qq-bridge-watchdog` 只管进程死活。合并脚本会把两边的超时和故障搅在一起。
- inbox hook 是单次轮询脚本（建议 poll 5~10 秒），两段式投递：本轮先把上一轮的 pending 名单提交进 seen 文件（上一轮的 wake 已发出），再 peek 出新文件写进 pending 并 wake。最坏情况是重复唤醒一次，agent 按 msg_id 去重即可；不存在「已标记已读但没唤醒」的丢消息窗口。
- watchdog 每 60 秒判活：凭据文件不存在则静默；进程不在或心跳文件过期则重启（优先走 `run.sh`），1 小时内重启超过 3 次才唤醒 agent 报告。
- 监督层三选一：`run.sh`（flock 单例监督循环，退出 3 秒拉起，任何环境可用，首选）、systemd 用户服务（`qq-bridge.service`，需要能连上用户总线；连不上就别硬试）、裸 nohup（只配 watchdog 兜底）。

## 验收与排障

- 离线自检：`.venv/bin/python tests/test_bridge_offline.py`（33 项：白名单、绑定、拆条序号去重、额度、过期、重试、致命错误、媒体校验、主动消息重试与召回周期）与 `.venv/bin/python tests/test_framework_offline.py`（31 项：访客分流、黑名单、限流、访客回复与记忆落盘、配置校验与热加载、动作执行、管理接口鉴权）。不联网不用凭据，两套都要全过。
- 真机验收顺序：绑定模式确认 pending → 填白名单重启 → 私聊文字往返 → 拆条回复 → 图片附件入箱 → 语音转写 → 主动推送（push.py）→ 杀掉 daemon 看 watchdog 拉起。框架 v3 追加：管理口改 `access_mode` 为 public 并把测试号移出白名单 → 以访客身份聊天（入 guest_inbox、回复、history 落盘）→ 连发撞每日上限看提示 → 动作文件调 `get_bot_info` 看 result。
- 排障先看三处：`logs/daemon.log`（收发与错误原文）、`done/` 里的标记文件（每条消息的最终结局）、`state/heartbeat.json`（daemon 是否还活着）。网关连不上先查代理环境变量与 NO_PROXY 清洗是否生效。
- 图片附件的 CDN（multimedia.nt.qq.com.cn）在某些机器上从 shell 网络拉不动（代理 TLS 停滞），这种情况让被唤醒的 agent 改走浏览器取图，别在 shell 里反复 curl 烧回复窗口。
