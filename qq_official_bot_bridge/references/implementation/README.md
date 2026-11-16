# qq-bridge — QQ 官方机器人 ⇄ Muse 文件桥（运维手册）

让 QQ 里的消息由 Muse agent 本人来回复。中间不经过任何外部
LLM：守护进程只负责收发，回复内容由一个 agent 回合写出。

## 流程

```
QQ 用户 ──消息──▶ QQ 开放平台 ──WebSocket 网关──▶ daemon.py
                                                     │ 写入（含附件/语音转写）
                                                     ▼
                                              inbox/<msg_id>.json
                                                     │ inbox hook 两段式唤醒
                                                     ▼
                                        Muse agent 回合（人格/记忆）
                                                     │ 写入 {"reply"/"replies": ...}
                                                     ▼
                                              outbox/<msg_id>.json
                                                     │ daemon 每 ~3 秒轮询
                                                     ▼
                  官方 OpenAPI v2 被动回复（引用原 msg_id；文本/Markdown/富媒体，可拆条）
                                                     │
                                                     ▼
                                        两文件归档到 done/ + 结果标记
```

另外两条旁路：

- **主动消息**：往 `proactive/` 放 `{"scene", "target", "content", ...}`
  文件（或跑 `push.py "文本"`），daemon 以主动消息发出（不引用任何
  msg_id），失败按配置重试，最终归档带 `.SENT` / `.FAILED` 标记，平台
  返回的错误原文记入归档文件与日志。payload 加 `"wakeup": true` 时走
  互动召回通道（见下）。
- **事件**：按钮交互（INTERACTION_CREATE）会被 daemon 通过官方
  `acknowledge_interaction` 接口确认，交互与生命周期事件
  （FRIEND_ADD、GROUP_ADD_ROBOT 等）逐条追加到 `events/events.jsonl`，
  不进 inbox、不回复。

组件：

- `daemon.py` — 基于腾讯官方 `qqbot-agent-sdk`（WebSocket Gateway +
  OpenAPI v2，纯 Python）。负责连接网关、收消息、发被动/主动消息、
  主人白名单校验、网关存活自检。不使用任何非官方协议端。
- hook `qq-bridge-inbox`（脚本 `hooks/qq-bridge-inbox.sh`）— 单次轮询
  `inbox/`，两段式投递（先提交上一轮 pending，再 peek 新文件并
  wake），建议 poll 间隔 5~10 秒。唤醒提示词见技能包的
  `references/inbox-hook-prompt.txt`。
- hook `qq-bridge-watchdog`（脚本 `hooks/qq-bridge-watchdog.sh`）—
  每 60 秒检查守护进程：凭据文件不存在时静默跳过；进程不在或心跳
  文件过期时重启并记 `logs/watchdog.log`；拉起失败、或 1 小时内重启
  超过 3 次时才唤醒 agent 向主聊天报告。
- `run.sh` — flock 单例监督循环（daemon 退出 3 秒后拉起），首选的
  保活方式。`qq-bridge.service` 是备选的 systemd 用户服务（需要能
  连上用户总线，连不上就用 run.sh，别硬试）。

## 覆盖矩阵

接收：

| 类型 | 状态 | 说明 |
|---|---|---|
| 单聊 C2C 文字 | ✅ | 私聊直接回复；发信人须在 `allow_openids` 内 |
| 群 @ 文字 | ✅ | 群里 @机器人 触发；可用 `allow_group_member_openids` 限发信人 |
| 图片/视频/文件附件 | ✅ | 附件元数据（文件名/类型/URL/大小）随记录入箱；纯附件消息以 `attachment_only: true` 入箱，不丢弃 |
| 语音消息 | ✅ | 走 SDK 的 STTPipeline：有 STT 引擎配置时调引擎转写，否则用平台事件自带的 `asr_refer_text`；两者都无时 `asr_text: null` 并附 `asr_note` 说明原因，不伪造转写 |
| 引用消息 | ✅ | `msg_elements`（被引用内容/附件）与 `message_reference` 随记录入箱 |
| 频道消息 / 频道私信 | ✅ | scene 为 `guild` / `dm`，回复分别走频道发送路径与 `/dms` 接口 |
| 群全量消息 GROUP_MESSAGE_CREATE | ⚠️ 需 opt-in | SDK 不支持（见下「SDK 限制」），daemon 自行注册解析器 + 子类分发；平台侧需开群全量消息权限，本侧需 `config.json` 的 `group_full_messages: true` |
| 按钮交互 INTERACTION_CREATE | ✅ | 自动 ACK，并记入 events.jsonl |
| 生命周期事件（FRIEND_ADD 等） | ✅ | 记入 events.jsonl；平台是否推送取决于应用订阅的事件范围 |

发送：

| 类型 | 状态 | 说明 |
|---|---|---|
| 纯文本被动回复 | ✅ | `{"reply": "..."}` |
| 拆条回复 | ✅ | `{"replies": ["...", "..."]}`，逐条独立发送，共享单条源消息的回复额度（默认 4） |
| Markdown | ✅ | outbox 加 `"markdown": true`（C2C/群/频道；频道私信仍为纯文本） |
| 富媒体（图片/视频/语音/文件） | ✅ | outbox 加 `"media": [...]`，经 SDK 上传接口（大文件自动分片）以 msg_type 7 发出；有文字时文字随第一条媒体消息一起发。仅 C2C/群：SDK 的上传接口只支持这两类，频道/私信的 media 会被跳过并记日志 |
| 正在输入状态 | ✅ | C2C 消息入箱时自动发 `send_typing`（尽力而为，失败只记日志），`config.json` 可关 |
| 主动消息 | ✅ | 见 `proactive/` 格式；失败自动重试（默认 3 次、间隔 5 秒），平台错误原文记日志与归档 |
| 互动召回 | ✅ | proactive payload 加 `"wakeup": true`（仅 C2C）；四周期记账在 `state/wakeup_usage.json` |

## 目录

| 路径 | 用途 |
|---|---|
| `inbox/` | 待回复的消息（owner），记录结构见下 |
| `outbox/` | agent 写出的回复，文件名与 inbox 对应，格式见下 |
| `actions/` | owner agent 的 API 动作文件，执行结果写 `done/<文件名>.result.json`，格式见下 |
| `proactive/` | 待发的主动消息文件，格式见下 |
| `events/events.jsonl` | 交互与生命周期事件日志（每行一个 JSON） |
| `done/` | 已处理归档：消息文件、`.reply.json`、`.proactive.json`、结果标记（`.SENT` / `.PARTIAL` / `.EXPIRED` / `.INVALID` / `.ORPHAN` / `.FAILED`，标记文件内是含时间与原因的 JSON） |
| `logs/daemon.log` | 守护进程日志（不含密钥） |
| `logs/watchdog.log` | 看门狗重启记录 |
| `logs/supervisor.log`、`logs/daemon-stdout.log` | run.sh 监督日志与 daemon 标准输出 |
| `state/` | WebSocket 会话恢复状态、回复计数与序号账本（reply_counts.json）、发送进度（send_progress.json）、召回记账（wakeup_usage.json / last_seen.json）、绑定候选（pending.json）、心跳（heartbeat.json）、附件缓存 |
| `tests/` | 离线测试套件（stub API）：`tests/test_bridge_offline.py` 33 项 + `tests/test_framework_offline.py` 31 项 |
| `.venv/` | Python 虚拟环境（qqbot-agent-sdk 1.2.2） |

### inbox 记录结构

```json
{
  "msg_id": "...", "scene": "c2c|group|guild|dm", "event_type": "...",
  "user_openid": "发送者", "group_openid": null, "channel_id": null,
  "chat_id": "回复路由目标",
  "content": "正文（可为空）", "message_type": 0,
  "attachments": [{"filename": "", "content_type": "", "url": "", "size": 0}],
  "attachment_only": false,
  "full_message": true,            // 仅群全量消息
  "msg_elements": [...],           // 仅引用消息
  "message_reference": {"message_id": "..."},   // 仅引用消息
  "asr_text": "转写文本或 null",    // 仅含语音附件时
  "asr_note": "无转写时的原因",     // 仅转写失败/不可用时
  "received_at": "ISO 时间"
}
```

### outbox 格式（被动回复，与 inbox 文件同名）

```json
{"reply": "单条文本"}
{"replies": ["拆条第一段", "第二段"], "markdown": false,
 "media": [{"kind": "image|video|voice|file", "path": "本地文件绝对路径"}]}
```

media 的 path 必须是已存在的本地文件，否则整份 payload 按 INVALID
归档。`replies` 的总条数与 media 条数共享单条源消息的被动回复额度
（`config.json` 的 `max_replies_per_message`，默认 4），超额部分不
发、归档 PARTIAL。

### proactive 格式（主动消息，任意文件名）

```json
{"scene": "c2c|group", "target": "用户或群 openid", "content": "文本",
 "markdown": false, "media": [{"kind": "image", "path": "..."}],
 "wakeup": false}
```

命令行快捷方式：`push.py "文本"`（默认收件人是 `allow_openids` 第一
位；`--group` 发群；`--wakeup` 走互动召回）。

### actions 格式（owner agent 驱动 API，任意文件名）

```json
{"action": "send_group", "args": {"group_openid": "...", "content": "..."}}
{"action": "recall_c2c", "args": {"openid": "...", "message_id": "..."}}
{"action": "get_bot_info", "args": {}}
```

daemon 按 `daemon.py` 的 `ACTION_NAMES` 白名单执行 `qqapi.py` 的同名
方法（发送/撤回/文件上传/私信/频道与成员与角色/公告/精华/日程/表态/
论坛/音频等），结果写 `done/<文件名>.result.json`：
`{"action": ..., "ok": true, "result": ...}` 或
`{"action": ..., "ok": false, "error": "..."}`。动作文件执行后删除。
参数签名以 `qqapi.py` 为准。

## 凭据与配置

凭据在桥目录的 `credentials.env`（格式见 `credentials.env.example`，
扫码绑定见 `qr-connect/`）：

```
QQBOT_APP_ID=<你的 AppID>
QQBOT_APP_SECRET=<你的 AppSecret>
```

文件权限 600。也可以直接在进程环境里导出这两个环境变量（环境变量
优先于文件）。没有凭据时守护进程会打印 `credentials missing` 并以
退出码 2 退出；看门狗在凭据文件缺失时静默不动作。

行为配置在 `config.json`（格式见 `config.example.json`），字段：

| 字段 | 默认 | 说明 |
|---|---|---|
| `allow_openids` | `[]` | 私聊白名单；空 = 绑定模式（首位发信人记入 state/pending.json，确认后填入并重启） |
| `allow_group_member_openids` | `[]` | 群发信人白名单（member_openid）；空 = 群内成员都可达 |
| `reply_window_seconds` | 300 | 被动回复窗口守卫，见下节 |
| `max_replies_per_message` | 4 | 单条源消息的被动回复条数上限（含拆条与媒体） |
| `proactive_retries` | 3 | 主动消息失败重试次数 |
| `proactive_retry_delay_seconds` | 5 | 主动消息重试间隔 |
| `typing` | true | C2C 入箱时发正在输入状态 |
| `group_full_messages` | false | 是否接收群全量消息 |
| `heartbeat_seconds` | 20 | heartbeat.json 刷新间隔 |
| `liveness_timeout_seconds` | 300 | 网关静默多久后硬退出等监督层拉起 |
| `block_openids` | `[]` | 任何情况下都直接丢弃的发信人（先于白名单检查） |
| `actions_enabled` | true | 是否处理 `actions/` 动作文件 |
| `admin_port` | 0 | 本地管理接口端口（127.0.0.1，令牌 `state/admin.token`）；0 = 关闭。改端口需重启 daemon |

配置支持热加载：daemon 每 5 秒检查 `config.json` 的修改时间，变了就
先校验（`sanitize_config`：未知键、类型错误都
整份拒绝）再整体换入，失败保留旧配置并记日志。管理接口
`PUT /api/config` 走同一套校验。凭据不在热加载范围。

管理接口（`admin_port` > 0 时）：

| 路由 | 说明 |
|---|---|
| `GET /api/status` | pid、运行时长、白名单人数、inbox 待处理数、网关是否 READY |
| `GET /api/config` | 当前生效配置 |
| `PUT /api/config` | 提交补丁 JSON，校验后落盘，热加载随即生效 |
| `GET /api/events?n=50` | events.jsonl 最近 n 条 |
| `POST /api/push` | `{"scene","target","content"}` 排一条主动消息 |

平台侧确认：应用需开通「群与 C2C 消息」相关事件订阅
（`C2C_MESSAGE_CREATE` / `GROUP_AT_MESSAGE_CREATE`，intent
`GROUP_MESSAGES`，SDK 默认已带）。群全量消息另需在平台给机器人开
「群全量消息」权限。

## 启动

```bash
cd ~/workspace/qq-bridge
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp credentials.env.example credentials.env   # 或跑 qr-connect 扫码绑定
cp config.example.json config.json           # 绑定模式下 allow_openids 先留空
./run.sh                                     # 监督循环拉起 daemon
```

日志里出现 `gateway READY` 即已连上。绑定流程：让主人给机器人发一
条私聊 → `state/pending.json` 出现候选 openid → 在主聊天与用户确认
→ 填进 `config.json` 的 `allow_openids` → 重启 daemon（杀掉进程，
run.sh 会自动拉起）。

离线自检（不联网、不用凭据）：

```bash
.venv/bin/python tests/test_bridge_offline.py
```

## 关键约束：被动回复窗口

QQ 的被动回复只在原消息发出后的一段窗口内有效。官方文档自相矛盾：
接口页标题写 60 分钟，同一页 msg_id 字段写「5 分钟内有效」。本桥默
认按保守的 **5 分钟**守（守护进程以自身收到消息的时刻 `received_at`
计时）。超时才写出的回复不会被发送：守护进程会打日志
`reply for <msg_id> closed ...`，并把文件归档到 `done/` 加
`.EXPIRED` 标记（已发出一部分的标 `.PARTIAL`）。所以 agent 回合必
须尽快产出回复；inbox hook 的唤醒间隔压到 5~10 秒就是为此。自己实
测确认更晚的回复也能发成之后，可以调大 `reply_window_seconds`。

其他行为：

- 机器人自己发的消息会被忽略；重复投递按 msg_id 去重（网关层与
  daemon 层各一份）。
- 瞬时发送失败时守护进程会在窗口关闭前持续重试；致命错误（SDK
  `is_fatal_send_error` 判定，如 invalid / forbidden）立即归档
  FAILED。每条发送都领新 msg_seq，失败不复用——平台可能已经记下
  了那条序号，复用会被按重复消息丢弃。
- `outbox/` 里没有对应 inbox 记录的文件会被按 `.ORPHAN` 归档。
- 主动消息没有窗口概念，失败按 `proactive_retries` 重试；互动召回
  （`wakeup: true`）的周期账在发送前领取、最终失败时释放，不会双花
  也不会白丢。

## SDK 限制（qqbot-agent-sdk 1.2.2，含证据）

1. **GROUP_MESSAGE_CREATE 不被 SDK 支持**：
   - `dto.py` 的 `EventType` 枚举只列了 READY / RESUMED /
     C2C_MESSAGE_CREATE / GROUP_AT_MESSAGE_CREATE / DIRECT_MESSAGE_CREATE /
     GUILD_MESSAGE_CREATE / GUILD_AT_MESSAGE_CREATE / INTERACTION_CREATE，
     没有 GROUP_MESSAGE_CREATE；`MESSAGE_EVENT_TYPES` 同样不含它。
   - `websocket.py` 的 `QQWebSocket._handle_dispatch` 只路由 READY、
     RESUMED、`MESSAGE_EVENT_TYPES` 与 `INTERACTION_EVENT_TYPES`，
     其余一律落到 `Unhandled dispatch` 的 debug 日志被丢弃——所以即使
     平台推来群全量消息，SDK 的回调也永远收不到。
   - 本桥的对策（不改 SDK 文件）：daemon 里把一个解析函数注册进
     `EventParser._EVENT_HANDLERS["GROUP_MESSAGE_CREATE"]`，并用
     `QQWebSocket` 子类覆写 `_handle_dispatch`，把该事件与生命周期
     事件经 SDK 自身的 `_dispatch_to_main` 机制转给回调。生命周期
     事件（FRIEND_ADD 等）在 SDK 枚举里同样缺失，走同一条子类通道。
2. **频道私信没有发送方法**：`QQApiClient` 只有 post_c2c/group/guild
   三个发送方法。本桥用 SDK 的通用 `request()` + `Route` 直调官方
   `POST /dms/{guild_id}/messages`。
3. **富媒体上传只支持 C2C/群**：`MediaUploader.upload` 对其他
   chat_type 直接抛 ValueError，SDK 也没有频道上传接口。故频道/私信
   场景下 media 被跳过（有日志），文字照常发送。
4. **msg_seq 生成是伪随机**：`QQApiClient.next_msg_seq()` 在
   0–65535 内取随机值，同一 msg_id 下两条回复撞序号时，后一条会被
   平台按重复消息静默丢弃。本桥在 daemon 里按 msg_id 维护持久化
   的序号登记表，生成后查重、撞了顺延，保证同一条源消息的每条回
   复（含每次重试）序号都不同。
5. **语音全量转写需要外部条件**：SDK 的 STTPipeline 在未配置 STT
   引擎时只回退平台自带的 `asr_refer_text`；要引擎转写需设置环境变量
   `QQ_STT_API_KEY`（可选 `QQ_STT_BASE_URL` / `QQ_STT_MODEL`），且 silk
   解码依赖 `pilk`（当前未安装；系统有 ffmpeg）。未配置时语音消息的
   `asr_text` 可能为 null，原因会写在 `asr_note` 里。

## 图片拉取：某些机器要走浏览器

入站图片附件是 multimedia.nt.qq.com.cn 上的 CDN URL。在某些机器
（例如走共享出口代理的 VM）上，这些 URL 从 shell 网络拉不动：TLS
握手完成但响应停滞在零字节，curl、httpx、直连、SDK 自带的
AttachmentDownloader 表现一致；而同一台机器的浏览器（不同出口路
径）能秒开。遇到这种情况的部署：inbox 唤醒提示词应要求 agent 只走
browser.browse 取图，不要用 curl 反复试——预期图片回复延迟
1.5–3 分钟，仍在 5 分钟窗口内。附件 URL 本身（att.resolved_url 原
文）已随记录入箱，URL 处理没有问题，卡的是网络路径。
