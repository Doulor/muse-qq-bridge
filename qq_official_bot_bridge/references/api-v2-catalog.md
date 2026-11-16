# QQ 机器人 API v2 接口目录（bot.q.qq.com/wiki/develop/api-v2）

> 调研日期：2026-11-16。来源以官方文档为准：在线 wiki（bot.q.qq.com，部署版本 v1.50.0）与其开源源码仓库 `tencent-connect/bot-docs`（main 分支）逐页核对。
>
> **重要版本提示**：在线 wiki 比 GitHub 仓库快照**新**——「群聊管理」整节、单聊流式消息、输入状态、自定义菜单/指令面板、分片上传等只在在线 wiki（2026 版文档）中存在，仓库 `config.js` 侧边栏里没有。下表中凡标注「※在线版」者，端点细节引自对在线版官方文档做过逐页调研的第三方资料（AstrBot 群管理调研 2026-09、多个 2026 年 SDK 文档），未能在仓库快照中复核，使用前请在在线 wiki 再确认一次。
>
> 场景缩写：**C2C**=QQ 单聊，**群**=QQ 群聊，**频道**=QQ 频道文字子频道（guild），**DM**=频道私信。
> 主体准入（文档首页）：单聊场景为**定向邀请**；群聊场景**仅企业主体**（个人主体暂不支持）；频道场景企业与个人均可申请。

## 0. 基础框架（鉴权 / 网关）

| 方法与路径 | 说明 | 备注 |
|---|---|---|
| `POST https://bots.qq.com/app/getAppAccessToken` | 用 `{appId, clientSecret}` 换取 AccessToken | 有效期 7200s；剩余 <60s 时再请求会发新 token，老 token 60s 内仍有效；请求头 `Authorization: QQBot {ACCESS_TOKEN}` |
| `GET /gateway` | 获取通用 WSS 接入点 | 事件订阅页链接的「获取通用WSS接入点」 |
| `GET /gateway/bot` | 获取带分片 WSS 接入点 | 返回建议 shard 数与 `session_start_limit`（total/remaining/reset_after/max_concurrency） |

- 统一 API 域名：仓库版 `api-use.md` 写 `https://api.sgroup.qq.com`；2026-08 变更记录已统一为 `https://api.bot.qq.com`（旧域名在现行在线文档中已无记载）※在线版。
- 旧的 `Token`（Bot Token）鉴权方式**已弃用**，官方要求改用 AccessToken。
- 事件接入二选一：WebSocket 长连接（Identify/Resume/心跳，op 0/1/2/6/7/9/10/11）或 Webhook HTTP 回调（op 12 ACK、op 13 地址验证，Ed25519 签名，回调端口仅允许 80/443/8080/8443）。

## 1. 消息发送与撤回

### 1.1 发送消息（四场景共用一套参数模型）

| 方法与路径 | 说明 | 场景 | 配额 / 窗口（文档口径） |
|---|---|---|---|
| `POST /v2/users/{openid}/messages` | 给单个用户发消息 | C2C | 主动消息每月 4 条/用户；被动回复窗口 60 分钟、每条消息最多 5 次（另有第三方引官方文档作「最多 4 次」，官方文档自身口径不一） |
| `POST /v2/groups/{group_openid}/messages` | 给群发消息 | 群 | 仓库版：主动每月 4 条/群；被动窗口 5 分钟、≤5 次。在线版频控：Bot 维度 60 QPM（未认证 30）、单关系 20 QPM、每群每天 ≤1000 条，接口本身 100 QPS ※在线版 |
| `POST /channels/{channel_id}/messages` | 给文字子频道发消息 | 频道 | 主动默认 20 条/天/子频道、每频道每天最多 2 个子频道（频道主/管理可另行设置）；单子频道 5 条/秒；被动窗口 5 分钟；**要求机器人 WS 保持在线**；支持 `multipart/form-data` 直传图片（`file_image`） |
| `POST /dms/{guild_id}/messages` | 给频道私信发消息（需先创建私信会话） | DM | 主动 2 条/天/用户、全 Bot 200 条/天；被动窗口 5 分钟、条数不限 |

通用请求字段：`content`、`msg_type`（必填）、`markdown`、`keyboard`、`ark`、`media`（富媒体 file_info）、`message_reference`、`event_id`（被动回复事件凭证）、`msg_id`（被动回复消息凭证）、`msg_seq`（与 msg_id 联合防重，相同组合重复发送会失败）。
可作被动凭证的 `event_id` 事件：C2C 支持 `INTERACTION_CREATE` / `C2C_MSG_RECEIVE` / `FRIEND_ADD`；群支持 `INTERACTION_CREATE` / `GROUP_ADD_ROBOT` / `GROUP_MSG_RECEIVE`；频道侧另支持 GUILD_MEMBER_ADD/UPDATE/REMOVE、MESSAGE_REACTION_ADD/REMOVE、FORUM_* 系列事件。
含 URL 的消息须先在管理端「开发设置-消息 URL 配置」中预配，否则发送失败。频道场景主动消息需过审核（见 MESSAGE_AUDIT 事件；错误码 304023/304024 表示审核中，响应带 `message_audit.audit_id`）。

**发送变体（同一 C2C 发送端点，靠字段区分）※在线版：**

| 变体 | 说明 |
|---|---|
| 「输入中」状态（typing） | `msg_type=6` + `input_notify` 对象（`input_second` 默认 60），向用户展示正在输入；仅 C2C |
| 流式消息（打字机） | 同端点带 stream 参数分片下发：`input_state`（1=生成中，10=结束）、`index` 递增、`content_type`（text/markdown）、首片返回流消息 id，后续片回填；仅 C2C，群聊不支持 |
| 互动召回 `is_wakeup` | C2C 专属：用户最后一次互动后 30 天内分 4 个周期（当天、1–3 天、3–7 天、7–30 天），每周期可下发 1 条；与 msg_id/event_id 被动凭证互斥 |

### 1.2 消息内容类型（msg_type）

| msg_type | 类型 | 说明与门槛 |
|---|---|---|
| 0 | 文本 text | `content` 纯文本；支持内嵌格式（见下） |
| 2 | Markdown | 两种形态：模板 Markdown（`custom_template_id`+`params`，模板需申请）与原生 Markdown（`markdown.content`，**需内邀开通**；开通后主动 MD 默认生效，被动 MD 仍需单独申请）。四场景均可发送 |
| 3 | ARK 结构化卡片 | 官方模板 23（链接+文本列表）/24（文本+缩略图）/37（大图），`ark.template_id`+`kv` 填充。**被动消息 ARK 需向平台运营申请**（主动 ARK 旧版文档称默认开放） |
| 4 | Embed | 一种特殊的 ARK（频道体系），`embed` 对象 |
| 6 | 输入状态 | 非内容消息，见 1.1 typing ※在线版 |
| 7 | 富媒体 media | `media.file_info`，须先走第 2 节上传流程；发送时 `content` 需至少填一个空格（文档注明的已知问题） |

文本内嵌格式（text-chain）：`<qqbot-at-user id=""/>` @用户、`<qqbot-at-everyone/>` @全体成员（仅频道）、`<qqbot-cmd-enter text=""/>` 点击直接执行指令、`<qqbot-cmd-input text="" show=""/>` 点击填入输入框、`<#channel_id>` 子频道跳转、`<emoji:id>` 系统表情。
接收侧附件类型（事件 `attachments[]`）：图片（image/jpeg、png、gif）、语音（voice，群/C2C 事件带 `asr_refer_text` 语音转写）、视频（video/mp4）、文件（file）、表情包；引用消息以 `msg_elements[]` 给出（元素 message_type 含 101/102/103 等引用类型）※部分为在线版。

### 1.3 撤回消息

| 方法与路径 | 说明 | 场景 | 备注 |
|---|---|---|---|
| `DELETE /v2/users/{openid}/messages/{message_id}` | 撤回机器人发给该用户的消息 | C2C | 超过 2 分钟不可撤回 |
| `DELETE /v2/groups/{group_openid}/messages/{message_id}` | 撤回群内消息 | 群 | 超 2 分钟不可撤；10 QPS；在线版文档：机器人为群管理员时可撤回普通成员消息 ※在线版 |
| `DELETE /channels/{channel_id}/messages/{message_id}?hidetip=` | 撤回子频道消息 | 频道 | `hidetip` 控制是否隐藏撤回小灰条；管理员可撤普通成员消息，频道主可撤所有人消息 |
| `DELETE /dms/{guild_id}/messages/{message_id}?hidetip=` | 撤回私信消息 | DM | 仅能撤回机器人自己发送的 |

## 2. 富媒体（文件上传）

仅用于 C2C 与群聊发送图片/视频/语音/文件。两种用法：`srv_send_msg=true` 上传即直发（占主动消息频次）；`srv_send_msg=false` 只换取 `file_info` 再填入发送接口的 `media` 字段（官方推荐）。`file_info` 有有效期（返回 `ttl` 秒，0=长期），且**上传场景与发送场景绑定**：`/v2/users/...` 上传的只能发单聊，`/v2/groups/...` 上传的只能发群聊。

| 方法与路径 | 说明 | 场景 | 备注 |
|---|---|---|---|
| `POST /v2/users/{openid}/files` | 按 URL 上传媒体换 file_info（或直发） | C2C | `file_type`：1 图片（png/jpg）、2 视频（mp4）、3 语音（silk）、4 文件（**暂不开放**）；入参只有 `url`，`file_data` 直传**暂未支持** |
| `POST /v2/groups/{group_openid}/files` | 同上，群聊版 | 群 | 频控 50 QPS ※在线版 |
| `POST /v2/users/{openid}/files/upload_prepare` + `.../upload_part_finish` | 分片上传（大文件）：先 prepare 再逐片 finish | C2C | 10 QPS ※在线版（仓库版文档未收录） |
| `POST /v2/groups/{group_openid}/files/upload_prepare` + `.../upload_part_finish` | 分片上传，群聊版 | 群 | 10 QPS ※在线版 |

频道/DM 场景没有独立上传端点：频道发图走发送接口的 `image`（URL 转存）或 form-data `file_image` 直传。

## 3. 互动与按钮

### 3.1 消息按钮（keyboard）与互动回应

按钮挂在 Markdown 消息底部（发送接口的 `keyboard` 字段）：模板按钮（`keyboard.id`，模板需**申请**）或自定义按钮（`keyboard.content.rows`，**内邀开通**；最多 5 行 × 每行 5 个）。
Button 结构：`id`、`render_data{label, visited_label, style(0 灰/1 蓝)}`、`action{type, permission, data, unsupport_tips}`。
action.type：0=跳转（http/小程序 scheme）、1=回调（data 回传后台）、2=指令（点击把 @bot+data 填入输入框，可设 `enter` 直接发送、`reply` 带引用、`anchor=1` 唤起选图器，仅单聊手机端）。
action.permission.type：0=指定用户、1=仅管理者、2=所有人、3=指定身份组（仅频道）。

| 方法与路径 | 说明 | 场景 | 备注 |
|---|---|---|---|
| `PUT /interactions/{interaction_id}` | 回应互动事件（按钮点击后必须回，否则客户端一直 loading 到超时） | C2C / 群 / 频道 | body `{code}`：0 成功、1 操作失败、2 操作频繁、3 重复操作、4 没有权限、5 仅管理员操作 |

点击回调按钮触发 `INTERACTION_CREATE` 事件（intent 1<<26）：`type` 11=消息按钮、12=单聊快捷菜单；`scene` 为 c2c/group/guild；`data.resolved` 带 `button_data`、`button_id`（频道另有 `user_id`、`message_id`，自定义菜单另有 `feature_id`）。事件的 `id` 本身也可作被动发消息的 `event_id` 凭证。

### 3.2 自定义菜单与指令面板 ※在线版（仓库版文档未收录，路径据官方 SDK/DTO 资料）

| 方法与路径 | 说明 | 场景 | 备注 |
|---|---|---|---|
| `GET /v2/menu` | 查询全局自定义菜单 | C2C | 菜单显示在单聊窗口底部，全局一份、对所有 C2C 用户生效 |
| `PUT /v2/menu` | 整体覆盖设置自定义菜单 | C2C | 约 5 QPM；菜单项 type：switch/send_message/link/menu，一级 ≤10 项，二级 ≤5 项 |
| `GET /v2/panels?scope=` | 按场景分页列出指令面板 | C2C / 群 / 频道 / DM | scope=c2c/group/channel/dm |
| `POST /v2/panels` | 创建指令面板 | 同上 | `target_type` all/specific；specific 一次最多关联 20 个 openid；channel/dm 只能 all |
| `GET /v2/panels/{panel_id}` | 查询单个面板详情 | 同上 |  |
| `PUT /v2/panels/{panel_id}` | 整体覆盖面板内容 | 同上 | 不改 scope 与关联对象 |
| `DELETE /v2/panels/{panel_id}` | 删除面板（不可恢复） | 同上 |  |
| `PUT /v2/panels/{panel_id}/target`（路径未逐字复核） | 增删 specific 面板的关联用户/群（op=add/del） | C2C / 群 | all 面板调用会失败 |

## 4. 用户与好友

| 方法与路径 | 说明 | 场景 | 备注 |
|---|---|---|---|
| `GET /users/@me` | 获取当前机器人自身的用户详情（id、username、avatar、union_openid 等） | 全局 | 位于频道模块文档的「用户管理」下 |

说明：C2C 侧**没有**好友列表/用户资料查询类接口；与用户相关的只有事件（FRIEND_ADD / FRIEND_DEL / C2C_MSG_RECEIVE / C2C_MSG_REJECT，见第 8 节）。另有第三方框架提及「查询用户主动消息开关状态」的接口（goqbot 的 `get_c2c_msg_state` 动作），但其官方原始路径未能在文档中复核，此处不列为正式端点。

## 5. 群管理 ※整节为在线版（仓库侧边栏无此节；端点与权限据 AstrBot 对在线版官方文档的逐页调研，2026-09）

权限三档：**白名单**（未获权限报 11253）、**白名单+内邀**、**机器人须为群管理员**（未通过管理员检查报 11282）。

| 方法与路径 | 说明 | 权限 | 频控 |
|---|---|---|---|
| `GET /v2/groups/{gid}/info` | 群信息（群名、简介、分类、标签、成员数） | 仅白名单 | 30 QPM |
| `GET /v2/groups/{gid}/bot_state` | 机器人群内状态：自身 member_openid、joined_at、allow_proactive_msg、`recv_msg_setting`（all/only_mention/mention_and_context）、member_role（member/owner/admin） | 仅白名单 | 30 QPM |
| `GET /v2/groups/{gid}/join_request_list` | 入群申请列表（cursor 分页，limit ≤50；含 join_request_id、风险提示、验证问答等） | 群管理员 | 30 QPM |
| `POST /v2/groups/{gid}/approval_join_request/{member_openid}` | 审批入群申请（op=approve/decline，可带 join_request_id、拒绝理由、是否拉黑） | 群管理员 | 60 QPM |
| `GET /v2/groups/{gid}/restrict_chat_setting` | 查禁言设置：全员规则 `global_rule`（none/always/schedule，**只读**）+ 定时/周期规则 + 成员禁言列表 | 群管理员 | 30 QPM |
| `POST /v2/groups/{gid}/restrict_chat_setting` | 设置成员禁言：`members[{op:add/update/del, member_openid, mute_expire_at}]`，单次 ≤20 人、最长 30 天，只能操作普通成员；**无接口可改全员禁言** | 群管理员 | 60 QPM |
| `GET /v2/groups/join_approval_strategy` | 入群自动审批策略列表（Bot 级，非按群） | 文档未标 | 60 QPM |
| `POST /v2/groups/join_approval_strategy` | 创建自动审批策略（group_openids 与 group_ids 二选一 ≤100 群；每 Bot ≤20 个策略；默认有效期一年） | 文档未标 | 60 QPM |
| `PATCH /v2/groups/join_approval_strategy/{strategy_id}` | 修改策略 | 文档未标 | 60 QPM |
| `DELETE /v2/groups/join_approval_strategy/{strategy_id}` | 删除策略 | 文档未标 | 60 QPM |
| `POST /v2/groups/join_approval_strategy/{strategy_id}/execute` | 立即执行策略（异步全量扫描，约 10 分钟） | 文档未标 | 60 QPM |
| `POST /v2/groups/join_approval_strategy/{strategy_id}/whitelist_users` | 维护策略白名单用户（单次 ≤10000，总上限 10 万） | 文档未标 | 60 QPM |
| `GET /v2/groups/{gid}/members` | 群成员列表（游标分页，≤30 条/页） | 白名单+内邀 | 60 QPM |
| `GET /v2/groups/{gid}/members/{member_openid}` | 群成员详情（username、member_role、joined_at、union_openid 等） | 白名单+内邀 | 30 QPM |
| `POST /v2/groups/{gid}/batch_remove_members` | 批量移除群成员（≤20 人） | 白名单+内邀 | 30 QPM |
| `GET /v2/groups/{gid}/member_blacklist` | 群黑名单列表（limit ≤100） | 白名单+内邀 | 30 QPM |
| `POST /v2/groups/{gid}/member_blacklist` | 增删群黑名单（op add/del，≤20 人） | 白名单+内邀 | 60 QPM |

官方明确**没有**的群能力：主动退群、群公告、设置/取消管理员、转让群主、解散群、修改全员禁言、群名片/加群链接、openid 反查 QQ 号。

## 6. 频道管理（guild，全部为频道场景）

### 6.1 频道与子频道

| 方法与路径 | 说明 | 备注 |
|---|---|---|
| `GET /users/@me/guilds` | 机器人已加入的频道列表（before/after/limit 分页，limit ≤100） |  |
| `GET /guilds/{guild_id}` | 频道详情 |  |
| `GET /guilds/{guild_id}/channels` | 子频道列表 |  |
| `GET /channels/{channel_id}` | 子频道详情 |  |
| `POST /guilds/{guild_id}/channels` | 创建子频道 | 需「管理频道」权限，机器人须为管理员 |
| `PATCH /channels/{channel_id}` | 修改子频道（名称/排序/分组/私密类型/发言权限） | 需管理子频道权限（机器人管理员） |
| `DELETE /channels/{channel_id}` | 删除子频道 | 需管理子频道权限；**删除不可恢复** |

### 6.2 频道成员与在线人数

| 方法与路径 | 说明 | 备注 |
|---|---|---|
| `GET /guilds/{guild_id}/members` | 成员列表（after/limit 分页，limit 1–400；翻页可能重复，需按 user id 去重） |  |
| `GET /guilds/{guild_id}/members/{user_id}` | 成员详情 |  |
| `GET /guilds/{guild_id}/roles/{role_id}/members` | 某身份组的成员列表（start_index/limit 分页） |  |
| `DELETE /guilds/{guild_id}/members/{user_id}` | 移除成员，可同时拉黑（`add_blacklist`）并撤回其消息（`delete_history_msg_days`：3/7/15/30 天或 -1 全部） | 需踢人权限（机器人管理员）；不能移除管理员 |
| `GET /channels/{channel_id}/online_nums` | 音视频/直播子频道的在线人数 | 仅音视频/直播子频道 |

### 6.3 身份组（roles）与子频道权限

| 方法与路径 | 说明 | 备注 |
|---|---|---|
| `GET /guilds/{guild_id}/roles` | 身份组列表（含 role_num_limit） |  |
| `POST /guilds/{guild_id}/roles` | 创建身份组（name/color/hoist） | 需相应权限（机器人管理员） |
| `PATCH /guilds/{guild_id}/roles/{role_id}` | 修改身份组 | 同上 |
| `DELETE /guilds/{guild_id}/roles/{role_id}` | 删除身份组 | 同上 |
| `PUT /guilds/{guild_id}/members/{user_id}/roles/{role_id}` | 把成员加入身份组（身份组 5=子频道管理员时需带 channel 对象指定子频道） | 同上 |
| `DELETE /guilds/{guild_id}/members/{user_id}/roles/{role_id}` | 把成员移出身份组 | 同上 |
| `GET /channels/{channel_id}/members/{user_id}/permissions` | 查某用户在子频道的权限位图 | 需管理子频道权限（机器人管理员） |
| `PUT /channels/{channel_id}/members/{user_id}/permissions` | 改用户子频道权限（add/remove 位图，同位同置 1 时以删除为准；不支持改「可管理子频道」权限） | 同上 |
| `GET /channels/{channel_id}/roles/{role_id}/permissions` | 查某身份组在子频道的权限位图 | 同上 |
| `PUT /channels/{channel_id}/roles/{role_id}/permissions` | 改身份组子频道权限（add/remove 位图） | 同上 |

### 6.4 接口授权（API 权限）

| 方法与路径 | 说明 | 备注 |
|---|---|---|
| `GET /guilds/{guild_id}/api_permission` | 机器人在该频道可用的接口权限列表（path/method/desc/auth_status） |  |
| `POST /guilds/{guild_id}/api_permission/demand` | 向频道发送一条接口权限授权链接，请求频道主授权某接口 | 每频道每天最多 3 条（默认） |

### 6.5 发言管理（禁言与消息设置）

| 方法与路径 | 说明 | 备注 |
|---|---|---|
| `GET /guilds/{guild_id}/message/setting` | 频道消息频率设置（disable_create_dm、disable_push_msg、可推送子频道列表、channel_push_max_num） |  |
| `PATCH /guilds/{guild_id}/mute` | 频道全员禁言/解禁（mute_end_timestamp 或 mute_seconds，传 "0" 解禁） | 需管理员权限 |
| `PATCH /guilds/{guild_id}/mute`（带 `user_ids`） | 批量成员禁言/解禁（同一路径，靠参数区分） | 需管理员权限；返回实际成功的 user_ids |
| `PATCH /guilds/{guild_id}/members/{user_id}/mute` | 指定成员禁言/解禁 | 需管理员权限 |

### 6.6 内容管理：公告 / 精华 / 日程 / 音频 / 论坛 / 表情表态

| 方法与路径 | 说明 | 备注 |
|---|---|---|
| `POST /guilds/{guild_id}/announces` | 创建频道公告：消息型公告（message_id+channel_id）或推荐子频道型（announces_type + recommend_channels，≤3 个，两种类型互斥顶替） |  |
| `DELETE /guilds/{guild_id}/announces/{message_id}` | 删除公告（message_id=all 删除推荐子频道型公告） |  |
| `PUT /channels/{channel_id}/pins/{message_id}` | 添加精华消息（每子频道 ≤20 条） |  |
| `DELETE /channels/{channel_id}/pins/{message_id}` | 删除精华消息（message_id=all 清空） |  |
| `GET /channels/{channel_id}/pins` | 精华消息列表 |  |
| `GET /channels/{channel_id}/schedules` | 日程子频道当天日程列表（since 毫秒时间戳可选） |  |
| `GET /channels/{channel_id}/schedules/{schedule_id}` | 日程详情 |  |
| `POST /channels/{channel_id}/schedules` | 创建日程 | 需管理频道权限（机器人管理员）；频控：单个管理员 10 次/天、单频道 100 次/天 |
| `PATCH /channels/{channel_id}/schedules/{schedule_id}` | 修改日程 | 需管理频道权限 |
| `DELETE /channels/{channel_id}/schedules/{schedule_id}` | 删除日程 | 需管理频道权限 |
| `POST /channels/{channel_id}/audio` | 音频播放控制（audio_url/text/status） | **仅音频类机器人**，需向平台申请权限 |
| `PUT /channels/{channel_id}/mic` | 机器人上麦 | 同上 |
| `DELETE /channels/{channel_id}/mic` | 机器人下麦 | 同上 |
| `GET /channels/{channel_id}/threads` | 论坛帖子列表 | 论坛类子频道；相关事件需 FORUMS_EVENT（仅私域） |
| `GET /channels/{channel_id}/threads/{thread_id}` | 帖子详情 |  |
| `PUT /channels/{channel_id}/threads` | 发表帖子（format：1 文本/2 HTML/3 Markdown/4 JSON 富文本；异步，返回 task_id） |  |
| `DELETE /channels/{channel_id}/threads/{thread_id}` | 删除帖子 |  |
| `PUT /channels/{channel_id}/messages/{message_id}/reactions/{type}/{id}` | 机器人对消息发表情表态 | **仅频道内可用** |
| `DELETE /channels/{channel_id}/messages/{message_id}/reactions/{type}/{id}` | 删除机器人自己的表情表态 | 仅频道 |
| `GET /channels/{channel_id}/messages/{message_id}/reactions/{type}/{id}` | 拉取某表情表态的用户列表（cookie/limit 分页，limit ≤50） | 仅频道 |

### 6.7 小程序相关（非机器人服务端接口，仅备注）

频道模块下有「小程序相关」两页（开放数据域签名校验、接口说明），内容是 QQ 小程序前端/开放数据签名，不是机器人 OpenAPI 端点，此处不计入端点数。

## 7. 频道私信（DM）

| 方法与路径 | 说明 | 备注 |
|---|---|---|
| `POST /users/@me/dms` | 创建私信会话（`recipient_id` + `source_guild_id`，双方须有共同频道），返回私信用 guild_id/channel_id | 发私信的前置步骤 |
| `POST /dms/{guild_id}/messages` | 发送私信（参数同频道发消息） | 配额见 1.1 |
| `DELETE /dms/{guild_id}/messages/{message_id}` | 撤回私信 | 见 1.3 |

注意：私信场景**不支持沙箱环境**，调试只能用用户 id 白名单方式。

## 8. 事件与 Intents（WebSocket / Webhook 推送）

订阅规则：除 `GUILDS`、`PUBLIC_GUILD_MESSAGES`、`GUILD_MEMBERS` 为基础权限默认可订阅外，其余 intent 均需向平台**申请**；Identify 时带了无权限的 intent，网关会直接报错断开（错误 4014）。
下表以官方「事件订阅与通知」页的 intents 总表为准；标 ※ 的事件不在总表中，只出现在各自的事件页（在线版）。

| Intent | 事件 | 触发 |
|---|---|---|
| GUILDS (1<<0) | GUILD_CREATE / GUILD_UPDATE / GUILD_DELETE | 机器人加入频道 / 频道资料变更 / 退出或频道解散 |
| GUILDS (1<<0) | CHANNEL_CREATE / CHANNEL_UPDATE / CHANNEL_DELETE | 子频道创建 / 更新 / 删除 |
| GUILD_MEMBERS (1<<1) | GUILD_MEMBER_ADD / GUILD_MEMBER_UPDATE / GUILD_MEMBER_REMOVE | 频道成员加入 / 资料变更 / 被移除 |
| GUILD_MESSAGES (1<<9) | MESSAGE_CREATE | 频道内全部消息（不限于 @机器人） |
| GUILD_MESSAGES (1<<9) | MESSAGE_DELETE | 频道消息被删除（撤回） |
| GUILD_MESSAGE_REACTIONS (1<<10) | MESSAGE_REACTION_ADD / MESSAGE_REACTION_REMOVE | 消息表情表态添加 / 移除（频道） |
| DIRECT_MESSAGE (1<<12) | DIRECT_MESSAGE_CREATE | 收到频道私信消息 |
| DIRECT_MESSAGE (1<<12) | DIRECT_MESSAGE_DELETE | 私信消息被删除（撤回） |
| GROUP_AND_C2C_EVENT (1<<25) | C2C_MESSAGE_CREATE | 用户单聊给机器人发消息 |
| GROUP_AND_C2C_EVENT (1<<25) | FRIEND_ADD / FRIEND_DEL | 用户添加 / 删除机器人好友 |
| GROUP_AND_C2C_EVENT (1<<25) | C2C_MSG_RECEIVE / C2C_MSG_REJECT | 用户在资料卡开启 / 关闭「主动消息」推送 |
| GROUP_AND_C2C_EVENT (1<<25) | GROUP_AT_MESSAGE_CREATE | 群内 @机器人 的消息 |
| GROUP_AND_C2C_EVENT (1<<25) ※ | GROUP_MESSAGE_CREATE | 群开启「接收全部消息」（recv_msg_setting=all）后的每条群消息 |
| GROUP_AND_C2C_EVENT (1<<25) | GROUP_ADD_ROBOT / GROUP_DEL_ROBOT | 机器人被拉入 / 移出群 |
| GROUP_AND_C2C_EVENT (1<<25) | GROUP_MSG_RECEIVE / GROUP_MSG_REJECT | 群管理员开启 / 关闭机器人通知（接收全部消息设置变更） |
| GROUP_AND_C2C_EVENT (1<<25) ※ | SUBSCRIBE_MESSAGE_STATUS | 订阅消息授权状态变更 |
| (1<<24) ※ | GROUP_MEMBER_ADD / GROUP_MEMBER_REMOVE | 群成员入群 / 退群（新 intent 位，总表未收录） |
| (1<<24) ※ | GROUP_JOIN_REQUEST | 用户申请加群（仅机器人为群管理员时可见） |
| INTERACTION (1<<26) | INTERACTION_CREATE | 按钮点击、单聊快捷菜单等互动（须用 PUT /interactions 回应） |
| MESSAGE_AUDIT (1<<27) | MESSAGE_AUDIT_PASS / MESSAGE_AUDIT_REJECT | 主动消息审核通过 / 不通过（带 audit_id 与消息 id） |
| FORUMS_EVENT (1<<28) | FORUM_THREAD_CREATE / FORUM_THREAD_UPDATE / FORUM_THREAD_DELETE | 论坛主题创建 / 更新 / 删除 |
| FORUMS_EVENT (1<<28) | FORUM_POST_CREATE / FORUM_POST_DELETE | 论坛帖子创建 / 删除 |
| FORUMS_EVENT (1<<28) | FORUM_REPLY_CREATE / FORUM_REPLY_DELETE | 论坛评论回复创建 / 删除（文档原文两处均写「回复评论时」） |
| FORUMS_EVENT (1<<28) | FORUM_PUBLISH_AUDIT_RESULT | 论坛发表内容审核结果 |
| AUDIO_ACTION (1<<29) | AUDIO_START / AUDIO_FINISH / AUDIO_ON_MIC / AUDIO_OFF_MIC | 音频播放开始 / 结束 / 上麦 / 下麦 |
| PUBLIC_GUILD_MESSAGES (1<<30) | AT_MESSAGE_CREATE | 频道内 @机器人 的消息（公域） |
| PUBLIC_GUILD_MESSAGES (1<<30) | PUBLIC_MESSAGE_DELETE | 频道消息被删除 |
| —（无 intent，频道事件页单列） ※ | AUDIO_OR_LIVE_CHANNEL_MEMBER_ENTER / AUDIO_OR_LIVE_CHANNEL_MEMBER_EXIT | 用户进入 / 离开音视频或直播子频道 |
| 网关生命周期 | READY / RESUMED | Identify 成功 / Resume 补发完成 |

消息事件通用注意：同一 msg_id 可能重复推送，需结合 msg_seq 做回复排重；群消息事件的 content 已去除 @机器人前缀，author 带 member_openid 与 member_role。

## 9. 其他 / 未单列项

- **错误与调试**：文档有 openapi / websocket 两份错误码页。关键码：11243 token 无效、11252/11253/11254 权限检查失败/未获接口权限（白名单）/接口封禁、11265 机器人被封禁、11281/11282 管理员检查失败/未通过、22009 发消息超频、304082/304083 富媒体拉取/转换失败、304023/304024 消息审核中、40034100 主动消息超频、40034005 被动回复 msg_id 已过期、620002 表情表态超限。
- **唯一 ID 说明**（dev-prepare/unique-id）：openid 在不同场景不互通——同一用户在 C2C、每个群（member_openid）、频道（user id）下标识不同，union_openid 为跨场景统一标识（部分接口/事件提供）。
- **客户端功能页**（client-func：基础信息/资料卡/发现页）描述机器人在 QQ 客户端内的展示与入口配置，属管理端配置而非 API。
- **订阅消息**：SUBSCRIBE_MESSAGE_STATUS 事件与 C2C/群的资料卡开关事件（C2C_MSG_RECEIVE 等）共同构成「用户是否允许主动消息」的状态面；发送主动消息前应尊重该状态，否则发送失败。

## 10. 端点计数小结

| 分组 | 端点数 |
|---|---|
| 基础框架（token + 网关） | 3 |
| 消息发送（含 4 场景发送） | 4 |
| 消息撤回（4 场景） | 4 |
| 富媒体（URL 上传 2 + 分片上传 4） | 6 |
| 互动与按钮（互动回应 1 + 菜单 2 + 指令面板 6） | 9 |
| 用户与好友 | 1 |
| 群管理 ※在线版 | 17 |
| 频道管理（频道/子频道 7 + 成员 5 + 身份组与权限 10 + 接口授权 2 + 发言管理 3 + 内容管理 20） | 47 |
| 频道私信（创建会话 1，发送/撤回已计入消息组） | 1 |
| **合计** | **92** |

（发送接口的 typing / 流式 / is_wakeup 为同端点变体，未重复计数；与社区 SDK qq-botpy-v2 自述「91 个 BotAPI 方法」大体吻合。）

## 11. 调研说明与缺口

- 在线 wiki 为 SPA，逐页深链抓取不稳定；本目录以官方开源仓库 `tencent-connect/bot-docs`（即 wiki 的源码）逐页读取为主，在线 wiki 页面（启动接入、事件订阅、ARK 等）复核为辅，两者内容一致的部分占绝大多数。
- 仓库快照明显落后于在线 wiki：群聊管理整节、流式消息、输入状态（msg_type=6）、自定义菜单/指令面板、分片上传、统一域名 api.bot.qq.com 等均为 2025–2026 年新增，仓库 config.js 侧边栏未收录。标 ※在线版 的条目来自对在线版官方文档的第三方逐页调研与多个 SDK 的接口映射，端点路径可信但个别字段细节建议使用前在在线 wiki 复核。
- `PUT /v2/panels/{panel_id}/target` 的路径未逐字复核（仅确认存在「修改面板关联对象」这一接口与 UpdatePanelTarget 请求结构）。
- 第三方框架提及的 `get_c2c_msg_state`（查用户主动消息开关）未找到官方原始路径，未列入正式端点。
- 少数页面本身残缺：仓库内 `delete_thread.md` 几乎是空壳（仅标题+接口行）；`FORUM_REPLY_DELETE` 的触发说明在官方总表中与 CREATE 复制了同一句文案。
