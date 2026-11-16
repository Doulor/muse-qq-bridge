"""Extended QQ Bot API v2 surface, on top of qqbot-agent-sdk.

The SDK's QQApiClient covers the hot paths (sending messages, typing,
interactions, media upload for sends).  This module wraps the client's
generic ``request()`` to expose the rest of the documented api-v2
surface — recalls per scene, rich-media file upload, guild reads and
management (channels, members, roles, announcements, pins, schedules,
reactions, forum, audio), DMs and bot identity — so the agent can drive
them through the bridge's ``actions/`` directory without the daemon
growing a special case per endpoint.

Every method returns the decoded JSON body (usually a dict) and raises
whatever the SDK raises on transport/API errors; the actions runner
records the exception text as the action result.  Paths follow the
official api-v2 documentation (bot.q.qq.com/wiki/develop/api-v2/).
Guild-management endpoints additionally need the bot to hold the
matching guild permissions; the platform enforces that, not this code.
"""

from __future__ import annotations

from typing import Any, Optional


class QQApi:
    def __init__(self, client: Any) -> None:
        self.c = client

    async def _req(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> Any:
        # SDK request() is (method, path, body) positional with no
        # params support — query parameters are baked into the path.
        if params:
            from urllib.parse import urlencode

            qs = urlencode(
                {k: v for k, v in params.items() if v not in (None, "")}
            )
            if qs:
                path = f"{path}?{qs}"
        return await self.c.request(method, path, json_body)

    # -- identity ---------------------------------------------------------

    async def get_bot_info(self) -> Any:
        """GET /users/@me — the bot's own profile."""
        return await self._req("GET", "/users/@me")

    async def list_my_guilds(self, before: str = "", after: str = "", limit: int = 100) -> Any:
        """GET /users/@me/guilds — guilds the bot belongs to."""
        params = {"limit": limit}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        return await self._req("GET", "/users/@me/guilds", params=params)

    # -- sending (thin wrappers over the SDK's send helpers) ---------------

    async def send_c2c(self, openid: str, content: str, **kw) -> Any:
        return await self.c.send_text("c2c", openid, content, **kw)

    async def send_group(self, group_openid: str, content: str, **kw) -> Any:
        return await self.c.send_text("group", group_openid, content, **kw)

    async def send_guild(self, channel_id: str, content: str, **kw) -> Any:
        return await self.c.send_text("guild", channel_id, content, **kw)

    async def send_dm(self, guild_id: str, content: str, **kw) -> Any:
        return await self.c.send_text("dm", guild_id, content, **kw)

    async def send_typing(self, openid: str, msg_id: str) -> Any:
        return await self.c.send_typing(openid, msg_id)

    async def ack_interaction(self, interaction_id: str, code: int = 0) -> Any:
        return await self.c.acknowledge_interaction(interaction_id, code)

    # -- recalls ------------------------------------------------------------

    async def recall_c2c(self, openid: str, message_id: str) -> Any:
        """DELETE /v2/users/{openid}/messages/{message_id}."""
        return await self._req("DELETE", f"/v2/users/{openid}/messages/{message_id}")

    async def recall_group(self, group_openid: str, message_id: str) -> Any:
        """DELETE /v2/groups/{group_openid}/messages/{message_id}."""
        return await self._req(
            "DELETE", f"/v2/groups/{group_openid}/messages/{message_id}"
        )

    async def recall_guild(self, channel_id: str, message_id: str, hidetip: bool = False) -> Any:
        """DELETE /channels/{channel_id}/messages/{message_id}."""
        return await self._req(
            "DELETE",
            f"/channels/{channel_id}/messages/{message_id}",
            params={"hidetip": str(hidetip).lower()},
        )

    async def recall_dm(self, guild_id: str, message_id: str, hidetip: bool = False) -> Any:
        """DELETE /dms/{guild_id}/messages/{message_id}."""
        return await self._req(
            "DELETE",
            f"/dms/{guild_id}/messages/{message_id}",
            params={"hidetip": str(hidetip).lower()},
        )

    # -- rich media file upload ----------------------------------------------

    async def upload_c2c_file(self, openid: str, file_type: int, url: str = "",
                               file_data: str = "", srv_send_msg: bool = False) -> Any:
        """POST /v2/users/{openid}/files. file_type: 1 image, 2 video,
        3 audio, 4 file.  Exactly one of url / file_data (base64)."""
        body: dict = {"file_type": file_type, "srv_send_msg": srv_send_msg}
        if url:
            body["url"] = url
        if file_data:
            body["file_data"] = file_data
        return await self._req("POST", f"/v2/users/{openid}/files", json_body=body)

    async def upload_group_file(self, group_openid: str, file_type: int, url: str = "",
                                file_data: str = "", srv_send_msg: bool = False) -> Any:
        """POST /v2/groups/{group_openid}/files."""
        body: dict = {"file_type": file_type, "srv_send_msg": srv_send_msg}
        if url:
            body["url"] = url
        if file_data:
            body["file_data"] = file_data
        return await self._req("POST", f"/v2/groups/{group_openid}/files", json_body=body)

    # -- DMs -------------------------------------------------------------------

    async def create_dm(self, recipient_id: str, source_guild_id: str) -> Any:
        """POST /users/@me/dms — open a guild DM session."""
        return await self._req(
            "POST",
            "/users/@me/dms",
            json_body={"recipient_id": recipient_id, "source_guild_id": source_guild_id},
        )

    # -- guilds & channels -------------------------------------------------------

    async def get_guild(self, guild_id: str) -> Any:
        return await self._req("GET", f"/guilds/{guild_id}")

    async def list_channels(self, guild_id: str) -> Any:
        return await self._req("GET", f"/guilds/{guild_id}/channels")

    async def get_channel(self, channel_id: str) -> Any:
        return await self._req("GET", f"/channels/{channel_id}")

    async def create_channel(self, guild_id: str, name: str, type: int = 0,
                             sub_type: int = 0, parent_id: str = "",
                             private_type: int = 0, **kw) -> Any:
        body = {"name": name, "type": type, "sub_type": sub_type,
                "private_type": private_type}
        if parent_id:
            body["parent_id"] = parent_id
        body.update(kw)
        return await self._req("POST", f"/guilds/{guild_id}/channels", json_body=body)

    async def modify_channel(self, channel_id: str, **fields) -> Any:
        return await self._req("PATCH", f"/channels/{channel_id}", json_body=fields)

    async def delete_channel(self, channel_id: str) -> Any:
        return await self._req("DELETE", f"/channels/{channel_id}")

    async def get_channel_online_nums(self, channel_id: str) -> Any:
        """GET /channels/{id}/online_nums — audio/live channel occupancy."""
        return await self._req("GET", f"/channels/{channel_id}/online_nums")

    # -- members & roles ------------------------------------------------------------

    async def list_guild_members(self, guild_id: str, after: str = "0", limit: int = 100) -> Any:
        return await self._req(
            "GET", f"/guilds/{guild_id}/members", params={"after": after, "limit": limit}
        )

    async def get_guild_member(self, guild_id: str, user_id: str) -> Any:
        return await self._req("GET", f"/guilds/{guild_id}/members/{user_id}")

    async def delete_guild_member(self, guild_id: str, user_id: str,
                                  add_blacklist: bool = False,
                                  delete_history_msg_days: int = 0) -> Any:
        return await self._req(
            "DELETE",
            f"/guilds/{guild_id}/members/{user_id}",
            json_body={
                "add_blacklist": add_blacklist,
                "delete_history_msg_days": delete_history_msg_days,
            },
        )

    async def mute_all(self, guild_id: str, mute_end_timestamp: str = "",
                       mute_seconds: str = "") -> Any:
        body: dict = {}
        if mute_end_timestamp:
            body["mute_end_timestamp"] = mute_end_timestamp
        if mute_seconds:
            body["mute_seconds"] = mute_seconds
        return await self._req("PATCH", f"/guilds/{guild_id}/mute", json_body=body)

    async def mute_member(self, guild_id: str, user_id: str,
                          mute_end_timestamp: str = "", mute_seconds: str = "") -> Any:
        body: dict = {}
        if mute_end_timestamp:
            body["mute_end_timestamp"] = mute_end_timestamp
        if mute_seconds:
            body["mute_seconds"] = mute_seconds
        return await self._req(
            "PATCH", f"/guilds/{guild_id}/members/{user_id}/mute", json_body=body
        )

    async def list_roles(self, guild_id: str) -> Any:
        return await self._req("GET", f"/guilds/{guild_id}/roles")

    async def create_role(self, guild_id: str, name: str = "", color: int = 0,
                          hoist: int = 0) -> Any:
        return await self._req(
            "POST", f"/guilds/{guild_id}/roles",
            json_body={"name": name, "color": color, "hoist": hoist},
        )

    async def modify_role(self, guild_id: str, role_id: str, name: str = "",
                          color: int = 0, hoist: int = 0) -> Any:
        return await self._req(
            "PATCH", f"/guilds/{guild_id}/roles/{role_id}",
            json_body={"name": name, "color": color, "hoist": hoist},
        )

    async def delete_role(self, guild_id: str, role_id: str) -> Any:
        return await self._req("DELETE", f"/guilds/{guild_id}/roles/{role_id}")

    async def add_member_role(self, guild_id: str, user_id: str, role_id: str,
                              channel_id: str = "") -> Any:
        body = {"channel_id": channel_id} if channel_id else None
        return await self._req(
            "PUT", f"/guilds/{guild_id}/members/{user_id}/roles/{role_id}",
            json_body=body,
        )

    async def remove_member_role(self, guild_id: str, user_id: str, role_id: str,
                                 channel_id: str = "") -> Any:
        body = {"channel_id": channel_id} if channel_id else None
        return await self._req(
            "DELETE", f"/guilds/{guild_id}/members/{user_id}/roles/{role_id}",
            json_body=body,
        )

    async def get_channel_member_permissions(self, channel_id: str, user_id: str) -> Any:
        return await self._req(
            "GET", f"/channels/{channel_id}/members/{user_id}/permissions"
        )

    async def modify_channel_member_permissions(self, channel_id: str, user_id: str,
                                                add: str = "", remove: str = "") -> Any:
        return await self._req(
            "PUT", f"/channels/{channel_id}/members/{user_id}/permissions",
            json_body={"add": add, "remove": remove},
        )

    async def get_channel_role_permissions(self, channel_id: str, role_id: str) -> Any:
        return await self._req(
            "GET", f"/channels/{channel_id}/roles/{role_id}/permissions"
        )

    async def modify_channel_role_permissions(self, channel_id: str, role_id: str,
                                              add: str = "", remove: str = "") -> Any:
        return await self._req(
            "PUT", f"/channels/{channel_id}/roles/{role_id}/permissions",
            json_body={"add": add, "remove": remove},
        )

    # -- announcements ----------------------------------------------------------------

    async def create_announce(self, guild_id: str, channel_id: str = "",
                              message_id: str = "", announce_type: int = 0,
                              recommend_channels: Optional[list] = None) -> Any:
        body: dict = {"announce_type": announce_type}
        if channel_id:
            body["channel_id"] = channel_id
        if message_id:
            body["message_id"] = message_id
        if recommend_channels:
            body["recommend_channels"] = recommend_channels
        return await self._req("POST", f"/guilds/{guild_id}/announces", json_body=body)

    async def delete_announce(self, guild_id: str, announce_id: str) -> Any:
        return await self._req("DELETE", f"/guilds/{guild_id}/announces/{announce_id}")

    # -- pins ----------------------------------------------------------------------------

    async def put_pin(self, channel_id: str, message_id: str) -> Any:
        return await self._req("PUT", f"/channels/{channel_id}/pins/{message_id}")

    async def delete_pin(self, channel_id: str, message_id: str) -> Any:
        return await self._req("DELETE", f"/channels/{channel_id}/pins/{message_id}")

    async def list_pins(self, channel_id: str) -> Any:
        return await self._req("GET", f"/channels/{channel_id}/pins")

    # -- schedules --------------------------------------------------------------------------

    async def create_schedule(self, channel_id: str, schedule: dict) -> Any:
        return await self._req(
            "POST", f"/channels/{channel_id}/schedules", json_body={"schedule": schedule}
        )

    async def list_schedules(self, channel_id: str) -> Any:
        return await self._req("GET", f"/channels/{channel_id}/schedules")

    async def get_schedule(self, channel_id: str, schedule_id: str) -> Any:
        return await self._req("GET", f"/channels/{channel_id}/schedules/{schedule_id}")

    async def modify_schedule(self, channel_id: str, schedule_id: str, schedule: dict) -> Any:
        return await self._req(
            "PATCH", f"/channels/{channel_id}/schedules/{schedule_id}",
            json_body={"schedule": schedule},
        )

    async def delete_schedule(self, channel_id: str, schedule_id: str) -> Any:
        return await self._req("DELETE", f"/channels/{channel_id}/schedules/{schedule_id}")

    # -- reactions -----------------------------------------------------------------------------

    async def put_reaction(self, channel_id: str, message_id: str,
                           emoji_type: str, emoji_id: str) -> Any:
        return await self._req(
            "PUT",
            f"/channels/{channel_id}/messages/{message_id}/reactions/{emoji_type}/{emoji_id}",
        )

    async def delete_reaction(self, channel_id: str, message_id: str,
                              emoji_type: str, emoji_id: str) -> Any:
        return await self._req(
            "DELETE",
            f"/channels/{channel_id}/messages/{message_id}/reactions/{emoji_type}/{emoji_id}",
        )

    async def list_reaction_users(self, channel_id: str, message_id: str,
                                  emoji_type: str, emoji_id: str,
                                  cookie: str = "", limit: int = 20) -> Any:
        params = {"limit": limit}
        if cookie:
            params["cookie"] = cookie
        return await self._req(
            "GET",
            f"/channels/{channel_id}/messages/{message_id}/reactions/{emoji_type}/{emoji_id}",
            params=params,
        )

    # -- forum ------------------------------------------------------------------------------------

    async def list_threads(self, channel_id: str) -> Any:
        return await self._req("GET", f"/channels/{channel_id}/threads")

    async def get_thread(self, channel_id: str, thread_id: str) -> Any:
        return await self._req("GET", f"/channels/{channel_id}/threads/{thread_id}")

    async def put_thread(self, channel_id: str, title: str, content: str,
                         format: int = 1) -> Any:
        return await self._req(
            "PUT", f"/channels/{channel_id}/threads",
            json_body={"title": title, "content": content, "format": format},
        )

    async def delete_thread(self, channel_id: str, thread_id: str) -> Any:
        return await self._req("DELETE", f"/channels/{channel_id}/threads/{thread_id}")

    # -- audio / live ---------------------------------------------------------------------------------

    async def audio_control(self, channel_id: str, audio_url: str = "",
                            text: str = "", status: int = 0) -> Any:
        """POST /channels/{id}/audio — status 0 off-stage/end, 1 play
        audio_url, 2 play text (TTS)."""
        body: dict = {"status": status}
        if audio_url:
            body["audio_url"] = audio_url
        if text:
            body["text"] = text
        return await self._req("POST", f"/channels/{channel_id}/audio", json_body=body)

    async def put_mic(self, channel_id: str) -> Any:
        return await self._req("PUT", f"/channels/{channel_id}/mic")

    async def delete_mic(self, channel_id: str) -> Any:
        return await self._req("DELETE", f"/channels/{channel_id}/mic")

    # -- message settings --------------------------------------------------------------------------------

    async def get_guild_message_setting(self, guild_id: str) -> Any:
        """GET /guilds/{id}/message_setting — guild message push settings."""
        return await self._req("GET", f"/guilds/{guild_id}/message_setting")

    async def get_guild_api_permission(self, guild_id: str) -> Any:
        """GET /guilds/{id}/api_permission — APIs the bot may use in
        this guild, with per-endpoint auth status."""
        return await self._req("GET", f"/guilds/{guild_id}/api_permission")

    async def demand_guild_api_permission(self, guild_id: str, path: str,
                                          method: str) -> Any:
        """POST /guilds/{id}/api_permission/demand — post an auth link
        into the guild asking the owner to grant one endpoint
        (rate-limited per guild per day)."""
        return await self._req(
            "POST", f"/guilds/{guild_id}/api_permission/demand",
            json_body={"path": path, "method": method},
        )

    # -- C2C custom menu & command panels -------------------------------------------------------------

    async def get_menu(self) -> Any:
        """GET /v2/menu — the global C2C custom menu (one per bot,
        shown at the bottom of every C2C chat window)."""
        return await self._req("GET", "/v2/menu")

    async def put_menu(self, menu: dict) -> Any:
        """PUT /v2/menu — replace the whole custom menu.  Body shape
        per docs: {"menu": [...]} with item types switch /
        send_message / link / menu; ≤10 top-level, ≤5 second-level."""
        return await self._req("PUT", "/v2/menu", json_body=menu)

    async def list_panels(self, scope: str, page: int = 1, page_size: int = 20) -> Any:
        """GET /v2/panels?scope=c2c|group|channel|dm — command panels."""
        return await self._req(
            "GET", "/v2/panels",
            params={"scope": scope, "page": page, "page_size": page_size},
        )

    async def create_panel(self, panel: dict) -> Any:
        """POST /v2/panels — create a command panel (body per docs;
        target_type all/specific, specific links ≤20 openids at once,
        channel/dm panels can only be all)."""
        return await self._req("POST", "/v2/panels", json_body=panel)

    async def get_panel(self, panel_id: str) -> Any:
        return await self._req("GET", f"/v2/panels/{panel_id}")

    async def put_panel(self, panel_id: str, panel: dict) -> Any:
        """PUT /v2/panels/{id} — replace panel content (scope and
        linked targets are not changed by this call)."""
        return await self._req("PUT", f"/v2/panels/{panel_id}", json_body=panel)

    async def delete_panel(self, panel_id: str) -> Any:
        return await self._req("DELETE", f"/v2/panels/{panel_id}")

    async def put_panel_target(self, panel_id: str, body: dict) -> Any:
        """PUT /v2/panels/{id}/target — add/remove a specific panel's
        linked users/groups (op=add/del).  NOTE: this path is attested
        by the docs' request schema but was not verified verbatim
        against the live wiki; expect a possible 404 until confirmed."""
        return await self._req("PUT", f"/v2/panels/{panel_id}/target", json_body=body)

    # -- chunked upload (large files) ---------------------------------------------------------------------

    async def upload_prepare_c2c(self, openid: str, body: dict) -> Any:
        """POST /v2/users/{openid}/files/upload_prepare — start a
        chunked upload; body carries file metadata per docs."""
        return await self._req(
            "POST", f"/v2/users/{openid}/files/upload_prepare", json_body=body
        )

    async def upload_part_finish_c2c(self, openid: str, body: dict) -> Any:
        """POST /v2/users/{openid}/files/upload_part_finish — finish
        one uploaded chunk; the final chunk yields the file_info."""
        return await self._req(
            "POST", f"/v2/users/{openid}/files/upload_part_finish", json_body=body
        )

    async def upload_prepare_group(self, group_openid: str, body: dict) -> Any:
        return await self._req(
            "POST", f"/v2/groups/{group_openid}/files/upload_prepare", json_body=body
        )

    async def upload_part_finish_group(self, group_openid: str, body: dict) -> Any:
        return await self._req(
            "POST", f"/v2/groups/{group_openid}/files/upload_part_finish", json_body=body
        )

    # -- C2C streaming messages ------------------------------------------------------------------------------

    async def stream_c2c_message(self, openid: str, body: dict) -> Any:
        """POST /v2/users/{openid}/messages with the streaming variant:
        the body carries a stream descriptor (input_state 1=generating
        / 10=done, an increasing index, content_type text|markdown);
        the first chunk returns the stream message id, later chunks
        reference it.  Body is passed through verbatim so callers
        follow the current docs exactly.  C2C only."""
        return await self._req("POST", f"/v2/users/{openid}/messages", json_body=body)

    # -- group management (permission-gated: whitelist / bot must be group admin) ----------------------------

    async def get_group_info(self, group_openid: str) -> Any:
        """GET /v2/groups/{gid}/info. Whitelist-gated (11253 without)."""
        return await self._req("GET", f"/v2/groups/{group_openid}/info")

    async def get_group_bot_state(self, group_openid: str) -> Any:
        """GET /v2/groups/{gid}/bot_state — the bot's own member_openid,
        role, proactive permission and receive setting in this group."""
        return await self._req("GET", f"/v2/groups/{group_openid}/bot_state")

    async def list_group_join_requests(self, group_openid: str, cursor: str = "",
                                       limit: int = 50) -> Any:
        """GET /v2/groups/{gid}/join_request_list. Bot must be a group
        admin."""
        params: dict = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        return await self._req(
            "GET", f"/v2/groups/{group_openid}/join_request_list", params=params
        )

    async def approve_group_join_request(self, group_openid: str,
                                         member_openid: str, body: dict) -> Any:
        """POST /v2/groups/{gid}/approval_join_request/{member_openid}
        — body: {"op": "approve"|"decline", optional join_request_id,
        decline reason, blacklist flag}."""
        return await self._req(
            "POST",
            f"/v2/groups/{group_openid}/approval_join_request/{member_openid}",
            json_body=body,
        )

    async def get_group_restrict_setting(self, group_openid: str) -> Any:
        """GET /v2/groups/{gid}/restrict_chat_setting — the global rule
        is read-only; the response also lists timed rules and muted
        members."""
        return await self._req(
            "GET", f"/v2/groups/{group_openid}/restrict_chat_setting"
        )

    async def set_group_member_mutes(self, group_openid: str, members: list) -> Any:
        """POST /v2/groups/{gid}/restrict_chat_setting — members:
        [{op: add|update|del, member_openid, mute_expire_at}], ≤20 per
        call, ≤30 days, regular members only.  There is NO endpoint
        for the all-member mute rule."""
        return await self._req(
            "POST", f"/v2/groups/{group_openid}/restrict_chat_setting",
            json_body={"members": members},
        )

    async def list_join_strategies(self) -> Any:
        """GET /v2/groups/join_approval_strategy — bot-level automatic
        join-approval strategies."""
        return await self._req("GET", "/v2/groups/join_approval_strategy")

    async def create_join_strategy(self, body: dict) -> Any:
        """POST /v2/groups/join_approval_strategy — body per docs
        (group_openids or group_ids ≤100, ≤20 strategies per bot)."""
        return await self._req(
            "POST", "/v2/groups/join_approval_strategy", json_body=body
        )

    async def modify_join_strategy(self, strategy_id: str, body: dict) -> Any:
        return await self._req(
            "PATCH", f"/v2/groups/join_approval_strategy/{strategy_id}",
            json_body=body,
        )

    async def delete_join_strategy(self, strategy_id: str) -> Any:
        return await self._req(
            "DELETE", f"/v2/groups/join_approval_strategy/{strategy_id}"
        )

    async def execute_join_strategy(self, strategy_id: str) -> Any:
        """POST .../execute — run the strategy now (async full scan)."""
        return await self._req(
            "POST", f"/v2/groups/join_approval_strategy/{strategy_id}/execute",
            json_body={},
        )

    async def set_join_strategy_whitelist(self, strategy_id: str, body: dict) -> Any:
        """POST .../whitelist_users — maintain the strategy's
        auto-approve user whitelist (≤10000 per call)."""
        return await self._req(
            "POST",
            f"/v2/groups/join_approval_strategy/{strategy_id}/whitelist_users",
            json_body=body,
        )

    async def list_group_members(self, group_openid: str, cursor: str = "",
                                 limit: int = 30) -> Any:
        """GET /v2/groups/{gid}/members — cursor-paged, ≤30 per page.
        Whitelist + invite-only."""
        params: dict = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        return await self._req(
            "GET", f"/v2/groups/{group_openid}/members", params=params
        )

    async def get_group_member(self, group_openid: str, member_openid: str) -> Any:
        return await self._req(
            "GET", f"/v2/groups/{group_openid}/members/{member_openid}"
        )

    async def batch_remove_group_members(self, group_openid: str, body: dict) -> Any:
        """POST /v2/groups/{gid}/batch_remove_members — ≤20 members.
        Whitelist + invite-only."""
        return await self._req(
            "POST", f"/v2/groups/{group_openid}/batch_remove_members",
            json_body=body,
        )

    async def list_group_blacklist(self, group_openid: str, limit: int = 100) -> Any:
        return await self._req(
            "GET", f"/v2/groups/{group_openid}/member_blacklist",
            params={"limit": limit},
        )

    async def update_group_blacklist(self, group_openid: str, body: dict) -> Any:
        """POST /v2/groups/{gid}/member_blacklist — body carries
        op=add/del and ≤20 member openids."""
        return await self._req(
            "POST", f"/v2/groups/{group_openid}/member_blacklist", json_body=body
        )
