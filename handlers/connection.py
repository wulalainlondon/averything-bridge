"""
WebSocket connection handler — the per-connection entry point.

Extracted verbatim from bridge_v2.py (zero behaviour change). All bridge_v2
module state and helpers are accessed through `bv.` so they resolve to the live
values that main()/_init_paths() assign at runtime; bridge_v2 re-exports
`handler` from here (bottom-of-file import) so `bridge_v2.handler` is unchanged.

`import bridge_v2` is deferred into the handler body (not module top-level):
when bridge_v2 runs as __main__ (`python bridge_v2.py`), a top-level import here
would re-import bridge_v2 under its real name and hit a partially-initialized
circular import. Deferring to call time avoids the cycle entirely.
"""

import inspect

from websockets.asyncio.server import ServerConnection


async def _dispatch_bridge_command(
    *,
    bv,
    ws,
    client,
    command,
    system_ctx,
    runtime_ctx,
    file_ctx,
    router_ctx,
    op_started: float,
    handler_func,
) -> None:
    msg = command.payload
    mtype = command.type
    runtime_ctx["client"] = client

    if mtype == "hello":
        if isinstance(msg.get("device_id"), str) and msg.get("device_id", "").strip():
            client.device_id = msg["device_id"].strip()
            bv.client_manager.mark_latest(ws, client.device_id)
            await bv.client_manager.close_duplicate_device_clients(ws, client.device_id)
        if isinstance(msg.get("device_name"), str) and msg.get("device_name", "").strip():
            client.device_name = msg["device_name"].strip()
        _paired_token = bv._PAIRING.get("paired_token", "").strip()
        _provided_token = str(msg.get("auth_token") or "").strip()
        await bv.client_manager.send_json(ws, {
            "type": "hello_ack",
            "instance_id": bv._INSTANCE_ID,
            "gen": bv.get_generation(),
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
            "is_locked": bool(_paired_token),
            "locked_to_me": bool(_paired_token) and _paired_token == _provided_token,
            "instance_name": bv._INSTANCE_NAME,
            "root_dir": bv._ROOT_DIR,
            "data_dir": bv._DATA_DIR,
            **({"lan_ip": bv._LAN_IP} if bv._LAN_IP else {}),
            **({"tunnel_url": bv.get_current_tunnel_url()} if bv.get_current_tunnel_url() else {}),
        }, client)
        await bv.send_pending_interactions(ws)
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return

    if mtype == "claim_bridge":
        token = str(msg.get("auth_token") or "").strip()
        device_id = str(msg.get("device_id") or "").strip()
        if not token:
            await bv.client_manager.send_json(ws, bv._msg_error("auth_token required for claim_bridge"), client)
            bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
            return
        existing = bv._PAIRING.get("paired_token", "").strip()
        if existing and existing != token:
            await bv.client_manager.send_json(ws, bv._msg_error("Bridge already claimed by another device"), client)
            bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
            return
        # Synchronous block: asyncio single-thread guarantees atomicity.
        _new_pairing = {"paired_token": token, "paired_device_id": device_id, "paired_at": int(bv.time.time())}
        bv._PAIRING.clear()
        bv._PAIRING.update(_new_pairing)
        bv._save_pairing(bv._PAIRING)
        bv.log.info("Bridge claimed by device_id=%s", device_id)
        await bv.client_manager.send_json(ws, {"type": "claim_ack", "is_locked": True, "locked_to_me": True}, client)
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return

    if mtype == "unclaim_bridge":
        token = str(msg.get("auth_token") or "").strip()
        paired = bv._PAIRING.get("paired_token", "").strip()
        if paired and paired != token:
            await bv.client_manager.send_json(ws, bv._msg_error("Unauthorized: token mismatch"), client)
            bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
            return
        bv._PAIRING.clear()
        bv._clear_pairing()
        bv.log.info("Bridge unclaimed")
        await bv.client_manager.send_json(ws, {"type": "unclaim_ack", "is_locked": False}, client)
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return

    if mtype == "get_inbox":
        _inbox_conn = bv.client_manager.CLIENTS.get(ws)
        inbox_device_id = (_inbox_conn.device_id if _inbox_conn else "") or ""
        inbox_items = bv.pending_file_push_items(inbox_device_id, include_pushed_at=True)
        await bv.client_manager.send_json(ws, {"type": "inbox_list", "items": inbox_items}, client)
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return

    if mtype == "tunnel_url_ack":
        bv.mark_tunnel_url_delivered()
        bv.log.info("tunnel_url_ack received — FCM retry cancelled")
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return

    if await bv.handle_interaction_message(
        mtype=mtype,
        msg=msg,
        ws=ws,
        sessions=bv._SESSIONS,
        session_backend=bv._session_backend,
        broadcast_json=bv._broadcast_json,
        msg_error=bv._msg_error,
    ):
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return

    if await bv._dispatch_ws_message(ws, msg):
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return

    if await bv.handle_system_msg(mtype, msg, ws, system_ctx):
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return
    if await bv.handle_runtime_msg(mtype, msg, ws, runtime_ctx):
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return
    if await bv.handle_file_msg(mtype, msg, ws, file_ctx):
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return

    # WebRTC signaling: once the DataChannel opens we re-enter handler_func()
    # on the WebRTCChannel adapter so this same dispatch stack runs over P2P.
    if mtype in bv.WEBRTC_MESSAGE_TYPES:
        async def _on_channel_ready(adapter):
            try:
                await handler_func(adapter)
            except Exception:
                bv.log.exception("[webrtc] handler raised on adapter")
        if await bv.handle_webrtc_message(mtype, msg, ws, _on_channel_ready):
            bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
            return

    if await bv.handle_low_coupling_message(
        mtype=mtype,
        msg=msg,
        ws=ws,
        client=client,
        ctx=router_ctx,
    ):
        bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)
        return

    bv.log.debug("No direct handler matched for type=%s", mtype)
    bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)


async def handler(ws: ServerConnection) -> None:
    import bridge_v2 as bv

    # Liveness probe short-circuit.  The supervisor's bridge_healthcheck.py
    # opens a WS every 3s, sends a control PING, then closes.  Without this
    # gate the handler would (a) register the probe as a client, (b) reassign
    # session.ws_ref on every existing session to this dying socket — causing
    # the next broadcast event to be sent to a closed socket and dropped from
    # the real app — and (c) serialize+send a 29KB sessions_list and replay
    # offline buffers into a socket that closes 12ms later.  Probes only need
    # the TCP/WS handshake to succeed and their control PING to be ponged
    # (websockets library handles control PING automatically); we just need to
    # keep the connection open until the probe closes it.
    try:
        ua = ws.request.headers.get("User-Agent", "") if ws.request else ""
    except Exception:
        ua = ""
    if ua.startswith("bridge-healthcheck/"):
        try:
            async for _ in ws:
                pass  # discard any frames; probe normally sends none
        except Exception:
            pass
        return

    # Cancel pending auto-tunnel — client is back
    if bv._AUTO_TUNNEL_TASK and not bv._AUTO_TUNNEL_TASK.done():
        bv._AUTO_TUNNEL_TASK.cancel()
        bv._AUTO_TUNNEL_TASK = None

    # Proactively start tunnel so client gets the URL while still on WiFi,
    # avoiding FCM dependency on the next reconnect.
    # Skip when external tunnel mode is active (_TUNNEL_URL_FILE set): in that
    # case cloudflared_launcher.sh (managed by launchd) owns the tunnel lifecycle;
    # starting an in-process tunnel here would create a second trycloudflare URL.
    if (
        bv.os.environ.get("BRIDGE_AUTO_TUNNEL") == "1"
        and not bv._TUNNEL_URL_FILE
        and not bv._is_cloudflared_running()
    ):
        bv._spawn_task("cloudflared-start:on-connect", bv._start_cloudflared_tunnel(bv._BRIDGE_PORT))

    remote = ws.remote_address
    client = bv.ClientConn(
        client_id=f"c_{bv.uuid.uuid4().hex[:8]}",
        device_id=f"device_{bv.uuid.uuid4().hex[:8]}",
        device_name="Unknown device",
        ws=ws,
        connected_at=bv.time.time(),
        last_seen=bv.time.time(),
    )
    try:
        raw_first = await bv.asyncio.wait_for(ws.recv(), timeout=20)
        first_msg = bv.json.loads(str(raw_first))
    except bv.asyncio.TimeoutError:
        await bv.client_manager.send_json(ws, bv._msg_error("Handshake timeout: expected hello"))
        bv.client_manager.remove(ws)
        return
    except Exception:
        await bv.client_manager.send_json(ws, bv._msg_error("Handshake failed: invalid JSON"))
        bv.client_manager.remove(ws)
        return

    first_err = bv.validate_client_msg(first_msg)
    if first_err or first_msg.get("type") != "hello":
        await bv.client_manager.send_json(ws, bv._msg_error("Protocol error: first message must be hello"))
        bv.client_manager.remove(ws)
        return
    if not bv._is_auth_token_valid(first_msg):
        await bv.client_manager.send_json(ws, bv._msg_error("Unauthorized: invalid auth token"))
        bv.client_manager.remove(ws)
        return

    if isinstance(first_msg.get("device_id"), str) and first_msg.get("device_id", "").strip():
        client.device_id = first_msg["device_id"].strip()
    if isinstance(first_msg.get("device_name"), str) and first_msg.get("device_name", "").strip():
        client.device_name = first_msg["device_name"].strip()

    bv.client_manager.register(ws, client)
    await bv.client_manager.close_duplicate_device_clients(ws, client.device_id)
    bv.log.info("Client connected: %s (%s) device=%s", remote, client.client_id, client.device_id)
    bv._mark_client_activity()

    # Inject this ws into all existing sessions (reconnect scenario).
    # ws_ref must be set before hello_ack/sessions_list so live events dispatched
    # during the handshake go to this client instead of the offline buffer.
    for session in list(bv._SESSIONS.values()):
        session.ws_ref = ws

    try:
        _paired_token = bv._PAIRING.get("paired_token", "").strip()
        _provided_token = str(first_msg.get("auth_token") or "").strip()
        tunnel_url = bv.get_current_tunnel_url()
        bv.log.info("hello_ack → client=%s instance_id=%s tunnel=%s",
                 client.client_id, bv._INSTANCE_ID, bool(tunnel_url))
        ok = await bv.client_manager.send_json(ws, {
            "type": "hello_ack",
            "instance_id": bv._INSTANCE_ID,
            "gen": bv.get_generation(),
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
            "is_locked": bool(_paired_token),
            "locked_to_me": bool(_paired_token) and _paired_token == _provided_token,
            "instance_name": bv._INSTANCE_NAME,
            "root_dir": bv._ROOT_DIR,
            "data_dir": bv._DATA_DIR,
            **({"lan_ip": bv._LAN_IP} if bv._LAN_IP else {}),
            **({"tunnel_url": tunnel_url} if tunnel_url else {}),
        }, client)
        if not ok:
            return

        async def _hydrate_reconnected_client() -> None:
            if not bv.client_manager.is_current(ws, client):
                return
            await bv.client_manager.send_json(ws, bv.build_sessions_list(), client)

            # Replay offline buffers AFTER sessions_list so the frontend has
            # hydrated its session state before it processes buffered events.
            await bv.replay_offline_buffers(ws, bv._SESSIONS.values())
            if not bv.client_manager.is_current(ws, client):
                return
            await bv._send_unread_snapshot_deferred(ws, client)
            device_id = client.device_id or ""
            for item in bv.pending_file_push_items(device_id):
                if not bv.client_manager.is_current(ws, client):
                    return
                payload = {"type": "file_push", **item}
                ok = await bv.client_manager.send_json(ws, payload, client)
                if not ok:
                    return

        try:
            spawn_sig = inspect.signature(bv._spawn_task)
            supports_owner = "owner" in spawn_sig.parameters
        except Exception:
            supports_owner = True
        if supports_owner:
            bv._spawn_task(
                f"client-hydrate:connect:{client.client_id}",
                _hydrate_reconnected_client(),
                owner=client.client_id,
            )
        else:
            bv._spawn_task(f"client-hydrate:connect:{client.client_id}", _hydrate_reconnected_client())
    except Exception:
        pass

    try:
        system_ctx = {
            "asyncio": bv.asyncio,
            "client": client,
            "is_current_client": lambda: bv.client_manager.is_current(ws, client),
            "sessions": bv._SESSIONS,
            "backends": bv._BACKENDS,
            "session_backend": bv._session_backend,
            "msg_resumable_sessions": bv._msg_resumable_sessions,
            "permission_mode": bv._PERMISSION_MANAGER.mode() if bv._PERMISSION_MANAGER else "off",
            "restart_trigger_path": bv._RESTART_TRIGGER_PATH,
            "msg_agent_tree": bv._msg_agent_tree,
        }
        runtime_ctx = {
            "sessions": bv._SESSIONS,
            "shell_sessions": bv._SHELL_SESSIONS,
            "max_shells": bv.MAX_SHELLS,
            "session_backend": bv._session_backend,
            "shell_cls": bv.ShellSession,
            "root_dir": bv._ROOT_DIR,
            "shell_reader": bv._shell_reader,
            "msg_error": bv._msg_error,
            "msg_shell_created": bv._msg_shell_created,
            "msg_tasks_list": bv._msg_tasks_list,
            "msg_task_killed": bv._msg_task_killed,
            "msg_processes_list": bv._msg_processes_list,
            "msg_process_killed": bv._msg_process_killed,
            "permission_manager": bv._PERMISSION_MANAGER,
            "client": client,
        }
        file_ctx = {
            "sessions": bv._SESSIONS,
            "client": client,
            "is_current_client": lambda: bv.client_manager.is_current(ws, client),
            "backends": bv._BACKENDS,
            "msg_dir_listing": bv._msg_dir_listing,
            "fcm_token_file": bv.FCM_TOKEN_FILE,
            "log": bv.log,
            "root_dir": bv._ROOT_DIR,
            "get_tunnel_url": bv.get_current_tunnel_url,
            "is_tunnel_delivered": bv.is_tunnel_url_delivered,
            "notify_tunnel_fcm_once": bv._do_send_tunnel_fcm,
        }
        def _spawn_client_task(name, coro, **kwargs):
            kwargs.setdefault("owner", client.client_id)
            return bv._spawn_task(name, coro, **kwargs)

        router_ctx = bv.RouterContext(
            sessions=bv._SESSIONS,
            build_sessions_list=bv.build_sessions_list,
            broadcast_json=bv._broadcast_json,
            persist_session_meta=bv._persist_session_meta,
            send_all_sessions=bv._send_all_sessions,
            spawn_task=_spawn_client_task,
            handle_push_file=bv._handle_push_file,
            handle_file_push_ack=bv._handle_file_push_ack,
            msg_pong=bv._msg_pong,
            msg_session_history=bv._msg_session_history,
            send_unread_snapshot=bv._send_unread_snapshot,
            send_unread_for_client_session=bv._send_unread_for_client_session,
            mark_read=bv._mark_read,
            persist_read_cursors=bv._persist_read_cursors,
            send_session_history_response=bv._send_session_history_response,
            history_runtime_payload=bv._history_runtime_payload,
            emit_resume_progress=bv._emit_resume_progress,
            close_duplicate_device_clients=bv.client_manager.close_duplicate_device_clients,
            log_warning=bv.log.warning,
            log_debug=bv.log.debug,
            sessions_lock=bv._SESSIONS_LOCK,
            max_sessions=bv.MAX_SESSIONS,
            default_cwd=bv.DEFAULT_CWD,
            normalize_backend_name=bv._normalize_backend_name,
            session_cls=bv.Session,
            queued_command_cls=bv.QueuedCommand,
            msg_session_created=bv._msg_session_created,
            msg_error=bv._msg_error,
            msg_session_renamed=bv._msg_session_renamed,
            session_backend=bv._session_backend,
            send_event=bv.send_event,
            stop_session_drain=bv.stop_session_drain,
            evt_session_warning=bv._evt_session_warning,
            evt_error=bv._evt_error,
            persist_session=bv._persist_session,
            read_cursors=bv._READ_CURSORS,
            remove_saved_session=lambda sid: bv.session_registry.remove_saved_session(
                sid,
                saved_sessions_file=bv.SAVED_SESSIONS_FILE,
                log_warning=bv.log.warning,
            ),
            invalidate_sessions_cache=bv.invalidate_sessions_cache,
            preload_sessions_cache=bv.preload_sessions_cache,
            backends=bv._BACKENDS,
            load_session_history_for_transfer=bv._load_session_history_for_transfer,
            build_handoff_prompt=bv._build_handoff_prompt,
            run_session_queue=bv._run_session_queue,
            search_enabled=bv._search_enabled,
            get_search_worker=bv.get_worker,
            strip_turn_aborted_notice=bv._strip_turn_aborted_notice,
            log_prompt_lifecycle=bv._log_prompt_lifecycle,
            root_dir=bv._ROOT_DIR,
            data_dir=bv._DATA_DIR,
            instance_name=bv._INSTANCE_NAME,
            pairing=bv._PAIRING,
        )
        async for raw in ws:
            op_started = bv.time.perf_counter()
            bv._mark_client_activity()
            raw_text = str(raw)
            raw_len = len(raw_text.encode("utf-8", errors="ignore"))

            command, validation_err = bv.parse_client_command(raw_text, raw_len=raw_len)
            if command is None:
                if validation_err == "invalid JSON":
                    bv.log.warning("Non-JSON from client: bytes=%d", raw_len)
                    continue
                bv.log.warning("Invalid client msg: %s | bytes=%d", validation_err, raw_len)
                await bv.client_manager.send_json(ws, bv._msg_error(f"Protocol error: {validation_err}"), client)
                continue

            msg = command.payload
            bv.log.debug("Received: %s", bv._summarize_client_msg(msg, raw_len))
            await _dispatch_bridge_command(
                bv=bv,
                ws=ws,
                client=client,
                command=command,
                system_ctx=system_ctx,
                runtime_ctx=runtime_ctx,
                file_ctx=file_ctx,
                router_ctx=router_ctx,
                op_started=op_started,
                handler_func=handler,
            )

    except Exception as exc:
        name = type(exc).__name__
        if "ConnectionClosed" in name:
            bv.log.info("Client disconnected: %s (%s)", remote, exc)
        else:
            bv.log.exception("Unhandled error in handler: %s", exc)
    finally:
        bv._cancel_client_tasks(client.client_id)
        bv.client_manager.remove(ws)
        for session in list(bv._SESSIONS.values()):
            if session.ws_ref is ws:
                session.ws_ref = None
        for shell in list(bv._SHELL_SESSIONS.values()):
            if shell.ws_ref is ws:
                shell.ws_ref = None
        # Tear down any pending WebRTC PC anchored on this signaling socket
        # (the DC adapter, if promoted, has its own lifecycle).
        bv._webrtc_cleanup_for_ws(ws)
        bv.log.info("Client gone: %s (%s)", remote, client.client_id)

        if (
            bv.os.environ.get("BRIDGE_AUTO_TUNNEL") == "1"
            and not bv._TUNNEL_URL_FILE
            and not bv.client_manager.has_clients()
            and not bv._is_cloudflared_running()
        ):
            bv._AUTO_TUNNEL_TASK = bv._spawn_task("auto-tunnel-delayed", bv._auto_tunnel_after_delay(120))
