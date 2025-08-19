# websocket_handler.py
# -*- coding: utf-8 -*-
"""Anova websocket handler with full raw JSON passthrough and cooking_stage extraction."""

from __future__ import annotations

import asyncio
import json
import logging
from asyncio import Future
from typing import Any, Optional

from aiohttp import ClientSession, ClientWebSocketResponse, WebSocketError

from . import WebsocketFailure
from .web_socket_containers import (
    AnovaCommand,
    APCWifiDevice,
    build_a3_payload,
    build_a6_a7_payload,
    build_wifi_cooker_state_body,
)

_LOGGER = logging.getLogger(__name__)


def _dig(d: dict, path: list[str], default: Any = None) -> Any:
    """Safely traverse nested dicts."""
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _attach_raw_fields(update: Any, message: dict[str, Any], device: APCWifiDevice) -> None:
    """
    Enrich the APCUpdate/APCUpdateSensor with:
      - the COMPLETE raw websocket JSON (no filtering, no mutation),
      - convenience fields used by the HA layer (timer, versions, etc.),
      - and the cooking_stage (cook.activeStageMode).

    This must never raise, so the WS pipeline keeps flowing.
    """
    try:
        # (A) ALWAYS pass through full raw JSON for later use/debugging
        setattr(update, "raw_message", message)
        setattr(device, "last_raw_message", message)

        # If we don't have a sensor container, stop here (but still keep raw_message!)
        sensor = getattr(update, "sensor", None)
        if sensor is None:
            return

        # (B) Convenience handles from the raw JSON (optional but handy)
        payload = message.get("payload", {}) if isinstance(message, dict) else {}
        state = payload.get("state", {}) or {}
        nodes = state.get("nodes", {}) or {}
        sysinfo = state.get("systemInfo", {}) or {}

        # Mode as delivered by the device (e.g., 'cook' / 'idle')
        state_state = state.get("state", {}) or {}
        setattr(sensor, "mode_raw", state_state.get("mode"))

        # Timer details (these are present in a6/a7; may be absent in other frames)
        t = nodes.get("timer", {}) or {}
        setattr(sensor, "timer_initial", t.get("initial"))
        setattr(sensor, "timer_mode", t.get("mode"))  # 'idle'|'running'|'paused'|'completed' (device-dependent)
        setattr(sensor, "timer_started_at", t.get("startedAtTimestamp"))

        # Low water diagnostics
        lw = nodes.get("lowWater", {}) or {}
        setattr(sensor, "low_water_warning", lw.get("warning"))
        setattr(sensor, "low_water_empty", lw.get("empty"))

        # Versions / online diagnostics
        setattr(sensor, "firmware_version", sysinfo.get("firmwareVersion"))
        setattr(sensor, "hardware_version", sysinfo.get("hardwareVersion"))
        setattr(sensor, "online", sysinfo.get("online"))

        # (C) COOKING STAGE (exact passthrough, no guessing)
        # Reads raw value as provided by device, e.g. 'entering', 'holding', ...
        cooking_stage = (
            nodes.get("cook", {}) or {}
        ).get("activeStageMode")
        setattr(sensor, "cooking_stage", cooking_stage)

    except Exception:
        _LOGGER.debug("Raw enrichment failed (non-fatal).", exc_info=True)


class AnovaWebsocketHandler:
    def __init__(self, firebase_jwt: str, jwt: str, session: ClientSession):
        self._firebase_jwt = firebase_jwt
        self.jwt = jwt
        self.session = session
        self.url = (
            "https://devices.anovaculinary.io/"
            f"?token={self._firebase_jwt}&supportedAccessories=APC&platform=android"
        )
        self.devices: dict[str, APCWifiDevice] = {}
        self.ws: ClientWebSocketResponse | None = None
        self._message_listener: Future[None] | None = None

    async def connect(self) -> None:
        try:
            self.ws = await self.session.ws_connect(self.url)
        except WebSocketError as ex:
            raise WebsocketFailure("Failed to connect to the websocket") from ex
        # prefer create_task over ensure_future in modern asyncio
        self._message_listener = asyncio.create_task(self.message_listener())

    async def disconnect(self) -> None:
        if self.ws is not None:
            await self.ws.close()
            self.ws = None
        if self._message_listener is not None:
            self._message_listener.cancel()
            self._message_listener = None

    def on_message(self, message: dict[str, Any]) -> None:
        _LOGGER.debug("WS message: %s", message)

        cmd = message.get("command")
        if cmd == AnovaCommand.EVENT_APC_WIFI_LIST:
            payload = message.get("payload", [])
            for device in payload:
                cooker_id = device.get("cookerId")
                if not cooker_id:
                    continue
                if cooker_id not in self.devices:
                    self.devices[cooker_id] = APCWifiDevice(
                        cooker_id=cooker_id,
                        type=device.get("type"),
                        paired_at=device.get("pairedAt"),
                        name=device.get("name"),
                    )
            return

        if cmd == AnovaCommand.EVENT_APC_STATE:
            cooker_id: Optional[str] = _dig(message, ["payload", "cookerId"])
            if not cooker_id:
                return

            device = self.devices.get(cooker_id)
            if device is None:
                # state for unknown device → ignore silently
                return

            # Determine payload/state and type
            payload_state = _dig(message, ["payload", "state"], {}) or {}
            update = None

            if "job" in payload_state:
                # legacy wifi-cooker-state body
                update = build_wifi_cooker_state_body(payload_state).to_apc_update()
            else:
                payload_type = _dig(message, ["payload", "type"])
                if payload_type == "a3":
                    update = build_a3_payload(payload_state)
                elif payload_type in {"a6", "a7"}:
                    update = build_a6_a7_payload(payload_state)
                else:
                    # unknown type → ignore
                    return

            # Attach FULL raw JSON + convenience + cooking_stage
            _attach_raw_fields(update, message, device)

            # Notify integration
            if (ul := device.update_listener) is not None:
                ul(update)

    async def message_listener(self) -> None:
        if self.ws is None:
            return
        async for msg in self.ws:
            # aiohttp WSMessage: msg.data is already text for text frames
            try:
                data = json.loads(msg.data)
            except Exception:
                _LOGGER.debug("Ignoring non-JSON WS frame: %r", msg.data)
                continue
            self.on_message(data)
