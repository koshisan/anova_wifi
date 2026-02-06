# websocket_handler.py
# -*- coding: utf-8 -*-
"""Anova websocket handler with full raw JSON passthrough and cooking_stage extraction."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
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
    Never raises.
    """
    try:
        # (A) Vollständiges Raw-JSON immer anhängen
        setattr(update, "raw_message", message)
        setattr(device, "last_raw_message", message)

        sensor = getattr(update, "sensor", None)
        if sensor is None:
            return  # kein Sensorcontainer, aber raw_message bleibt erhalten

        # (B) Convenience-Felder
        payload = message.get("payload", {}) if isinstance(message, dict) else {}
        state = payload.get("state", {}) or {}
        nodes = state.get("nodes", {}) or {}
        sysinfo = state.get("systemInfo", {}) or {}

        # Zustand/Modus
        state_state = state.get("state", {}) or {}
        setattr(sensor, "mode_raw", state_state.get("mode"))

        # Timer
        t = nodes.get("timer", {}) or {}
        setattr(sensor, "timer_initial", t.get("initial"))
        setattr(sensor, "timer_mode", t.get("mode"))  # 'idle'|'running'|'paused'|'completed'
        setattr(sensor, "timer_started_at", t.get("startedAtTimestamp"))

        # Low water
        lw = nodes.get("lowWater", {}) or {}
        setattr(sensor, "low_water_warning", lw.get("warning"))
        setattr(sensor, "low_water_empty", lw.get("empty"))

        # Versionen / Online
        setattr(sensor, "firmware_version", sysinfo.get("firmwareVersion"))
        setattr(sensor, "hardware_version", sysinfo.get("hardwareVersion"))
        setattr(sensor, "online", sysinfo.get("online"))

        # (C) Cooking Stage roh durchreichen:
        # WICHTIG: cook ist unter state, NICHT unter nodes.
        cook = state.get("cook", {}) or {}
        cooking_stage = cook.get("activeStageMode")
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
                return

            payload_state = _dig(message, ["payload", "state"], {}) or {}
            update = None

            if "job" in payload_state:
                update = build_wifi_cooker_state_body(payload_state).to_apc_update()
            else:
                payload_type = _dig(message, ["payload", "type"])
                if payload_type == "a3":
                    update = build_a3_payload(payload_state)
                elif payload_type in {"a6", "a7"}:
                    update = build_a6_a7_payload(payload_state)
                else:
                    return

            # Rohdaten + Cooking-Stage + Convenience an Update/Sensor hängen
            _attach_raw_fields(update, message, device)

            if (ul := device.update_listener) is not None:
                ul(update)

    async def message_listener(self) -> None:
        if self.ws is None:
            return
        async for msg in self.ws:
            try:
                data = json.loads(msg.data)
            except Exception:
                _LOGGER.debug("Ignoring non-JSON WS frame: %r", msg.data)
                continue
            self.on_message(data)

    async def send_command(self, command: dict[str, Any]) -> bool:
        """Send a command to the websocket."""
        if self.ws is None or self.ws.closed:
            _LOGGER.error("Cannot send command: WebSocket not connected")
            return False
        try:
            await self.ws.send_json(command)
            _LOGGER.debug("Sent command: %s", command.get("command"))
            return True
        except Exception as ex:
            _LOGGER.error("Failed to send command: %s", ex)
            return False

    async def start_cook(self, cooker_id: str, target_temperature: float, timer_seconds: int = 0) -> bool:
        """Start cooking on a sous vide device."""
        device = self.devices.get(cooker_id)
        if device is None:
            _LOGGER.error("Device %s not found", cooker_id)
            return False
        command = {
            "command": AnovaCommand.CMD_APC_START.value,
            "requestId": str(uuid.uuid4()),
            "payload": {"cookerId": cooker_id, "type": device.type, "targetTemperature": target_temperature, "unit": "C", "timer": timer_seconds},
        }
        return await self.send_command(command)

    async def stop_cook(self, cooker_id: str) -> bool:
        """Stop cooking on a sous vide device."""
        device = self.devices.get(cooker_id)
        if device is None:
            _LOGGER.error("Device %s not found", cooker_id)
            return False
        command = {
            "command": AnovaCommand.CMD_APC_STOP.value,
            "requestId": str(uuid.uuid4()),
            "payload": {"cookerId": cooker_id, "type": device.type},
        }
        return await self.send_command(command)

    async def set_target_temperature(self, cooker_id: str, target_temperature: float) -> bool:
        """Set target temperature on a sous vide device."""
        device = self.devices.get(cooker_id)
        if device is None:
            _LOGGER.error("Device %s not found", cooker_id)
            return False
        command = {
            "command": AnovaCommand.CMD_APC_SET_TARGET_TEMP.value,
            "requestId": str(uuid.uuid4()),
            "payload": {"cookerId": cooker_id, "type": device.type, "targetTemperature": target_temperature, "unit": "C"},
        }
        return await self.send_command(command)

    async def set_timer(self, cooker_id: str, timer_seconds: int) -> bool:
        """Set timer on a sous vide device."""
        device = self.devices.get(cooker_id)
        if device is None:
            _LOGGER.error("Device %s not found", cooker_id)
            return False
        command = {
            "command": AnovaCommand.CMD_APC_SET_TIMER.value,
            "requestId": str(uuid.uuid4()),
            "payload": {"cookerId": cooker_id, "type": device.type, "timer": timer_seconds},
        }
        return await self.send_command(command)
