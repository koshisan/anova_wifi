# websocket_handler.py
# -*- coding: utf-8 -*-
"""Anova websocket handler with full raw JSON passthrough, reconnect handling, and debug logging."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from asyncio import Future
from datetime import datetime, timezone
from typing import Any, Optional

from aiohttp import ClientSession, ClientWebSocketResponse, WSMsgType, WebSocketError

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
    # Class-level storage - persists across handler instances!
    _global_devices: dict[str, APCWifiDevice] = {}
    _global_update_callbacks: list = []  # Global callbacks for ALL updates
    
    # Reconnect settings
    RECONNECT_MIN_DELAY = 1
    RECONNECT_MAX_DELAY = 60
    
    def __init__(self, firebase_jwt: str, jwt: str, session: ClientSession):
        self._firebase_jwt = firebase_jwt
        self.jwt = jwt
        self.session = session
        self.url = (
            "https://devices.anovaculinary.io/"
            f"?token={self._firebase_jwt}&supportedAccessories=APC&platform=android"
        )
        self.devices = AnovaWebsocketHandler._global_devices
        self.ws: ClientWebSocketResponse | None = None
        self._message_listener: Future[None] | None = None
        self._should_reconnect = True
        self._reconnect_delay = self.RECONNECT_MIN_DELAY
        self._last_message_time: datetime | None = None
        self._connection_count = 0
        _LOGGER.info(
            "[ANOVA-WS] Handler created. Devices: %d, Global callbacks: %d", 
            len(self.devices), len(AnovaWebsocketHandler._global_update_callbacks)
        )
    
    @classmethod
    def add_global_callback(cls, callback):
        """Add a global callback that receives ALL updates (cooker_id, update, raw_message)."""
        if callback not in cls._global_update_callbacks:
            cls._global_update_callbacks.append(callback)
            _LOGGER.info("[ANOVA-WS] Added global callback: %s (total: %d)", callback, len(cls._global_update_callbacks))

    async def connect(self) -> None:
        """Initial connection - starts the message listener with reconnect loop."""
        self._should_reconnect = True
        await self._do_connect()
        self._message_listener = asyncio.create_task(self._message_loop_with_reconnect())

    async def _do_connect(self) -> None:
        """Establish websocket connection."""
        self._connection_count += 1
        _LOGGER.info("[ANOVA-WS] Connecting... (attempt #%d)", self._connection_count)
        try:
            self.ws = await self.session.ws_connect(self.url)
            self._last_message_time = datetime.now(timezone.utc)
            self._reconnect_delay = self.RECONNECT_MIN_DELAY  # Reset on success
            _LOGGER.info("[ANOVA-WS] ✓ Connected successfully!")
        except WebSocketError as ex:
            _LOGGER.error("[ANOVA-WS] ✗ Connection failed: %s", ex)
            raise WebsocketFailure("Failed to connect to the websocket") from ex

    async def disconnect(self) -> None:
        """Disconnect and stop reconnect loop."""
        _LOGGER.info("[ANOVA-WS] Disconnecting (reconnect disabled)...")
        self._should_reconnect = False
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception as ex:
                _LOGGER.debug("[ANOVA-WS] Error closing WS: %s", ex)
            self.ws = None
        if self._message_listener is not None:
            self._message_listener.cancel()
            try:
                await self._message_listener
            except asyncio.CancelledError:
                pass
            self._message_listener = None
        _LOGGER.info("[ANOVA-WS] Disconnected.")

    def on_message(self, message: dict[str, Any]) -> None:
        """Process incoming websocket message."""
        self._last_message_time = datetime.now(timezone.utc)
        
        cmd = message.get("command")
        _LOGGER.debug("[ANOVA-WS] << Message: cmd=%s", cmd)

        if cmd == AnovaCommand.EVENT_APC_WIFI_LIST:
            payload = message.get("payload", [])
            _LOGGER.info("[ANOVA-WS] DEVICE_LIST received: %d devices", len(payload))
            for device in payload:
                cooker_id = device.get("cookerId")
                if not cooker_id:
                    continue
                existing = self.devices.get(cooker_id)
                if existing is None:
                    self.devices[cooker_id] = APCWifiDevice(
                        cooker_id=cooker_id,
                        type=device.get("type"),
                        paired_at=device.get("pairedAt"),
                        name=device.get("name"),
                    )
                    _LOGGER.info("[ANOVA-WS] + New device: %s (type=%s, name=%s)", 
                                cooker_id, device.get("type"), device.get("name"))
                else:
                    _LOGGER.debug("[ANOVA-WS] Device %s already exists, listener: %s", 
                                 cooker_id, existing.update_listener)
            return

        if cmd == AnovaCommand.EVENT_APC_STATE:
            cooker_id: Optional[str] = _dig(message, ["payload", "cookerId"])
            if not cooker_id:
                _LOGGER.warning("[ANOVA-WS] STATE event without cookerId!")
                return

            # === FULL DEBUG LOGGING ===
            payload = message.get("payload", {})
            state = _dig(message, ["payload", "state"], {}) or {}
            state_state = _dig(state, ["state"], {}) or {}
            nodes = _dig(state, ["nodes"], {}) or {}
            timer = _dig(nodes, ["timer"], {}) or {}
            cook = _dig(state, ["cook"], {}) or {}
            
            _LOGGER.info(
                "[ANOVA-WS] ══════════════════════════════════════════════════════════"
            )
            _LOGGER.info(
                "[ANOVA-WS] STATE EVENT for %s (type=%s)", 
                cooker_id, payload.get("type")
            )
            _LOGGER.info(
                "[ANOVA-WS]   mode=%s | activeStageMode=%s",
                state_state.get("mode"),
                cook.get("activeStageMode")
            )
            _LOGGER.info(
                "[ANOVA-WS]   timer: mode=%s | initial=%s | startedAt=%s | RAW=%s",
                timer.get("mode"),
                timer.get("initial"),
                timer.get("startedAtTimestamp"),
                timer  # Log the full timer object to see all fields
            )
            _LOGGER.info(
                "[ANOVA-WS]   temps: water=%s | target=%s | heater=%s",
                _dig(nodes, ["temperatureBulkSensor", "current"]),
                _dig(state_state, ["temperatureSetpoint", "value"]),
                _dig(nodes, ["temperatureHeaterSensor", "current"])
            )
            _LOGGER.info(
                "[ANOVA-WS]   lowWater: warning=%s | empty=%s",
                _dig(nodes, ["lowWater", "warning"]),
                _dig(nodes, ["lowWater", "empty"])
            )
            _LOGGER.info(
                "[ANOVA-WS]   systemInfo: online=%s | fw=%s",
                _dig(state, ["systemInfo", "online"]),
                _dig(state, ["systemInfo", "firmwareVersion"])
            )
            _LOGGER.info(
                "[ANOVA-WS] ══════════════════════════════════════════════════════════"
            )
            # === END DEBUG ===

            device = self.devices.get(cooker_id)
            if device is None:
                _LOGGER.warning("[ANOVA-WS] STATE for unknown device %s!", cooker_id)
                return

            payload_state = _dig(message, ["payload", "state"], {}) or {}
            update = None

            if "job" in payload_state:
                update = build_wifi_cooker_state_body(payload_state).to_apc_update()
                _LOGGER.debug("[ANOVA-WS] Built update from 'job' payload")
            else:
                payload_type = _dig(message, ["payload", "type"])
                if payload_type == "a3":
                    update = build_a3_payload(payload_state)
                    _LOGGER.debug("[ANOVA-WS] Built update from 'a3' payload")
                elif payload_type in {"a6", "a7"}:
                    update = build_a6_a7_payload(payload_state)
                    _LOGGER.debug("[ANOVA-WS] Built update from '%s' payload", payload_type)
                else:
                    _LOGGER.warning("[ANOVA-WS] Unknown payload type: %s", payload_type)
                    return

            # Rohdaten + Cooking-Stage + Convenience an Update/Sensor hängen
            _attach_raw_fields(update, message, device)

            # Notify listener
            if (ul := device.update_listener) is not None:
                _LOGGER.debug("[ANOVA-WS] Calling update_listener for %s", cooker_id)
                ul(update)
            else:
                _LOGGER.warning("[ANOVA-WS] Device %s has NO update_listener!", cooker_id)

    async def _message_loop_with_reconnect(self) -> None:
        """Main message loop with automatic reconnection."""
        while self._should_reconnect:
            try:
                if self.ws is None or self.ws.closed:
                    _LOGGER.warning("[ANOVA-WS] WebSocket not connected, reconnecting...")
                    try:
                        await self._do_connect()
                    except WebsocketFailure:
                        _LOGGER.error(
                            "[ANOVA-WS] Reconnect failed, retry in %ds", 
                            self._reconnect_delay
                        )
                        await asyncio.sleep(self._reconnect_delay)
                        self._reconnect_delay = min(
                            self._reconnect_delay * 2, 
                            self.RECONNECT_MAX_DELAY
                        )
                        continue

                # Process messages
                _LOGGER.info("[ANOVA-WS] Entering message loop...")
                async for msg in self.ws:
                    if msg.type == WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            self.on_message(data)
                        except json.JSONDecodeError:
                            _LOGGER.debug("[ANOVA-WS] Ignoring non-JSON frame: %r", msg.data[:100])
                    elif msg.type == WSMsgType.CLOSED:
                        _LOGGER.warning("[ANOVA-WS] WebSocket CLOSED by server")
                        break
                    elif msg.type == WSMsgType.ERROR:
                        _LOGGER.error("[ANOVA-WS] WebSocket ERROR: %s", self.ws.exception())
                        break
                    else:
                        _LOGGER.debug("[ANOVA-WS] Other msg type: %s", msg.type)

                # Loop ended - connection lost
                _LOGGER.warning(
                    "[ANOVA-WS] Message loop ended. Last message: %s. Reconnecting in %ds...",
                    self._last_message_time.isoformat() if self._last_message_time else "never",
                    self._reconnect_delay
                )
                self.ws = None

            except asyncio.CancelledError:
                _LOGGER.info("[ANOVA-WS] Message loop cancelled")
                raise
            except Exception as ex:
                _LOGGER.exception("[ANOVA-WS] Unexpected error in message loop: %s", ex)
                self.ws = None

            if self._should_reconnect:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, 
                    self.RECONNECT_MAX_DELAY
                )

        _LOGGER.info("[ANOVA-WS] Message loop exiting (reconnect disabled)")

    async def message_listener(self) -> None:
        """Legacy method - now wraps _message_loop_with_reconnect for compatibility."""
        await self._message_loop_with_reconnect()

    async def send_command(self, command: dict[str, Any]) -> bool:
        """Send a command to the websocket."""
        if self.ws is None or self.ws.closed:
            _LOGGER.error("[ANOVA-WS] Cannot send command: WebSocket not connected")
            return False
        try:
            await self.ws.send_json(command)
            _LOGGER.info("[ANOVA-WS] >> Sent: %s", command.get("command"))
            return True
        except Exception as ex:
            _LOGGER.error("[ANOVA-WS] Failed to send command: %s", ex)
            return False

    async def start_cook(self, cooker_id: str, target_temperature: float, timer_seconds: int = 0) -> bool:
        """Start cooking on a sous vide device."""
        device = self.devices.get(cooker_id)
        if device is None:
            _LOGGER.error("[ANOVA-WS] Device %s not found", cooker_id)
            return False
        command = {
            "command": AnovaCommand.CMD_APC_START.value,
            "requestId": str(uuid.uuid4()),
            "payload": {
                "cookerId": cooker_id, 
                "type": device.type, 
                "targetTemperature": target_temperature, 
                "unit": "C", 
                "timer": timer_seconds
            },
        }
        _LOGGER.info("[ANOVA-WS] start_cook: %s @ %.1f°C, timer=%ds", cooker_id, target_temperature, timer_seconds)
        return await self.send_command(command)

    async def stop_cook(self, cooker_id: str) -> bool:
        """Stop cooking on a sous vide device."""
        device = self.devices.get(cooker_id)
        if device is None:
            _LOGGER.error("[ANOVA-WS] Device %s not found", cooker_id)
            return False
        request_id = str(uuid.uuid4())
        command = {
            "command": AnovaCommand.CMD_APC_STOP.value,
            "requestId": request_id,
            "payload": {"cookerId": cooker_id, "type": device.type},
        }
        _LOGGER.info(
            "[ANOVA-WS] stop_cook: cooker=%s type=%s reqId=%s command=%s", 
            cooker_id, device.type, request_id, command
        )
        result = await self.send_command(command)
        _LOGGER.info("[ANOVA-WS] stop_cook result: %s", result)
        return result

    async def set_target_temperature(self, cooker_id: str, target_temperature: float) -> bool:
        """Set target temperature on a sous vide device."""
        device = self.devices.get(cooker_id)
        if device is None:
            _LOGGER.error("[ANOVA-WS] Device %s not found", cooker_id)
            return False
        command = {
            "command": AnovaCommand.CMD_APC_SET_TARGET_TEMP.value,
            "requestId": str(uuid.uuid4()),
            "payload": {
                "cookerId": cooker_id, 
                "type": device.type, 
                "targetTemperature": target_temperature, 
                "unit": "C"
            },
        }
        _LOGGER.info("[ANOVA-WS] set_target_temperature: %s → %.1f°C", cooker_id, target_temperature)
        return await self.send_command(command)

    async def set_timer(self, cooker_id: str, timer_seconds: int) -> bool:
        """Set timer on a sous vide device."""
        device = self.devices.get(cooker_id)
        if device is None:
            _LOGGER.error("[ANOVA-WS] Device %s not found", cooker_id)
            return False
        command = {
            "command": AnovaCommand.CMD_APC_SET_TIMER.value,
            "requestId": str(uuid.uuid4()),
            "payload": {"cookerId": cooker_id, "type": device.type, "timer": timer_seconds},
        }
        _LOGGER.info("[ANOVA-WS] set_timer: %s → %ds", cooker_id, timer_seconds)
        return await self.send_command(command)
