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
    Enrich the APCUpdate/APCUpdateSensor with raw fields from the websocket message
    and store the last raw message on the device for external consumers.
    """
    try:
        # 0) komplettes JSON immer durchreichen
        setattr(update, "raw_message", message)
        setattr(device, "last_raw_message", message)

        sensor = getattr(update, "sensor", None)
        if sensor is None:
            return  # nichts zu enrichen

        # convenience-fields wie gehabt
        payload = message.get("payload", {}) if isinstance(message, dict) else {}
        state = payload.get("state", {}) or {}
        nodes = state.get("nodes", {}) or {}
        sysinfo = state.get("systemInfo", {}) or {}

        setattr(sensor, "mode_raw", state.get("state", {}).get("mode"))
        t = nodes.get("timer", {}) or {}
        setattr(sensor, "timer_initial", t.get("initial"))
        setattr(sensor, "timer_mode", t.get("mode"))
        setattr(sensor, "timer_started_at", t.get("startedAtTimestamp"))

        lw = nodes.get("lowWater", {}) or {}
        setattr(sensor, "low_water_warning", lw.get("warning"))
        setattr(sensor, "low_water_empty", lw.get("empty"))

        setattr(sensor, "firmware_version", sysinfo.get("firmwareVersion"))
        setattr(sensor, "hardware_version", sysinfo.get("hardwareVersion"))
        setattr(sensor, "online", sysinfo.get("online"))

    except Exception:
        _LOGGER.debug("Raw enrichment failed (non-fatal).", exc_info=True)

class AnovaWebsocketHandler:
    def __init__(self, firebase_jwt: str, jwt: str, session: ClientSession):
        self._firebase_jwt = firebase_jwt
        self.jwt = jwt
        self.session = session
        self.url = (
            f"https://devices.anovaculinary.io/"
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
        self._message_listener = asyncio.ensure_future(self.message_listener())

    async def disconnect(self) -> None:
        if self.ws is not None:
            await self.ws.close()
        if self._message_listener is not None:
            self._message_listener.cancel()

    def on_message(self, message: dict[str, Any]) -> None:
        _LOGGER.debug("Found message %s", message)

        if message["command"] == AnovaCommand.EVENT_APC_WIFI_LIST:
            payload = message["payload"]
            for device in payload:
                if device["cookerId"] not in self.devices:
                    self.devices[device["cookerId"]] = APCWifiDevice(
                        cooker_id=device["cookerId"],
                        type=device["type"],
                        paired_at=device["pairedAt"],
                        name=device["name"],
                    )

        elif message["command"] == AnovaCommand.EVENT_APC_STATE:
            cooker_id: Optional[str] = _dig(message, ["payload", "cookerId"])
            if not cooker_id:
                return

            device = self.devices.get(cooker_id)
            if device is None:
                # We received state for an unknown device; ignore silently.
                return

            update = None
            payload_state = _dig(message, ["payload", "state"], {}) or {}

            # Build the appropriate update object based on payload type/state
            if "job" in payload_state:
                update = build_wifi_cooker_state_body(payload_state).to_apc_update()
            else:
                payload_type = _dig(message, ["payload", "type"])
                if payload_type == "a3":
                    update = build_a3_payload(payload_state)
                elif payload_type in {"a6", "a7"}:
                    update = build_a6_a7_payload(payload_state)
                else:
                    # Unknown / unsupported type
                    return

            # <<< NEW: attach raw JSON fields to the update + remember last raw on device
            _attach_raw_fields(update, message, device)

            # Notify listener as before
            if (ul := device.update_listener) is not None:
                ul(update)

    async def message_listener(self) -> None:
        if self.ws is not None:
            async for msg in self.ws:
                self.on_message(json.loads(msg.data))
