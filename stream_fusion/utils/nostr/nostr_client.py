"""
Nostr WebSocket client — fetch_events() async generator.

Connects to a Nostr relay, sends a REQ subscription, and yields raw
event dicts until EOSE (End Of Stored Events) or the connection closes.
"""

import json
import uuid
from typing import AsyncGenerator

try:
    import websockets
    from websockets.exceptions import WebSocketException
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "websockets is required: add 'websockets>=12.0' to your dependencies."
    ) from exc

from stream_fusion.logging_config import logger


async def fetch_events(
    relay_url: str,
    filter_dict: dict,
    timeout: int = 30,
) -> AsyncGenerator[dict, None]:
    """Connect to a Nostr relay, subscribe with filter_dict, yield raw event dicts.

    Yields each EVENT payload (the dict at message[2]) until EOSE or
    the relay closes the connection.

    Args:
        relay_url:   WebSocket URL, e.g. "wss://u2p.anhkagi.net"
        filter_dict: Nostr subscription filter, e.g.
                     {"kinds": [2003], "#l": ["u2p.pcat:2145"], "limit": 100}
        timeout:     Connection open timeout in seconds.
    """
    sub_id = uuid.uuid4().hex[:12]
    req_msg = json.dumps(["REQ", sub_id, filter_dict])

    try:
        async with websockets.connect(
            relay_url,
            open_timeout=timeout,
            ping_timeout=20,
            close_timeout=5,
        ) as ws:
            await ws.send(req_msg)
            logger.trace(f"[nostr] REQ sent sub_id={sub_id} filter={filter_dict}")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except (json.JSONDecodeError, ValueError):
                    continue

                if not isinstance(msg, list) or len(msg) < 2:
                    continue

                msg_type = msg[0]

                if msg_type == "EVENT" and len(msg) >= 3:
                    yield msg[2]

                elif msg_type == "EOSE":
                    logger.trace(f"[nostr] EOSE received sub_id={sub_id}")
                    break

                elif msg_type == "CLOSED":
                    reason = msg[1] if len(msg) > 1 else ""
                    logger.warning(f"[nostr] CLOSED by relay: {reason}")
                    break

                elif msg_type == "NOTICE":
                    notice = msg[1] if len(msg) > 1 else ""
                    logger.debug(f"[nostr] NOTICE: {notice}")

    except WebSocketException as exc:
        logger.warning(f"[nostr] WebSocket error ({relay_url}): {exc}")
    except OSError as exc:
        logger.warning(f"[nostr] Connection failed ({relay_url}): {exc}")
    except TimeoutError:
        logger.warning(f"[nostr] Connection timed out after {timeout}s ({relay_url})")
