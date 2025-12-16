# decoder.py
"""
Dhan v2 binary packet decoder (FULL / TICKER / QUOTE / DEPTH / OI / PREVCLOSE / STATUS)
Produces Python dicts with human-readable fields.
Author: generated for your microservice
"""

import struct
from datetime import datetime
from typing import Dict, Any, List, Tuple


# Common helpers
def _utc_from_epoch(ts: int) -> str:
    try:
        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


# Depth single-level format: <IIHHff> (20 bytes)
_DEPTH_STRUCT = struct.Struct("<IIHHff")


def _parse_depth(depth_bytes: bytes) -> List[Dict[str, Any]]:
    levels = []
    # depth_bytes should be 100 bytes (5 levels * 20 bytes)
    for i in range(0, len(depth_bytes), _DEPTH_STRUCT.size):
        chunk = depth_bytes[i:i + _DEPTH_STRUCT.size]
        if len(chunk) != _DEPTH_STRUCT.size:
            break
        bid_qty, ask_qty, bid_orders, ask_orders, bid_price, ask_price = _DEPTH_STRUCT.unpack(chunk)
        # If all zero, skip optionally or include
        levels.append({
            "bid_quantity": int(bid_qty),
            "ask_quantity": int(ask_qty),
            "bid_orders": int(bid_orders),
            "ask_orders": int(ask_orders),
            "bid_price": float(round(bid_price, 6)),
            "ask_price": float(round(ask_price, 6)),
        })
    return levels


# FULL packet struct (confirmed 162 bytes)
# Format discovered/validated: <B H B I f H I f I I I I I I f f f f 100s>
# Corresponds to: packet_type, packet_len, exch_seg, security_id, ltp, ltq, ltt,
#                 avg_price, volume, total_sell_qty, total_buy_qty, oi, oi_day_high, oi_day_low,
#                 open, close, high, low, depth_bytes(100)
_FULL_STRUCT = struct.Struct("<B H B I f H I f I I I I I I f f f f 100s")


# TICKER packet struct (16 bytes)
_TICKER_STRUCT = struct.Struct("<B H B I f I")  # packet_type,len,exch,secid,ltp,ltt

# PREV CLOSE/OI packet struct (12 bytes) example for OI
_OI_STRUCT = struct.Struct("<B H B I I")  # type,len,exch,secid,oi

# QUOTE packet struct (50 bytes) -- fallback; decode fields carefully
_QUOTE_STRUCT = struct.Struct("<B H B I f H I f I I I f f f f f")  # conservative match


def parse_packet(data: bytes) -> Dict[str, Any]:
    """
    Detect packet type and parse accordingly. Return a dict with 'type' and fields.
    """
    if not data or len(data) < 1:
        return {"error": "empty_packet"}

    first_byte = data[0]
    # first_byte mapping derived from v2 evidence:
    # 2 = ticker, 3 = depth, 4 = quote, 5 = oi, 6 = prev_close, 7 = status, 8 = full
    if first_byte == 2:
        return parse_ticker(data)
    if first_byte == 3:
        return parse_market_depth(data)
    if first_byte == 4:
        return parse_quote(data)
    if first_byte == 5:
        return parse_oi(data)
    if first_byte == 6:
        return parse_prev_close(data)
    if first_byte == 7:
        return parse_status(data)
    if first_byte == 8:
        return parse_full(data)
    if first_byte == 50:
        return parse_server_disconnect(data)

    return {"error": f"unknown_packet_type_{first_byte}", "raw_len": len(data)}


def parse_full(data: bytes) -> Dict[str, Any]:
    """
    Parse a FULL packet (expected 162 bytes).
    Format: _FULL_STRUCT
    """
    if len(data) < _FULL_STRUCT.size:
        return {"error": "truncated_full_packet", "length": len(data)}

    unpacked = _FULL_STRUCT.unpack(data[:_FULL_STRUCT.size])
    # Unpack mapping consistent with earlier analysis:
    # indexes:
    # 0: packet_type
    # 1: packet_length
    # 2: exchange_segment
    # 3: security_id
    # 4: ltp (float)
    # 5: ltq (ushort)
    # 6: ltt (uint epoch)
    # 7: avg_price (float)
    # 8..13: six uints => volume, total_sell_qty, total_buy_qty, oi, oi_day_high, oi_day_low
    # 14..17: open, close, high, low (floats)
    # 18: depth_bytes (100s)

    packet_type = unpacked[0]
    packet_len = unpacked[1]
    exchange_segment = unpacked[2]
    security_id = unpacked[3]
    ltp = float(unpacked[4])
    ltq = int(unpacked[5])
    ltt = int(unpacked[6])
    avg_price = float(unpacked[7])
    volume = int(unpacked[8])
    total_sell_quantity = int(unpacked[9])
    total_buy_quantity = int(unpacked[10])
    oi = int(unpacked[11])
    oi_day_high = int(unpacked[12])
    oi_day_low = int(unpacked[13])
    open_p = float(unpacked[14])
    close_p = float(unpacked[15])
    high_p = float(unpacked[16])
    low_p = float(unpacked[17])
    depth_bytes = unpacked[18]

    depth = _parse_depth(depth_bytes)

    return {
        "type": "FULL",
        "packet_type": int(packet_type),
        "packet_length": int(packet_len),
        "exchange_segment": int(exchange_segment),
        "security_id": int(security_id),
        "ltp": ltp,
        "ltq": ltq,
        "ltt_epoch": ltt,
        "ltt_utc": _utc_from_epoch(ltt),
        "avg_price": avg_price,
        "volume": volume,
        "total_sell_quantity": total_sell_quantity,
        "total_buy_quantity": total_buy_quantity,
        "oi": oi,
        "oi_day_high": oi_day_high,
        "oi_day_low": oi_day_low,
        "open": open_p,
        "close": close_p,
        "high": high_p,
        "low": low_p,
        "depth": depth,
    }


def parse_ticker(data: bytes) -> Dict[str, Any]:
    if len(data) < _TICKER_STRUCT.size:
        return {"error": "truncated_ticker", "length": len(data)}
    unpacked = _TICKER_STRUCT.unpack(data[:_TICKER_STRUCT.size])
    return {
        "type": "TICKER",
        "exchange_segment": int(unpacked[2]),
        "security_id": int(unpacked[3]),
        "ltp": float(unpacked[4]),
        "ltt_epoch": int(unpacked[5]),
        "ltt_utc": _utc_from_epoch(int(unpacked[5])),
    }


def parse_market_depth(data: bytes) -> Dict[str, Any]:
    # Market depth in v2 might vary; try to use a defensive approach.
    # Attempt to read header then a 100-byte depth block similar to full packet
    try:
        # First 8-16 bytes include header & partial fields; find depth region heuristically
        # We'll reuse full parser behavior where possible:
        return parse_full(data)  # full parser handles depth extraction properly
    except Exception:
        return {"error": "depth_parse_failed"}


def parse_quote(data: bytes) -> Dict[str, Any]:
    if len(data) < _QUOTE_STRUCT.size:
        # fallback try: if it's smaller, try ticker
        return parse_ticker(data)
    unpacked = _QUOTE_STRUCT.unpack(data[:_QUOTE_STRUCT.size])
    return {
        "type": "QUOTE",
        "exchange_segment": int(unpacked[2]),
        "security_id": int(unpacked[3]),
        "ltp": float(unpacked[4]),
        "ltq": int(unpacked[5]),
        "ltt_epoch": int(unpacked[6]),
        "ltt_utc": _utc_from_epoch(int(unpacked[6])),
        "avg_price": float(unpacked[7]),
        "volume": int(unpacked[8]),
        "total_sell_quantity": int(unpacked[9]),
        "total_buy_quantity": int(unpacked[10]),
        "open": float(unpacked[11]),
        "close": float(unpacked[12]),
        "high": float(unpacked[13]),
        "low": float(unpacked[14]),
    }


def parse_oi(data: bytes) -> Dict[str, Any]:
    if len(data) < _OI_STRUCT.size:
        return {"error": "truncated_oi", "length": len(data)}
    unpacked = _OI_STRUCT.unpack(data[:_OI_STRUCT.size])
    return {
        "type": "OI",
        "exchange_segment": int(unpacked[2]),
        "security_id": int(unpacked[3]),
        "oi": int(unpacked[4]),
    }


def parse_prev_close(data: bytes) -> Dict[str, Any]:
    # Prev close structure close to ticker in many feeds
    try:
        unpacked = _TICKER_STRUCT.unpack(data[:_TICKER_STRUCT.size])
        return {
            "type": "PREV_CLOSE",
            "exchange_segment": int(unpacked[2]),
            "security_id": int(unpacked[3]),
            "prev_close": float(unpacked[4]),
            "timestamp": _utc_from_epoch(int(unpacked[5])),
        }
    except Exception:
        return {"error": "prev_close_parse_failed"}


def parse_status(data: bytes) -> Dict[str, Any]:
    # Market status is simple; content varies; provide raw values
    if len(data) < 8:
        return {"error": "truncated_status", "length": len(data)}
    # best-effort unpack
    packet_type, packet_len, exchange_seg, maybe_status = struct.unpack("<B H B I", data[:8])
    return {
        "type": "STATUS",
        "exchange_segment": int(exchange_seg),
        "raw_status": int(maybe_status)
    }


def parse_server_disconnect(data: bytes) -> Dict[str, Any]:
    # parse the reason codes (best effort)
    try:
        packet_type, packet_len, code, dummy, err_code = struct.unpack("<B H B I H", data[:10])
        return {
            "type": "SERVER_DISCONNECT",
            "err_code": int(err_code)
        }
    except Exception:
        return {"type": "SERVER_DISCONNECT", "error": "cannot_parse"}
