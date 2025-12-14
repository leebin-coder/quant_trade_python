"""WebSocket endpoints for tick streaming backed by ClickHouse."""
from __future__ import annotations

import asyncio
from datetime import date, datetime, time
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List, Optional, Tuple
from clickhouse_driver import Client
from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    status,
)

from app.core.config import settings
from app.utils.logger import logger

router = APIRouter()
_MARKET_TZ = ZoneInfo(settings.market_timezone)

_TICK_FIELDS = [
    "ts_code",
    "name",
    "trade",
    "price",
    "open",
    "high",
    "low",
    "pre_close",
    "bid",
    "ask",
    "volume",
    "amount",
    "b1_v",
    "b1_p",
    "b2_v",
    "b2_p",
    "b3_v",
    "b3_p",
    "b4_v",
    "b4_p",
    "b5_v",
    "b5_p",
    "a1_v",
    "a1_p",
    "a2_v",
    "a2_p",
    "a3_v",
    "a3_p",
    "a4_v",
    "a4_p",
    "a5_v",
    "a5_p",
    "date",
    "time",
    "source",
    "raw_json",
]

_TRADING_WINDOWS = [
    (time(9, 15), time(9, 25)),
    (time(9, 30), time(11, 30)),
    (time(13, 0), time(15, 0)),
]

_CLICKHOUSE_COLUMNS = ", ".join(_TICK_FIELDS)
_TIME_FIELD_INDEX = _TICK_FIELDS.index("time")
_DATE_FIELD_INDEX = _TICK_FIELDS.index("date")


def _get_clickhouse_client() -> Client:
    return Client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        settings={"strings_as_bytes": False},
    )


_ch_client = _get_clickhouse_client()


@router.websocket("/ticks")
async def websocket_ticks(
    websocket: WebSocket,
    stock_code: str | None = Query(None, alias="stockCode"),
    trade_date: str | None = Query(None, alias="date"),
):
    """Stream tick data updates for a given stock."""
    await websocket.accept()

    if not stock_code:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="stockCode is required",
        )
        return

    logger.info(f"[WebSocket] Client connected for {stock_code}")

    if not trade_date:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="date is required",
        )
        return

    history_ticks, last_time = _query_ticks(stock_code, trade_date, None)

    status_text = _determine_status(datetime.now(_MARKET_TZ))
    await websocket.send_json(
        {
            "status": status_text,
            "historyTicks": history_ticks,
            "latestTicks": [],
        }
    )

    try:
        while True:
            await asyncio.sleep(3.5)
            status_text = _determine_status(datetime.now(_MARKET_TZ))
            latest_ticks: List[Dict[str, Any]] = []
            if status_text == "trading":
                latest_ticks, new_last_time = _query_ticks(
                    stock_code, trade_date, last_time
                )
                if new_last_time:
                    last_time = new_last_time
            await websocket.send_json(
                {
                    "status": status_text,
                    "historyTicks": [],
                    "latestTicks": latest_ticks,
                }
            )
    except WebSocketDisconnect:
        logger.info(f"[WebSocket] Client disconnected for {stock_code}")


def _determine_status(now: datetime) -> str:
    current_time = now.time()
    for start, end in _TRADING_WINDOWS:
        if start <= current_time <= end:
            return "trading"

    morning = time(9, 15)
    evening = time(15, 0)
    if evening <= current_time or current_time < morning:
        return "non_trading"

    return "rest"


def _query_ticks(
    stock_code: str, trade_date: str, last_time: Optional[datetime | str]
) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
    params = {"stock_code": stock_code, "trade_date": trade_date}
    query = (
        f"SELECT {_CLICKHOUSE_COLUMNS} "
        "FROM market_realtime_ticks "
        "WHERE ts_code = %(stock_code)s AND date = %(trade_date)s"
    )
    normalized_last = _coerce_last_time(last_time, trade_date)
    if last_time:
        query += " AND time > %(last_time)s"
        params["last_time"] = normalized_last or last_time
    query += " ORDER BY time ASC"

    try:
        rows = _ch_client.execute(query, params)
    except Exception as exc:
        _handle_clickhouse_exception(exc)
        return [], normalized_last

    ticks: List[Dict[str, Any]] = []
    last_seen = normalized_last
    for row in rows:
        ticks.append(_row_to_tick(row))
        row_time = _extract_row_datetime(row, trade_date)
        if row_time:
            last_seen = row_time

    return ticks, last_seen


def _row_to_tick(row: Iterable[Any]) -> Dict[str, Any]:
    tick = dict(zip(_TICK_FIELDS, row))
    tick_time = tick.get("time")
    if isinstance(tick_time, (datetime, time)):
        tick["time"] = tick_time.strftime("%H:%M:%S")
    elif isinstance(tick_time, str) and tick_time:
        tick["time"] = _normalize_time_str(tick_time)
    tick_date = tick.get("date")
    if isinstance(tick_date, (datetime, date)):
        tick["date"] = tick_date.strftime("%Y-%m-%d")
    elif isinstance(tick_date, str) and tick_date:
        tick["date"] = _normalize_date_str(tick_date)
    return tick


def _normalize_time_str(raw: str) -> str:
    parsed = _parse_time_value(raw)
    if parsed:
        return parsed.strftime("%H:%M:%S")
    return raw.strip()


def _normalize_date_str(raw: str) -> str:
    parsed = _parse_date_value(raw)
    if parsed:
        return parsed.strftime("%Y-%m-%d")
    return raw.strip()


def _parse_time_value(raw: str) -> Optional[time]:
    text = raw.strip()
    for fmt in ("%H:%M:%S", "%H:%M", "%H%M%S", "%H%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _parse_date_value(raw: str) -> Optional[date]:
    text = raw.strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None
def _handle_clickhouse_exception(exc: Exception) -> None:
    if hasattr(exc, "code"):
        logger.error(
            "ClickHouse query failed (code=%s): %s", getattr(exc, "code", "unknown"), exc
        )
        if getattr(exc, "code", None) == 232:  # INCOMPATIBLE_COLUMNS
            logger.error(
                "检测到 ClickHouse 字段类型与应用不一致，请核对 "
                "market_realtime_ticks.date/time 列类型并保持为 Date/DateTime。"
            )
    else:
        logger.error("ClickHouse query failed: %s", exc, exc_info=True)
def _coerce_last_time(
    last_time: Optional[datetime | str], trade_date: Optional[str]
) -> Optional[datetime]:
    if last_time is None:
        return None
    if isinstance(last_time, datetime):
        return last_time
    if isinstance(last_time, str):
        return _combine_date_time(trade_date, last_time)
    return None


def _combine_date_time(trade_date: Optional[str], time_str: str) -> Optional[datetime]:
    parsed_time = _parse_time_value(time_str) if time_str else None
    if not parsed_time:
        return None
    base_date = _parse_date_value(trade_date) if trade_date else None
    if not base_date:
        base_date = datetime.now(_MARKET_TZ).date()
    return datetime.combine(base_date, parsed_time)


def _extract_row_datetime(row: Iterable[Any], trade_date: str) -> Optional[datetime]:
    values = list(row) if not isinstance(row, (list, tuple)) else row
    row_time = values[_TIME_FIELD_INDEX]
    row_date = values[_DATE_FIELD_INDEX]
    if isinstance(row_time, datetime):
        return row_time
    if isinstance(row_time, time):
        base_date = _resolve_row_date(row_date, trade_date)
        if base_date:
            return datetime.combine(base_date, row_time)
        return None
    if isinstance(row_time, str):
        parsed_time = _parse_time_value(row_time)
        if not parsed_time:
            return None
        base_date = _resolve_row_date(row_date, trade_date)
        if not base_date:
            return None
        return datetime.combine(base_date, parsed_time)
    return None


def _resolve_row_date(raw_date: Any, trade_date: Optional[str]) -> Optional[date]:
    if isinstance(raw_date, datetime):
        return raw_date.date()
    if isinstance(raw_date, date):
        return raw_date
    if isinstance(raw_date, str):
        parsed = _parse_date_value(raw_date)
        if parsed:
            return parsed
    if trade_date:
        return _parse_date_value(trade_date)
    return None
