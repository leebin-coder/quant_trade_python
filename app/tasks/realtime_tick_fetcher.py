"""
实时Tick数据获取任务
在交易日的 9:15-9:25、9:30-11:30、13:00-15:00 运行，获取实时行情数据
"""
import asyncio
import concurrent.futures
import json
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, date
from typing import Any, Dict, List, Optional, Tuple
import time as time_module

import httpx
import tushare as ts
from clickhouse_driver import Client

from app.core.config import settings
from app.utils.logger import logger


class PerformanceMonitor:
    """简单的性能监控，记录抓取/写入耗时与队列长度"""

    def __init__(self, max_samples: int = 1000):
        self.max_samples = max_samples
        self.fetch_times: List[float] = []
        self.insert_times: List[float] = []
        self.queue_sizes: List[int] = []
        self._lock = threading.Lock()

    def _record(self, storage: List, value):
        storage.append(value)
        if len(storage) > self.max_samples:
            del storage[: len(storage) - self.max_samples]

    def record_fetch_time(self, duration: float):
        with self._lock:
            self._record(self.fetch_times, duration)

    def record_insert_time(self, duration: float):
        with self._lock:
            self._record(self.insert_times, duration)

    def record_queue_size(self, size: int):
        with self._lock:
            self._record(self.queue_sizes, size)

    def _average(self, values: List[float]) -> float:
        if not values:
            return 0.0
        return sum(values) / len(values)

    def report(self) -> Dict[str, float]:
        with self._lock:
            avg_fetch = self._average(self.fetch_times)
            avg_insert = self._average(self.insert_times)
            last_queue = self.queue_sizes[-1] if self.queue_sizes else 0
        return {
            "avg_fetch_ms": avg_fetch * 1000 if avg_fetch else 0.0,
            "avg_insert_ms": avg_insert * 1000 if avg_insert else 0.0,
            "last_queue_size": last_queue,
        }


class RealtimeTickFetcher:
    """实时Tick数据获取器"""

    def __init__(self):
        """初始化"""
        self.api_base_url = f"http://{settings.stock_api_host}:{settings.stock_api_port}/api"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.stock_api_token}"
        }
        self._auth_params = {"token": settings.stock_api_token}
        # 初始化 tushare pro
        self.tushare_token = "347ae3b92b9a97638f155512bc599767558b94c3dcb47f5abd058b95"
        ts.set_token(self.tushare_token)
        self.pro = ts.pro_api()

        # 配置参数
        self.batch_size = 50  # sina数据源一次最多获取50只股票
        self.max_workers = 5  # 线程池大小，每个线程处理50只股票，可根据股票数量动态调整
        self.fetch_interval = 3  # 每次获取间隔（秒）
        self.clickhouse_table = "market_realtime_ticks"
        self.writer_interval = 3  # ClickHouse写入线程批量落库间隔
        self.max_retries = 3
        self.retry_delay = 1  # 秒
        self.request_stats = {
            "success": 0,
            "failed": 0,
            "total_rows": 0,
            "last_report": time_module.time(),
            "dropped_rows": 0,
            "queue_max": 0,
            "retry_attempts": 0,
            "writer_failures": 0,
        }
        self._stats_lock = threading.Lock()
        self.insert_max_retries = 3
        self._clickhouse_client: Optional[Client] = None
        self._clickhouse_client_lock = threading.Lock()
        self.max_queue_size = 100_000
        self.insert_batch_size = 1000
        self.insert_buffer_limit = 10_000
        self.insert_retry_delay = 2
        self._tick_queue: "queue.Queue[Tuple]" = queue.Queue(maxsize=self.max_queue_size)
        self._writer_thread: Optional[threading.Thread] = None
        self._writer_stop_event = threading.Event()
        self.performance_monitor = PerformanceMonitor()
        self.queue_warning_ratio = 0.8
        self.source_name = "tushare_realtime_quote"
        self.trading_windows = [
            (time(9, 15), time(9, 25)),
            (time(9, 30), time(11, 30)),
            (time(13, 0), time(15, 0)),
        ]
        self.clickhouse_columns = [
            "ts_code", "name",
            "trade", "price", "open", "high", "low", "pre_close",
            "bid", "ask", "volume", "amount",
            "b1_v", "b1_p", "b2_v", "b2_p", "b3_v", "b3_p", "b4_v", "b4_p", "b5_v", "b5_p",
            "a1_v", "a1_p", "a2_v", "a2_p", "a3_v", "a3_p", "a4_v", "a4_p", "a5_v", "a5_p",
            "date", "time", "source", "raw_json"
        ]
        self.clickhouse_insert_settings = {
            "async_insert": 1,
            "wait_for_async_insert": 0
        }

    def _with_token(self, extra_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = dict(self._auth_params)
        if extra_params:
            params.update(extra_params)
        return params

    async def get_all_stocks_except_bse(self) -> List[str]:
        """
        获取除北交所外的所有股票代码

        Returns:
            List[str]: 股票代码列表（格式：000001.SZ）
        """
        try:
            logger.info("正在从后端API获取所有股票...")

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{self.api_base_url}/stocks/all",
                    headers=self.headers,
                    params=self._with_token()
                )
                response.raise_for_status()

                result = response.json()
                if result.get("code") == 200:
                    data = result.get("data", {})

                    # data可能是list或dict，需要兼容处理
                    if isinstance(data, list):
                        stocks = data
                    elif isinstance(data, dict):
                        stocks = data.get("records", [])
                    else:
                        stocks = []

                    # 过滤掉北交所（BSE）的股票
                    stock_codes = [
                        stock["stockCode"]
                        for stock in stocks
                        if stock.get("exchange") != "BSE"
                    ]

                    # 临时仅采集工商银行（601398.SH）
                    target_codes = {"601398.SH"}
                    stock_codes = [
                        code for code in stock_codes
                        if code in target_codes
                    ]

                    logger.info(f"成功获取 {len(stock_codes)} 只股票（已排除北交所）")
                    return stock_codes
                else:
                    logger.error(f"获取股票列表失败: {result.get('message')}")
                    return []

        except Exception as e:
            logger.error(f"获取股票列表异常: {e}", exc_info=True)
            return []

    def _get_clickhouse_client(self) -> Client:
        """获取线程独立的 ClickHouse 客户端"""
        with self._clickhouse_client_lock:
            if self._clickhouse_client is None:
                self._clickhouse_client = Client(
                    host=settings.clickhouse_host,
                    port=settings.clickhouse_port,
                    user=settings.clickhouse_user,
                    password=settings.clickhouse_password,
                    database=settings.clickhouse_database,
                    settings={"strings_as_bytes": False},
                )
        return self._clickhouse_client

    def _adjust_runtime_config(self, total_stocks: int):
        """根据股票数量动态调整线程数"""
        original_workers = self.max_workers
        if total_stocks <= 100:
            target_workers = 2
        elif total_stocks <= 500:
            target_workers = 5
        elif total_stocks <= 2000:
            target_workers = 10
        else:
            target_workers = 15
        self.max_workers = max(1, target_workers)
        if self.max_workers != original_workers:
            logger.info(f"根据股票数量调整线程数: {original_workers} -> {self.max_workers}")

    def _next_request_id(self) -> int:
        with self._stats_lock:
            return self.request_stats["success"] + self.request_stats["failed"] + 1

    def _record_request_success(self, rows: int):
        with self._stats_lock:
            self.request_stats["success"] += 1
            self.request_stats["total_rows"] += rows

    def _record_request_failure(self):
        with self._stats_lock:
            self.request_stats["failed"] += 1

    def _record_retry_attempt(self):
        with self._stats_lock:
            self.request_stats["retry_attempts"] += 1

    def _record_writer_failure(self):
        with self._stats_lock:
            self.request_stats["writer_failures"] += 1

    def _record_dropped_rows(self, count: int):
        with self._stats_lock:
            self.request_stats["dropped_rows"] += count

    def _update_queue_highwater(self):
        with self._stats_lock:
            current_size = self._tick_queue.qsize()
            if current_size > self.request_stats["queue_max"]:
                self.request_stats["queue_max"] = current_size

    def _maybe_warn_queue_pressure(self):
        current_size = self._tick_queue.qsize()
        capacity = self.max_queue_size
        if capacity <= 0:
            return
        if current_size >= capacity * self.queue_warning_ratio:
            logger.warning(
                f"写入队列压力增大：当前 {current_size}/{capacity} "
                f"({current_size / capacity:.0%})"
            )

    def _report_statistics(self):
        now = time_module.time()
        with self._stats_lock:
            if now - self.request_stats["last_report"] < 60:
                return
            success = self.request_stats["success"]
            failed = self.request_stats["failed"]
            total_rows = self.request_stats["total_rows"]
            dropped = self.request_stats["dropped_rows"]
            queue_max = self.request_stats["queue_max"]
            retry_attempts = self.request_stats["retry_attempts"]
            writer_failures = self.request_stats["writer_failures"]
            self.request_stats["last_report"] = now
            current_queue = self._tick_queue.qsize()
        perf = self.performance_monitor.report()
        logger.info(
            f"统计: 成功 {success} 次, 失败 {failed} 次, 总行数 {total_rows}, "
            f"重试 {retry_attempts} 次, 写入异常 {writer_failures} 次, "
            f"队列当前 {current_queue} / 峰值 {queue_max}, 丢弃 {dropped} 条, "
            f"平均抓取 {perf['avg_fetch_ms']:.1f}ms, 平均写入 {perf['avg_insert_ms']:.1f}ms, "
            f"最近队列 {perf['last_queue_size']}"
        )

    @staticmethod
    def _extract_first_value(data: Dict, keys: List[str]):
        """从数据字典中按顺序提取第一个有效字段"""
        for key in keys:
            if key in data:
                value = data.get(key)
                if value not in (None, "", "--", "-"):
                    return value
        return None

    @staticmethod
    def _normalize_string(value) -> Optional[str]:
        """处理字符串字段"""
        if value in (None, "", "--", "-"):
            return None
        result = str(value).strip()
        return result or None

    @staticmethod
    def _parse_float(value) -> Optional[float]:
        """安全地将数据转换为浮点数"""
        if value in (None, "", "--", "-"):
            return None
        try:
            if isinstance(value, str):
                cleaned = value.replace(",", "").strip()
                if cleaned == "":
                    return None
                return float(cleaned)
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_trade_datetime(date_str: Optional[str], time_str: Optional[str]) -> Tuple[date, datetime]:
        """解析交易日期和时间"""
        fallback = datetime.now()

        parsed_date = None
        if date_str:
            cleaned = date_str.strip().replace("/", "-")
            if len(cleaned) == 8 and cleaned.isdigit():
                cleaned = f"{cleaned[:4]}-{cleaned[4:6]}-{cleaned[6:]}"
            for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
                try:
                    parsed_date = datetime.strptime(cleaned, fmt).date()
                    break
                except ValueError:
                    continue
        if parsed_date is None:
            parsed_date = fallback.date()

        parsed_time = None
        if time_str:
            cleaned_time = time_str.strip().replace(".", ":")
            for fmt in ("%H:%M:%S", "%H%M%S"):
                try:
                    parsed_time = datetime.strptime(cleaned_time, fmt).time()
                    break
                except ValueError:
                    continue

        if parsed_time is None:
            parsed_time = fallback.time()

        combined = datetime.combine(parsed_date, parsed_time)
        return parsed_date, combined
    def _get_trading_window_status(self, current_time: time) -> Tuple[str, Optional[time]]:
        """
        判断当前时间所处的实时tick采集窗口状态

        Returns:
            Tuple[str, Optional[time]]:
                - status: "active" 当前在采集窗口内；"upcoming" 尚未开始；"finished" 今日全部结束
                - reference_time: 当 status 为 "upcoming" 时表示下一窗口开始时间
        """
        for start, end in self.trading_windows:
            if start <= current_time <= end:
                return "active", end

        for start, _ in self.trading_windows:
            if current_time < start:
                return "upcoming", start

        return "finished", None

    def _build_tick_row(self, tick_data: Dict) -> Optional[Tuple]:
        """将 tick 字典转换为 ClickHouse 行，字段名与 Tushare 文档保持一致"""
        field_aliases = {
            "ts_code": ["ts_code", "TS_CODE"],
            "name": ["name", "NAME", "sec_name", "SECNAME"],
            "trade": ["trade", "TRADE"],
            "price": ["price", "PRICE"],
            "open": ["open", "OPEN"],
            "high": ["high", "HIGH"],
            "low": ["low", "LOW"],
            "pre_close": ["pre_close", "PRE_CLOSE", "preclose"],
            "bid": ["bid", "BID", "b1_p", "B1_P"],
            "ask": ["ask", "ASK", "a1_p", "A1_P"],
            "volume": ["volume", "VOLUME", "vol", "VOL"],
            "amount": ["amount", "AMOUNT", "turnover", "TURNOVER"],
            "b1_v": ["b1_v", "B1_V"],
            "b1_p": ["b1_p", "B1_P"],
            "b2_v": ["b2_v", "B2_V"],
            "b2_p": ["b2_p", "B2_P"],
            "b3_v": ["b3_v", "B3_V"],
            "b3_p": ["b3_p", "B3_P"],
            "b4_v": ["b4_v", "B4_V"],
            "b4_p": ["b4_p", "B4_P"],
            "b5_v": ["b5_v", "B5_V"],
            "b5_p": ["b5_p", "B5_P"],
            "a1_v": ["a1_v", "A1_V"],
            "a1_p": ["a1_p", "A1_P"],
            "a2_v": ["a2_v", "A2_V"],
            "a2_p": ["a2_p", "A2_P"],
            "a3_v": ["a3_v", "A3_V"],
            "a3_p": ["a3_p", "A3_P"],
            "a4_v": ["a4_v", "A4_V"],
            "a4_p": ["a4_p", "A4_P"],
            "a5_v": ["a5_v", "A5_V"],
            "a5_p": ["a5_p", "A5_P"],
            "date": ["date", "DATE"],
            "time": ["time", "TIME"],
        }

        ts_code = self._normalize_string(
            self._extract_first_value(tick_data, field_aliases["ts_code"])
        )
        if not ts_code:
            logger.debug("跳过一条缺少股票代码的tick数据")
            return None

        str_fields = {}
        str_fields["name"] = self._normalize_string(
            self._extract_first_value(tick_data, field_aliases["name"])
        )

        float_fields = {}
        numeric_keys = [
            "trade", "price", "open", "high", "low", "pre_close",
            "bid", "ask", "volume", "amount",
            "b1_v", "b1_p", "b2_v", "b2_p", "b3_v", "b3_p", "b4_v", "b4_p", "b5_v", "b5_p",
            "a1_v", "a1_p", "a2_v", "a2_p", "a3_v", "a3_p", "a4_v", "a4_p", "a5_v", "a5_p",
        ]
        for key in numeric_keys:
            float_fields[key] = self._parse_float(
                self._extract_first_value(tick_data, field_aliases.get(key, []))
            )

        date_raw = self._extract_first_value(tick_data, field_aliases["date"])
        time_raw = self._extract_first_value(tick_data, field_aliases["time"])
        trade_date, trade_datetime = self._parse_trade_datetime(
            date_raw, time_raw
        )

        row = (
            ts_code,
            str_fields.get("name") or ts_code,
            float_fields["trade"],
            float_fields["price"],
            float_fields["open"],
            float_fields["high"],
            float_fields["low"],
            float_fields["pre_close"],
            float_fields["bid"],
            float_fields["ask"],
            float_fields["volume"],
            float_fields["amount"],
            float_fields["b1_v"],
            float_fields["b1_p"],
            float_fields["b2_v"],
            float_fields["b2_p"],
            float_fields["b3_v"],
            float_fields["b3_p"],
            float_fields["b4_v"],
            float_fields["b4_p"],
            float_fields["b5_v"],
            float_fields["b5_p"],
            float_fields["a1_v"],
            float_fields["a1_p"],
            float_fields["a2_v"],
            float_fields["a2_p"],
            float_fields["a3_v"],
            float_fields["a3_p"],
            float_fields["a4_v"],
            float_fields["a4_p"],
            float_fields["a5_v"],
            float_fields["a5_p"],
            trade_date,
            trade_datetime,
            self.source_name,
            json.dumps(tick_data, ensure_ascii=False),
        )
        return row

    def _enqueue_tick_rows(self, rows: List[Tuple]):
        """将已转换的tick行放入队列，供写线程批量写入"""
        dropped = 0
        for row in rows:
            try:
                self._tick_queue.put(row, timeout=1)
            except queue.Full:
                dropped += 1
        self._update_queue_highwater()
        self.performance_monitor.record_queue_size(self._tick_queue.qsize())
        self._maybe_warn_queue_pressure()
        if dropped:
            logger.error(f"写入队列已满，丢弃 {dropped} 条实时tick数据")
            self._record_dropped_rows(dropped)

    def _start_clickhouse_writer(self):
        """启动ClickHouse写入线程"""
        if self._writer_thread and self._writer_thread.is_alive():
            return
        self._writer_stop_event.clear()
        self._writer_thread = threading.Thread(
            target=self._clickhouse_writer_loop,
            name="clickhouse-writer",
            daemon=True,
        )
        self._writer_thread.start()

    def _stop_clickhouse_writer(self):
        """停止ClickHouse写入线程，并在退出前刷新残余数据"""
        if not self._writer_thread:
            return
        self._writer_stop_event.set()
        self._writer_thread.join(timeout=self.writer_interval * 2)
        self._writer_thread = None

    def _clickhouse_writer_loop(self):
        """ClickHouse写入线程循环，固定间隔批量落库"""
        logger.info("ClickHouse写入线程启动")
        buffer: List[Tuple] = []
        last_flush = time_module.time()
        try:
            while True:
                should_stop = self._writer_stop_event.is_set()
                try:
                    row = self._tick_queue.get(timeout=0.2)
                    buffer.append(row)
                except queue.Empty:
                    pass

                now = time_module.time()
                flush_due_to_time = (now - last_flush) >= self.writer_interval
                flush_due_to_size = len(buffer) >= self.insert_batch_size
                flush_due_to_limit = len(buffer) >= self.insert_buffer_limit

                if buffer and (flush_due_to_time or flush_due_to_size or flush_due_to_limit):
                    if self._bulk_insert_with_retry(buffer):
                        buffer.clear()
                        last_flush = now

                if should_stop and self._tick_queue.empty():
                    break

            # writer stop requested, flush remaining buffer
            if buffer:
                self._bulk_insert_with_retry(buffer)
        finally:
            logger.info("ClickHouse写入线程停止")

    def _bulk_insert_with_retry(self, rows: List[Tuple]) -> bool:
        """带重试的批量写入 ClickHouse，失败时尝试回写队列"""
        attempts = 0
        while attempts < self.insert_max_retries:
            try:
                self._bulk_insert_to_clickhouse(rows)
                return True
            except Exception as e:
                attempts += 1
                self._record_writer_failure()
                self._record_retry_attempt()
                logger.error(
                    f"批量写入ClickHouse失败（第{attempts}次）: {e}",
                    exc_info=True,
                )
                time_module.sleep(self.insert_retry_delay)

        logger.error("写入ClickHouse多次失败，尝试将数据重新放回队列")
        for row in rows:
            try:
                self._tick_queue.put_nowait(row)
            except queue.Full:
                logger.critical("队列仍满，部分实时tick数据可能丢失")
                break
        return False

    def _bulk_insert_to_clickhouse(self, rows: List[Tuple]):
        if not rows:
            return
        client = self._get_clickhouse_client()
        columns_sql = ", ".join(self.clickhouse_columns)
        start_time = time_module.time()
        client.execute(
            f"INSERT INTO {self.clickhouse_table} ({columns_sql}) VALUES",
            rows,
            settings={
                **self.clickhouse_insert_settings,
                "max_block_size": self.insert_buffer_limit,
            },
        )
        duration = time_module.time() - start_time
        self.performance_monitor.record_insert_time(duration)
        logger.debug(f"批量写入 {len(rows)} 条实时tick数据到 ClickHouse 表 {self.clickhouse_table}")


    def fetch_realtime_tick_batch(
        self,
        stock_codes: List[str],
        batch_id: int,
        round_num: int,
    ) -> int:
        """获取一批股票的实时tick数据，不对快照数据重试"""
        try:
            current_time = datetime.now().strftime('%H:%M:%S')
            ts_codes = ",".join(stock_codes)
            start_time = time_module.time()
            df = ts.realtime_quote(ts_code=ts_codes, src='sina')
            elapsed = time_module.time() - start_time
            self.performance_monitor.record_fetch_time(elapsed)
            if elapsed > 5:
                logger.warning(f"[批次{batch_id:>3}] 第{round_num:>3}次 - 慢请求: {elapsed:.2f}秒")

            if df is None or df.empty:
                logger.warning(f"[批次{batch_id:>3}] 第{round_num:>3}次 {current_time} - 未获取到数据")
                return 0

            rows = []
            for _, row in df.iterrows():
                tick_data = row.to_dict()
                built_row = self._build_tick_row(tick_data)
                if built_row:
                    rows.append(built_row)

            if rows:
                logger.debug(
                    f"[批次{batch_id:>3}] 第{round_num:>3}次获取 {len(rows)} 条实时数据"
                )
                self._enqueue_tick_rows(rows)
                self._record_request_success(len(rows))
            return len(rows)

        except Exception as e:
            logger.error(f"[批次{batch_id}] 第{round_num}次 - 获取失败: {e}", exc_info=True)
            self._record_request_failure()
            return 0

    async def check_is_trading_day(self, date_str: str = None) -> bool:
        """
        检查指定日期是否为交易日

        Args:
            date_str: 日期字符串，格式：YYYY-MM-DD，默认为今天

        Returns:
            bool: 是否为交易日
        """
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')

        try:
            logger.info(f"正在检查 {date_str} 是否为交易日...")

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{self.api_base_url}/trading-calendar/is-trading-day",
                    headers=self.headers,
                    params=self._with_token({"date": date_str})
                )
                response.raise_for_status()

                result = response.json()
                if result.get("code") == 200:
                    data = result.get("data", {})
                    is_trading_day = data.get("isTradingDay", False)
                    logger.info(f"{date_str} {'是' if is_trading_day else '不是'}交易日")
                    return is_trading_day
                else:
                    logger.error(f"检查交易日失败: {result.get('message')}")
                    return False

        except Exception as e:
            logger.error(f"检查交易日异常: {e}", exc_info=True)
            return False

    async def start_realtime_tick_sync(self):
        """
        启动实时tick数据同步任务
        前提条件：今天是交易日
        运行时间：9:15-9:25、9:30-11:30、13:00-15:00
        按50只股票分组，所有批次同时发起请求，严格3秒一轮
        """
        logger.info("=" * 80)
        logger.info("启动实时Tick数据同步任务...")
        logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)

        # 1. 检查今天是否为交易日
        today = datetime.now().strftime('%Y-%m-%d')
        is_trading_day = await self.check_is_trading_day(today)

        if not is_trading_day:
            logger.warning(f"今天 {today} 不是交易日，任务不执行")
            return

        window_desc = "、".join([f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}" for start, end in self.trading_windows])
        logger.info(f"实时Tick采集窗口: {window_desc}")

        try:
            # 3. 获取所有股票（除北交所）
            logger.info("\n步骤3: 获取所有股票列表（除北交所）...")
            all_stocks = await self.get_all_stocks_except_bse()

            if not all_stocks:
                logger.error("未获取到股票列表，任务终止")
                return

            total_stocks = len(all_stocks)
            logger.info(f"共需要处理 {total_stocks} 只股票")
            self._adjust_runtime_config(total_stocks)

            # 4. 按50只股票分组
            batches = [
                all_stocks[i:i + self.batch_size]
                for i in range(0, len(all_stocks), self.batch_size)
            ]

            logger.info(f"共分为 {len(batches)} 组，每组最多{self.batch_size}只股票")
            worker_count = max(1, min(len(batches), self.max_workers))
            logger.info(f"使用 {worker_count} 个线程进行抓取，严格{self.fetch_interval}秒一轮")
            logger.info("按Ctrl+C停止所有线程")
            logger.info("=" * 80)

            # 5. 创建长期运行的线程池
            executor = ThreadPoolExecutor(
                max_workers=worker_count,
                thread_name_prefix="tick-fetcher",
            )
            request_count = 0

            self._start_clickhouse_writer()
            try:
                while True:
                    now_time = datetime.now().time()
                    window_status, reference_time = self._get_trading_window_status(now_time)

                    if window_status != "active":
                        if window_status == "upcoming" and reference_time:
                            logger.info(
                                f"当前时间 {now_time} 不在实时采集窗口，将在 {reference_time.strftime('%H:%M:%S')} 再次开始..."
                            )
                            wait_seconds = (
                                datetime.combine(datetime.now().date(), reference_time)
                                - datetime.combine(datetime.now().date(), now_time)
                            ).total_seconds()
                            await asyncio.sleep(min(max(wait_seconds, 10), 60))
                            continue
                        else:
                            logger.info(f"当前时间 {now_time} 已过今日实时采集时间，任务结束")
                            break  # 结束任务

                    request_count += 1
                    round_start_time = time_module.time()
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    logger.debug(f"[第{request_count}轮请求] {current_time}")

                    # 所有批次同时发起请求
                    futures_map: Dict[concurrent.futures.Future, int] = {}
                    request_id = self._next_request_id()
                    for batch_id, batch in enumerate(batches, start=1):
                        future = executor.submit(
                            self.fetch_realtime_tick_batch,
                            batch,
                            batch_id,
                            request_id
                        )
                        futures_map[future] = batch_id

                    done, not_done = concurrent.futures.wait(
                        futures_map.keys(),
                        timeout=self.fetch_interval,
                        return_when=concurrent.futures.ALL_COMPLETED,
                    )

                    if not_done:
                        logger.warning(f"本轮有 {len(not_done)} 个批次未完成，将后台继续执行")

                    # 统计报告
                    self._report_statistics()

                    # 严格保证间隔
                    elapsed = time_module.time() - round_start_time
                    sleep_time = max(0, self.fetch_interval - elapsed)

                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                    else:
                        logger.warning(f"警告：本轮耗时 {elapsed:.2f}秒，超过{self.fetch_interval}秒间隔")

            except KeyboardInterrupt:
                logger.info("\n\n收到停止信号，正在关闭线程池...")
                executor.shutdown(wait=False)  # 不等待未完成的任务
                logger.info(f"共完成 {request_count} 轮请求")
            finally:
                self._stop_clickhouse_writer()

            logger.info(f"\n✅ 实时Tick数据同步任务结束")

        except Exception as e:
            logger.error(f"实时Tick数据同步失败: {e}", exc_info=True)
            raise
