"""VPN 换线 / PMTU 黑洞演练引擎。

与 Web 框架无关，Flask 路由只做参数解析和序列化。

核心规则（与题目要求一一对应）：
* 报文置 DF（禁止分片），``payload + overhead > mtu`` 即被线路丢弃；
* 超长通知开关打开时，丢弃同时产生“超长通知”，由页面手动投递；
* 每次发包带当前“轮次”号；只有当前轮次报文的通知/超时才能缩包，
  处理一次有效信号后轮次 +1，同批其它旧轮次信号全部失效；
* 换线会把当前载荷重置为“起始载荷”，轮次 +1，并立即重发全部未确认字节；
* 任何线路（包括旧线）报文的成功回执都有效；
* 已确认字节用合并区间的并集维护，重叠范围只计一次；
* 重发打包时跳过所有已确认区间，只取尚未确认的连续字节。

连发（burst）规则：
* 连发是后台分批执行的：``start_burst`` 只登记任务并返回受理信息，
  由外部调度线程反复调用 ``burst_tick`` / ``burst_step`` 推进；
* 同一场演练同时只允许一次连发；
* 连发期间的单发与连发都只从 ``next_offset``（下一段未首发字节）取载荷，
  且单发不占用连发请求的包数；
* 连发按请求包数收尾；先到报表末尾时按实发包数收尾；
* 换线、当前轮有效缩包（超时/超长通知）、重置或全部确认后，
  本次连发的剩余包立即停发，并记录结束原因。
"""

from __future__ import annotations

import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Optional

# ---- 输入/规模限制，防止误输入产生海量包 -------------------------------
MAX_TOTAL_BYTES = 1_000_000
MAX_PAYLOAD = 65_535
MAX_STEP = 65_535
MAX_PACKETS = 5_000
MAX_BURST_INTERVAL_MS = 60_000
BURST_BATCH_SIZE = 5  # 每批最多发出的包数
EVENT_KEEP = 400


class DrillError(Exception):
    """业务错误。status 为建议的 HTTP 状态码。"""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message = message
        self.status = status


def _as_int(data: dict, key: str, default: int, lo: int, hi: int) -> int:
    raw = data.get(key, default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise DrillError(f"参数 {key} 必须是整数，收到：{raw!r}")
    if not lo <= value <= hi:
        raise DrillError(f"参数 {key} 必须在 {lo}~{hi} 之间，当前为 {value}")
    return value


def _as_bool(data: dict, key: str, default: bool) -> bool:
    raw = data.get(key, default)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)) and raw in (0, 1):
        return bool(raw)
    if isinstance(raw, str):
        if raw.lower() in ("1", "true", "yes", "on", "开"):
            return True
        if raw.lower() in ("0", "false", "no", "off", "关"):
            return False
    raise DrillError(f"参数 {key} 必须是布尔值，收到：{raw!r}")


@dataclass
class LineConfig:
    key: str
    label: str
    mtu: int = 1500
    overhead: int = 100
    notify: bool = False

    @property
    def max_payload(self) -> int:
        """不分片时这条线路允许的最大三层载荷。"""
        return self.mtu - self.overhead

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "label": self.label,
            "mtu": self.mtu,
            "overhead": self.overhead,
            "notify": self.notify,
            "max_payload": self.max_payload,
        }


@dataclass
class DrillConfig:
    total_bytes: int = 10_000
    start_payload: int = 1400
    timeout_step: int = 200
    lines: dict = field(default_factory=dict)

    @staticmethod
    def from_raw(data: Optional[dict]) -> "DrillConfig":
        data = data or {}
        total = _as_int(data, "total_bytes", 10_000, 1, MAX_TOTAL_BYTES)
        start = _as_int(data, "start_payload", 1400, 1, MAX_PAYLOAD)
        step = _as_int(data, "timeout_step", 200, 1, MAX_STEP)

        raw_lines = data.get("lines") if isinstance(data.get("lines"), dict) else {}
        defaults = {
            "A": {"mtu": 1500, "overhead": 120, "notify": False},
            "B": {"mtu": 1500, "overhead": 60, "notify": True},
        }
        lines: dict[str, LineConfig] = {}
        for key in ("A", "B"):
            ld = raw_lines.get(key) if isinstance(raw_lines.get(key), dict) else {}
            merged = dict(defaults[key])
            merged.update({k: v for k, v in ld.items() if k in merged})
            lines[key] = LineConfig(
                key=key,
                label=f"线路 {key}",
                mtu=_as_int(merged, "mtu", defaults[key]["mtu"], 1, MAX_PAYLOAD),
                overhead=_as_int(merged, "overhead", defaults[key]["overhead"], 0, MAX_PAYLOAD),
                notify=_as_bool(merged, "notify", defaults[key]["notify"]),
            )
        return DrillConfig(total, start, step, lines)

    def to_dict(self) -> dict:
        return {
            "total_bytes": self.total_bytes,
            "start_payload": self.start_payload,
            "timeout_step": self.timeout_step,
            "lines": {k: v.to_dict() for k, v in self.lines.items()},
        }


class AckedRanges:
    """已确认字节的合并区间并集（左闭右开），并负责未确认区间的打包。"""

    def __init__(self, total: int):
        self.total = total
        self.ranges: list[tuple[int, int]] = []
        self.count = 0

    def add(self, left: int, right: int) -> int:
        """加入 [left, right)，返回本次真正新增的确认字节数。"""
        if not (0 <= left < right <= self.total):
            raise DrillError(f"非法字节区间 [{left}, {right})")
        old_count = self.count
        merged: list[tuple[int, int]] = []
        lo, hi = left, right
        placed = False
        for a, b in self.ranges:
            if b < lo:  # 完全在左侧
                merged.append((a, b))
            elif a > hi:  # 完全在右侧
                if not placed:
                    merged.append((lo, hi))
                    placed = True
                merged.append((a, b))
            else:  # 相交或相邻，合并
                lo, hi = min(lo, a), max(hi, b)
        if not placed:
            merged.append((lo, hi))
        merged.sort()
        self.ranges = merged
        self.count = sum(b - a for a, b in merged)
        return self.count - old_count

    def first_unacked(self) -> Optional[int]:
        pos = 0
        for a, b in self.ranges:
            if a > pos:
                return pos
            if b > pos:
                pos = b
        return pos if pos < self.total else None

    def packetize_unacked(self, max_len: int) -> list[tuple[int, int]]:
        """按 max_len 把所有未确认字节切成连续区间；遇到已确认区间就断开。

        每一段内部保证全部未确认，因此“重发只取尚未确认的字节”。
        """
        if max_len < 1:
            raise DrillError("打包长度必须 >= 1")
        runs: list[tuple[int, int]] = []
        pos = 0
        i = 0
        rs = self.ranges
        n = len(rs)
        total = self.total
        while pos < total:
            while i < n and rs[i][1] <= pos:
                i += 1
            if i < n and rs[i][0] <= pos < rs[i][1]:
                pos = rs[i][1]  # 落在已确认区间内，跳到区间末尾
                continue
            end = min(total, pos + max_len)
            if i < n and rs[i][0] > pos:
                end = min(end, rs[i][0])  # 不跨过下一个已确认区间
            runs.append((pos, end))
            pos = end
        return runs


@dataclass
class Packet:
    id: int
    line: str
    size: int
    start: int
    end: int
    wire_size: int
    round: int
    kind: str  # new | burst | missing | timeout | switch
    # 发送端看到的协议状态
    status: str = "outstanding"  # outstanding | acked | timeout
    # 上帝视角的物理结果
    physical: str = "in_flight"  # in_flight | silent_drop | notify_drop
    notice_state: Optional[str] = None  # None | pending | applied | ignored
    late_ack: bool = False
    created_order: int = 0
    burst_id: Optional[int] = None  # 由哪次连发发出（单发为 None）

    def to_dict(self) -> dict:
        return asdict(self)


# 连发结束原因
BURST_END_TEXT = {
    "done": "请求包数已发完",
    "eof": "已到报表末尾，按实发数收尾",
    "complete": "报表已全部确认",
    "switch": "换线生效，剩余包停发",
    "shrink": "有效缩包生效，剩余包停发",
    "reset": "演练被重置",
    "user": "用户手动停止",
    "error": "连发执行出错",
    "none": "—",
}


@dataclass
class Burst:
    """一次分批连发任务。``sent`` 只统计本任务真实发出的包。"""

    id: int
    requested: int  # 请求包数（None 表示“发完剩余新包”，内部换算成上限）
    all_remaining: bool
    interval_ms: int
    state: str = "pending"  # pending | running | finished
    sent: int = 0
    end_reason: Optional[str] = None
    started_at: float = 0.0
    finished_at: Optional[float] = None
    scheduled_at: float = 0.0  # 下一批最早执行时间
    last_batch: int = 0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "requested": self.requested,
            "all_remaining": self.all_remaining,
            "interval_ms": self.interval_ms,
            "state": self.state,
            "sent": self.sent,
            "end_reason": self.end_reason,
            "end_text": BURST_END_TEXT.get(self.end_reason, "—"),
            "running": self.state in ("pending", "running"),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "last_batch": self.last_batch,
        }


class Drill:
    """一次演练的全部可变状态。

    ``burst_tick`` 内部持 ``lock``；Web 层再用一把全局锁把所有请求与
    调度线程串行化，因此调度线程与单发/回执可以安全交错。
    """

    def __init__(self, config: Optional[DrillConfig] = None, clock=None):
        self.lock = threading.RLock()
        self._now = clock or time.monotonic  # 可注入时钟，便于确定性地测试分批连发
        self.next_burst_id = 1
        self.burst: Optional[Burst] = None
        self.last_burst: Optional[dict] = None
        self.reset(config or DrillConfig.from_raw(None))

    # ---- 初始化 / 事件 -------------------------------------------------
    def reset(self, config: DrillConfig) -> None:
        # 重置生效：旧连发的剩余包立即停发；快照保留供页面显示结束原因。
        carried_last = None
        if self._burst_active():
            self._finish_burst("reset")
            carried_last = self.last_burst
        self.cfg = config
        self.active_line = "A"
        self.cur_payload = config.start_payload
        self.round = 1
        self.next_offset = 0
        self.next_pkt_id = 1
        self.packets: list[Packet] = []
        self.acked = AckedRanges(config.total_bytes)
        self.complete = False
        self.completed_at: Optional[str] = None
        self.events: list[dict] = []
        self._event_seq = 0
        self.next_burst_id = getattr(self, "next_burst_id", 1)  # 重置后编号继续递增
        self.burst: Optional[Burst] = None      # 正在进行的连发（同一时刻最多一个）
        self.last_burst: Optional[dict] = carried_last  # 最近一次已结束连发
        self._log("reset", f"演练开始：报表 {config.total_bytes} 字节，"
                           f"起始载荷 {config.start_payload}，初始走线路 A（第 1 轮）")
        if carried_last:
            self._log(
                "burst-end",
                f"连发 #{carried_last['id']} 因重置停发：实发 {carried_last['sent']} / "
                f"请求 {carried_last['requested']} 包",
            )

    def _log(self, kind: str, text: str) -> dict:
        self._event_seq += 1
        event = {
            "seq": self._event_seq,
            "time": time.strftime("%H:%M:%S"),
            "kind": kind,
            "text": text,
        }
        self.events.append(event)
        if len(self.events) > EVENT_KEEP:
            self.events = self.events[-EVENT_KEEP:]
        return event

    def _line(self, key: Optional[str] = None) -> LineConfig:
        return self.cfg.lines[key or self.active_line]

    def _get_packet(self, pid: int) -> Packet:
        for p in self.packets:
            if p.id == pid:
                return p
        raise DrillError(f"找不到报文 #{pid}", 404)

    def _sync_complete(self) -> None:
        if not self.complete and self.acked.count >= self.cfg.total_bytes:
            self.complete = True
            self.completed_at = time.strftime("%H:%M:%S")
            self._log("complete",
                      f"累计确认 {self.acked.count}/{self.cfg.total_bytes} 字节，全部传完 ✅")
            if self._burst_active():
                self._finish_burst("complete")

    # ---- 发送 ----------------------------------------------------------
    def _dispatch(self, start: int, end: int, kind: str,
                  line: Optional[str] = None,
                  burst_id: Optional[int] = None) -> Packet:
        line_key = line or self.active_line
        lc = self._line(line_key)
        size = end - start
        wire = size + lc.overhead
        pkt = Packet(
            id=self.next_pkt_id,
            line=line_key,
            size=size,
            start=start,
            end=end,
            wire_size=wire,
            round=self.round,
            kind=kind,
            created_order=self.next_pkt_id,
            burst_id=burst_id,
        )
        self.next_pkt_id += 1

        if wire > lc.mtu:  # DF：连封装超 MTU，立即丢弃，不分片
            if lc.notify:
                pkt.physical = "notify_drop"
                pkt.notice_state = "pending"
                self._log(
                    "drop-notify",
                    f"#{pkt.id} [{start},{end}) 载荷 {size}+封装 {lc.overhead}"
                    f"={wire} > MTU {lc.mtu}，在{lc.label}被丢弃；已生成超长通知（待投递）",
                )
            else:
                pkt.physical = "silent_drop"
                self._log(
                    "drop-silent",
                    f"#{pkt.id} [{start},{end}) 载荷 {size}+封装 {lc.overhead}"
                    f"={wire} > MTU {lc.mtu}，在{lc.label}被静默丢弃（对端无任何回音）",
                )
        else:
            self._log(
                "send",
                f"#{pkt.id} 经{lc.label}发送 [{start},{end})，载荷 {size} 字节，"
                f"封装后 {wire}/{lc.mtu}，第 {self.round} 轮（{self._kind_text(kind)}）",
            )
        self.packets.append(pkt)
        return pkt

    @staticmethod
    def _kind_text(kind: str) -> str:
        return {
            "new": "新发顺序段",
            "burst": "连发顺序段",
            "missing": "重发未确认",
            "timeout": "超时缩包后重发",
            "switch": "换线后立即重发",
        }.get(kind, kind)

    def send_next(self) -> Packet:
        """发下一包：优先推进尚未首次发送的新字节；新字节发完后补最早的缺口。

        连发进行中也可单发，但同样只从下一段未首发字节取载荷（不去补缺口）；
        单发不占用连发请求的包数，连发会相应少发。
        """
        if self.complete:
            raise DrillError("报表已全部确认，无需再发", 409)
        total = self.cfg.total_bytes
        burst_active = self._burst_active()
        if self.next_offset < total:
            start = self.next_offset
            end = min(total, start + self.cur_payload)
            self.next_offset = end
            kind = "new"
        elif burst_active:
            # 连发期间只发未首发字节；没有了就按实发数收尾
            self._finish_burst("eof")
            raise DrillError("新字节已全部首发，连发已按实发数收尾", 409)
        else:
            runs = self.acked.packetize_unacked(self.cur_payload)
            if not runs:
                self._sync_complete()
                raise DrillError("已经没有未确认字节", 409)
            start, end = runs[0]
            kind = "missing"
        pkt = self._dispatch(start, end, kind)
        if burst_active and self.next_offset >= total:
            # 单发把新字节推到末尾：连发没有新字节可发，按实发数提前结束
            self._finish_burst("eof")
        return pkt

    # ---- 连发任务 ------------------------------------------------------
    def _burst_active(self) -> bool:
        return self.burst is not None and self.burst.state in ("pending", "running")

    def start_burst(self, count: Optional[int], interval_ms) -> Burst:
        """受理一次连发：只登记任务，立即返回；后台按批推进。

        ``count is None`` 表示“连续发完剩余新包”。会被换算成一个足够的
        请求上限，收尾时仍按实际发出的包数记录。
        """
        if self.complete:
            raise DrillError("报表已全部确认，无需再发", 409)
        if self._burst_active():
            raise DrillError(
                f"已有一次连发正在执行（#{self.burst.id}，已发 {self.burst.sent} 包），"
                "同一场演练同时只能有一次连发",
                409,
            )
        try:
            interval = int(interval_ms if interval_ms not in (None, "") else 0)
        except (TypeError, ValueError):
            raise DrillError(f"批间隔必须是整数毫秒，收到：{interval_ms!r}")
        if not 0 <= interval <= MAX_BURST_INTERVAL_MS:
            raise DrillError(f"批间隔必须在 0~{MAX_BURST_INTERVAL_MS} 毫秒之间，"
                             f"当前为 {interval}")

        total = self.cfg.total_bytes
        all_remaining = count is None
        if all_remaining:
            remaining = total - self.next_offset
            requested = max(1, (remaining + self.cur_payload - 1) // self.cur_payload)
        else:
            requested = _as_int({"count": count}, "count", 1, 1, MAX_PACKETS)
        if requested > MAX_PACKETS:
            raise DrillError(
                f"连发需要约 {requested} 个包，超过上限 {MAX_PACKETS}，请调大载荷或报表后重试",
            )
        if self.next_offset >= total:
            raise DrillError("新字节已全部分段发送过，请用“重发未确认字节”补缺口", 409)

        now = self._now()
        burst = Burst(
            id=self.next_burst_id,
            requested=requested,
            all_remaining=all_remaining,
            interval_ms=interval,
            started_at=now,
            scheduled_at=now,
        )
        self.next_burst_id += 1
        self.burst = burst
        self._log(
            "burst-start",
            f"连发 #{burst.id} 已受理：请求 {requested} 包"
            f"{'（发完剩余新包）' if all_remaining else ''}，"
            f"批间隔 {interval} ms，分批后台执行",
        )
        return burst

    def burst_wait(self, max_wait: float = 0.25) -> float:
        """调度线程的等待时长：离下一批还有多久（无连发时 0.25s 兜底）。"""
        if not self._burst_active():
            return max_wait
        remaining = self.burst.scheduled_at - self._now()
        if remaining <= 0:
            return 0.0
        return min(max_wait, remaining)

    def burst_tick(self) -> list[Packet]:
        """若已到点，执行一批；返回本批发出的包（未到点/无任务返回空）。

        由调度线程周期调用；内部持可重入锁，可与 Web 请求的写操作安全交错。
        """
        if not self._burst_active():
            return []
        with self.lock:
            if not self._burst_active():
                return []
            burst = self.burst
            now = self._now()
            if now < burst.scheduled_at:
                return []

            burst.state = "running"
            total = self.cfg.total_bytes
            quota = burst.requested - burst.sent
            batch = min(BURST_BATCH_SIZE, quota)
            created: list[Packet] = []
            for _ in range(batch):
                if self.complete:
                    self._finish_burst("complete")
                    break
                if self.next_offset >= total:
                    # 新字节已发到末尾，剩余请求没有可发的“未首发”载荷
                    self._finish_burst("eof")
                    break
                start = self.next_offset
                end = min(total, start + self.cur_payload)
                self.next_offset = end
                created.append(self._dispatch(start, end, "burst", burst_id=burst.id))
                burst.sent += 1
            if created:
                burst.last_batch = len(created)
                # 下一批最早执行时间：严格按批间隔，避免忙等
                burst.scheduled_at = self._now() + burst.interval_ms / 1000.0
            if self._burst_active() and burst.sent >= burst.requested:
                self._finish_burst("done")
            elif self._burst_active() and self.next_offset >= total:
                self._finish_burst("eof")
            return created

    def _finish_burst(self, reason: str) -> None:
        burst = self.burst
        if burst is None or burst.state not in ("pending", "running"):
            return
        burst.state = "finished"
        burst.end_reason = reason
        burst.finished_at = self._now()
        snapshot = burst.to_dict()
        self.last_burst = snapshot
        self.burst = None
        text = BURST_END_TEXT.get(reason, reason)
        self._log(
            "burst-end",
            f"连发 #{snapshot['id']} 结束：实发 {snapshot['sent']} / 请求 "
            f"{snapshot['requested']} 包，原因：{text}",
        )

    def stop_burst(self, reason: str) -> None:
        """外部事件（换线/缩包/重置）触发时停掉进行中的连发。"""
        if self._burst_active():
            self._finish_burst(reason)

    def resend_missing(self, kind: str = "missing") -> list[Packet]:
        """按当前载荷把所有未确认字节重新打包发出；只含尚未确认的字节。"""
        if self.complete:
            raise DrillError("报表已全部确认，无需重发", 409)
        runs = self.acked.packetize_unacked(self.cur_payload)
        if not runs:
            self._sync_complete()
            raise DrillError("没有需要重发的未确认字节", 409)
        if len(runs) > MAX_PACKETS:
            raise DrillError(
                f"按当前载荷 {self.cur_payload} 会产生 {len(runs)} 个包，"
                f"超过上限 {MAX_PACKETS}，请调大载荷或报表后重试",
            )
        pkts = [self._dispatch(s, e, kind) for s, e in runs]
        self._log("resend", f"按载荷 {self.cur_payload} 重新打包未确认字节，共 {len(pkts)} 包")
        return pkts

    # ---- 回执 / 超时 / 通知 -------------------------------------------
    def acknowledge(self, pid: int) -> tuple[Packet, int]:
        pkt = self._get_packet(pid)
        if pkt.status == "acked":
            self._log("ack-dup", f"#{pid} 的回执重复到达，忽略")
            return pkt, 0
        if pkt.physical != "in_flight":
            raise DrillError(
                f"#{pid} 实际已被线路丢弃（{pkt.physical}），不可能收到成功回执；"
                "静默丢包只能触发超时",
                409,
            )
        late = pkt.status == "timeout"
        added = self.acked.add(pkt.start, pkt.end)
        pkt.status = "acked"
        pkt.late_ack = late
        tag = "迟到回执" if late else "回执"
        if added > 0:
            self._log(
                "ack",
                f"#{pid} {tag}确认 [{pkt.start},{pkt.end})，新增 {added} 字节，"
                f"累计确认 {self.acked.count}/{self.cfg.total_bytes}",
            )
        else:
            self._log(
                "ack-overlap",
                f"#{pid} {tag}到达，但其区间 [{pkt.start},{pkt.end}) 已被其它回执覆盖，"
                f"新增 0 字节（重叠不重复计数），累计仍为 {self.acked.count}",
            )
        self._sync_complete()
        return pkt, added

    def trigger_timeout(self, pid: int) -> dict:
        pkt = self._get_packet(pid)
        if pkt.status != "outstanding":
            raise DrillError(f"#{pid} 当前状态为 {pkt.status}，不能再触发超时", 409)
        pkt.status = "timeout"

        if pkt.round != self.round:
            self._log(
                "timeout-stale",
                f"#{pid}（第 {pkt.round} 轮）超时，但当前已是第 {self.round} 轮；"
                "按规则旧轮次超时不再触发缩包",
            )
            return {"shrunk": False, "stale": True, "packet": pkt.to_dict(),
                    "resent": None}

        old = self.cur_payload
        self.cur_payload = max(1, old - self.cfg.timeout_step)
        self.round += 1
        self.stop_burst("shrink")  # 有效缩包：旧连发剩余包立即停发
        self._log(
            "timeout-shrink",
            f"#{pid} 超时（当前轮）：载荷 {old} -> {self.cur_payload}"
            f"（步长 {self.cfg.timeout_step}），进入第 {self.round} 轮",
        )
        # 立即用新载荷重发最早的未确认连续段
        resent: Optional[Packet] = None
        runs = self.acked.packetize_unacked(self.cur_payload)
        if runs:
            s, e = runs[0]
            resent = self._dispatch(s, e, "timeout")
        self._sync_complete()
        return {"shrunk": True, "stale": False, "packet": pkt.to_dict(),
                "old_payload": old, "new_payload": self.cur_payload,
                "resent": resent.to_dict() if resent else None}

    def deliver_notice(self, pid: int) -> dict:
        pkt = self._get_packet(pid)
        if pkt.notice_state is None:
            raise DrillError(f"#{pid} 没有待处理的超长通知", 409)
        if pkt.notice_state != "pending":
            raise DrillError(f"#{pid} 的超长通知已经处理过（{pkt.notice_state}）", 409)

        if pkt.round != self.round:
            pkt.notice_state = "ignored"
            self._log(
                "notice-stale",
                f"#{pid} 的超长通知属于第 {pkt.round} 轮，当前已是第 {self.round} 轮"
                "（可能已缩包或换线）；按规则忽略，当前载荷不变",
            )
            return {"applied": False, "stale": True, "packet": pkt.to_dict()}

        lc = self._line(pkt.line)
        target = max(1, lc.max_payload)
        old = self.cur_payload
        self.cur_payload = min(old, target)
        pkt.notice_state = "applied"
        self.round += 1
        # 通知生效会推进轮次；旧连发的剩余包若继续发就会带错轮次，立即停发。
        self.stop_burst("shrink")
        warn = ""
        if lc.max_payload < 1:
            warn = "；注意 MTU-封装开销 <= 0，配置有误，任何载荷都无法通过"
        self._log(
            "notice-apply",
            f"投递#{pid} 的{lc.label}超长通知：允许的最大载荷为 "
            f"{lc.mtu}-{lc.overhead}={lc.max_payload}；载荷 {old} -> "
            f"{self.cur_payload}，进入第 {self.round} 轮{warn}",
        )
        return {"applied": True, "stale": False, "packet": pkt.to_dict(),
                "old_payload": old, "new_payload": self.cur_payload}

    def switch_line(self, line_key: str) -> dict:
        if line_key not in self.cfg.lines:
            raise DrillError("线路只能是 A 或 B", 400)
        if self.complete:
            raise DrillError("报表已全部确认并传完；如需再次换线演练请重置", 409)
        old_line = self.active_line
        self.active_line = line_key
        self.cur_payload = self.cfg.start_payload  # 每次换线都从起始载荷重新试发
        self.round += 1
        self.stop_burst("switch")  # 换线生效：旧连发剩余包立即停发
        self._log(
            "switch",
            f"{old_line} -> {line_key} 换线：当前载荷重置为起始载荷 "
            f"{self.cur_payload}，进入第 {self.round} 轮，并立即重发未确认字节",
        )
        pkts = self.resend_missing(kind="switch")
        return {
            "active_line": self.active_line,
            "cur_payload": self.cur_payload,
            "round": self.round,
            "packets": [p.to_dict() for p in pkts],
        }

    # ---- 序列化 -------------------------------------------------------
    def state_dict(self) -> dict:
        burst = self.burst.to_dict() if self.burst else None
        return {
            "config": self.cfg.to_dict(),
            "active_line": self.active_line,
            "cur_payload": self.cur_payload,
            "round": self.round,
            "next_offset": self.next_offset,
            "acked_count": self.acked.count,
            "acked_ranges": [list(r) for r in self.acked.ranges],
            "complete": self.complete,
            "completed_at": self.completed_at,
            "packets": [p.to_dict() for p in self.packets],
            "events": self.events[-150:],
            "burst": burst,
            "burst_active": burst is not None and burst["running"],
            "last_burst": self.last_burst,
            "burst_batch_size": BURST_BATCH_SIZE,
            "limits": {
                "max_total_bytes": MAX_TOTAL_BYTES,
                "max_packets": MAX_PACKETS,
                "max_burst_interval_ms": MAX_BURST_INTERVAL_MS,
            },
        }
