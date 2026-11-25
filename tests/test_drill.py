"""端到端 API 测试：静默丢包、超长通知轮次、换线后回包交错、重叠去重、分批连发。"""

import time

import pytest

from app import app as flask_app
from drill_engine import BURST_BATCH_SIZE, Drill, DrillConfig, AckedRanges


# ---------------------------------------------------------------- helpers

@pytest.fixture()
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


@pytest.fixture(autouse=True)
def _fresh_drill(client):
    rv = client.post("/api/reset", json={})
    assert rv.status_code == 200
    yield


def make_config(total=4000, start=1400, step=200,
                A=(1500, 120, False), B=(1500, 60, True)):
    return {
        "total_bytes": total,
        "start_payload": start,
        "timeout_step": step,
        "lines": {
            "A": {"mtu": A[0], "overhead": A[1], "notify": A[2]},
            "B": {"mtu": B[0], "overhead": B[1], "notify": B[2]},
        },
    }


def reset(client, **kw):
    rv = client.post("/api/reset", json=make_config(**kw))
    assert rv.status_code == 200, rv.get_json()
    return rv.get_json()["state"]


def post(client, path, body=None, expect=200):
    rv = client.post(path, json=body or {})
    assert rv.status_code == expect, (path, rv.status_code, rv.get_json())
    return rv.get_json()


def ack(client, pid):
    return post(client, f"/api/packets/{pid}/ack")


def timeout(client, pid):
    return post(client, f"/api/packets/{pid}/timeout")


def notice(client, pid):
    return post(client, f"/api/packets/{pid}/deliver-notice")


def wait_burst(client, timeout_s=8.0):
    """等后台调度把连发跑完，返回最终 state。"""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        state = client.get("/api/state").get_json()["state"]
        if not state["burst_active"]:
            return state
        time.sleep(0.02)
    raise AssertionError("连发未在期限内结束")


def burst(client, count, interval_ms=0):
    """提交连发（立即受理）→ 等结束 → 返回最终 state。"""
    rv = post(client, "/api/send-burst",
              {"count": count, "interval_ms": interval_ms})
    assert rv["accepted"] is True
    return wait_burst(client)


class FakeClock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


# ---------------------------------------------------------------- 基础页面

def test_index_and_healthz(client):
    assert client.get("/healthz").get_json()["ok"] is True
    html = client.get("/").get_data(as_text=True)
    assert "VPN 换线" in html
    assert "报表字节数" in html


def test_bad_config_rejected(client):
    rv = client.post("/api/reset", json={"total_bytes": 0})
    assert rv.status_code == 400
    rv = client.post("/api/reset",
                    json={"lines": {"A": {"mtu": "abc"}}})
    assert rv.status_code == 400


# ---------------------------------------------------------------- 区间单元

def test_acked_ranges_union_and_packetize():
    ar = AckedRanges(100)
    assert ar.add(10, 20) == 10
    assert ar.add(15, 30) == 10       # 重叠 5，新增到 30
    assert ar.ranges == [(10, 30)]
    assert ar.add(40, 50) == 10
    assert ar.count == 30
    assert ar.first_unacked() == 0

    # 从 0 开始，每 15 字节一段；遇到已确认区间 [10,30) 必须断开
    runs = ar.packetize_unacked(15)
    assert runs[0] == (0, 10)
    # 跳过 [10,30) 后从 30 继续
    assert (30, 40) in runs
    assert (50, 65) in runs

    ar.add(0, 10)
    assert ar.first_unacked() == 30


# ---------------------------------------------------------------- 场景一：静默丢包

def test_silent_drop_timeout_shrink_then_complete(client):
    reset(client, total=4000)  # A: 1400+120=1520 > 1500，且通知关
    state = burst(client, 3)
    p1, p2, p3 = state["packets"]

    assert (p1["start"], p1["end"]) == (0, 1400)
    assert p1["physical"] == "silent_drop"
    assert p1["notice_state"] is None
    assert p2["physical"] == "silent_drop"
    # 最后一包只有 1200 载荷：1200+120=1320 <= 1500，小包实际能通
    assert (p3["start"], p3["end"]) == (2800, 4000)
    assert p3["size"] == 1200
    assert p3["physical"] == "in_flight"
    assert state["last_burst"]["sent"] == 3
    assert state["last_burst"]["end_reason"] == "done"

    # 静默丢弃的包不可能 ACK
    rv = client.post("/api/packets/1/ack")
    assert rv.status_code == 409

    st = ack(client, 3)["state"]
    assert st["acked_count"] == 1200
    assert st["complete"] is False

    # 触发当前轮 #1 超时：1400 -> 1200，轮次 1 -> 2，并立即重发最早缺口
    r = timeout(client, 1)["result"]
    assert r["shrunk"] is True
    assert r["old_payload"] == 1400 and r["new_payload"] == 1200
    assert r["resent"]["start"] == 0 and r["resent"]["end"] == 1200
    assert r["resent"]["physical"] == "in_flight"
    state = client.get("/api/state").get_json()["state"]
    assert state["round"] == 2 and state["cur_payload"] == 1200

    assert ack(client, 4)["state"]["acked_count"] == 2400

    # 重发剩余缺口：[1200,2400) 与被确认区间截断出的 [2400,2800)
    pkts = post(client, "/api/resend-missing")["packets"]
    assert [(p["start"], p["end"]) for p in pkts] == [(1200, 2400), (2400, 2800)]

    assert ack(client, 5)["state"]["acked_count"] == 3600
    final = ack(client, 6)["state"]
    assert final["acked_count"] == 4000
    assert final["complete"] is True

    # 传完后不再允许发送
    assert client.post("/api/send-next").status_code == 409
    assert client.post("/api/send-burst", json={"count": 1}).status_code == 409


# ---------------------------------------------------------------- 场景二：超长通知与轮次

def test_notice_shrinks_once_per_round(client):
    # A 打开通知，eff=1380；total=4200 让三连包全是 1400
    reset(client, total=4200, A=(1500, 120, True))
    state = burst(client, 3)
    pkts = state["packets"]
    assert all(p["physical"] == "notify_drop" for p in pkts)
    assert all(p["notice_state"] == "pending" for p in pkts)

    # 先投递 #2 的通知：生效，载荷直接降到 1380，进入第 2 轮
    r = notice(client, 2)["result"]
    assert r["applied"] is True and r["new_payload"] == 1380
    state = client.get("/api/state").get_json()["state"]
    assert state["round"] == 2

    # 同批 #1/#3 的通知属于旧轮，全部忽略，不能再缩
    r1 = notice(client, 1)["result"]
    r3 = notice(client, 3)["result"]
    assert r1["applied"] is False and r1["stale"] is True
    assert r3["stale"] is True
    state = client.get("/api/state").get_json()["state"]
    assert state["cur_payload"] == 1380 and state["round"] == 2

    # 重复投递已处理通知报错
    assert client.post("/api/packets/2/deliver-notice").status_code == 409


def test_timeout_stale_round_does_not_shrink(client):
    reset(client, total=2800, A=(1500, 0, False), B=(1500, 60, True))
    burst(client, 2)
    # 直接换线：轮次 1 -> 2，载荷重置 1400
    sw = post(client, "/api/switch", {"line": "B"})["result"]
    assert sw["round"] == 2 and sw["cur_payload"] == 1400

    # 旧轮 #1 超时：只标记，不缩包
    r = timeout(client, 1)["result"]
    assert r["stale"] is True and r["shrunk"] is False
    state = client.get("/api/state").get_json()["state"]
    assert state["cur_payload"] == 1400 and state["round"] == 2
    assert state["packets"][0]["status"] == "timeout"

    # 但旧线报文之后仍可补到“迟到回执”
    res = ack(client, 1)
    assert res["added_bytes"] == 1400
    p1 = next(p for p in res["state"]["packets"] if p["id"] == 1)
    assert p1["late_ack"] is True

    # 当前轮（B 线）报文超时则正常缩包 + 重发
    b_pkt_id = sw["packets"][0]["id"]
    r = timeout(client, b_pkt_id)["result"]
    assert r["shrunk"] is True
    assert r["old_payload"] == 1400 and r["new_payload"] == 1200
    assert r["resent"]["line"] == "B"


# ---------------------------------------------------------------- 场景三：换线后回包交错 + 重叠去重

def test_switch_then_interleaved_acks_dedup(client):
    reset(client, total=4200, A=(1500, 200, False))  # A eff=1300 全超
    burst(client, 3)                                   # #1..#3 静默丢
    timeout(client, 1)                                 # ->1200, #4 [0,1200)
    ack(client, 4)

    pkts = post(client, "/api/resend-missing")["packets"]
    assert [(p["id"], p["start"], p["end"]) for p in pkts] == [
        (5, 1200, 2400), (6, 2400, 3600), (7, 3600, 4200)]

    # 回执前换 B：载荷重置 1400，立即重发全部未确认 -> #8..#10
    sw = post(client, "/api/switch", {"line": "B"})["result"]
    assert sw["cur_payload"] == 1400 and sw["round"] == 3
    assert [(p["id"], p["start"], p["end"], p["line"]) for p in sw["packets"]] == [
        (8, 1200, 2600, "B"), (9, 2600, 4000, "B"), (10, 4000, 4200, "B")]

    # 乱序 1：先回执 B 线 #9 [2600,4000)
    st = ack(client, 9)["state"]
    assert st["acked_count"] == 2600  # [0,1200)+[2600,4000)

    # 乱序 2：旧 A 线 #5 [1200,2400) 照样确认
    assert ack(client, 5)["state"]["acked_count"] == 3800

    # 乱序 3：B 线 #8 [1200,2600) 与 #5 大面积重叠，只新增 200
    r = ack(client, 8)
    assert r["added_bytes"] == 200
    assert r["state"]["acked_count"] == 4000
    assert r["state"]["complete"] is False

    # 旧 A 线 #6 完全落在已确认并集内：新增 0
    r = ack(client, 6)
    assert r["added_bytes"] == 0
    assert r["state"]["acked_count"] == 4000

    # B 线尾包 #10 补齐最后 200
    r = ack(client, 10)
    assert r["added_bytes"] == 200
    assert r["state"]["acked_count"] == 4200
    assert r["state"]["complete"] is True


def test_switch_with_no_prior_ack_exact_duplicates_count_once(client):
    reset(client, total=2800, A=(1500, 0, True), B=(1500, 60, True))
    burst(client, 2)  # #1 [0,1400) #2 [1400,2800)

    sw = post(client, "/api/switch", {"line": "B"})["result"]
    assert [(p["start"], p["end"]) for p in sw["packets"]] == [
        (0, 1400), (1400, 2800)]  # 完全重叠的新副本 #3 #4

    assert ack(client, 3)["state"]["acked_count"] == 1400
    r = ack(client, 1)             # 旧线相同区间的回执
    assert r["added_bytes"] == 0
    assert r["state"]["acked_count"] == 1400

    final = ack(client, 2)["state"]
    assert final["acked_count"] == 2800 and final["complete"] is True

    # 完成后重复/重叠回执仍幂等，但不再允许发新包或换线
    assert ack(client, 4)["added_bytes"] == 0
    assert client.post("/api/send-next").status_code == 409
    assert client.post("/api/switch", json={"line": "A"}).status_code == 409


def test_burst_all_sends_until_frontier(client):
    reset(client, total=3000, start=1000)
    state = burst(client, "all")
    assert [(p["start"], p["end"]) for p in state["packets"]] == [
        (0, 1000), (1000, 2000), (2000, 3000)]
    assert state["last_burst"]["sent"] == 3
    assert state["last_burst"]["all_remaining"] is True
    # 新字节已到末尾，再来普通连发会报 409 并提示用重发
    assert client.post("/api/send-burst", json={"count": 2}).status_code == 409


def test_send_next_advances_then_fills_gap(client):
    reset(client, total=3000, start=1000)
    p1 = post(client, "/api/send-next")["packet"]
    p2 = post(client, "/api/send-next")["packet"]
    p3 = post(client, "/api/send-next")["packet"]
    assert [(p["start"], p["end"]) for p in (p1, p2, p3)] == [
        (0, 1000), (1000, 2000), (2000, 3000)]

    ack(client, p1["id"])
    # 新字节已发完，send-next 转为补最早缺口 [1000,2000)
    p4 = post(client, "/api/send-next")["packet"]
    assert (p4["start"], p4["end"]) == (1000, 2000)


# ---------------------------------------------------------------- 分批连发：受理 / 分批 / 排他

def test_burst_accepted_immediately_and_executed_in_batches(client):
    reset(client, total=100000, start=1400, A=(1500, 0, True))
    rv = post(client, "/api/send-burst", {"count": 12, "interval_ms": 300})
    burst_id = rv["burst"]["id"]
    # 提交后立即返回受理结果，不等待发包完成
    assert rv["accepted"] is True
    assert rv["state"]["burst"]["state"] == "pending"
    assert rv["state"]["burst"]["sent"] == 0
    assert rv["state"]["burst_active"] is True
    assert rv["state"]["packets"] == []

    first = time.monotonic()
    state = wait_burst(client, timeout_s=10)
    elapsed = time.monotonic() - first
    assert state["last_burst"]["sent"] == 12
    assert state["last_burst"]["end_reason"] == "done"
    # 12 包 / 每批 5 包 → 3 批；两段批间隔 300ms 至少各等一次
    assert elapsed >= 0.55
    assert len(state["packets"]) == 12
    assert all(p["kind"] == "burst" and p["burst_id"] == burst_id
               for p in state["packets"])


def test_only_one_burst_at_a_time(client):
    reset(client, total=100000, start=1400, A=(1500, 0, True))
    post(client, "/api/send-burst", {"count": 20, "interval_ms": 2000})
    assert client.post("/api/send-burst",
                       json={"count": 3}).status_code == 409
    assert client.post("/api/send-burst",
                       json={"count": "all"}).status_code == 409
    # 停止后可再提交
    post(client, "/api/burst/stop")
    assert client.get("/api/state").get_json()["state"]["burst_active"] is False
    assert client.post("/api/send-burst", json={"count": 3}).status_code == 200


def test_burst_interval_validation(client):
    reset(client)
    for bad in (-1, 60001, "abc"):
        rv = client.post("/api/send-burst",
                         json={"count": 1, "interval_ms": bad})
        assert rv.status_code == 400, bad


def test_send_next_during_burst_takes_unfirst_sent_bytes(client):
    """连发期间单发：从下一段未首发字节取载荷，不占连发包数。"""
    reset(client, total=100000, start=1400, A=(1500, 0, True))
    rv = post(client, "/api/send-burst", {"count": 10, "interval_ms": 400})
    burst_id = rv["burst"]["id"]
    # 等它完成第一批
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        st = client.get("/api/state").get_json()["state"]
        if st["next_offset"] > 0:
            break
        time.sleep(0.02)
    st = client.get("/api/state").get_json()["state"]
    before_offset = st["next_offset"]
    sent_before = st["burst"]["sent"]

    pkt = post(client, "/api/send-next")["packet"]
    # 单发与连发取的是同一段“下一段未首发字节”
    assert pkt["start"] == before_offset
    assert pkt["burst_id"] is None and pkt["kind"] == "new"

    state = wait_burst(client)
    # 单发不占用连发的包数：连发仍按请求数发满（只要还有新字节）
    assert state["last_burst"]["sent"] == 10
    # 连发自己的 10 包加上单发 1 包，共 11 包且区间连续不重叠
    burst_pkts = [p for p in state["packets"] if p["burst_id"] == burst_id]
    assert len(burst_pkts) == 10
    assert state["next_offset"] == 11 * 1400
    spans = sorted((p["start"], p["end"]) for p in state["packets"])
    assert spans == [(i * 1400, (i + 1) * 1400) for i in range(11)]
    assert sent_before == 5


def test_send_next_to_frontier_ends_burst_with_eof(client):
    reset(client, total=4200, start=1400, A=(1500, 0, True))
    # 请求 5 包，但报表只剩 3 包：到末尾按实发数 3 收尾
    post(client, "/api/send-burst", {"count": 5, "interval_ms": 3000})
    state = wait_burst(client, timeout_s=2)
    assert state["last_burst"]["sent"] == 3
    assert state["last_burst"]["requested"] == 5
    assert state["last_burst"]["end_reason"] == "eof"
    assert state["next_offset"] == 4200
    # 连发已结束，此后单发不会再有新字节可发
    assert client.post("/api/send-next").status_code in (200, 409)


def test_burst_stops_on_switch_and_stale_packets_still_ack(client):
    reset(client, total=100000, start=1400, A=(1500, 0, True))
    rv = post(client, "/api/send-burst", {"count": 40, "interval_ms": 500})
    burst_id = rv["burst"]["id"]
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        st = client.get("/api/state").get_json()["state"]
        if st["next_offset"] > 0:
            break
        time.sleep(0.02)
    before = st["burst"]["sent"]
    assert before == BURST_BATCH_SIZE

    sw = post(client, "/api/switch", {"line": "B"})["state"]
    assert sw["burst_active"] is False
    assert sw["last_burst"]["end_reason"] == "switch"
    assert sw["last_burst"]["sent"] == before
    # 换线本身立即重发的包（kind=switch）
    assert sum(1 for p in sw["packets"] if p["kind"] == "switch") > 0

    # 等待远超批间隔，确认旧连发剩余包不再发出
    time.sleep(0.8)
    after = client.get("/api/state").get_json()["state"]
    assert after["burst_active"] is False
    assert after["last_burst"]["sent"] == before
    # 只新增了换线重发的包，没有新的连发包
    assert sum(1 for p in after["packets"] if p["burst_id"] == burst_id) == before

    # 同场已发的连发包，回执照原规则处理（重叠只计一次）
    first = next(p for p in after["packets"] if p["burst_id"] == burst_id)
    r = ack(client, first["id"])
    assert r["added_bytes"] == first["size"]


def test_burst_stops_on_shrink_and_on_reset(client):
    # A 通知关：连发射出的满包静默丢弃，超时缩包属“有效缩包”
    reset(client, total=100000, start=1400, A=(1500, 120, False))
    post(client, "/api/send-burst", {"count": 30, "interval_ms": 500})
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        st = client.get("/api/state").get_json()["state"]
        if st["next_offset"] > 0:
            break
        time.sleep(0.02)
    r = timeout(client, 1)["result"]
    assert r["shrunk"] is True
    st = client.get("/api/state").get_json()["state"]
    assert st["burst_active"] is False
    assert st["last_burst"]["end_reason"] == "shrink"

    # 重置也会停掉在途连发，并保留结束快照
    post(client, "/api/send-burst", {"count": 20, "interval_ms": 500})
    st = post(client, "/api/reset", make_config(total=4000))["state"]
    assert st["burst_active"] is False
    assert st["last_burst"]["end_reason"] == "reset"
    assert st["packets"] == [] and st["next_offset"] == 0


def test_burst_stops_on_effective_notice_shrink(client):
    reset(client, total=100000, start=1400, A=(1500, 120, True))
    post(client, "/api/send-burst", {"count": 30, "interval_ms": 500})
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        st = client.get("/api/state").get_json()["state"]
        if st["next_offset"] > 0:
            break
        time.sleep(0.02)
    r = notice(client, 1)["result"]
    assert r["applied"] is True
    st = client.get("/api/state").get_json()["state"]
    assert st["burst_active"] is False
    assert st["last_burst"]["end_reason"] == "shrink"


# ---------------------------------------------------------------- 分批连发：纯引擎确定性测试

def test_engine_burst_batches_with_fake_clock():
    cfg = DrillConfig.from_raw(make_config(total=100000, start=1400,
                                           A=(1500, 0, True)))
    clock = FakeClock()
    drill = Drill(cfg, clock=clock)

    b = drill.start_burst(12, 1000)
    assert b.sent == 0
    # 首批不等待批间隔，立即发出
    assert len(drill.burst_tick()) == BURST_BATCH_SIZE
    assert drill.burst.sent == 5
    clock.advance(0.5)
    assert drill.burst_tick() == []                        # 未到批间隔
    clock.advance(0.6)
    assert len(drill.burst_tick()) == BURST_BATCH_SIZE     # 第 2 批
    clock.advance(1.1)
    assert len(drill.burst_tick()) == 2                    # 收尾 2 包
    assert drill.burst is None
    assert drill.last_burst["sent"] == 12
    assert drill.last_burst["end_reason"] == "done"
    assert drill.next_offset == 12 * 1400


def test_engine_burst_respects_payload_change_midway():
    cfg = DrillConfig.from_raw(make_config(total=100000, start=1400,
                                           A=(1500, 0, True)))
    clock = FakeClock()
    drill = Drill(cfg, clock=clock)
    drill.start_burst(6, 1000)
    clock.advance(0.01)
    drill.burst_tick()
    # 测试用：模拟载荷在连发中途变化（真实场景由换线/缩包触发并停发）
    drill.cur_payload = 1000
    clock.advance(1.1)
    pkts = drill.burst_tick()
    assert [(p.start, p.end) for p in pkts] == [(7000, 8000)]
    assert drill.last_burst["sent"] == 6
