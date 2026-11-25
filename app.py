"""VPN 换线 / MTU 黑洞演练页 —— Flask 入口。

单实例内存状态 + 一把锁，适合单人演练；部署见 Dockerfile / compose.yaml
（gunicorn 固定 1 个 worker、多线程，保证内存状态一致）。
"""

from __future__ import annotations

import logging
import threading

from flask import Flask, jsonify, render_template, request

from drill_engine import Drill, DrillConfig, DrillError

app = Flask(__name__)

_LOCK = threading.RLock()
_DRILL = Drill()

# 连发由一条后台调度线程按批推进；所有状态访问仍用同一把 _LOCK。
_BURST_WAKE = threading.Event()
_SCHEDULER_STOP = threading.Event()


def _scheduler_loop() -> None:
    """按批执行连发：到点发一批、记录状态，再按批间隔等待。"""
    while not _SCHEDULER_STOP.is_set():
        with _LOCK:
            try:
                drill = _DRILL
                drill.burst_tick()
                wait = drill.burst_wait()
            except Exception:  # 调度线程不能死，出错记日志并稍后重试
                logging.exception("连发调度异常")
                wait = 0.25
        _BURST_WAKE.wait(wait)
        _BURST_WAKE.clear()


def _start_scheduler() -> None:
    thread = threading.Thread(target=_scheduler_loop, name="burst-scheduler",
                              daemon=True)
    thread.start()


_start_scheduler()


def _wake_scheduler() -> None:
    _BURST_WAKE.set()


def _json_body() -> dict:
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


@app.errorhandler(DrillError)
def handle_drill_error(err: DrillError):
    return jsonify({"ok": False, "error": err.message}), err.status


@app.errorhandler(404)
def handle_404(_err):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "接口不存在"}), 404
    return "Not Found", 404


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/state")
def get_state():
    with _LOCK:
        return jsonify({"ok": True, "state": _DRILL.state_dict()})


@app.post("/api/reset")
def reset_drill():
    data = _json_body()
    with _LOCK:
        cfg = DrillConfig.from_raw(data if data else None)
        # 原地重置：旧连发的剩余包立即停发，并保留其结束快照供页面显示。
        _DRILL.reset(cfg)
        _wake_scheduler()
        return jsonify({"ok": True, "state": _DRILL.state_dict()})


@app.post("/api/send-next")
def send_next():
    with _LOCK:
        pkt = _DRILL.send_next()
        _wake_scheduler()
        return jsonify({"ok": True, "packet": pkt.to_dict(),
                        "state": _DRILL.state_dict()})


@app.post("/api/send-burst")
def send_burst():
    """受理一次分批连发：立即返回受理结果，后台按批发出。"""
    data = _json_body()
    count = data.get("count")
    with _LOCK:
        burst = _DRILL.start_burst(
            None if count in (None, "", "all") else count,
            data.get("interval_ms", 0),
        )
        _wake_scheduler()
        return jsonify({"ok": True, "accepted": True,
                        "burst": burst.to_dict(),
                        "state": _DRILL.state_dict()})


@app.post("/api/resend-missing")
def resend_missing():
    with _LOCK:
        pkts = _DRILL.resend_missing(kind="missing")
        return jsonify({"ok": True, "packets": [p.to_dict() for p in pkts],
                        "state": _DRILL.state_dict()})


@app.post("/api/switch")
def switch_line():
    line = _json_body().get("line", "")
    with _LOCK:
        result = _DRILL.switch_line(str(line).upper())
        _wake_scheduler()
        return jsonify({"ok": True, "result": result,
                        "state": _DRILL.state_dict()})


@app.post("/api/packets/<int:pid>/ack")
def ack_packet(pid: int):
    with _LOCK:
        _pkt, added = _DRILL.acknowledge(pid)
        _wake_scheduler()
        return jsonify({"ok": True, "added_bytes": added,
                        "state": _DRILL.state_dict()})


@app.post("/api/packets/<int:pid>/timeout")
def timeout_packet(pid: int):
    with _LOCK:
        result = _DRILL.trigger_timeout(pid)
        _wake_scheduler()
        return jsonify({"ok": True, "result": result,
                        "state": _DRILL.state_dict()})


@app.post("/api/packets/<int:pid>/deliver-notice")
def deliver_notice(pid: int):
    with _LOCK:
        result = _DRILL.deliver_notice(pid)
        _wake_scheduler()
        return jsonify({"ok": True, "result": result,
                        "state": _DRILL.state_dict()})


@app.post("/api/burst/stop")
def stop_burst():
    with _LOCK:
        _DRILL.stop_burst("user")
        _wake_scheduler()
        return jsonify({"ok": True, "state": _DRILL.state_dict()})


if __name__ == "__main__":
    # 本机直接 `python app.py` 时使用；容器内走 gunicorn。
    app.run(host="0.0.0.0", port=8000, threaded=True)
