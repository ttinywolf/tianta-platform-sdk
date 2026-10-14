#!/usr/bin/env python3
"""Tests for the TIANTA high-level task layer (python/tianta_tasks.py).

Verifies the four business task objects:
  - structured post -> wait round-trip
  - on() subscribe callback delivery + off()
  - key mapping correct per task type
  - cross-task isolation (each task only consumes its own messages)

Run:  python3 tests/test_tianta_tasks.py
"""
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
import tianta as T
import tianta_tasks as TT


def make_db(tag):
    d = tempfile.mkdtemp(prefix=f"tasks-{tag}-")
    return os.path.join(d, "platform.dat")


def test_key_mapping():
    from tianta_tasks import (TmRecordDone, CmdFileDone, DownloadReq, SystemNotice)
    assert TmRecordDone.KEY == T.TI_KEY_TM_DONE
    assert CmdFileDone.KEY == T.TI_KEY_CMD_DONE
    assert DownloadReq.KEY == T.TI_KEY_DL_REQ
    assert SystemNotice.KEY == T.TI_KEY_SYS_NTF
    assert TT.TASK_BY_KEY[T.TI_KEY_TM_DONE] is TmRecordDone
    assert TT.task_factory(T.TI_KEY_DL_REQ) is DownloadReq
    print("[keys] four task keys map to correct classes")


def test_post_wait_roundtrip():
    db = make_db("rw")
    ch = T.TI_connect(db)
    # TM
    TmRecordDone = TT.TmRecordDone
    eid = TmRecordDone.post(ch, file_path="/data/tm-2026.bin", size=4096,
                            start_utc="2026-10-14T00:00:00Z",
                            end_utc="2026-10-14T01:00:00Z", station="Station-A")
    ev = TmRecordDone.wait(ch, timeout_ms=2000)
    assert ev is not None
    assert ev.file_path == "/data/tm-2026.bin"      # attribute access
    assert ev["size"] == 4096                        # dict access
    assert ev.station == "Station-A"
    assert ev.start_utc == "2026-10-14T00:00:00Z"
    # CMD
    Cmd = TT.CmdFileDone
    Cmd.post(ch, cmd_id="CMD-77", file_path="/cmds/yh.cnf",
             cmd_type="uplink", priority=9)
    cele = Cmd.wait(ch, timeout_ms=2000)
    assert cele is not None and cele.cmd_id == "CMD-77" and cele.priority == 9
    # DL
    DL = TT.DownloadReq
    DL.post(ch, req_id="req-0001", data_type="image", target="192.168.1.10")
    dlev = DL.wait(ch, timeout_ms=2000)
    assert dlev is not None and dlev.req_id == "req-0001" and dlev.data_type == "image"
    # NTF
    NT = TT.SystemNotice
    NT.post(ch, level="warn", title="电池低压", body="电压跌破阈值", code="BATT-LOW")
    ntev = NT.wait(ch, timeout_ms=2000)
    assert ntev is not None and ntev.level == "warn" and ntev.code == "BATT-LOW"
    print("[roundtrip] four task types post->wait with structured fields OK")


def test_subscribe_on_off():
    db = make_db("sub")
    ch = T.TI_connect(db)
    got = []
    tag = TT.SystemNotice.on(ch, lambda ev: got.append((ev.level, ev.title)))
    time.sleep(0.4)
    TT.SystemNotice.post(ch, level="error", title="A", body="b1")
    TT.SystemNotice.post(ch, level="info", title="B", body="b2")
    time.sleep(0.8)
    TT.SystemNotice.off(ch, tag)
    time.sleep(0.3)
    TT.SystemNotice.post(ch, level="info", title="C", body="b3")  # after off
    time.sleep(0.3)
    assert ("error", "A") in got, got
    assert ("info", "B") in got, got
    assert ("info", "C") not in got, f"delivered after off: {got}"
    print(f"[subscribe] on/off delivered {got}")


def test_cross_task_isolation():
    db = make_db("iso")
    ch = T.TI_connect(db)
    # post a TM and a CMD
    TT.TmRecordDone.post(ch, file_path="tm.bin", size=1)
    TT.CmdFileDone.post(ch, cmd_id="c1", file_path="c.bin")
    # waiting for TM should NOT consume the CMD message
    ev = TT.TmRecordDone.wait(ch, timeout_ms=1000)
    assert ev is not None and ev.file_path == "tm.bin"
    # CMD still pending
    assert T.TI_queue_pending(ch, key=T.TI_KEY_CMD_DONE) == 1
    assert T.TI_queue_pending(ch, key=T.TI_KEY_TM_DONE) == 0
    print("[isolation] TM/CMD tasks isolated per key")


def test_custom_extension_fields():
    """预定义字段之外的任意自定义字段应完整保留,且可用 metadata 辅助读写。"""
    db = make_db("ext")
    ch = T.TI_connect(db)
    TM = TT.TmRecordDone
    # 传入自定义/扩展字段(未在 FIELDS 声明)
    TM.post(ch, file_path="tm.bin", size=99,
            mission_id="MISS-2026",      # 自定义
            orbit_height_km=600,          # 自定义
            freq=145.8)                   # 自定义
    ev = TM.wait(ch, timeout_ms=2000)
    assert ev is not None
    # 预定义字段
    assert ev.file_path == "tm.bin" and ev.size == 99
    # 自定义字段:属性 + dict 访问均可
    assert ev["mission_id"] == "MISS-2026"
    assert ev.orbit_height_km == 600
    assert ev.freq == 145.8
    # metadata() 只返回自定义字段(不含预定义)
    md = ev.metadata()
    assert md == {"mission_id": "MISS-2026", "orbit_height_km": 600, "freq": 145.8}, md
    # add_metadata 追加
    ev.add_metadata("antenna", "S-BAND")
    assert ev.antenna == "S-BAND"
    assert "antenna" in ev.metadata()
    # on() 订阅路径同样保留自定义字段
    got = []
    tag = TT.DownloadReq.on(ch, lambda e: got.append(e.metadata()))
    time.sleep(0.4)
    TT.DownloadReq.post(ch, req_id="r1", data_type="image", custom_flag=1)
    time.sleep(0.8)
    TT.DownloadReq.off(ch, tag)
    assert got and got[0] == {"custom_flag": 1}, got
    print(f"[extension] custom fields preserved, metadata()={md}")


if __name__ == "__main__":
    test_key_mapping()
    test_post_wait_roundtrip()
    test_subscribe_on_off()
    test_cross_task_isolation()
    test_custom_extension_fields()
    print("\nALL TIANTA TASKS TESTS PASSED")
