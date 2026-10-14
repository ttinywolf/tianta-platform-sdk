"""
TIANTA 高层任务封装层 — 四类业务的"开箱即用"对象。

基于 python/tianta.py(基础 + 任务 API)之上,把四类卫星业务任务封装成
**任务专属对象**,提供结构化 payload 和"订阅回调 + 阻塞等待"双接口:

    1. TmRecordDone   — 卫星遥测数据文件记录完成 (key=TI_KEY_TM_DONE)
    2. CmdFileDone    — 新的指令文件存储完成     (key=TI_KEY_CMD_DONE)
    3. DownloadReq    — 有下传申请               (key=TI_KEY_DL_REQ)
    4. SystemNotice   — 有系统通知               (key=TI_KEY_SYS_NTF)

用法:

    from tianta_tasks import TmRecordDone

    # 订阅回调(后台线程推送)
    TmRecordDone.on(ch, lambda ev: print("收到遥测完成", ev.file_path))

    # 阻塞等待
    ev = TmRecordDone.wait(ch, timeout_ms=5000)

    # 投递
    ev_id = TmRecordDone.post(ch, file_path="/data/tm.bin", size=1024)

事件对象是 dict + 属性访问(ev.file_path / ev["file_path"]),payload 自动
序列化为 JSON 存储,消费端自动解析回对象。
"""
import json
import time

import tianta as T

_TAG = {}  # per-task subscribe tag registry: key -> set(tag)


class TaskEvent(dict):
    """结构化任务事件:支持 dict['f'] 与 ev.f 两种访问,并携带触发时间。

    扩展性:消息 payload 顶层可携带任意用户自定义/扩展字段(不在预定义
    FIELDS 里的 key),SDK 不做白名单过滤,一律保留并可读写。用
    `TaskEvent.metadata()` 读取所有扩展字段,`TaskEvent.add_metadata(k, v)`
    追加扩展字段。
    """

    def __init__(self, *args, predefined=frozenset(), **kwargs):
        kwargs.setdefault("_ts", int(time.time() * 1000))
        super().__init__(*args, **kwargs)
        self.__dict__ = self
        # predefined field names of the owning task type; used to tell
        # "extended/custom" fields apart from the built-in convenience fields.
        self._predefined = frozenset(predefined)

    # -- 扩展字段辅助 --------------------------------------------------------
    def add_metadata(self, key, value):
        """追加/覆盖一个自定义扩展字段。返回 self 便于链式。"""
        if key == "_ts" or key == "_predefined":
            raise ValueError(f"`{key}` is reserved")
        self[key] = value
        return self

    def metadata(self):
        """返回仅包含自定义扩展字段(非预定义)的 dict。"""
        return {k: v for k, v in self.items()
                if k not in self._predefined and k not in ("_ts", "_predefined")}


class _BaseTask:
    """任务对象基类:post / on / wait / off 四接口,key 到具体任务。

    子类通过 class 属性声明:KEY(队列键)、FIELDS(结构化字段默认值)。

    扩展性:FIELDS 仅作为"预定义便捷字段 + 默认值";`post()` 可传入任意
    自定义字段(不限于 FIELDS),统统保留到 payload 顶层,`wait`/`on` 返回的
    TaskEvent 完整携带,可通过 `ev.metadata()` / `ev.add_metadata()` 读写。
    """

    KEY = None
    FIELDS = {}  # field -> default value

    # -- 生产 ---------------------------------------------------------------
    @classmethod
    def post(cls, ch, **fields):
        """投递一条该类型任务,返回事件 id。

        字段自动做结构性默认补全;未在 FIELDS 中声明的自定义字段会被原样
        保留到 payload 顶层,便于扩展。
        """
        payload = _build(cls.FIELDS, fields)
        return T.TI_task_post(ch, cls.KEY, json.dumps(payload))

    # -- 消费:订阅回调 -------------------------------------------------------
    @classmethod
    def on(cls, ch, callback, tag=None):
        """订阅该任务。收到即回调 callback(TaskEvent)。返回唯一 tag。"""

        def _cb(raw):
            callback(_to_event(cls, raw))

        tag = T.TI_task_subscribe(ch, cls.KEY, _cb, tag=tag)
        _TAG.setdefault(cls.KEY, set()).add(tag)
        return tag

    @classmethod
    def off(cls, ch, tag):
        """退订。"""
        T.TI_task_unsubscribe(ch, cls.KEY, tag)
        _TAG.get(cls.KEY, set()).discard(tag)

    # -- 消费:阻塞等待 -------------------------------------------------------
    @classmethod
    def wait(cls, ch, timeout_ms=0):
        """阻塞等待一条该类型任务,返回 TaskEvent;超时或空返回 None。"""
        raw = T.TI_task_wait(ch, cls.KEY, timeout_ms=timeout_ms)
        return _to_event(cls, raw) if raw else None


def _to_event(task_cls, raw):
    """把原始 JSON 字符串还原为带预定义字段集信息的 TaskEvent。"""
    return TaskEvent(json.loads(raw), predefined=task_cls.FIELDS.keys())


def _build(field_defaults, given):
    """合并给定字段与默认字段。

    - 预定义字段:给定值优先,未提供时用默认值。
    - 自定义字段:任何不在 FIELDS 中的 key 都原样保留到顶层(不设默认)。
    - 空字符串/None 的给定值也保留(允许显式置空),不做丢弃。
    """
    out = dict(field_defaults)
    out.update(given)
    return out


# ---------------------------------------------------------------------------
# 1) 卫星遥测数据文件记录完成
# ---------------------------------------------------------------------------
class TmRecordDone(_BaseTask):
    """卫星遥测(TM)数据文件记录完成事件。

    post(TmRecordDone...) 字段:
      file_path : 遥测文件路径
      size      : 文件字节数
      start_utc / end_utc : 采样起止(ISO8601 字符串,可选)
      station   : 测控站标识(可选)
    """
    KEY = T.TI_KEY_TM_DONE
    FIELDS = {
        "file_path": "",
        "size": 0,
        "start_utc": "",
        "end_utc": "",
        "station": "",
    }


# ---------------------------------------------------------------------------
# 2) 新的指令文件存储完成
# ---------------------------------------------------------------------------
class CmdFileDone(_BaseTask):
    """新指令文件存储完成事件。

    post(...) 字段:
      cmd_id    : 指令/任务标识
      file_path : 指令文件路径
      cmd_type  : 指令类型(上注/遥控/程序加载...)
      priority  : 优先级 0-9
    """
    KEY = T.TI_KEY_CMD_DONE
    FIELDS = {
        "cmd_id": "",
        "file_path": "",
        "cmd_type": "",
        "priority": 5,
    }


# ---------------------------------------------------------------------------
# 3) 有下传申请
# ---------------------------------------------------------------------------
class DownloadReq(_BaseTask):
    """下传申请事件。

    post(...) 字段:
      req_id     : 申请号
      data_type  : 申请下传的数据类型(遥测/图像/日志...)
      target     : 下传目标地址
      priority   : 优先级
    """
    KEY = T.TI_KEY_DL_REQ
    FIELDS = {
        "req_id": "",
        "data_type": "",
        "target": "",
        "priority": 5,
    }


# ---------------------------------------------------------------------------
# 4) 有系统通知
# ---------------------------------------------------------------------------
class SystemNotice(_BaseTask):
    """系统通知事件。

    post(...) 字段:
      level : 通知级别 info/warn/error
      title : 标题
      body  : 内容
      code  : 可选的错误/状态码
    """
    KEY = T.TI_KEY_SYS_NTF
    FIELDS = {
        "level": "info",
        "title": "",
        "body": "",
        "code": "",
    }


# 统一注册表,便于按 key 反查任务类
TASK_BY_KEY = {c.KEY: c for c in (TmRecordDone, CmdFileDone, DownloadReq, SystemNotice)}


def task_factory(key):
    """根据 key 返回对应的任务类。"""
    return TASK_BY_KEY[key]
