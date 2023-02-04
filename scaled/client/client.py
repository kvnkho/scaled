import json
import logging
import threading
import uuid
from concurrent.futures import Future

from typing import Any, Callable, Dict, Iterable, List, Optional

import zmq

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.function import FunctionSerializer
from scaled.protocol.python.message import MessageVariant, MonitorRequest, MonitorResponse, Task, TaskEcho, TaskResult
from scaled.protocol.python.objects import MessageType


class Client:
    def __init__(self, config: ZMQConfig):
        self._stop_event = threading.Event()
        self._connector = SyncConnector(
            stop_event=self._stop_event,
            prefix="C",
            context=zmq.Context.instance(),
            socket_type=zmq.DEALER,
            bind_or_connect="connect",
            address=config,
            callback=self.__on_receive,
        )

        self._futures: Dict[bytes, Future] = dict()

        # debug info
        self._count: int = 0
        self._monitor_future: Optional[Future] = None

    def __del__(self):
        self.disconnect()

    def submit(self, fn: Callable, *args) -> Future:
        task_id = uuid.uuid1().bytes
        task = Task(task_id, FunctionSerializer.serialize_function(fn), FunctionSerializer.serialize_arguments(args))
        self._connector.send(MessageType.Task, task)

        future = Future()
        self._futures[task_id] = future
        return future

    def statistics(self):
        self._monitor_future = Future()
        self._connector.send(MessageType.MonitorRequest, MonitorRequest(b""))
        return self._monitor_future.result()

    def disconnect(self):
        self._stop_event.set()

    def __on_receive(self, message_type: MessageType, data: MessageVariant):
        match message_type:
            case MessageType.TaskEcho:
                self.__on_task_echo(data)
            case MessageType.TaskResult:
                self.__on_task_result(data)
            case MessageType.MonitorResponse:
                self.__on_monitor_response(data)
            case _:
                raise TypeError(f"Unknown {message_type=}")

    def __on_task_echo(self, data: TaskEcho):
        if data.task_id not in self._futures:
            return
        future = self._futures[data.task_id]
        future.set_running_or_notify_cancel()

    def __on_task_result(self, result: TaskResult):
        if result.task_id not in self._futures:
            return

        future = self._futures.pop(result.task_id)
        future.set_result(FunctionSerializer.deserialize_result(result.result))

        self._count += 1
        # logging.debug(f"finished: {self._count}, connector: {json.dumps(self._connector.monitor())}")

    def __on_monitor_response(self, data: MonitorResponse):
        self._monitor_future.set_result(data.data)
