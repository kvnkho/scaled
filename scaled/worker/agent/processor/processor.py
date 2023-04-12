import logging
import multiprocessing
import pickle
import threading
import time
from typing import Callable, Dict, Optional

import tblib.pickling_support
import zmq

from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.message import (
    FunctionRequest,
    FunctionRequestType,
    MessageType,
    MessageVariant,
    Task,
    TaskResult,
    TaskStatus,
)
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.agent.processor.memory_cleaner import MemoryCleaner


class Processor(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        event_loop: str,
        address: ZMQConfig,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        serializer: FunctionSerializerType,
    ):
        multiprocessing.Process.__init__(self, name="Processor")

        self._event_loop = event_loop
        self._address = address

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._serializer = serializer

        self._memory_cleaner: Optional[MemoryCleaner] = None
        self._cached_functions: Dict[bytes, Callable] = {}

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def __initialize(self):
        tblib.pickling_support.install()

        self._connector = SyncConnector(
            stop_event=threading.Event(),
            prefix="IP",
            context=zmq.Context(),
            socket_type=zmq.PAIR,
            bind_or_connect="connect",
            address=self._address,
            callback=self.__on_connector_receive,
            exit_callback=None,
            daemonic=False,
        )

        self._memory_cleaner = MemoryCleaner(
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
        )
        self._memory_cleaner.start()

    def __run_forever(self):
        try:
            self._connector.run()
        except KeyboardInterrupt:
            pass

    def __on_connector_receive(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.FunctionRequest:
            self.__on_receive_function_request(message)
            return

        if message_type == MessageType.Task:
            self.__on_received_task(message)
            return

        logging.error(f"unknown {message=}")

    def __on_receive_function_request(self, request: FunctionRequest):
        if request.type == FunctionRequestType.Add:
            self._cached_functions[request.function_id] = self._serializer.deserialize_function(request.content)
            return

        if request.type == FunctionRequestType.Delete:
            self._cached_functions.pop(request.function_id)
            return

        logging.error(f"unknown request function request type {request=}")

    def __on_received_task(self, task: Task):
        begin = time.monotonic()
        try:
            function = self._cached_functions[task.function_id]
            args = self._serializer.deserialize_arguments(tuple(arg.data for arg in task.function_args))
            result = function(*args)
            result_bytes = self._serializer.serialize_result(result)
            self._connector.send_immediately(
                MessageType.TaskResult,
                TaskResult(task.task_id, TaskStatus.Success, time.monotonic() - begin, result_bytes),
            )

        except Exception as e:
            logging.exception(f"exception when processing task_id={task.task_id.hex()}:")
            self._connector.send_immediately(
                MessageType.TaskResult,
                TaskResult(
                    task.task_id,
                    TaskStatus.Failed,
                    time.monotonic() - begin,
                    pickle.dumps(e, protocol=pickle.HIGHEST_PROTOCOL),
                ),
            )