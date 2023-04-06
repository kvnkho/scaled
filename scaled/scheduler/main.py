import asyncio
import functools
import json
import logging
import uuid
from typing import Tuple

import zmq.asyncio

from scaled.io.async_connector import AsyncConnector
from scaled.io.config import CLEANUP_INTERVAL_SECONDS, STATUS_REPORT_INTERVAL_SECONDS
from scaled.scheduler.client_manager.vanilla import VanillaClientManager
from scaled.scheduler.function_manager.vanilla import VanillaFunctionManager
from scaled.scheduler.status_reporter import StatusReporter
from scaled.utility.event_loop import create_async_loop_routine
from scaled.utility.zmq_config import ZMQConfig, ZMQType
from scaled.io.async_binder import AsyncBinder
from scaled.protocol.python.message import MessageType, MessageVariant
from scaled.scheduler.task_manager.vanilla import VanillaTaskManager
from scaled.scheduler.worker_manager.vanilla import VanillaWorkerManager
from scaled.utility.logging.network import NetworkLogForwarder

class Scheduler:
    def __init__(
        self,
        address: ZMQConfig,
        io_threads: int,
        max_number_of_tasks_waiting: int,
        per_worker_queue_size: int,
        worker_timeout_seconds: int,
        function_retention_seconds: int,
        load_balance_seconds: int,
        load_balance_trigger_times: int,
        network_log_forwarding_address: ZMQConfig,
    ):
        if address.type != ZMQType.tcp:
            raise TypeError(f"{self.__class__.__name__}: scheduler address must be tcp type: {address.to_address()}")

        self._address_monitor = ZMQConfig(type=ZMQType.ipc, host=f"/tmp/{address.host}_{address.port}_monitor")

        logging.info(f"{self.__class__.__name__}: monitor address is {self._address_monitor.to_address()}")
        context = zmq.asyncio.Context()
        self._binder = AsyncBinder(prefix="S", address=address, io_threads=io_threads)
        self._binder_monitor = AsyncConnector(
            context = context,
            prefix="R",
            socket_type=zmq.PUB,
            address=self._address_monitor,
            bind_or_connect="bind",
            callback=None,
        )

        self._client_manager = VanillaClientManager()
        self._function_manager = VanillaFunctionManager(function_retention_seconds=function_retention_seconds)
        self._task_manager = VanillaTaskManager(max_number_of_tasks_waiting=max_number_of_tasks_waiting)
        self._worker_manager = VanillaWorkerManager(
            per_worker_queue_size=per_worker_queue_size,
            timeout_seconds=worker_timeout_seconds,
            load_balance_seconds=load_balance_seconds,
            load_balance_trigger_times=load_balance_trigger_times,
        )
        self._status_reporter = StatusReporter(self._binder_monitor)

        # Setting up log forwarder responsible for bringing worker logs to client
        # It subscribes to workers and publishes to the client
        if network_log_forwarding_address:
            address_pair = _get_log_pub_sub_addresses(network_log_forwarding_address)
            self._log_forwarder = NetworkLogForwarder(
                frontend_connector = AsyncConnector(
                    context = context,
                    prefix="WL",
                    socket_type=zmq.SUB,
                    address=address_pair[0],
                    bind_or_connect="bind",
                    callback=None,
                ),
                backend_connector = AsyncConnector(
                    context = context,
                    prefix="CL",
                    socket_type=zmq.PUB,
                    address=address_pair[1],
                    bind_or_connect="bind",
                    callback=None,
                ),
            )
        else:
            self._log_forwarder = NetworkLogForwarder()

        self._binder.register(self.on_receive_message)
        self._function_manager.hook(self._binder)
        self._task_manager.hook(self._binder, self._client_manager, self._function_manager, self._worker_manager)
        self._worker_manager.hook(self._binder, self._task_manager)

        self._status_reporter.register_manager(
            [self._binder, self._client_manager, self._function_manager, self._task_manager, self._worker_manager]
        )

    async def on_receive_message(self, source: bytes, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.Heartbeat:
            await self._worker_manager.on_heartbeat(source, message)
            return

        if message_type == MessageType.BalanceResponse:
            await self._worker_manager.on_balance_response(message)
            return

        if message_type == MessageType.Task:
            await self._task_manager.on_task_new(source, message)
            return

        if message_type == MessageType.TaskCancel:
            await self._task_manager.on_task_cancel(source, message)
            return

        if message_type == MessageType.TaskCancelEcho:
            await self._worker_manager.on_task_cancel_echo(source, message)
            return

        if message_type == MessageType.TaskResult:
            await self._worker_manager.on_task_done(message)
            return

        if message_type == MessageType.FunctionRequest:
            await self._function_manager.on_function(source, message)
            return

        if message_type == MessageType.DisconnectRequest:
            await self._worker_manager.on_disconnect(source, message)
            return

        logging.error(f"{self.__class__.__name__}: unknown {message_type} from {source=}: {message}")

    async def get_loops(self):
        try:
            await asyncio.gather(
                create_async_loop_routine(self._binder.routine, 0),
                create_async_loop_routine(self._task_manager.routine, 0),
                create_async_loop_routine(self._function_manager.routine, CLEANUP_INTERVAL_SECONDS),
                create_async_loop_routine(self._worker_manager.routine, CLEANUP_INTERVAL_SECONDS),
                create_async_loop_routine(self._status_reporter.routine, STATUS_REPORT_INTERVAL_SECONDS),
                create_async_loop_routine(self._log_forwarder.routine, 0),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            pass

def _get_log_pub_sub_addresses(address) -> Tuple[ZMQConfig, ZMQConfig]:
    """
    Takes a ZMQConfig. Returns the address as the subcriber address,
    and adds one to the port to generate the publisher address.

    These will be used to forward logs from workers to the Client.
    """
    sub_address = ZMQConfig.to_address(address)
    host = sub_address.split(':')[0]
    pub_port = int(sub_address.split(':')[1])
    pub_address = host + ":" + pub_port
    return ZMQConfig.from_string(sub_address), ZMQConfig.from_string(pub_address)

@functools.wraps(Scheduler)
async def scheduler_main(*args, **kwargs):
    scheduler = Scheduler(*args, **kwargs)
    await scheduler.get_loops()