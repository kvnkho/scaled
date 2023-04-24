import asyncio
import multiprocessing
import signal
import time
from typing import Optional, Tuple

import zmq.asyncio

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import BalanceResponse, DisconnectRequest, MessageType, MessageVariant
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.utility.event_loop import create_async_loop_routine, register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.agent.heartbeat import VanillaHeartbeatManager
from scaled.worker.agent.task_manager import VanillaTaskManager
from scaled.worker.agent.processor_manager import VanillaProcessorManager


class Worker(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        event_loop: str,
        address: ZMQConfig,
        heartbeat_interval_seconds: int,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        serializer: FunctionSerializerType,
        function_retention_seconds: int,
        death_timeout_seconds: int,
    ):
        multiprocessing.Process.__init__(self, name="Agent")

        self._event_loop = event_loop
        self._address = address

        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._serializer = serializer
        self._function_retention_seconds = function_retention_seconds
        self._death_timeout_seconds = death_timeout_seconds

        self._connector_external: Optional[AsyncConnector] = None
        self._task_manager: Optional[VanillaTaskManager] = None
        self._heartbeat: Optional[VanillaHeartbeatManager] = None
        self._processor_manager: Optional[VanillaProcessorManager] = None
        self._scheduler_last_seen = time.time()

    @property
    def identity(self):
        return self._connector_external.identity

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def __initialize(self):
        register_event_loop(self._event_loop)

        context = zmq.asyncio.Context()
        self._connector_external = AsyncConnector(
            context=context,
            prefix="W",
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
        )

        # This connector is used for PUB/SUB uses like scheduler heartbeat
        self._connector_subscribe = AsyncConnector(
            context=context,
            prefix="WT",
            socket_type=zmq.SUB,
            address=__get_next_address(self._address),
            bind_or_connect="connect",
            callback=self.__on_receive_subscribe,
        )

        self._task_manager = VanillaTaskManager()
        self._heartbeat = VanillaHeartbeatManager()
        self._processor_manager = VanillaProcessorManager(
            event_loop=self._event_loop,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            function_retention_seconds=self._function_retention_seconds,
            serializer=self._serializer,
        )

        # register
        self._task_manager.register(connector=self._connector_external, processor_manager=self._processor_manager)
        self._heartbeat.register(connector_external=self._connector_external, worker_task_manager=self._task_manager)
        self._processor_manager.register(
            heartbeat=self._heartbeat, task_manager=self._task_manager, connector_external=self._connector_external
        )

        self._loop = asyncio.get_event_loop()
        self.__register_signal()
        self._task = self._loop.create_task(self.__get_loops())

    async def __on_receive_external(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.Task:
            await self._task_manager.on_task_new(message)
            return

        if message_type == MessageType.TaskCancel:
            await self._task_manager.on_cancel_task(message)
            return

        if message_type == MessageType.BalanceRequest:
            task_ids = self._task_manager.on_balance_request(message)
            await self._connector_external.send(MessageType.BalanceResponse, BalanceResponse(task_ids))
            return

        if message_type == MessageType.FunctionResponse:
            await self._processor_manager.on_add_function(message)
            return

        raise TypeError(f"Unknown {message_type=} {message=}")

    async def __on_receive_subscribe(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.SchedulerHeartbeat:
            # Refresh scheduler last seen
            self._scheduler_last_seen = time.time()
            return

        # Client issues command to shut down worker
        if message_type == MessageType.ClientShutdown:
            self.__destroy()

        # Scheduler will also be emitting a status
        if message_type == MessageType.SchedulerStatus:
            pass

    async def __get_loops(self):
        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._task_manager.routine, 0),
                create_async_loop_routine(self._heartbeat.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._processor_manager.routine, 0),
                create_async_loop_routine(self.__check_death_timeout, 30),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass

        await self._connector_external.send(
            MessageType.DisconnectRequest, DisconnectRequest(self._connector_external.identity)
        )

        self._connector_external.destroy()
        self._processor_manager.destroy()

    async def __check_death_timeout(self):
        # Shutdown the worker if it hasn't heard from the scheduler
        if (time.time() - self._scheduler_last_seen) >= self._death_timeout_seconds:
            self.__destroy()

    def __run_forever(self):
        self._loop.run_until_complete(self._task)

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)
        self._loop.add_signal_handler(signal.SIGTERM, self.__destroy)

    def __destroy(self):
        self._task.cancel()

def __get_next_address(address: ZMQConfig) -> Tuple[ZMQConfig, ZMQConfig]:
    """
    Takes a ZMQConfig. Returns the next port.
    """
    address_str = ZMQConfig.to_address(address)
    host = ':'.join(ZMQConfig.to_address(address).split(':')[0:2])
    port = int(address_str.split(':')[2]) + 1
    next_address = f"{host}:{port}"

    return ZMQConfig.from_string(next_address)