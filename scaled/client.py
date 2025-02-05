import hashlib
import logging
import pickle
import threading
import uuid
from collections import defaultdict
from concurrent.futures import Future
from graphlib import TopologicalSorter
from inspect import signature

from typing import Any, Callable, Dict, List, Tuple, Union

import zmq

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.protocol.python.serializer.default import DefaultSerializer
from scaled.protocol.python.message import (
    Argument,
    ArgumentType,
    FunctionRequest,
    FunctionRequestType,
    FunctionResponse,
    FunctionResponseType,
    GraphTask,
    GraphTaskCancel,
    GraphTaskResult,
    MessageType,
    MessageVariant,
    Task,
    TaskCancel,
    TaskEcho,
    TaskEchoStatus,
    TaskResult,
    TaskStatus,
)


class ScaledDisconnect(Exception):
    pass


class Client:
    def __init__(self, address: str, serializer: FunctionSerializerType = DefaultSerializer()):
        self._address = address
        self._serializer = serializer

        self._stop_event = threading.Event()
        self._connector = SyncConnector(
            stop_event=self._stop_event,
            prefix="C",
            context=zmq.Context.instance(),
            socket_type=zmq.DEALER,
            bind_or_connect="connect",
            address=ZMQConfig.from_string(address),
            callback=self.__on_receive,
            exit_callback=self.__on_exit,
            daemonic=True,
        )
        self._connector.start()
        logging.info(f"ScaledClient: connect to {address}")

        self._function_to_function_id_cache: Dict[Callable, Tuple[bytes, bytes]] = dict()

        self._task_id_to_task: Dict[bytes, Task] = dict()
        self._task_id_to_function: Dict[bytes, bytes] = dict()
        self._task_id_to_future: Dict[bytes, Future] = dict()

        self._function_id_to_not_ready_tasks: Dict[bytes, List[Task]] = defaultdict(list)

        self._graph_task_id_to_futures: Dict[bytes, List[Future]] = dict()

    def __del__(self):
        logging.info(f"ScaledClient: disconnect from {self._address}")
        self.disconnect()

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        function_id, function_bytes = self.__get_function_id(fn)

        task_id = uuid.uuid1().bytes
        all_args = Client.__convert_kwargs_to_args(fn, args, kwargs)

        task = Task(
            task_id,
            function_id,
            [Argument(ArgumentType.Data, self._serializer.serialize_argument(data)) for data in all_args],
        )
        self._task_id_to_task[task_id] = task
        self._task_id_to_function[task_id] = function_bytes

        self.__on_buffer_task_send(task)

        future = Future()
        self._task_id_to_future[task_id] = future
        return future

    def submit_graph(self, graph: Dict[str, Union[Any, Tuple[Callable, Any, ...]]], keys: List[str]) -> List[Future]:
        """
        graph = {
            "a": 1,
            "b": 2,
            "c": (inc, "a"),
            "d": (inc, "b"),
            "d": (add, "c", "d")
        }
        """

        node_name_to_data_argument, graph = self.__split_data_and_graph(graph)
        self.__check_graph(node_name_to_data_argument, graph, keys)

        graph, futures = self.__construct_graph(node_name_to_data_argument, graph, keys)
        self._connector.send(MessageType.GraphTask, graph)
        self._graph_task_id_to_futures[graph.task_id] = futures
        return futures

    def disconnect(self):
        self._stop_event.set()

    def __on_receive(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.TaskEcho:
            self.__on_task_echo(message)
            return

        if message_type == MessageType.FunctionResponse:
            self.__on_function_response(message)
            return

        if message_type == MessageType.TaskResult:
            self.__on_task_result(message)
            return

        if message_type == MessageType.GraphTaskResult:
            self.__on_graph_task_result(message)
            return

        raise TypeError(f"Unknown {message_type=}")

    def __on_task_echo(self, task_echo: TaskEcho):
        if task_echo.task_id not in self._task_id_to_task:
            return

        if task_echo.status == TaskEchoStatus.Duplicated:
            return

        if task_echo.status == TaskEchoStatus.FunctionNotExists:
            task = self._task_id_to_task[task_echo.task_id]
            self.__on_buffer_task_send(task)
            return

        if task_echo.status == TaskEchoStatus.NoWorker:
            raise NotImplementedError(f"please implement that handles no worker error")

        assert task_echo.status == TaskEchoStatus.SubmitOK, f"Unknown task status: " f"{task_echo=}"

        self._task_id_to_task.pop(task_echo.task_id)
        self._task_id_to_function.pop(task_echo.task_id)
        self._task_id_to_future[task_echo.task_id].set_running_or_notify_cancel()

    def __on_buffer_task_send(self, task):
        if task.function_id not in self._function_id_to_not_ready_tasks:
            function_bytes = self._task_id_to_function[task.task_id]
            self._connector.send(
                MessageType.FunctionRequest,
                FunctionRequest(FunctionRequestType.Add, function_id=task.function_id, content=function_bytes),
            )

        self._function_id_to_not_ready_tasks[task.function_id].append(task)

    def __on_function_response(self, response: FunctionResponse):
        assert response.status in {FunctionResponseType.OK, FunctionResponseType.Duplicated}
        if response.function_id not in self._function_id_to_not_ready_tasks:
            return

        for task in self._function_id_to_not_ready_tasks.pop(response.function_id):
            self._connector.send(MessageType.Task, task)

    def __on_task_result(self, result: TaskResult):
        if result.task_id not in self._task_id_to_future:
            return

        future = self._task_id_to_future.pop(result.task_id)
        if result.status == TaskStatus.Success:
            future.set_result(self._serializer.deserialize_result(result.result))
            return

        if result.status == TaskStatus.Failed:
            future.set_exception(pickle.loads(result.result))
            return

    def __on_graph_task_result(self, graph_result: GraphTaskResult):
        if graph_result.task_id not in self._graph_task_id_to_futures:
            return

        futures = self._graph_task_id_to_futures.pop(graph_result.task_id)
        if graph_result.status == TaskStatus.Success:
            for i, result in enumerate(graph_result.results):
                futures[i].set_result(self._serializer.deserialize_result(result))
            return

        if graph_result.status == TaskStatus.Failed:
            exception = pickle.loads(graph_result.results[0])
            for i, result in enumerate(graph_result.results):
                futures[i].set_exception(exception)
            return

    def __on_exit(self):
        if self._task_id_to_future:
            logging.info(f"canceling {len(self._task_id_to_future)} task(s)")
            for task_id, future in self._task_id_to_future.items():
                self._connector.send_immediately(MessageType.TaskCancel, TaskCancel(task_id))
                future.set_exception(ScaledDisconnect(f"disconnected from {self._address}"))

        if self._graph_task_id_to_futures:
            logging.info(f"canceling {len(self._graph_task_id_to_futures)} graph task(s)")
            for task_id, future in self._graph_task_id_to_futures:
                self._connector.send_immediately(MessageType.GraphTaskCancel, GraphTaskCancel(task_id))
                future.set_exception(ScaledDisconnect(f"disconnected from {self._address}"))

    def __get_function_id(self, fn: Callable) -> Tuple[bytes, bytes]:
        if fn in self._function_to_function_id_cache:
            return self._function_to_function_id_cache[fn]

        function_id, function_bytes = self.__generate_function_id_bytes(fn)
        self._function_to_function_id_cache[fn] = (function_id, function_bytes)
        return function_id, function_bytes

    def __generate_function_id_bytes(self, fn) -> Tuple[bytes, bytes]:
        function_bytes = self._serializer.serialize_function(fn)
        function_id = hashlib.md5(function_bytes).digest()
        return function_id, function_bytes

    @staticmethod
    def __convert_kwargs_to_args(fn: Callable, args: Tuple[Any], kwargs: Dict[str, Any]) -> Tuple:
        all_params = [p for p in signature(fn).parameters.values()]

        params = [p for p in all_params if p.kind in {p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD}]

        if len(args) >= len(params):
            return args

        number_of_required = len([p for p in params if p.default is p.empty])

        args = list(args)
        kwargs = kwargs.copy()
        kwargs.update({p.name: p.default for p in all_params if p.kind == p.KEYWORD_ONLY if p.default != p.empty})

        for p in params[len(args) : number_of_required]:
            try:
                args.append(kwargs.pop(p.name))
            except KeyError:
                missing = tuple(p.name for p in params[len(args) : number_of_required])
                raise TypeError(f"{fn} missing {len(missing)} arguments: {missing}")

        for p in params[len(args) :]:
            args.append(kwargs.pop(p.name, p.default))

        return tuple(args)

    def __split_data_and_graph(
        self, graph: Dict[str, Union[Any, Tuple[Callable, Any, ...]]]
    ) -> Tuple[Dict[str, Argument], Dict[str, Tuple[Callable, Any, ...]]]:
        graph = graph.copy()
        node_name_to_data_argument = {}
        for node_name, node in graph.items():
            if isinstance(node, tuple) and len(node) > 0 and callable(node[0]):
                continue

            node_name_to_data_argument[node_name] = Argument(
                ArgumentType.Data, self._serializer.serialize_argument(node)
            )

        for node_name in node_name_to_data_argument.keys():
            graph.pop(node_name)

        return node_name_to_data_argument, graph

    @staticmethod
    def __check_graph(
        node_to_argument: Dict[str, Argument], graph: Dict[str, Union[Any, Tuple[Callable, Any, ...]]], keys: List[str]
    ):
        # sanity check graph
        for key in keys:
            if key not in graph:
                raise KeyError(f"key {key} has to be in graph")

        sorter = TopologicalSorter()
        for node_name, node in graph.items():
            assert isinstance(node, tuple) and len(node) > 0 and callable(node[0]), (
                "node has to be tuple and first " "item should be function"
            )

            for arg in node[1:]:
                if arg not in node_to_argument and arg not in graph:
                    raise KeyError(f"argument {arg} in node '{node_name}': {tuple(node)} is not defined in graph")

            sorter.add(node_name, *node[1:])

        # check cyclic dependencies
        sorter.prepare()

    def __construct_graph(
        self, data_arguments: Dict[str, Argument], graph: Dict[str, Tuple[Callable, Any, ...]], keys: List[str]
    ) -> Tuple[GraphTask, List[Future]]:
        node_name_to_task_id = {node_name: uuid.uuid1().bytes for node_name in graph.keys()}
        task_id_to_future = {node_name_to_task_id[node_name]: Future() for node_name in keys}

        functions = {}
        tasks = []
        for node_name, node in graph.items():
            task_id = node_name_to_task_id[node_name]

            function, *args = node
            function_id, function_bytes = self.__generate_function_id_bytes(function)
            functions[function_id] = function_bytes

            arguments = []
            for arg in args:
                assert arg in graph or arg in data_arguments

                if arg in graph:
                    argument = Argument(ArgumentType.Task, node_name_to_task_id[arg])
                else:
                    argument = data_arguments[arg]

                arguments.append(argument)

            tasks.append(Task(task_id, function_id, arguments))

        # send graph task
        graph = GraphTask(uuid.uuid1().bytes, functions, list(task_id_to_future.keys()), tasks)
        return graph, list(task_id_to_future.values())
