import time

from scaled.cluster.scheduler import SchedulerProcess
from scaled.io.config import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_FUNCTION_RETENTION_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
)
from scaled.protocol.python.serializer.default import DefaultSerializer
from scaled.utility.zmq_config import ZMQConfig
from scaled.cluster.cluster import ClusterProcess
from scaled.cluster.combo import SchedulerClusterCombo
from scaled.client import Client

# This is a manual test because it can just loop infinitely if it fails

if __name__ == '__main__':

    # Test 1: Spinning up cluster with no scheduler. Should die in death timeout.
    cluster = ClusterProcess(
        address=ZMQConfig.from_string("tcp://127.0.0.1:2345"),
        n_workers=2,
        heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        function_retention_seconds=DEFAULT_FUNCTION_RETENTION_SECONDS,
        garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        death_timeout_seconds=20,
        event_loop="builtin",
        serializer=DefaultSerializer()
    )

    cluster.start()

    # Test 2: Running the Combo and sending shutdown
    cluster = SchedulerClusterCombo(address="tcp://127.0.0.1:2345", n_workers=2, per_worker_queue_size=2, event_loop="builtin")
    client = Client(address="tcp://127.0.0.1:2345")

    time.sleep(10)
    print("shutting down")
    client.shutdown()