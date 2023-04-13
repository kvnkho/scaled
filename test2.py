import attrs

from typing import Tuple, List

@attrs.define
class TaskLog():
    message: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.message,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskLog(data[0])

test = TaskLog(message=b"123")
print(test.message)