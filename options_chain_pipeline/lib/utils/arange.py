from typing import Iterator, overload, SupportsIndex, Sequence, Self


class AsyncRange(Sequence[int]):
    """
    Usage:
        ```
        _l = list(range(120))

        async for i in AsyncRange(0, 120 20):
            _b = _l[slice(i, i+20)]
            print(_b)
            await asyncio.sleep(1)

        # output: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        #         [20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39]
        #         [40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59]
        #         [60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79]
        #         [80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]
        #         [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119]
        ```

    """

    __slots__ = ("_start", "_stop", "_step", "value", "_incr")

    @property
    def start(self) -> int:
        return self._start

    @property
    def stop(self) -> int:
        return self._stop

    @property
    def step(self) -> int:
        return self._step

    @overload
    def __new__(cls, stop: SupportsIndex, /) -> Self: ...

    @overload
    def __new__(
        cls, start: SupportsIndex, stop: SupportsIndex, step: SupportsIndex = 1, /
    ) -> Self: ...

    def __new__(
        cls,
        stop: SupportsIndex,
        start: SupportsIndex = 0,
        step: SupportsIndex = 1,
        /,
    ) -> Self:
        return super().__new__(cls)  # Return the instance itself

    def __init__(self, *args: SupportsIndex) -> None:
        if len(args) == 1:
            self._start = 0
            self._stop = int(args[0])
            self._step = 1
        elif len(args) == 2:
            self._start = int(args[0])
            self._stop = int(args[1])
            self._step = 1
        elif len(args) == 3:
            self._start = int(args[0])
            self._stop = int(args[1])
            self._step = int(args[2])
        else:
            raise TypeError(
                "AsyncRange expected 1 to 3 arguments, got {}".format(len(args))
            )

        self.value = self._start

        # Set the increment/decrement method
        if self._step > 0:
            self._incr = self._incr_positive
        else:
            self._incr = self._incr_negative

    def count(self, value: int, /) -> int:
        # Return how many times 'value' appears in this range
        if value in self:
            return 1
        return 0

    def index(self, value: int, /) -> int:
        # Return the position of 'value' in this range, if it exists
        if value in self:
            return value
        raise ValueError(f"{value} is not in range")

    def __len__(self) -> int:
        # Calculate the number of steps between start and stop
        if self._step > 0:
            return max(0, (self._stop - self._start + self._step - 1) // self._step)
        else:
            return max(0, (self._start - self._stop - self._step - 1) // (-self._step))

    def __eq__(self, value: object, /) -> bool:
        if isinstance(value, AsyncRange):
            return (self._start, self._stop, self._step) == (
                value._start,
                value._stop,
                value._step,
            )
        return False

    def __hash__(self) -> int:
        return hash((self._start, self._stop, self._step))

    def __contains__(self, key: object, /) -> bool:
        # Check if 'key' is part of the range
        if isinstance(key, int):
            if self._step > 0:
                return (
                    self._start <= key < self._stop
                    and (key - self._start) % self._step == 0
                )
            else:
                return (
                    self._start >= key > self._stop
                    and (key - self._start) % self._step == 0
                )
        return False

    def __iter__(self) -> Iterator[int]:
        current = self._start
        while (self._step > 0 and current < self._stop) or (
            self._step < 0 and current > self._stop
        ):
            yield current
            current += self._step

    @overload
    def __getitem__(self, key: SupportsIndex, /) -> int: ...

    @overload
    def __getitem__(self, key: slice, /) -> "AsyncRange": ...

    def __getitem__(self, key):
        if isinstance(key, slice):
            start, stop, step = key.indices(len(self))
            return AsyncRange(
                self._start + start * self._step,
                self._start + stop * self._step,
                self._step * step,
            )
        elif isinstance(key, int):
            if key < 0:
                key += len(self)
            if key < 0 or key >= len(self):
                raise IndexError("AsyncRange index out of range")
            return self._start + key * self._step
        else:
            raise TypeError(
                "AsyncRange indices must be integers or slices, not {}".format(
                    type(key).__name__
                )
            )

    def __reversed__(self) -> Iterator[int]:
        # Return an iterator in reverse order
        current = self._start + (len(self) - 1) * self._step
        while (self._step > 0 and current >= self._start) or (
            self._step < 0 and current <= self._start
        ):
            yield current
            current -= self._step

    # Async iterator functionality
    def __aiter__(self):
        return self

    async def __anext__(self):
        if (self._step > 0 and self.value >= self._stop) or (
            self._step < 0 and self.value <= self._stop
        ):
            raise StopAsyncIteration

        current_value = self.value
        self._incr()
        return current_value

    def _incr_positive(self):
        self.value += self._step

    def _incr_negative(self):
        self.value += self._step


arange = AsyncRange
