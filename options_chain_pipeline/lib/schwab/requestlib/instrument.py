from abc import ABC, abstractmethod
import json
from typing import TYPE_CHECKING

from daily.utils.logging import ClassNameLoggerMixin

if TYPE_CHECKING:
    import logging

    from .task import BaseRequestTaskModel


class Instrument(ABC):

    @abstractmethod
    def on_task_create(self, task: "BaseRequestTaskModel"):
        """
        Callback on task creation

        :param task: BaseRequestTaskModel
        """
        ...

    @abstractmethod
    def on_task_stageed(self, task: "BaseRequestTaskModel"):
        """
        Callback on task staged

        :param task: BaseRequestTaskModel
        """
        ...

    @abstractmethod
    def on_task_scheduled(self, task: "BaseRequestTaskModel"):
        """
        Callback on task scheduled

        :param task: BaseRequestTaskModel
        """
        ...

    @abstractmethod
    def on_task_executed(self, task: "BaseRequestTaskModel"):
        """
        Callback on task execution

        :param task: BaseRequestTaskModel
        """
        ...

    @abstractmethod
    def on_task_completion(self, task: "BaseRequestTaskModel"):
        """
        Callback on task completion

        :param task: BaseRequestTaskModel
        """
        ...

    @abstractmethod
    def on_task_cancellation(self, task: "BaseRequestTaskModel"):
        """
        Callback on task cancellation

        :param task: BaseRequestTaskModel
        """
        ...

    @abstractmethod
    def on_set_error(self, task: "BaseRequestTaskModel"):
        """
        Callback on task set error

        :param task: BaseRequestTaskModel
        """
        ...

    @abstractmethod
    def on_sink(self, task: "BaseRequestTaskModel"):
        """
        Callback on task set error

        :param task: BaseRequestTaskModel
        """
        ...


class LoggingInstrument(Instrument, ClassNameLoggerMixin):
    LEVEL = "INFO"
    CH = True
    CACHED = True

    def __init__(self, logger: "logging.Logger | logging.LoggerAdapter | None" = None):
        self.logger = logger or self.get_logger()

    def on_task_create(self, task: "BaseRequestTaskModel"):
        self.logger.debug(f"Task {task.uuid} created at {task.time_created}")

    def on_task_completion(self, task: "BaseRequestTaskModel"):
        if task.summary_json.error is not None:
            self.logger.warning(
                f"Task {task.uuid} complete with error {task.summary_json.error} at {task.time_executed}"
            )
        elif task.summary_json.response is not None:
            if task.summary_json.response_data is not None:
                resp_text = json.dumps(task.summary_json.response_data)[:100] + "..."
                self.logger.info(
                    f"Task {task.uuid} complete with data {resp_text} at {task.time_executed}"
                )
            elif task.summary_json.response.reason.lower() != "ok":
                self.logger.warning(
                    f"Task {task.uuid} complete with an error; response.reason={task.summary_json.response.reason} at {task.time_executed}"
                )

    def on_task_cancellation(self, task: "BaseRequestTaskModel"):
        self.logger.warning(f"Task {task.uuid} cancelled")

    def on_task_executed(self, task: "BaseRequestTaskModel"):
        self.logger.info(f"Task {task.uuid} executed at {task.time_executed}")

    def on_task_scheduled(self, task: "BaseRequestTaskModel"):
        self.logger.info(
            f"Task {task.uuid} scheduled at {task.time_scheduled} with client {task.client_idx}"
        )

    def on_task_stageed(self, task: "BaseRequestTaskModel"):
        if task.client_idx is not None:
            from ..client.functions import client_from_idx

            client = client_from_idx(task.client_idx)
            self.logger = client.get_logger_adapter()

        self.logger.info(
            f"Task {task.uuid} staged at {task.time_staged} with client {task.client_idx}"
        )

    def on_set_error(self, task: "BaseRequestTaskModel"):
        self.logger.error(
            f"Task {task.uuid} error set to {str(task.summary_json.error)}"
        )

    def on_sink(self, task: "BaseRequestTaskModel"):
        self.logger.debug(f"Task {task.uuid} sunk to Kafka")
