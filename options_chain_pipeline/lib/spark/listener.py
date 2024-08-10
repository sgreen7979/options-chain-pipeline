import json


from pyspark.streaming import StreamingListener  # noqa
from pyspark.status import SparkJobInfo, SparkStageInfo, StatusTracker  # noqa
from pyspark.sql.streaming.listener import (  # noqa
    StreamingQueryListener,
    StateOperatorProgress,
    QueryStartedEvent,
    QueryTerminatedEvent,
    QueryIdleEvent,
    JStreamingQueryListener,
    QueryProgressEvent,
    SinkProgress,
    SourceProgress,
    StreamingQueryProgress,
)

from daily.utils.logging import get_logger

# logger = get_logger(__name__, level="DEBUG", ch=True, fh=True)


class MyListener(StreamingQueryListener):
    def __init__(self, _logger=None):
        # self.logger = logger or get_logger(__name__)
        self.logger = _logger or get_logger(__name__, level="DEBUG", ch=True, fh=True)

    def onQueryStarted(self, event: QueryStartedEvent):
        try:
            self.logger.info(
                f"Query started: id={event.id}, runId={event.runId}, name={event.name}"
            )
        except Exception as e:
            self.logger.error(
                "`MyListener.onQueryStarted` failed to report `QueryStartedEvent`",
                exc_info=e,
            )

    def onQueryProgress(self, event: QueryProgressEvent):
        self.logger.info(
            json.dumps(
                {
                    "id": str(event.progress.id),
                    "runId": str(event.progress.runId),
                    "name": event.progress.name,
                    "timestamp": event.progress.timestamp,
                    "batchId": event.progress.batchId,
                    "batchDuration": event.progress.batchDuration,
                    "numInputRows": event.progress.numInputRows,
                    "inputRowsPerSecond": event.progress.inputRowsPerSecond,
                    "processedRowsPerSecond": event.progress.processedRowsPerSecond,
                    "durationMs": {
                        "addBatch": event.progress.durationMs["addBatch"],
                        "commitOffsets": event.progress.durationMs["commitOffsets"],
                        "getBatch": event.progress.durationMs["getBatch"],
                        "latestOffset": event.progress.durationMs["latestOffset"],
                        "queryPlanning": event.progress.durationMs["queryPlanning"],
                        "triggerExecution": event.progress.durationMs[
                            "triggerExecution"
                        ],
                        "walCommit": event.progress.durationMs["walCommit"],
                    },
                    "state_operators": [
                        stateOperator.prettyJson
                        for stateOperator in event.progress.stateOperators
                    ],
                    "sources": [src.prettyJson for src in event.progress.sources],
                    "sink": event.progress.sink.prettyJson,
                },
                indent=4,
            )
        )

        # try:
        #     row = event.progress.observedMetrics.get("metric")
        #     if row is not None:
        #         if row.malformed / row.cnt > 0.5:
        #             print(
        #                 "ALERT! Ouch! there are too many malformed "
        #                 f"records {row.malformed} out of {row.cnt}!"
        #             )
        #         else:
        #             print(f"{row.cnt} rows processed!")
        #     self.logger.info(
        #         f"Query made progress: id={event.progress.id}, runId={event.progress.runId}"
        #     )
        #     self.logger.info(
        #         f"BatchId: {event.progress.batchId}, Timestamp: {event.progress.timestamp}"
        #     )
        #     # self.logger.info(f"Sources: {event.progress.sources}")
        #     for src in event.progress.sources:
        #         self.logger.info(f"Description: {src.description}")
        #         self.logger.info(f"Start offset: {src.startOffset}")
        #         self.logger.info(f"End offset: {src.endOffset}")
        #         self.logger.info(f"Latest offset: {src.latestOffset}")
        #         self.logger.info(f"Input rows: {src.numInputRows}")
        #         self.logger.info(f"Input rows/s: {src.inputRowsPerSecond}")
        #         self.logger.info(f"Processed rows/s: {src.processedRowsPerSecond}")
        #         self.logger.info(f"Metrics: {src.metrics}")

        #     self.logger.info(f"Sink: {event.progress.sink.json}")
        #     self.logger.info(f"Input rows: {event.progress.numInputRows}")
        #     self.logger.info(f"Progress batch duration: {event.progress.batchDuration}")
        #     self.logger.info(f"Progress batch id: {event.progress.batchId}")
        #     # for source in event.progress.sources:
        #     #     self.logger.info(
        #     #         f"Source: {source.description}, Input Rows: {source.numInputRows}"
        #     #     )
        #     for state_operator in event.progress.stateOperators:
        #         self.logger.info(
        #             f"State Operator: {state_operator.operatorName}, Num Rows Updated: {state_operator.numRowsUpdated}"
        #         )
        # except Exception as e:
        #     self.logger.error(
        #         "`MyListener.onQueryProgress` failed to report `QueryProgressEvent`",
        #         exc_info=e,
        #     )

    def onQueryIdle(self, event: QueryIdleEvent):
        try:
            self.logger.info(
                f"Query idle: id={event.id}, runId={event.runId}, timestamp={event.timestamp}"
            )
        except Exception as e:
            self.logger.error(
                "`MyListener.onQueryIdle` failed to report `QueryIdleEvent`",
                exc_info=e,
            )

    def onQueryTerminated(self, event: QueryTerminatedEvent):
        try:
            self.logger.info(
                f"Query terminated: id={event.id}, runId={event.runId}, exception={event.exception}, errorClassOnException: {event.errorClassOnException}"
            )
        except Exception as e:
            self.logger.error(
                "`MyListener.onQueryTerminated` failed to report `QueryTerminatedEvent`",
                exc_info=e,
            )


# # Observe metric
# observed_df = df.observe("metric", count(lit(1)).as("cnt"), count(col("error")).as("malformed"))
# observed_df.writeStream.format("...").start()


# Define my listener.
class MyListener2(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"'{event.name}' [{event.id}] got started!")

    def onQueryProgress(self, event):
        row = event.progress.observedMetrics.get("metric")
        if row is not None:
            if row.malformed / row.cnt > 0.5:
                print(
                    "ALERT! Ouch! there are too many malformed "
                    f"records {row.malformed} out of {row.cnt}!"
                )
            else:
                print(f"{row.cnt} rows processed!")

    def onQueryTerminated(self, event):
        print(
            f"Event terminated! (id={event.id}, runId={event.runId})\n"
            # f"{event.id} got terminated!\n"
            f"errorClassOnException: {event.errorClassOnException}\n"
            f"exception: {event.exception}"
        )
