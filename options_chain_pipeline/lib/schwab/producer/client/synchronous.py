from typing import Optional

from ...client.synchronous import SyncSchwabClient, ResponseHandler
from ..mixins.synchronous import SyncSchwabProducerMixin


class SyncSchwabProducer(SyncSchwabProducerMixin, SyncSchwabClient):
    def __init__(self, idx, *args, credentials=None, **kwargs) -> None:
        SyncSchwabClient.__init__(
            self,
            idx,
            *args,
            credentials=credentials,
            **kwargs,
        )
        SyncSchwabProducerMixin.__init__(self)
