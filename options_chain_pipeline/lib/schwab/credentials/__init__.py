#!/usr/bin/env python3
import os

from daily.utils.logging import get_logger

from .credentials import SchwabCredentials
from .util import get_number_of_accounts

logger = get_logger(__name__)

__all__ = ["SchwabCredentials", "NUM_ACCOUNTS", "CREDENTIALS_DICT"]


NUM_ACCOUNTS = get_number_of_accounts()
"""Number of Schwab accounts"""


CREDENTIALS_DICT: dict[int, SchwabCredentials] = {
    i: SchwabCredentials(
        os.environ[f"SCHWAB_API_KEY_{i}"],
        os.environ[f"SCHWAB_SECRET_{i}"],
        os.environ[f"SCHWAB_REDIRECT_URI_{i}"],
        os.environ[f"SCHWAB_TOKEN_PATH_{i}"],
        os.getenv(f"SCHWAB_ACCOUNT_NUMBER_{i}"),
        os.getenv(f"SCHWAB_ACCOUNT_USERNAME_{i}"),
        os.getenv(f"SCHWAB_ACCOUNT_PASSWORD_{i}"),
        i,
        os.getenv(f"SCHWAB_APP_STATUS_{i}", "APPROVED-PENDING"),
        os.getenv(f"SCHWAB_DEVELOPER_EMAIL_{i}", None),
    )
    for i in range(1, NUM_ACCOUNTS + 1)
}
"""
non-zero indexed dictionary of credentials (`SchwabCredentials` instances) whose data
are read from environment variables

```python
    {  1:
           api_key="<SCHWAB_API_KEY_1>",
           secret="<SCHWAB_SECRET_1>",
           ...,
           idx=1,
           app_status="READY_FOR_USE",
       2:
           api_key="<SCHWAB_API_KEY_2>",
           secret="<SCHWAB_SECRET_2>",
           ...,
           idx=2,
           app_status="READY_FOR_USE",
       3:  api_key="<SCHWAB_API_KEY_3>",
           secret="<SCHWAB_SECRET_3>",
           ...,
           idx=3,
           app_status="READY_FOR_USE",

    \n       ...,

       n:  api_key="<SCHWAB_API_KEY_n>",
           secret="<SCHWAB_SECRET_n>",
           ...,
           idx=n,
           app_status="READY_FOR_USE"  }
```

where `n` = `NUM_ACCOUNTS` (number of Schwab accounts)
"""

del os
