#!/usr/bin/env python3
import numpy as np

def continuously_compounding_rate(
    rate: float, round_digits: Optional[int] = None
) -> np.float64:
    if round_digits:
        return round(np.log(1 + rate), round_digits)
    val: np.float64 = np.log(1 + rate)
    return val