from dataclasses import dataclass
from typing import List, Optional
from enum import Enum


class PowerUnit(Enum):
    mw = "MW"
    kw = "KW"

@dataclass
class PowerTimeSeries:
    timestamp: int
    value: Optional[float]

@dataclass
class TurbinePowerDataPoints:
    turbine: str
    power_unit: PowerUnit
    timeseries: List[PowerTimeSeries]