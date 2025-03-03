# Timeseries Equalizer

Transform an irregular time series into an equally spaced time (30 min) series.


## Basic Usage

the code should run in an asyncronous context (async function)

```python
import asyncio
from src.normalizer.timeseries_normalizer import TimeseriesNormalizer
from src.normalizer.turbine_power_data_types import TurbinePowerDataPoints, PowerTimeSeries, PowerUnit

normalizer = TimeseriesNormalizer()
input_data = TurbinePowerDataPoints(turbine="<TURBINE NAME>", power_unit=PowerUnit("<MV\KW>") , timeseries=[<List of PowerTimeSeries>])
result = await normalizer.normalize(input_data)
```

## Running the simple app

```bash
python simple_app.py <PATH TO INPUT JSON FILE>
```
this will override the results.json file in the output folder
