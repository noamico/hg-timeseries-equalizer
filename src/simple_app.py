import sys
import json
import asyncio
from src.normalizer.timeseries_normalizer import TimeseriesNormalizer
from src.normalizer.turbine_power_data_types import TurbinePowerDataPoints, PowerTimeSeries, PowerUnit

def obj_to_dict(obj):
    return obj.__dict__

async def generate_result(json_path: str):
    with open(json_path) as f:
        json_content = json.load(f)
        normalizer = TimeseriesNormalizer()
        timeseries_input = []
        for item in json_content['timeseries']:
            timeseries_input.append(PowerTimeSeries(timestamp= item['timestamp'], value= item['value']))
        input_data = TurbinePowerDataPoints(turbine="B", power_unit=PowerUnit(json_content['power_unit']) , timeseries=timeseries_input)
        result = await normalizer.normalize(input_data)
        result_dict = {
            "turbine": result.turbine,
            "power_unit": result.power_unit.value,
            "timeseries": [{"timestamp": x.timestamp, "value": x.value} for x in result.timeseries]
        }
        with open("./output/result.json", "w") as f:
            f.write(json.dumps(result_dict))

if __name__ == '__main__':
    input_path = sys.argv[1]
    asyncio.run(generate_result(input_path))
