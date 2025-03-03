import ast
import json
from datetime import datetime
from src.normalizer.turbine_power_data_types import TurbinePowerDataPoints, PowerTimeSeries
import asyncio

# Note: class methods are not static on purpose even though the class has no state\fields (can explain)
class TimeseriesNormalizer:

    def validate_input(self, data: TurbinePowerDataPoints):
        turbine = data.turbine
        power_unit = data.power_unit
        timeseries = data.timeseries

        if not turbine:
            raise Exception('Missing turbine name in input')
        if not power_unit:
            raise Exception('Missing power-unit in input')
        if not timeseries:
            raise Exception('Missing timeseries in input')

        #timeseries_list = ast.literal_eval(data['timeseries'])
        timestamps = [x.timestamp for x in data.timeseries]
        if len(timestamps) != len(set(timestamps)):
            raise Exception('Duplicate timestamps found in input')

        return turbine, power_unit, data.timeseries

    def convert_timestamp_to_datetime(self, timestamp):
        return datetime.fromtimestamp(timestamp / 1000)

    async def normalize(self, data: TurbinePowerDataPoints) -> TurbinePowerDataPoints:
        turbine, power_unit, timeseries_list = self.validate_input(data)
        try:
            buckets = await self.get_buckets(timeseries_list)
            timeseries_res = []
            for bucket in buckets:
                bucket_value = await self.calculate_bucket_value(bucket)
                timeseries_res.append(PowerTimeSeries(timestamp=bucket[0].timestamp, value=bucket_value))
            return  TurbinePowerDataPoints(turbine=turbine, power_unit= power_unit, timeseries=timeseries_res)
        except Exception as e:
            print(f"Error normalizing data: {e}")
            return  TurbinePowerDataPoints(turbine=turbine, power_unit= power_unit, timeseries=[])

    async def get_buckets(self, timeseries_list):
        timeseries_list.sort(key=lambda x: x.timestamp, reverse=False)
        buckets = []
        current_bucket = []
        for data_point in timeseries_list:
            measure_time = self.convert_timestamp_to_datetime(data_point.timestamp)
            # Filter out data points that are not in a full range of 30 minutes
            if not measure_time.minute in [0,30] and len(current_bucket) == 0:
                continue
            if measure_time.minute in [0,30] and len(current_bucket) > 0:
                current_bucket.append(data_point)
                buckets.append(current_bucket.copy())
                # if it`s not the last item next bucket should start with this point
                if data_point.timestamp != timeseries_list[-1].timestamp:
                    current_bucket = [data_point]
                continue
            current_bucket.append(data_point)

        return buckets

    async def calculate_bucket_value(self, bucket):
        value_sum = 0
        for i in range(len(bucket) -1): # iterate over all but the last element (so we always have a next element)
            curr_item = bucket[i]
            next_item = bucket[i+1]
            curr_measure_time = self.convert_timestamp_to_datetime(curr_item.timestamp)
            next_measure_time = self.convert_timestamp_to_datetime(next_item.timestamp)
            next_item_minutes = next_measure_time.minute if next_measure_time.minute != 0 else 60
            value_weight = (next_item_minutes - curr_measure_time.minute) / 30
            value_sum += curr_item.value * value_weight

        return value_sum
