import json
import unittest
from src.normalizer.timeseries_normalizer import TimeseriesNormalizer
from unittest import IsolatedAsyncioTestCase

class TestNormalizer(IsolatedAsyncioTestCase):

    def setUp(self):
        self.normalizer = TimeseriesNormalizer()

    async def test_happy_flow(self):
        # Arrange
        input_dict = {
            "turbine": "B",
            "power_unit": "MW",
            "timeseries": [
                {"timestamp": 1581609600000, "value": 16},  # Thursday, February 13, 2020 4:00:00 PM
                {"timestamp": 1581610500000, "value": 0},   # Thursday, February 13, 2020 4:15:00 PM
                {"timestamp": 1581611400000, "value": 4},   # Thursday, February 13, 2020 4:30:00 PM
                {"timestamp": 1581612300000, "value": 60},  # Thursday, February 13, 2020 4:45:00 PM
                {"timestamp": 1581613200000, "value": None} # Thursday, February 13, 2020 5:00:00 PM
            ]
        }

        # Act
        result = await self.normalizer.normalize(json.dumps(input_dict))

        # Assert
        self.assertEqual(result.turbine, "B")
        self.assertEqual(result.power_unit, "MW")
        self.assertEqual(len(result.timeseries), 2)
        self.assertEqual(result.timeseries[0].timestamp, 1581609600000)
        self.assertEqual(result.timeseries[0].value, 8.0)
        self.assertEqual(result.timeseries[1].timestamp, 1581611400000)
        self.assertEqual(result.timeseries[1].value, 32.0)

    async def test_partial_input(self):
        # Arrange
        input_dict = {
            "turbine": "A",
            "power_unit": "MW",
            "timeseries": [
                {"timestamp": 1581609600000, "value": 16},  # Thursday, February 13, 2020 4:00:00 PM
                {"timestamp": 1581610500000, "value": None} # Thursday, February 13, 2020 4:15:00 PM
            ]
        }

        # Act
        result = await self.normalizer.normalize(json.dumps(input_dict))

        # Assert
        self.assertEqual(result.turbine, "A")
        self.assertEqual(result.power_unit, "MW")
        self.assertEqual(len(result.timeseries), 0)

    async def test_mixed_and_long_input(self):
        # Arrange
        input_dict = {
            "turbine": "LONG",
            "power_unit": "MW",
            "timeseries": [
                 {'timestamp': 1581608700000, 'value': 1000}, # Thursday, February 13, 2020 3:45:00 PM
                 {'timestamp': 1581609600000, 'value': 1},    # Thursday, February 13, 2020 4:00:00 PM
                 {'timestamp': 1581609960000, 'value': 0},    # Thursday, February 13, 2020 4:06:00 PM
                 {'timestamp': 1581611400000, 'value': 1},    # Thursday, February 13, 2020 4:30:00 PM
                 {'timestamp': 1581612000000, 'value': 4},    # Thursday, February 13, 2020 4:40:00 PM
                 {'timestamp': 1581613200000, 'value': 5},    #  Thursday, February 13, 2020 5:00:00 PM
                 {'timestamp': 1581613800000, 'value': 8},    # Thursday, February 13, 2020 5:10:00 PM
                 {'timestamp': 1581614400000, 'value': 14},   # Thursday, February 13, 2020 5:20:00 PM
                 {'timestamp': 1581615000000, 'value': None}  #  Thursday, February 13, 2020 5:30:00 PM
            ]
        }

        # Act
        result = await self.normalizer.normalize(json.dumps(input_dict))

        # Assert
        self.assertEqual(result.turbine, "LONG")
        self.assertEqual(result.power_unit, "MW")
        self.assertEqual(len(result.timeseries), 3)
        self.assertEqual(result.timeseries[0].timestamp, 1581609600000)
        self.assertEqual(result.timeseries[0].value, 0.2)
        self.assertEqual(result.timeseries[1].timestamp, 1581611400000)
        #self.assertEqual(result.timeseries[1].value, 3.0) # TODO: fix this case
        self.assertEqual(result.timeseries[2].timestamp, 1581613200000)
        self.assertEqual(result.timeseries[2].value, 9.0)

if __name__ == '__main__':
    unittest.main()