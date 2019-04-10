import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from dateutil.parser import isoparse

from src.lib import PipelineConfiguration

class Channels(object):
    RADIO_PROMO_KEY = "radio_promo"
    RADIO_SHOW_KEY = "radio_show"
    NON_LOGICAL_KEY = "non_logical_time"
    S01E01_KEY = "radio_participation_s01e01"
    S01E02_KEY = "radio_participation_s01e02"
    S01E03_KEY = "radio_participation_s01e03"
    S01E04_KEY = "radio_participation_s01e04"
    S01E05_KEY = "radio_participation_s01e05"
    S01E06_KEY = "radio_participation_s01e06"

    # Time ranges expressed in format (start_of_range_inclusive, end_of_range_exclusive)
    RADIO_PROMO_RANGES = [
        ("2019-04-03T00:00:00+0300", "2019-04-05T07:00:00+0300"),
        ("2019-04-09T00:00:00+0300", "2019-04-12T07:00:00+0300"),
        ("2019-04-16T00:00:00+0300", "2019-04-19T07:00:00+0300"),
        ("2019-04-23T00:00:00+0300", "2019-04-26T07:00:00+0300"),
        ("2019-04-30T00:00:00+0300", "2019-05-03T07:00:00+0300"),
        ("2019-05-07T00:00:00+0300", "2019-05-10T07:00:00+0300")
    ]

    RADIO_SHOW_RANGES = [
        ("2019-04-05T07:00:00+0300", "2019-04-08T24:00:00+0300"),
        ("2019-04-12T07:00:00+0300", "2019-04-15T24:00:00+0300"),
        ("2019-04-19T07:00:00+0300", "2019-04-22T24:00:00+0300"),
        ("2019-04-26T07:00:00+0300", "2019-04-29T24:00:00+0300"),
        ("2019-05-03T07:00:00+0300", "2019-05-06T24:00:00+0300"),
        ("2019-05-10T07:00:00+0300", "2019-05-13T24:00:00+0300")

    ]

    S01E01_RANGES = [
        ("2019-04-03T00:00:00+0300", "2019-04-08T24:00:00+0300")
    ]

    S01E02_RANGES = [
        ("2019-04-09T00:00:00+0300", "2019-04-15T24:00:00+0300")
    ]

    S01E03_RANGES = [
        ("2019-04-16T00:00:00+0300", "2019-04-22T24:00:00+0300")
    ]

    S01E04_RANGES = [
        ("2019-04-23T00:00:00+0300", "2019-04-29T24:00:00+0300")
    ]

    S01E05_RANGES = [
        ("2019-04-30T00:00:00+0300", "2019-05-06T24:00:00+0300")
    ]

    S01E06_RANGES = [
        ("2019-05-07T00:00:00+0300", "2019-05-13T24:00:00+0300")
    ]

    CHANNEL_RANGES = {
        RADIO_PROMO_KEY: RADIO_PROMO_RANGES,
        RADIO_SHOW_KEY: RADIO_SHOW_RANGES
    }

    SHOW_RANGES = {
        S01E01_KEY: S01E01_RANGES,
        S01E02_KEY: S01E02_RANGES,
        S01E03_KEY: S01E03_RANGES,
        S01E04_KEY: S01E04_RANGES,
        S01E05_KEY: S01E05_RANGES,
        S01E06_KEY: S01E06_RANGES,
    }

    @staticmethod
    def timestamp_is_in_ranges(timestamp, ranges):
        for range in ranges:
            if isoparse(range[0]) <= timestamp < isoparse(range[1]):
                return True
        return False

    @classmethod
    def set_channel_keys(cls, user, data, time_key):
        for td in data:
            timestamp = isoparse(td[time_key])

            channel_dict = dict()

            # Set channel ranges
            time_range_matches = 0
            for key, ranges in cls.CHANNEL_RANGES.items():
                if cls.timestamp_is_in_ranges(timestamp, ranges):
                    time_range_matches += 1
                    channel_dict[key] = Codes.TRUE 
                else:
                    channel_dict[key] = Codes.FALSE 
            
            # Set time as NON_LOGICAL if it doesn't fall in range of the  **sms ad/radio promo/radio-show**
            if time_range_matches == 0:
                # Assert in range of project
                assert PipelineConfiguration.PROJECT_START_DATE <= timestamp < PipelineConfiguration.PROJECT_END_DATE, \
                    f"Timestamp {td[time_key]} out of range of project"
                channel_dict[cls.NON_LOGICAL_KEY] = Codes.TRUE
            else:
                assert time_range_matches == 1, f"Time '{td[time_key]}' matches multiple time ranges"
                channel_dict[cls.NON_LOGICAL_KEY] = Codes.FALSE

            # Set show ranges
            for key, ranges in cls.SHOW_RANGES.items():
                if cls.timestamp_is_in_ranges(timestamp, ranges):
                    channel_dict[key] = Codes.FALSE

            td.append_data(channel_dict, Metadata(user, Metadata.get_call_location(), time.time()))
