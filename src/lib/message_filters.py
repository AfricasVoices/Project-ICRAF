from dateutil.parser import isoparse

from core_data_modules.logging import Logger

log = Logger(__name__)


# TODO: Move to Core once adapted for and tested on a pipeline that supports multiple radio shows
class MessageFilters(object):
    # TODO: Log which data is being dropped?
    @staticmethod
    def filter_test_messages(messages, test_run_key="test_run"):
        return [td for td in messages if not td.get(test_run_key, False)]

    @staticmethod
    def filter_empty_messages(messages, message_keys):
        # TODO: Before using on future projects, consider whether messages which are "" should be considered as empty
        non_empty = []
        for td in messages:
            for message_key in message_keys:
                if message_key in td:
                    non_empty.append(td)
                    continue
        return non_empty

    @staticmethod
    def filter_time_range(messages, time_key, start_time, end_time):
        return [td for td in messages if start_time <= isoparse(td.get(time_key)) <= end_time]

    @staticmethod
    def filter_rqa_noise_other_project(messages):
        """
        Filters out RQA messages which have been labelled as Noise_Other_Project(NOP).

        :param messages: List of message objects to filter.
        :type messages: list of TracedData
        :return: Filtered list.
        :rtype: list of TracedData
        """
        not_noise = []
        for td in messages:
            if (td["rqa_s01e01_yes_no_amb_coded"]["CodeID"] != "code-NOP-4eb70633") and \
                    (td["rqa_s01e01_yes_no_amb_coded"]["CodeID"] != "code-NOP-4eb70633") and \
                    (td["rqa_s01e02_yes_no_amb_coded"]["CodeID"] != "code-NOP-4eb70633") and \
                    (td["rqa_s01e03_yes_no_amb_coded"]["CodeID"] != "code-NOP-4eb70633") and \
                    (td["rqa_s01e04_yes_no_amb_coded"]["CodeID"] != "code-NOP-4eb70633") and \
                    (td["rqa_s01e05_coded"][0]["CodeID"] != "code-NOP-4eb70633") and \
                    (td["rqa_s01e06_coded"][0]["CodeID"] != "code-NOP-4eb70633") and \
                    (td["rqa_s01e07_coded"][0]["CodeID"] != "code-NOP-4eb70633"):
                not_noise.append(td)

        log.info(f"Filtered out rqa noise other project messages. "
                 f"Returning {len(not_noise)}/{len(messages)} messages.")

        return not_noise

