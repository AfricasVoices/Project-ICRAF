from dateutil.parser import isoparse

from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger

from .pipeline_configuration import PipelineConfiguration

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
    def filter_noise_other_project(messages):
        """
        Filters a list of  RQA and followup survey messages which have been labelled as Noise_Other_Project(NOP).

        :param messages: List of message objects to filter.
        :type messages: list of TracedData
        :return: Filtered list.
        :rtype: list of TracedData
        """
        not_noise = []
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
            for td in messages:
                if plan.binary_coded_field is not None and td[plan.binary_coded_field]["CodeID"] != plan.code_scheme.get_code_with_control_code(Codes.NOISE_OTHER_PROJECT) and \
                        td[plan.binary_coded_field]["Checked"]:
                    not_noise.append(td)

                # Checking for NOP label in datasets that don't have a binary scheme therefore it was labelled in normal scheme
                if plan.binary_coded_field is None and td[plan.coded_field][0]["CodeID"] != plan.code_scheme.get_code_with_control_code(Codes.NOISE_OTHER_PROJECT) and \
                        td[plan.coded_field][0]["Checked"]:
                    not_noise.append(td)

        log.info(f"Filtered out noise messages. "
                 f"Returning {len(not_noise)}/{len(messages)} messages.")

        return not_noise
