from core_data_modules.logging import Logger
from src.lib.pipeline_configuration import PipelineConfiguration

log = Logger(__name__)


class FilterNoise(object):
    @staticmethod
    def filter_noise_radio_messages(messages):
        """
        Filters a list of messages which have been labelled as Noise_Other_Project.

        :param messages: List of message objects to filter.
        :type messages: list of TracedData
        :param message_keys: Keys in each TracedData to search for a message.
        :type message_keys: list of str
        :return: Filtered list.
        :rtype: list of TracedData
        """
        not_noise = []
        log.info(len(messages))
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            for td in messages:
                if plan.binary_coded_field is not None and td[plan.binary_coded_field]["CodeID"] != "code-NOP-4eb70633" and \
                        td[plan.binary_coded_field]["Checked"]:
                    not_noise.append(td)
                if plan.binary_coded_field is None and td[plan.coded_field][0]["CodeID"] != "code-NOP-4eb70633" and \
                        td[plan.coded_field][0]["Checked"]:
                    not_noise.append(td)

        log.info(f"Filtered out RQA noise messages. "
                 f"Returning {len(not_noise)}/{len(messages)} messages.")

        return not_noise

