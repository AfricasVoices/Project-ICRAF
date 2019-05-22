from src.lib import MessageFilters


class FilterNOP(object):
    @staticmethod
    def filter_noise_other_project(messages):
        """
        Filters a list of  RQA and followup survey messages which have been labelled as Noise_Other_Project(NOP).

        :param messages: List of message objects to filter.
        :type messages: list of TracedData
        :return: Filtered list.
        :rtype: list of TracedData
        """

        # Filter radio + follow up survey messages labelled as Noise_Other_Project
        data = MessageFilters.filter_noise_radio_messages(messages)

        return data

