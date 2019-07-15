from src.lib import MessageFilters


class FilterNOP(object):
    @staticmethod
    def filter_rqa_noise_other_project(messages):
        """
        Filters out RQA messages which have been labelled as Noise_Other_Project(NOP).

        :param messages: List of message objects to filter.
        :type messages: list of TracedData
        :return: Filtered list.
        :rtype: list of TracedData
        """

        # Filter radio question answers labelled as Noise_Other_Project
        data = MessageFilters.filter_rqa_noise_other_project(messages)

        return data

