from core_data_modules.util import PhoneNumberUuidTable

from src.lib.pipeline_configuration import PipelineConfiguration

class Engagement(object):
    @staticmethod
    def generate(data, phone_number_uuid_table):
        """
        prints out engagements numbers per Episode
        :param data: TracedData objects that have been manually labelled.
        :type: list of TracedData
        :param: phone_number_uuid_table: uuid <-> phone number look up table
        :type phone_number_uuid_table: core_data_modules.util.PhoneNumberUuidTable.
        :return: data
        :rtype: list of TracedData
        """
        s01e01_uids = set()
        s01e02_uids = set()
        s01e03_uids = set()
        s01e04_uids = set()
        s01e05_uids = set()
        s01e06_uids = set()
        s01e07_uids = set()
        repeated_all_episodes = set()
        repeated_any_episodes = set()
        icraf_all_uids = set()

        for td in data:
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if plan.raw_field == "rqa_s01e01_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e01_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e01_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e02_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e02_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e02_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e03_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e03_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e03_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e04_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e04_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e04_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e05_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e05_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e05_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e06_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e06_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e06_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e07_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e07_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e07_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        icraf_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))

        for uid in icraf_all_uids:
            if uid in s01e01_uids and uid in s01e02_uids and uid in s01e03_uids and uid in s01e04_uids \
                    and uid in s01e05_uids and uid in s01e06_uids and uid in s01e07_uids:
                repeated_all_episodes.add(uid)
        for uid in s01e07_uids:
            if uid in s01e01_uids or uid in s01e02_uids or uid in s01e03_uids or uid in s01e04_uids \
                    or uid in s01e05_uids or uid in s01e06_uids:
                repeated_any_episodes.add(uid)

        print(f"No uids in episode 1: {len(s01e01_uids)}")
        print(f"No uids in episode 2: {len(s01e02_uids)}")
        print(f"No uids in episode 3: {len(s01e03_uids)}")
        print(f"No uids in episode 4: {len(s01e04_uids)}")
        print(f"No uids in episode 5: {len(s01e05_uids)}")
        print(f"No uids in episode 6: {len(s01e06_uids)}")
        print(f"No uids in episode 7: {len(s01e07_uids)}")

        print(f"No total unique ids: {len(icraf_all_uids)}")
        print(f"No of uids in episode 1 and 2:{len(s01e01_uids.intersection(s01e02_uids))}")
        print(f"No of uids in episode 2 and 3:{len(s01e02_uids.intersection(s01e03_uids))}")
        print(f"No of uids in episode 3 and 4:{len(s01e03_uids.intersection(s01e04_uids))}")
        print(f"No of uids in episode 4 and 5:{len(s01e04_uids.intersection(s01e05_uids))}")
        print(f"No of uids in episode 5 and 6:{len(s01e05_uids.intersection(s01e06_uids))}")
        print(f"No of uids in episode 6 and 7:{len(s01e06_uids.intersection(s01e07_uids))}")

        print((f"No of repeated in all 7 episodes:{len(repeated_all_episodes)}"))
        print((f"Episode 7 uid in any other episodes:{len(repeated_any_episodes)}"))

        return data
