from core_data_modules.util import PhoneNumberUuidTable

from src.lib.pipeline_configuration import PipelineConfiguration

class Engagement(object):
    @staticmethod
    def generate(data, phone_number_uuid_table):
        '''
        Generates repeat engagements per Episode
        :param data: TracedData objects that have been manually labelled.
        :type: list of TracedData
        :param: phone_number_uuid_table: uuid <-> phone number look up table
        :type phone_number_uuid_table: core_data_modules.util.PhoneNumberUuidTable.
        :return:
        :rtype: set of str
        '''
        s01e01_uids = set()
        s01e02_uids = set()
        s01e03_uids = set()
        s01e04_uids = set()
        s01e05_uids = set()
        s01e06_uids = set()
        s01e07_uids = set()
        rep_all_episodes = set()
        rep_any_episodes = set()
        imaqal_all_uids = set()

        for td in data:
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if plan.raw_field == "rqa_s01e01_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e01_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e01_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e02_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e02_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e02_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e03_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e03_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e03_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e04_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e04_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e04_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e05_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e05_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e05_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e06_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e06_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e06_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                elif plan.raw_field == "rqa_s01e07_raw":
                    if plan.binary_code_scheme is not None and td[plan.binary_coded_field][
                        "CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e07_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                    if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.NON_ENGAGEMENT_CODES:
                        s01e07_uids.add(phone_number_uuid_table.get_phone(td['uid']))
                        imaqal_all_uids.add(phone_number_uuid_table.get_phone(td['uid']))

        for uid in imaqal_all_uids:
            if uid in s01e01_uids and uid in s01e02_uids and uid in s01e03_uids and uid in s01e04_uids:
                rep_all_episodes.add(uid)
        for uid in s01e04_uids:
            if uid in s01e02_uids or uid in s01e01_uids or uid in s01e03_uids:
                rep_any_episodes.add(uid)

        log.info(f"No uids in episode 1: {len(s01e01_uids)}")
        log.info(f"No uids in episode 2: {len(s01e02_uids)}")
        log.info(f"No uids in episode 3: {len(s01e03_uids)}")
        log.info(f"No uids in episode 4: {len(s01e04_uids)}")
        log.info(f"No uids in episode 5: {len(s01e05_uids)}")
        log.info(f"No uids in episode 6: {len(s01e06_uids)}")
        log.info(f"No uids in episode 7: {len(s01e07_uids)}")

        log.info(f"No total unique ids: {len(imaqal_all_uids)}")
        log.info(f"No of uids in episode 1 and 2:{len(s01e01_uids.intersection(s01e02_uids))}")
        log.info(f"No of uids in episode 2 and 3:{len(s01e02_uids.intersection(s01e03_uids))}")
        log.info(f"No of uids in episode 3 and 4:{len(s01e03_uids.intersection(s01e04_uids))}")
        log.info(f"No of uids in episode 4 and 5:{len(s01e04_uids.intersection(s01e05_uids))}")
        log.info(f"No of uids in episode 5 and 6:{len(s01e05_uids.intersection(s01e06_uids))}")
        log.info(f"No of uids in episode 6 and 7:{len(s01e06_uids.intersection(s01e07_uids))}")

        log.info((f"No of repeated in all 4 episodes:{len(rep_all_episodes)}"))
        log.info((f"Episode 4 uid in any other episodes:{len(rep_any_episodes)}"))

        return data
