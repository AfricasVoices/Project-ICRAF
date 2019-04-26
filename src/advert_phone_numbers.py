from core_data_modules.util import PhoneNumberUuidTable
from core_data_modules.logging import Logger

from src.lib.pipeline_configuration import PipelineConfiguration

import csv

log = Logger(__name__)

class AdvertPhoneNumbers(object):
    @staticmethod
    def generate(data, phone_number_uuid_table, advert_phone_numbers_csv_output_path):
        advert_phone_numbers = set()

        '''
        Generates a csv file with normalised phone numbers for respondents who sent messages 
        that were not labelled as Noise_Other_Project.

        :param data:TracedData objects that have been manually labelled. 
        :type: TracedData
        :param: phone_number_uuid_table
        :type: a look up table containing uuids to retrieve phone numbers from.
        :return: advert_phone_numbers_csv_output_path
        :rtype: csv file
        '''
        for td in data:
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                    if (plan.binary_coded_field is not None and td[plan.binary_coded_field]["CodeID"] != "code-NOP-4eb70633") \
                        or td[plan.coded_field][0]["CodeID"] != "code-NOP-4eb70633":
                        advert_phone_numbers.add(phone_number_uuid_table.get_phone(td['uid']))
        
        with open(advert_phone_numbers_csv_output_path,'w') as f:
            writer = csv.writer(f)
            for contact in advert_phone_numbers:
                writer.writerow([contact,])
        log.info(f"{len(advert_phone_numbers)} phone numbers generated")
        
        return data
