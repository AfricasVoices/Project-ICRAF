from core_data_modules.util import PhoneNumberUuidTable
from core_data_modules.logging import Logger

from src.lib.pipeline_configuration import PipelineConfiguration

import csv

log = Logger(__name__)

class AdvertPhoneNumbers(object):
    @staticmethod
    def generate(data, phone_number_uuid_table, advert_phone_numbers_csv_output_path):
        '''
        Generates a csv file with normalised phone numbers for respondents who sent messages 
        that were not labelled as Noise_Other_Project.

        :param data: TracedData objects that have been manually labelled. 
        :type: List of TracedData
        :param: phone_number_uuid_table
        :type: a coredatamodules.PhoneNumberUuidTable look up table containing uuids to retrieve phone numbers from.
        :return: advert_phone_numbers
        :rtype: csv file
        '''

        advert_phone_numbers = set()
        
        for td in data:
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if plan.binary_code_scheme is not None and td[plan.binary_coded_field]["CodeID"] not in ["code-NOP-4eb70633", "code-STOP-08b832a8", "code-NA-f93d3eb7","code-NR-5e3eee23"]:
                    advert_phone_numbers.add(phone_number_uuid_table.get_phone(td['uid']))
                if td[plan.coded_field][0]["CodeID"] not in ["code-NOP-4eb70633", "code-STOP-08b832a8", "code-NA-f93d3eb7","code-NR-5e3eee23"]:
                    advert_phone_numbers.add(phone_number_uuid_table.get_phone(td['uid']))

        with open(advert_phone_numbers_csv_output_path,'w') as f:
            writer = csv.writer(f)
            for phone_number in advert_phone_numbers:
                writer.writerow([phone_number,])
        log.info(f"{len(advert_phone_numbers)} phone numbers exported")
        
        return advert_phone_numbers