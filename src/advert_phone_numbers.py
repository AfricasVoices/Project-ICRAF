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
        :type: list of TracedData
        :param: phone_number_uuid_table: uuid <-> phone number look up table
        :type phone_number_uuid_table: core_data_modules.util.PhoneNumberUuidTable.
        :return: advert_phone_numbers
        :rtype: set of str
        '''
        advert_phone_numbers = set()
        for td in data:
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if plan.binary_code_scheme is not None and td[plan.binary_coded_field]["CodeID"] not in PipelineConfiguration.ADVERT_PHONE_NUMBERS_CODE_FILTERS:
                    advert_phone_numbers.add(phone_number_uuid_table.get_phone(td['uid']))
                if td[plan.coded_field][0]["CodeID"] not in PipelineConfiguration.ADVERT_PHONE_NUMBERS_CODE_FILTERS:
                    advert_phone_numbers.add(phone_number_uuid_table.get_phone(td['uid']))
        
        with open(advert_phone_numbers_csv_output_path,'w') as f:
            writer = csv.writer(f)
            for phone_number in advert_phone_numbers:
                writer.writerow([phone_number,])
        log.info(f"{len(advert_phone_numbers)} phone numbers exported")
        
        return advert_phone_numbers
