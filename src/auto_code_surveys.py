import time
from os import path

from core_data_modules.cleaners import Codes 
from core_data_modules.cleaners.cleaning_utils import CleaningUtils 
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO
from core_data_modules.util import IOUtils

from src.lib import PipelineConfiguration, CodeSchemes

class AutoCodeSurveys(object):
    SENT_ON_KEY = "sent_on"

    @classmethod
    def auto_code_surveys(cls, user, data, phone_uuid_table, coda_output_dir):
        # Label missing data
        for td in data:
            missing_dict = dict()
            for plan in PipelineConfiguration.DEMOGS_CODING_PLANS:
                if td.get(plan.raw_field, "") == "":
                    na_label = CleaningUtils.make_label_from_cleaner_code(
                        plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                        Metadata.get_call_location()
                    )
                    missing_dict[plan.coded_field] = na_label.to_dict()
            td.append_data(missing_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Auto-code remaining data
        for plan in PipelineConfiguration.DEMOGS_CODING_PLANS:
            if plan.cleaner is not None:
                CleaningUtils.apply_cleaner_to_traced_data_iterable(user, data, plan.raw_field, plan.coded_field,
                                                                    plan.cleaner, plan.code_scheme)

        # For any locations where the cleaners assigned a code to a constituency, set the county code to NC
        # (this is because only one column should have a value set in Coda)
        for td in data:
            if "constituency_coded" in td:
                constituency_code_id = td["constituency_coded"]["CodeID"]
                if CodeSchemes.CONSTITUENCY.get_code_with_id(constituency_code_id).code_type == "Normal":
                    nc_label = CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.CONSTITUENCY,
                        CodeSchemes.CONSTITUENCY.get_code_with_control_code(Codes.NOT_CODED),
                        Metadata.get_call_location(),
                    )
                    td.append_data({"county_coded": nc_label.to_dict()},
                                   Metadata(user, Metadata.get_call_location(), time.time()))
                                                                    
        # Output survey answers to coda for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.DEMOGS_CODING_PLANS:
            if plan.raw_field == "location_raw":
                continue
            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field)
            coda_output_path = path.join(coda_output_dir, plan.coda_filename)
            with open(coda_output_path, 'w') as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field, {plan.coded_field: plan.code_scheme}, f
                )

        # Output location scheme to coda for manual verification + coding
        output_path = path.join(coda_output_dir, "constituency.json")
        TracedDataCodaV2IO.compute_message_ids(user, data, "constituency_raw", "constituency_raw_id")
        with open(output_path, "w") as f:
            TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                data, "constituency_raw", "constituency_time", "constituency_raw_id",
                {"constituency_coded": CodeSchemes.CONSTITUENCY,
                 "county_coded": CodeSchemes.COUNTY}, f
            )

        print("Coda demogs files successfully exported")   
        
        return data 
