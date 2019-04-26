import json
from urllib.parse import urlparse

from core_data_modules.cleaners import swahili, Codes 
from core_data_modules.data_models import Scheme, validators
from dateutil.parser import isoparse

def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return Scheme.from_firebase_map(firebase_map)

class CodeSchemes(object):
    ICRAF_S01E01 = _open_scheme("icraf_s01e01.json")
    ICRAF_S01E02 = _open_scheme("icraf_s01e02.json")
    ICRAF_S01E03 = _open_scheme("icraf_s01e03.json")
    ICRAF_S01E04 = _open_scheme("icraf_s01e04.json")
    ICRAF_S01E05 = _open_scheme("icraf_s01e05.json")
    ICRAF_S01E06 = _open_scheme("icraf_s01e06.json")
    
    AGE = _open_scheme("age.json")
    LIVELIHOOD = _open_scheme("livelihood.json")
    GENDER = _open_scheme("gender.json")
    CONSTITUENCY = _open_scheme("constituency.json")
    ORGANIZATIONS = _open_scheme("organizations.json")

    CURRENT_PRACTICES = _open_scheme("current_practices.json")
    NEW_PRACTICES = _open_scheme("new_practices.json")
    NEW_PRACTICES_CHALLENGES = _open_scheme("new_practices_challenges.json")
    UPPER_TANA_PRACTICES = _open_scheme("upper_tana_practices.json")
    SO1EO1_YES_NO = _open_scheme("s01e01_yes_no.json")
    SO1EO2_YES_NO = _open_scheme("s01e02_yes_no_amb.json")
    SO1EO3_YES_NO = _open_scheme("s01e03_yes_no.json")
    SO1EO5_YES_NO = _open_scheme("s01e05_yes_no.json")

    WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")

class CodingPlan(object):
    def __init__(self, raw_field, coded_field, coda_filename, cleaner=None, code_scheme=None, time_field=None,
                run_id_field=None, icr_filename=None, analysis_file_key=None, id_field=None,
                binary_code_scheme=None, binary_coded_field=None, binary_analysis_file_key=None):
        self.raw_field = raw_field
        self.coded_field = coded_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.cleaner = cleaner
        self.code_scheme = code_scheme
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.analysis_file_key = analysis_file_key
        self.binary_code_scheme = binary_code_scheme
        self.binary_coded_field = binary_coded_field
        self.binary_analysis_file_key = binary_analysis_file_key

        if id_field is None:
            id_field = "{}_id".format(self.raw_field)
        self.id_field = id_field

class PipelineConfiguration(object):  
    DEV_MODE = False
    
    PROJECT_START_DATE = isoparse("2019-04-02T00:00:00+0300") 
    PROJECT_END_DATE = isoparse("2019-05-14T00:00:00+0300")

    ADVERT_PHONE_NUMBERS_CODE_FILTERS = ["code-NOP-4eb70633", "code-STOP-08b832a8", "code-NA-f93d3eb7","code-NR-5e3eee23"]

    # Radio and follow up questions coding plans
    
    RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01e01_raw",
                    coded_field="rqa_s01e01_coded",
                    time_field="sent_on",
                    coda_filename="s01e01.json",
                    icr_filename="icraf_s01e01.csv",
                    run_id_field="rqa_s01e01_run_id",
                    analysis_file_key="rqa_s01e01_",
                    cleaner=None,
                    code_scheme=CodeSchemes.ICRAF_S01E01,
                    binary_code_scheme=CodeSchemes.SO1EO1_YES_NO,
                    binary_coded_field="rqa_s01e01_yes_no_coded",
                    binary_analysis_file_key="rqa_s01e01_yes_no"),
        
        CodingPlan(raw_field="rqa_s01e02_raw",
                    coded_field="rqa_s01e02_coded",
                    time_field="sent_on",
                    coda_filename="s01e02.json",
                    icr_filename="icraf_s01e02.csv",
                    run_id_field="rqa_s01e02_run_id",
                    analysis_file_key="rqa_s01e02_",
                    cleaner=None,
                    code_scheme=CodeSchemes.ICRAF_S01E02,
                    binary_code_scheme=CodeSchemes.SO1EO2_YES_NO,
                    binary_coded_field="rqa_s01e02_yes_no_coded",
                    binary_analysis_file_key="rqa_s01e02_yes_no"),
        
        CodingPlan(raw_field="rqa_s01e03_raw",
                    coded_field="rqa_s01e03_coded",
                    time_field="sent_on",
                    coda_filename="s01e03.json",
                    icr_filename="icraf_s01e03.csv",
                    run_id_field="rqa_s01e03_run_id",
                    analysis_file_key="rqa_s01e03_",
                    cleaner=None,
                    code_scheme=CodeSchemes.ICRAF_S01E03,
                    binary_code_scheme=CodeSchemes.SO1EO3_YES_NO,
                    binary_coded_field="rqa_s01e03_yes_no_coded",
                    binary_analysis_file_key="rqa_s01e03_yes_no"),

        CodingPlan(raw_field="rqa_s01e04_raw",
                    coded_field="rqa_s01e04_coded",
                    time_field="sent_on",
                    coda_filename="s01e04.json",
                    icr_filename="icraf_s01e04.csv",
                    run_id_field="rqa_s01e04_run_id",
                    analysis_file_key="rqa_s01e04_",
                    cleaner=None,
                    code_scheme=CodeSchemes.ICRAF_S01E04),
                    
        CodingPlan(raw_field="rqa_s01e05_raw",
                    coded_field="rqa_s01e05_coded",
                    time_field="sent_on",
                    coda_filename="s01e05.json",
                    icr_filename="icraf_s01e05.csv",
                    run_id_field="rqa_s01e05_run_id",
                    analysis_file_key="rqa_s01e05_",
                    cleaner=None,
                    code_scheme=CodeSchemes.ICRAF_S01E05,
                    binary_code_scheme=CodeSchemes.SO1EO5_YES_NO,
                    binary_coded_field="rqa_s01e05_yes_no_coded",
                    binary_analysis_file_key="rqa_s01e05_yes_no"),
        
        CodingPlan(raw_field="rqa_s01e06_raw",
                    coded_field="rqa_s01e06_coded",
                    time_field="sent_on",
                    coda_filename="s01e06.json",
                    icr_filename="icraf_s01e06.csv",
                    run_id_field="rqa_s01e06_run_id",
                    analysis_file_key="rqa_s01e06_",
                    cleaner=None,
                    code_scheme=CodeSchemes.ICRAF_S01E06)
        
    ]
    #TODO: Add an assert for binary schemes = NONE for both follow_ups and demogs coding plans
    FOLLOW_UP_CODING_PLANS = [

        CodingPlan(raw_field="current_practices_raw",
                    coded_field="current_practices_coded",
                    time_field="current_practices_time",
                    coda_filename="current_practices.json",
                    icr_filename="current_practices_icr.csv",
                    run_id_field="current_practices_run_id",
                    analysis_file_key="current_practices_",
                    cleaner=None,
                    code_scheme=CodeSchemes.CURRENT_PRACTICES),
        
        CodingPlan(raw_field="upper_tana_practices_raw",
                    coded_field="upper_tana_practices_coded",
                    time_field="upper_tana_practices_time",
                    coda_filename="upper_tana_practices.json",
                    icr_filename="upper_tana_practices_icr.csv",
                    run_id_field="upper_tana_practices_run_id",
                    analysis_file_key="upper_tana_practices_",
                    cleaner=None,
                    code_scheme=CodeSchemes.UPPER_TANA_PRACTICES),
        
        CodingPlan(raw_field="new_practices_raw",
                    coded_field="new_practices_coded",
                    time_field="new_practices_time",
                    coda_filename="new_practices.json",
                    icr_filename="new_practices_icr.csv",
                    run_id_field="new_practices_run_id",
                    analysis_file_key="new_practices_",
                    cleaner=None,
                    code_scheme=CodeSchemes.NEW_PRACTICES),
        
        CodingPlan(raw_field="new_practices_challenges_raw",
                    coded_field="new_practices_challenges_coded",
                    time_field="new_practices_challenges_time",
                    coda_filename="new_practices_challenges.json",
                    icr_filename="new_practices_challenges_icr.csv",
                    run_id_field="new_practices_challenges_run_id",
                    analysis_file_key="new_practices_challenges_",
                    cleaner=None,
                    code_scheme=CodeSchemes.NEW_PRACTICES_CHALLENGES),

        CodingPlan(raw_field="organizations_raw",
                    coded_field="organizations_coded",
                    time_field="organizations_time",
                    coda_filename="organizations.json",
                    icr_filename="organizations_icr.csv",
                    run_id_field="organizations_run_id",
                    analysis_file_key="organizations_",
                    cleaner=None,
                    code_scheme=CodeSchemes.ORGANIZATIONS)
    ]
    
    @staticmethod
    def clean_age_with_range_filter(text):
        """
        Cleans age from the given `text`, setting to NC if the cleaned age is not in the range 10 <= age < 100.
        """
        age = swahili.DemographicCleaner.clean_age(text)
        if type(age) == int and 10 <= age < 100:
            return str(age)
            # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
            #       NC in the case where age is an int but is out of range
        else:
            return Codes.NOT_CODED
  
    DEMOGS_CODING_PLANS = [
        CodingPlan(raw_field="age_raw",
                    coded_field="age_coded",
                    time_field="age_time",
                    coda_filename="age.json",
                    analysis_file_key="age",
                    cleaner=lambda text: PipelineConfiguration.clean_age_with_range_filter(text),
                    code_scheme=CodeSchemes.AGE),

        CodingPlan(raw_field="constituency_raw",
                    id_field="constituency_raw_id",
                    coded_field="constituency_coded",
                    time_field="constituency_time",
                    coda_filename="constituency.json",
                    analysis_file_key="constituency",
                    cleaner=None,
                    code_scheme=CodeSchemes.CONSTITUENCY),

        CodingPlan(raw_field="gender_raw",
                    coded_field="gender_coded",
                    time_field="gender_time",
                    coda_filename="gender.json",
                    analysis_file_key="gender",
                    cleaner=swahili.DemographicCleaner.clean_gender,
                    code_scheme=CodeSchemes.GENDER),

        CodingPlan(raw_field="livelihood_raw",
                    coded_field="livelihood_coded",
                    time_field="livelihood_time",
                    coda_filename="livelihood.json",
                    analysis_file_key="livelihood",
                    cleaner=None,
                    code_scheme=CodeSchemes.LIVELIHOOD)
    ]
    
    def __init__(self, rapid_pro_domain, rapid_pro_token_file_url, rapid_pro_test_contact_uuids,
                    rapid_pro_key_remappings, drive_upload=None):
            """
            :param rapid_pro_domain: URL of the Rapid Pro server to download data from.
            :type rapid_pro_domain: str
            :param rapid_pro_token_file_url: GS URL of a text file containing the authorisation token for the Rapid Pro
                                            server.
            :type rapid_pro_token_file_url: str
            :param rapid_pro_test_contact_uuids: Rapid Pro contact UUIDs of test contacts.
                                                Runs for any of those test contacts will be tagged with {'test_run': True},
                                                and dropped when the pipeline is in production mode.
            :type rapid_pro_test_contact_uuids: list of str
            :param rapid_pro_key_remappings: List of rapid_pro_key -> pipeline_key remappings.
            :type rapid_pro_key_remappings: list of RapidProKeyRemapping
            :param drive_upload: Configuration for uploading to Google Drive, or None.
                                If None, does not upload to Google Drive.
            :type drive_upload: DriveUploadPaths | None
            """
            self.rapid_pro_domain = rapid_pro_domain
            self.rapid_pro_token_file_url = rapid_pro_token_file_url
            self.rapid_pro_test_contact_uuids = rapid_pro_test_contact_uuids
            self.rapid_pro_key_remappings = rapid_pro_key_remappings
            self.drive_upload = drive_upload

            self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        rapid_pro_domain = configuration_dict["RapidProDomain"]
        rapid_pro_token_file_url = configuration_dict["RapidProTokenFileURL"]
        rapid_pro_test_contact_uuids = configuration_dict["RapidProTestContactUUIDs"]
        rapid_pro_key_remappings = []
        for remapping_dict in configuration_dict["RapidProKeyRemappings"]:
            rapid_pro_key_remappings.append(RapidProKeyRemapping.from_configuration_dict(remapping_dict))

        drive_upload_paths = None
        if "DriveUpload" in configuration_dict:
            drive_upload_paths = DriveUpload.from_configuration_dict(configuration_dict["DriveUpload"])

        return cls(rapid_pro_domain, rapid_pro_token_file_url, rapid_pro_test_contact_uuids,
                   rapid_pro_key_remappings, drive_upload_paths)

    @classmethod
    def from_configuration_file(cls, f):
        return cls.from_configuration_dict(json.load(f))
    
    def validate(self):
        validators.validate_string(self.rapid_pro_domain, "rapid_pro_domain")
        validators.validate_string(self.rapid_pro_token_file_url, "rapid_pro_token_file_url")

        validators.validate_list(self.rapid_pro_test_contact_uuids, "rapid_pro_test_contact_uuids")
        for i, contact_uuid in enumerate(self.rapid_pro_test_contact_uuids):
            validators.validate_string(contact_uuid, f"rapid_pro_test_contact_uuids[{i}]")

        validators.validate_list(self.rapid_pro_key_remappings, "rapid_pro_key_remappings")
        for i, remapping in enumerate(self.rapid_pro_key_remappings):
            assert isinstance(remapping, RapidProKeyRemapping), \
                f"rapid_pro_key_mappings[{i}] is not of type RapidProKeyRemapping"
            remapping.validate()

        if self.drive_upload is not None:
            assert isinstance(self.drive_upload, DriveUpload), \
                "drive_upload is not of type DriveUpload"
            self.drive_upload.validate()


class RapidProKeyRemapping(object):
    def __init__(self, rapid_pro_key, pipeline_key):
        """
        :param rapid_pro_key: Name of key in the dataset exported via RapidProTools.
        :type rapid_pro_key: str
        :param pipeline_key: Name to use for that key in the rest of the pipeline.
        :type pipeline_key: str
        """
        self.rapid_pro_key = rapid_pro_key
        self.pipeline_key = pipeline_key
        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        rapid_pro_key = configuration_dict["RapidProKey"]
        pipeline_key = configuration_dict["PipelineKey"]
        
        return cls(rapid_pro_key, pipeline_key)
    
    def validate(self):
        validators.validate_string(self.rapid_pro_key, "rapid_pro_key")
        validators.validate_string(self.pipeline_key, "pipeline_key")

class DriveUpload(object):
    def __init__(self, drive_credentials_file_url, production_upload_path, messages_upload_path,
                 individuals_upload_path, traced_data_upload_path):
        """
        :param drive_credentials_file_url: GS URL to the private credentials file for the Drive service account to use
                                           to upload the output files.
        :type drive_credentials_file_url: str
        :param production_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                       production CSV to.
        :type production_upload_path: str
        :param messages_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                     messages analysis CSV to.
        :type messages_upload_path: str
        :param individuals_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                        individuals analysis CSV to.
        :type individuals_upload_path: str
        :param traced_data_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                        serialized TracedData from this pipeline run to.
        :type traced_data_upload_path: str
        """
        self.drive_credentials_file_url = drive_credentials_file_url
        self.production_upload_path = production_upload_path
        self.messages_upload_path = messages_upload_path
        self.individuals_upload_path = individuals_upload_path
        self.traced_data_upload_path = traced_data_upload_path

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        drive_credentials_file_url = configuration_dict["DriveCredentialsFileURL"]
        production_upload_path = configuration_dict["ProductionUploadPath"]
        messages_upload_path = configuration_dict["MessagesUploadPath"]
        individuals_upload_path = configuration_dict["IndividualsUploadPath"]
        traced_data_upload_path = configuration_dict["TracedDataUploadPath"]

        return cls(drive_credentials_file_url, production_upload_path, messages_upload_path,
                   individuals_upload_path, traced_data_upload_path)

    def validate(self):
        validators.validate_string(self.drive_credentials_file_url, "drive_credentials_file_url")
        assert urlparse(self.drive_credentials_file_url).scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                                                         "URL (i.e. of the form gs://bucket-name/file)"

        validators.validate_string(self.production_upload_path, "production_upload_path")
        validators.validate_string(self.messages_upload_path, "messages_upload_path")
        validators.validate_string(self.individuals_upload_path, "individuals_upload_path")
        validators.validate_string(self.traced_data_upload_path, "traced_data_upload_path")
