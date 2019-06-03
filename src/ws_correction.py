import time

from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO

from src.lib import PipelineConfiguration
from src.lib.pipeline_configuration import CodeSchemes

log = Logger(__name__)


class _WSUpdate(object):
    def __init__(self, message, sent_on, source):
        self.message = message
        self.sent_on = sent_on
        self.source = source

class WSCorrection(object):
    @staticmethod
    def move_wrong_scheme_messages(user, data, coda_input_dir):
        log.info("Importing manually coded Coda files to '_WS_correct_dataset' coded fields...")
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.DEMOGS_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field + "_WS")
            with open(f"{coda_input_dir}/{plan.coda_filename}") as f:
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                    user, data, plan.id_field + "_WS",
                    {f"{plan.coded_field}_WS_correct_dataset": CodeSchemes.WS_CORRECT_DATASET}, f
                )

        # TODO: Check for coding errors i.e. WS but no correct_dataset or correct_dataset but no WS

        # Construct a map from WS normal code id to the raw field that code indicates a requested move to.
        ws_code_to_raw_field_map = dict()
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.DEMOGS_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
            if plan.ws_code is not None:
                ws_code_to_raw_field_map[plan.ws_code.code_id] = plan.raw_field

        # Group the TracedData by uid.
        data_grouped_by_uid = dict()
        for td in data:
            uid = td["uid"]
            if uid not in data_grouped_by_uid:
                data_grouped_by_uid[uid] = []
            data_grouped_by_uid[uid].append(td)

        # Perform the WS correction for each uid.
        log.info("Performing WS correction...")
        corrected_data = []  # List of TracedData with the WS data moved.
        for group in data_grouped_by_uid.values():
            log.debug(f"\n\nStarting re-map for {group[0]['uid']}...")
            for i, td in enumerate(group):
                log.debug(f"--------------td[{i}]--------------")
                for _plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.DEMOGS_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
                    log.debug(f"{_plan.raw_field}: {td.get(_plan.raw_field)}")
                    log.debug(f"{_plan.time_field}: {td.get(_plan.time_field)}")

            # Find all the demogs data being moved.
            # (Note: we only need to check one td in this group because all the demographics are the same)
            td = group[0]
            demogs_moves = dict()  # of source_field -> target_field
            for plan in PipelineConfiguration.DEMOGS_CODING_PLANS:
                if plan.raw_field not in td:
                    continue
                ws_code = CodeSchemes.WS_CORRECT_DATASET.get_code_with_id(
                    td[f"{plan.coded_field}_WS_correct_dataset"]["CodeID"])
                if ws_code.code_type == "Normal":
                            log.debug(f"Detected redirect from {plan.raw_field} -> {ws_code_to_raw_field_map.get(ws_code.code_id, ws_code.code_id)} for message {td[plan.raw_field]}")
                            demogs_moves[plan.raw_field] = ws_code_to_raw_field_map[ws_code.code_id]

            # Find all the follow_up_surveys_moves data being moved.
            follow_up_surveys_moves = dict()  # of source_field -> target_field
            for plan in PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
                if plan.raw_field not in td:
                    continue
                ws_code = CodeSchemes.WS_CORRECT_DATASET.get_code_with_id(
                    td[f"{plan.coded_field}_WS_correct_dataset"]["CodeID"])
                if ws_code.code_type == "Normal":
                                log.debug(f"Detected redirect from ({i}, {plan.raw_field}) -> {ws_code_to_raw_field_map.get(ws_code.code_id, ws_code.code_id)} for message {td[plan.raw_field]}")
                                follow_up_surveys_moves[(i, plan.raw_field)] = ws_code_to_raw_field_map[ws_code.code_id]

            # Find all the RQA data being moved.
            rqa_moves = dict()  # of (index in group, source_field) -> target_field
            for i, td in enumerate(group):
                for plan in PipelineConfiguration.RQA_CODING_PLANS:
                    if plan.raw_field not in td:
                        continue
                    ws_code = CodeSchemes.WS_CORRECT_DATASET.get_code_with_id(td[f"{plan.coded_field}_WS_correct_dataset"]["CodeID"])
                    if ws_code.code_type == "Normal":
                        log.debug(f"Detected redirect from ({i}, {plan.raw_field}) -> {ws_code_to_raw_field_map.get(ws_code.code_id, ws_code.code_id)} for message {td[plan.raw_field]}")
                        rqa_moves[(i, plan.raw_field)] = ws_code_to_raw_field_map[ws_code.code_id]

            # Build a dictionary of the demog fields that haven't been moved, and clear fields for those which have.
            demogs_updates = dict()  # of raw_field -> updated value
            for plan in PipelineConfiguration.DEMOGS_CODING_PLANS:
                if plan.raw_field in demogs_moves.keys():
                    # Data is moving
                    demogs_updates[plan.raw_field] = []
                elif plan.raw_field in td:
                    # Data is not moving
                    demogs_updates[plan.raw_field] = [_WSUpdate(td[plan.raw_field], td[plan.time_field], plan.raw_field)]

            # Build a dictionary of the follow-up fields that haven't been moved, and clear fields for those which have.
            follow_up_survey_updates = dict()  # of raw_field -> updated value
            for plan in PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
                if plan.raw_field in follow_up_surveys_moves.keys():
                    # Data is moving
                    follow_up_survey_updates[plan.raw_field] = []
                elif plan.raw_field in td:
                    # Data is not moving
                    follow_up_surveys_moves[plan.raw_field] = [
                        _WSUpdate(td[plan.raw_field], td[plan.time_field], plan.raw_field)]

            # Build a list of the rqa fields that haven't been moved.
            rqa_updates = []  # of (field, value)
            for i, td in enumerate(group):
                for plan in PipelineConfiguration.RQA_CODING_PLANS:
                    if plan.raw_field in td:
                        if (i, plan.raw_field) in rqa_moves.keys():
                            # Data is moving
                            pass
                        else:
                            # Data is not moving
                            rqa_updates.append((plan.raw_field, _WSUpdate(td[plan.raw_field], td[plan.time_field], plan.raw_field)))

            raw_demog_fields = {plan.raw_field for plan in PipelineConfiguration.DEMOGS_CODING_PLANS}
            raw_follow_up_survey_fields = {plan.raw_field for plan in PipelineConfiguration.FOLLOW_UP_CODING_PLANS}
            raw_rqa_fields = {plan.raw_field for plan in PipelineConfiguration.RQA_CODING_PLANS}

            # Add data moving from demogs fields to the relevant demog_/follow_up/rqa_updates
            for plan in PipelineConfiguration.DEMOGS_CODING_PLANS + PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
                if plan.raw_field not in demogs_moves:
                    continue
                target_field = demogs_moves[plan.raw_field]
                update = _WSUpdate(td[plan.raw_field], td[plan.time_field], plan.raw_field)
                if target_field in raw_demog_fields:
                    demogs_updates[target_field] = demogs_updates.get(target_field, []) + [update]
                elif target_field in raw_follow_up_survey_fields:
                    follow_up_survey_updates[target_field] = follow_up_survey_updates.get(target_field, []) + [update]
                else:
                    assert target_field in raw_rqa_fields, f"Raw field '{target_field}' not in any coding plan"
                    rqa_updates.append((target_field, update))

            # Add data moving from demog fields to the relevant demogs_/rqa_updates
            for (i, source_field), target_field in rqa_moves.items():
                for plan in PipelineConfiguration.DEMOGS_CODING_PLANS + PipelineConfiguration.RQA_CODING_PLANS:
                    if plan.raw_field == source_field:
                        _td = group[i]
                        update = _WSUpdate(_td[plan.raw_field], _td[plan.time_field], plan.raw_field)
                        if target_field in raw_demog_fields:
                            demogs_updates[target_field] = demogs_updates.get(target_field, []) + [update]
                        elif target_field in raw_follow_up_survey_fields:
                            follow_up_survey_updates[target_field] = follow_up_survey_updates.get(target_field, []) + [update]
                        else:
                            assert target_field in raw_rqa_fields, f"Raw field '{target_field}' not in any coding plan"
                            rqa_updates.append((target_field, update))

            # Add data moving from follow up survey fields to the relevant demogs_/rqa_updates
            for (i, source_field), target_field in rqa_moves.items():
                for plan in PipelineConfiguration.FOLLOW_UP_CODING_PLANS + PipelineConfiguration.RQA_CODING_PLANS:
                    if plan.raw_field == source_field:
                        _td = group[i]
                        update = _WSUpdate(_td[plan.raw_field], _td[plan.time_field], plan.raw_field)
                        if target_field in raw_follow_up_survey_fields:
                            follow_up_survey_updates[target_field] = follow_up_survey_updates.get(target_field, []) + [update]
                        elif target_field in demogs_updates:
                            demogs_updates[target_field] = demogs_updates.get(target_field, []) + [update]
                        else:
                            assert target_field in raw_rqa_fields, f"Raw field '{target_field}' not in any coding plan"
                            rqa_updates.append((target_field, update))

            # Re-format the demogs updates to a form suitable for use by the rest of the pipeline
            flattened_demogs_updates = {}
            for plan in PipelineConfiguration.DEMOGS_CODING_PLANS:
                if plan.raw_field in demogs_updates:
                    plan_updates = demogs_updates[plan.raw_field]

                    if len(plan_updates) > 0:
                        flattened_demogs_updates[plan.raw_field] = "; ".join([u.message for u in plan_updates])
                        flattened_demogs_updates[plan.time_field] = sorted([u.sent_on for u in plan_updates])[0]
                        flattened_demogs_updates[f"{plan.raw_field}_source"] = "; ".join(
                            [u.source for u in plan_updates])
                    else:
                        flattened_demogs_updates[plan.raw_field] = None
                        flattened_demogs_updates[plan.time_field] = None
                        flattened_demogs_updates[f"{plan.raw_field}_source"] = None

            # Re-format the follow up updates to a form suitable for use by the rest of the pipeline
            flattened_follow_up_survey_updates = {}
            for plan in PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
                if plan.raw_field in demogs_updates:
                    plan_updates = demogs_updates[plan.raw_field]

                    if len(plan_updates) > 0:
                        flattened_follow_up_survey_updates[plan.raw_field] = "; ".join([u.message for u in plan_updates])
                        flattened_follow_up_survey_updates[plan.time_field] = sorted([u.sent_on for u in plan_updates])[0]
                        flattened_follow_up_survey_updates[f"{plan.raw_field}_source"] = "; ".join(
                            [u.source for u in plan_updates])
                    else:
                        flattened_follow_up_survey_updates[plan.raw_field] = None
                        flattened_follow_up_survey_updates[plan.time_field] = None
                        flattened_follow_up_survey_updates[f"{plan.raw_field}_source"] = None

            # Hide the demog keys currently in the TracedData which have had data moved away.
            td.hide_keys({k for k, v in flattened_demogs_updates.items() if v is None}.intersection(td.keys()),
                         Metadata(user, Metadata.get_call_location(), time.time()))

            # Hide the follow up keys currently in the TracedData which have had data moved away.
            td.hide_keys({k for k, v in flattened_follow_up_survey_updates.items() if v is None}.intersection(td.keys()),
                         Metadata(user, Metadata.get_call_location(), time.time()))

            # Update with the corrected demog + follow up data
            td.append_data({k: v for k, v in flattened_demogs_updates.items() if v is not None},
                           Metadata(user, Metadata.get_call_location(), time.time()))

            # Hide all the RQA fields (they will be added back, in turn, in the next step).
            td.hide_keys({plan.raw_field for plan in PipelineConfiguration.RQA_CODING_PLANS}.intersection(td.keys()),
                         Metadata(user, Metadata.get_call_location(), time.time()))

            # For each rqa message, create a copy of this td, append the rqa message, and add this to the
            # list of TracedData.
            for target_field, update in rqa_updates:
                rqa_dict = {
                    target_field: update.message,
                    "sent_on": update.sent_on,
                    f"{target_field}_source": update.source
                }

                corrected_td = td.copy()
                corrected_td.append_data(rqa_dict, Metadata(user, Metadata.get_call_location(), time.time()))
                corrected_data.append(corrected_td)

                log.debug(f"----------Created TracedData--------------")
                for _plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.DEMOGS_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
                    log.debug(f"{_plan.raw_field}: {corrected_td.get(_plan.raw_field)}")
                    log.debug(f"{_plan.time_field}: {corrected_td.get(_plan.time_field)}")

                corrected_data.append(corrected_td)

        return corrected_data
