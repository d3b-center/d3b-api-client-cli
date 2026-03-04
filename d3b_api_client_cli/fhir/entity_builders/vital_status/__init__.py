# noqa

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.vital_status.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
    extension_age_at_event,
)


class VitalStatus(FhirResourceBuilder):
    """
    Build FHIR Observation from Dataservice Outcome
    """

    sources = {"required": ["Outcome"], "optional": []}
    resource_type = "Observation"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.OUTCOME.TARGET_SERVICE_ID
    external_id_col = CONCEPT.OUTCOME.ID
    kf_id_system = "outcomes"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["patient"]

    def _build_resource(self, row, patient_reference):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)
        vital_status = row.get(CONCEPT.OUTCOME.VITAL_STATUS)
        event_age_days = row.get(CONCEPT.OUTCOME.EVENT_AGE_DAYS)

        # Add references
        participant_kf_id = row[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
        resource["subject"] = patient_reference(participant_kf_id)

        # Add other content
        resource["status"] = "final"
        resource["code"] = {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "263493007",
                    "display": "Clinical status (attribute)",
                }
            ],
            "text": "Clinical status",
        }
        # effectiveDateTime
        try:
            resource["_effectiveDateTime"] = extension_age_at_event(
                patient_reference(participant_kf_id), event_age_days
            )
        except (ValueError, TypeError):
            pass

        # valueCodeableConcept
        if vital_status:
            value = {"text": vital_status}
            if code_coding.get(vital_status):
                value.setdefault("coding", []).append(code_coding[vital_status])
            resource["valueCodeableConcept"] = value

        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        self.source_tables = self.import_source_data(source_dir)
        df = list(self.source_tables.values())[0]

        builders = self.import_reference_builders()
        patient_reference = builders["patient"].fhir_reference

        resources = []
        total = df.shape[0]
        for i, row in df.iterrows():
            resource = self._build_resource(row, patient_reference)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )
            resources.append(resource)

        return resources
