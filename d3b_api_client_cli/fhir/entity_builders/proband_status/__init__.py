# noqa
"""
Module responsible for building FHIR JSON that capture the patient's 
proband status
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.proband_status.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)


class ProbandStatus(FhirResourceBuilder):
    """
    Build FHIR Observation from Dataservice Participant
    """

    sources = {"required": ["Participant"], "optional": []}
    resource_type = "Observation"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
    external_id_col = CONCEPT.PARTICIPANT.ID
    kf_id_system = "participants"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["patient"]

    def _build_resource(self, row, patient_reference):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)
        proband_status = row[CONCEPT.PARTICIPANT.IS_PROBAND]

        # Add references
        participant_kf_id = row[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
        resource["subject"] = patient_reference(participant_kf_id)

        # Add other content
        resource["status"] = "final"
        resource["code"] = {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "85900004",
                    "display": "Proband (finding)",
                }
            ],
            "text": "Proband status",
        }
        resource["valueCodeableConcept"] = {
            "coding": [value_coding[proband_status]],
            "text": proband_status,
        }
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
            resources.append(resource)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
