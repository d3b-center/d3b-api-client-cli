# noqa
"""
Module responsible for building FHIR JSON that captures the patient phenotype
info 
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.phenotype.mapping import *
from d3b_api_client_cli.fhir.constants import MISSING_DATA_VALUES
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
    extension_age_at_event,
)


class Phenotype(FhirResourceBuilder):
    """
    Build FHIR Condition from Dataservice Phenotype
    """

    sources = {"required": ["Phenotype"], "optional": []}
    resource_type = "Condition"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.PHENOTYPE.TARGET_SERVICE_ID
    external_id_col = CONCEPT.PHENOTYPE.ID
    kf_id_system = "phenotypes"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["patient"]

    def _build_resource(self, row, patient_reference):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)

        observed = row.get(CONCEPT.PHENOTYPE.OBSERVED)
        name = row.get(CONCEPT.PHENOTYPE.NAME)
        hpo_id = row.get(CONCEPT.PHENOTYPE.HPO_ID)
        snomed_id = row.get(CONCEPT.PHENOTYPE.SNOMED_ID)
        event_age_days = row.get(CONCEPT.PHENOTYPE.EVENT_AGE_DAYS)

        # Add references
        participant_kf_id = row[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
        resource["subject"] = patient_reference(participant_kf_id)

        # Add other content
        resource["meta"]["profile"] = [
            "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/phenotype"
        ]

        # verificationStatus
        verification_status = {"text": observed}
        observed_code = verification_status_coding.get(observed)
        if not observed_code:
            observed_code = verification_status_coding["default"]
        verification_status.setdefault("coding", []).append(observed_code)
        resource["verificationStatus"] = verification_status

        # code
        code = {"text": name}
        if hpo_id and hpo_id not in MISSING_DATA_VALUES:
            code.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/hp.owl",
                    "code": hpo_id,
                }
            )
        if snomed_id and snomed_id not in MISSING_DATA_VALUES:
            code.setdefault("coding", []).append(
                {
                    "system": "http://snomed.info/sct",
                    "code": snomed_id,
                }
            )
        resource["code"] = code

        # recordedDate
        try:
            resource["_recordedDate"] = extension_age_at_event(
                patient_reference(participant_kf_id), event_age_days
            )
        except (ValueError, TypeError):
            pass

        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        self.source_tables = self.import_source_data(source_dir)
        builders = self.import_reference_builders()
        patient_reference = builders["patient"].fhir_reference

        df = list(self.source_tables.values())[0]

        resources = []
        total = df.shape[0]
        for i, row in df.iterrows():
            resource = self._build_resource(row, patient_reference)
            resources.append(resource)

            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
