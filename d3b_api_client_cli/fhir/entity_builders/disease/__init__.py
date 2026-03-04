# noqa
"""
Module responsible for building FHIR JSON that captures the patient disease
info 
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.constants import MISSING_DATA_VALUES
from d3b_api_client_cli.fhir.entity_builders.disease.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
    extension_age_at_event,
)


class Disease(FhirResourceBuilder):
    """
    Build FHIR Condition from Dataservice Diagnosis
    """

    sources = {"required": ["Diagnosis"], "optional": []}
    resource_type = "Condition"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID
    external_id_col = CONCEPT.DIAGNOSIS.ID
    kf_id_system = "diagnoses"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["patient"]

    def _build_resource(self, row, patient_reference):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)

        # Add references
        participant_kf_id = row[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
        resource["subject"] = patient_reference(participant_kf_id)

        name = row[CONCEPT.DIAGNOSIS.NAME]
        mondo_id = row.get(CONCEPT.DIAGNOSIS.MONDO_ID)
        icd_id = row.get(CONCEPT.DIAGNOSIS.ICD_ID)
        ncit_id = row.get(CONCEPT.DIAGNOSIS.NCIT_ID)
        tumor_location = row.get(CONCEPT.DIAGNOSIS.TUMOR_LOCATION)
        uberon_id = row.get(CONCEPT.DIAGNOSIS.UBERON_TUMOR_LOCATION_ID)
        event_age_days = row.get(CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS)

        # Add other content
        resource["meta"]["profile"] = [
            "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/disease"
        ]
        resource.update(
            {
                "clinicalStatus": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                            "code": "active",
                            "display": "Active",
                        }
                    ],
                    "text": "Active",
                },
                "category": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                                "code": "encounter-diagnosis",
                                "display": "Encounter Diagnosis",
                            }
                        ]
                    }
                ],
            }
        )
        # code
        code = {"text": name}
        if mondo_id and mondo_id not in MISSING_DATA_VALUES:
            code.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/mondo.owl",
                    "code": mondo_id,
                }
            )
        if icd_id and icd_id not in MISSING_DATA_VALUES:
            code.setdefault("coding", []).append(
                {
                    "system": "https://www.who.int/classifications/classification-of-diseases",
                    "code": icd_id,
                }
            )
        if ncit_id and ncit_id not in MISSING_DATA_VALUES:
            code.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/ncit.owl",
                    "code": ncit_id,
                }
            )
        resource["code"] = code

        # bodySite
        body_site = {}
        if tumor_location:
            body_site["text"] = tumor_location
        if uberon_id and uberon_id not in MISSING_DATA_VALUES:
            body_site.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/uberon.owl",
                    "code": uberon_id,
                }
            )
        if body_site:
            resource.setdefault("bodySite", []).append(body_site)

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
