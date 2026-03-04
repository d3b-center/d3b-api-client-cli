# noqa
"""
Module responsible for building FHIR JSON that captures the patient
histopathology info
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.constants import MISSING_DATA_VALUES
from d3b_api_client_cli.fhir.entity_builders.histopathology.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)


class Histopathology(FhirResourceBuilder):
    """
    Build FHIR Observation from Dataservice BiospecimenDiagnosis
    """

    sources = {"required": ["BiospecimenDiagnosis"], "optional": []}
    resource_type = "Observation"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.BIOSPECIMEN_DIAGNOSIS.TARGET_SERVICE_ID
    external_id_col = CONCEPT.BIOSPECIMEN_DIAGNOSIS.ID
    kf_id_system = "biospecimen-diagnoses"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["patient", "child_specimen", "disease"]

    def _build_resource(
        self, row, patient_reference, specimen_reference, disease_reference
    ):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)

        tumor_descriptor = row.get(CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR)

        # Add references
        participant_kf_id = row[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
        biospecimen_kf_id = row[CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID]
        diagnosis_kf_id = row[CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID]
        resource["subject"] = patient_reference(participant_kf_id)
        resource["specimen"] = specimen_reference(biospecimen_kf_id)
        resource["focus"] = [disease_reference(diagnosis_kf_id)]

        # Add other content
        resource["status"] = "final"
        resource["code"] = {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "250537006",
                    "display": "Histopathology finding (finding)",
                }
            ],
            "text": "Histopathology",
        }
        resource["category"] = [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "laboratory",
                        "display": "Laboratory",
                    }
                ],
                "text": "Histopathology",
            }
        ]

        if tumor_descriptor and tumor_descriptor not in MISSING_DATA_VALUES:
            resource["valueCodeableConcept"] = {"text": tumor_descriptor}

        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        self.source_tables = self.import_source_data(source_dir)
        df = list(self.source_tables.values())[0]

        builders = self.import_reference_builders()
        patient_reference = builders["patient"].fhir_reference
        specimen_reference = builders["child_specimen"].fhir_reference
        disease_reference = builders["disease"].fhir_reference

        resources = []
        total = df.shape[0]
        for i, row in df.iterrows():
            resource = self._build_resource(
                row, patient_reference, specimen_reference, disease_reference
            )
            resources.append(resource)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
