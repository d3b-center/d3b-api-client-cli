# noqa
"""
Module responsible for building FHIR JSON that captures the patient family
relationship info 
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.family_relationship.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)


class FamilyRelationship(FhirResourceBuilder):
    """
    Build FHIR Observation from Dataservice FamilyRelationship
    """

    sources = {"required": ["FamilyRelationship"], "optional": []}
    resource_type = "Observation"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.FAMILY_RELATIONSHIP.TARGET_SERVICE_ID
    external_id_col = CONCEPT.FAMILY_RELATIONSHIP.ID
    kf_id_system = "family_relationships"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["patient"]

    def _build_resource(self, row, patient_reference):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)
        relation_from_1_to_2 = row[
            CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2
        ]

        # Add references
        person1_kf_id = row[
            CONCEPT.FAMILY_RELATIONSHIP.PERSON1.TARGET_SERVICE_ID
        ]
        person2_kf_id = row[
            CONCEPT.FAMILY_RELATIONSHIP.PERSON2.TARGET_SERVICE_ID
        ]
        resource["subject"] = patient_reference(person1_kf_id)
        resource["focus"] = [patient_reference(person2_kf_id)]

        # Add other content
        resource["meta"]["profile"] = [
            "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/family-relationship"
        ]
        resource["status"] = "final"
        resource["code"] = {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                    "code": "FAMMEMB",
                    "display": "family member",
                }
            ],
            "text": "Family Relationship",
        }
        # valueCodeableConcept
        if relation_from_1_to_2:
            value = {"text": relation_from_1_to_2}
            if code_coding.get(relation_from_1_to_2):
                value.setdefault("coding", []).append(
                    code_coding[relation_from_1_to_2]
                )
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
            resources.append(resource)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
