# noqa
"""
Module responsible for building FHIR JSON that captures the practitioner_role
info 
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.practitioner_role.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)


class PractitionerRole(FhirResourceBuilder):
    """
    Build FHIR PractitionerRole from Dataservice Investigator
    """

    sources = {"required": ["Investigator"], "optional": []}
    resource_type = "PractitionerRole"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID
    external_id_col = CONCEPT.INVESTIGATOR.ID
    kf_id_system = "investigators"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["practitioner", "organization"]

    def _build_resource(
        self, row, practitioner_reference, organization_reference
    ):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)

        # Add other content
        resource["active"] = True
        resource["code"] = [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/practitioner-role",
                        "code": "researcher",
                        "display": "Researcher",
                    }
                ]
            }
        ]

        # Add references
        investigator_kf_id = row[CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID]
        resource["organization"] = organization_reference(investigator_kf_id)
        resource["practitioner"] = practitioner_reference(investigator_kf_id)

        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        self.source_tables = self.import_source_data(source_dir)

        builders = self.import_reference_builders()
        practitioner_reference = builders["practitioner"].fhir_reference
        organization_reference = builders["organization"].fhir_reference

        df = list(self.source_tables.values())[0]

        resources = []
        total = df.shape[0]
        for i, row in df.iterrows():
            resource = self._build_resource(
                row, practitioner_reference, organization_reference
            )
            resources.append(resource)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
