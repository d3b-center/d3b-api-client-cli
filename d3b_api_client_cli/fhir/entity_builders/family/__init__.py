# noqa
"""
Module responsible for building FHIR JSON that captures the patient family
info 
"""

import logging
import pandas

from d3b_api_client_cli import utils
from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.family.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)


class Family(FhirResourceBuilder):
    """
    Build FHIR Group from Dataservice Participant and Family
    """

    sources = {"required": ["Participant", "Family"], "optional": []}
    resource_type = "Group"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.FAMILY.TARGET_SERVICE_ID
    external_id_col = CONCEPT.FAMILY.ID
    kf_id_system = "families"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["patient"]

    def _transform(self, source_tables):
        """
        Do necessary transformations to source tables before FHIR resource
        creation. Return a resulting table as a list of dicts/rows.
        """
        participant_df = source_tables["Participant"]
        family_df = source_tables["Family"]

        participant_cols = [
            CONCEPT.FAMILY.TARGET_SERVICE_ID,
            CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            CONCEPT.PARTICIPANT.SPECIES,
        ]

        # Merge participants with families
        df = utils.merge_wo_duplicates(
            participant_df[participant_cols],
            family_df,
            on=CONCEPT.FAMILY.TARGET_SERVICE_ID,
        )
        # Group rows by family ID so we can have one row per family where
        # that row will have a list of family members
        rows = [
            {
                CONCEPT.FAMILY.TARGET_SERVICE_ID: family_id,
                CONCEPT.FAMILY.ID: group.get(CONCEPT.FAMILY.ID).unique()[0],
                CONCEPT.PARTICIPANT.SPECIES: group.get(
                    CONCEPT.PARTICIPANT.SPECIES
                ).unique()[0],
                CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: group.get(
                    CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
                ).unique(),
            }
            for family_id, group in df.groupby(CONCEPT.FAMILY.TARGET_SERVICE_ID)
        ]
        return rows

    def _build_resource(self, row, patient_reference):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)
        species = row.get(CONCEPT.PARTICIPANT.SPECIES)

        # Add references
        members = [
            {
                "entity": patient_reference(participant_kf_id),
                "inactive": False,
            }
            for participant_kf_id in row.get(
                CONCEPT.PARTICIPANT.TARGET_SERVICE_ID, []
            )
        ]
        if members:
            resource["quantity"] = len(members)
            resource["member"] = members

        # Add other content
        resource.update(
            {
                "type": type_code.get(species) or "person",
                "actual": True,
                "code": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                            "code": "FAMMEMB",
                            "display": "family member",
                        },
                    ]
                },
            }
        )

        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        self.source_tables = self.import_source_data(source_dir)

        # Transform the source tables before processing to collect
        # family members in each family row
        rows = self._transform(self.source_tables)

        builders = self.import_reference_builders()
        patient_reference = builders["patient"].fhir_reference

        resources = []
        total = len(rows)
        for i, row in enumerate(rows):
            resource = self._build_resource(row, patient_reference)
            resources.append(resource)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
