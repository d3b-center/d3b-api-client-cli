# noqa
"""
Module responsible for building FHIR JSON that captures the patient info 
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.patient.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)


class Patient(FhirResourceBuilder):
    """
    Build FHIR Patient from Dataservice Participant
    """

    sources = {"required": ["Participant"], "optional": []}
    resource_type = "Patient"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
    external_id_col = CONCEPT.PARTICIPANT.ID
    kf_id_system = "participants"
    external_id_system = f"{kf_id_system}?external_id="

    def _build_resource(self, row):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)

        race = row.get(CONCEPT.PARTICIPANT.RACE)
        ethnicity = row.get(CONCEPT.PARTICIPANT.ETHNICITY)
        gender = row.get(CONCEPT.PARTICIPANT.GENDER)

        # US Core Race
        us_core_race = {}
        if race:
            us_core_race.update(
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                    "extension": [{"url": "text", "valueString": race}],
                }
            )
            if omb_race_category.get(race):
                us_core_race["extension"].append(omb_race_category[race])
        if us_core_race:
            resource.setdefault("extension", []).append(us_core_race)

        # US Core Ethnicity
        us_core_ethnicity = {}
        if ethnicity:
            us_core_ethnicity.update(
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": [{"url": "text", "valueString": ethnicity}],
                }
            )
            if omb_ethnicity_category.get(ethnicity):
                us_core_ethnicity["extension"].append(
                    omb_ethnicity_category[ethnicity]
                )
        if us_core_ethnicity:
            resource.setdefault("extension", []).append(us_core_ethnicity)

        # gender
        if administrative_gender_code.get(gender):
            resource["gender"] = administrative_gender_code[gender]

        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        self.source_tables = self.import_source_data(source_dir)
        df = list(self.source_tables.values())[0]

        resources = []
        total = df.shape[0]
        for i, row in df.iterrows():
            resource = self._build_resource(row)
            resources.append(resource)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
