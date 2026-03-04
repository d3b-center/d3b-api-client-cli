# noqa
"""
Module responsible for building FHIR JSON that captures the practitioner info 
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.practitioner.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)


class Practitioner(FhirResourceBuilder):
    """
    Build FHIR Practitioner from Dataservice Investigator
    """

    sources = {"required": ["Investigator"], "optional": []}
    resource_type = "Practitioner"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID
    external_id_col = CONCEPT.INVESTIGATOR.ID
    kf_id_system = "investigators"
    external_id_system = f"{kf_id_system}?external_id="

    def _build_resource(self, row):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)
        name = row.get(CONCEPT.INVESTIGATOR.NAME)

        # Add other content
        resource["active"] = True
        if name:
            resource["name"] = [{"text": name}]

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
