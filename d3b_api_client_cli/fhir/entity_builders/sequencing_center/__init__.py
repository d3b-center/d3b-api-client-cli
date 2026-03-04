# noqa
"""
Module responsible for building FHIR JSON that captures the sequencing center 
info 
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.sequencing_center.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)


class SequencingCenter(FhirResourceBuilder):
    """
    Build FHIR Organization from Dataservice SequencingCenter
    """

    sources = {"required": ["SequencingCenter"], "optional": []}
    resource_type = "Organization"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID
    external_id_col = CONCEPT.SEQUENCING.CENTER.ID
    kf_id_system = "sequencing_centers"
    external_id_system = f"{kf_id_system}?external_id="

    def _build_resource(self, row):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)
        name = row.get(CONCEPT.SEQUENCING.CENTER.NAME)

        # Add other content
        # name
        if name:
            resource["name"] = name

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
