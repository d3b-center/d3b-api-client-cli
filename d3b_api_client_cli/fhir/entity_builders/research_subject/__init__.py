# noqa

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.research_subject.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)


class ResearchSubject(FhirResourceBuilder):
    """
    Build FHIR ResearchSubject from Dataservice Participant
    """

    sources = {"required": ["Participant"], "optional": []}
    resource_type = "ResearchSubject"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
    external_id_col = CONCEPT.PARTICIPANT.ID
    kf_id_system = "participants"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["research_study", "patient"]

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        self.source_tables = self.import_source_data(source_dir)
        builders = self.import_reference_builders()
        patient_reference = builders["patient"].fhir_reference
        study_reference = builders["research_study"].fhir_reference

        df = list(self.source_tables.values())[0]

        resources = []
        total = df.shape[0]
        for i, row in df.iterrows():
            # Initalize resource with basic content
            resource = self.init_resource(row)
            participant_kf_id = row[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
            study_kf_id = self.study_id

            resource["status"] = "off-study"

            # Add references
            resource["study"] = study_reference(study_kf_id)
            resource["individual"] = patient_reference(participant_kf_id)

            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )
            resources.append(resource)

        return resources
