# noqa
"""
Module responsible for building FHIR JSON that captures the patient child
specimen info
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.child_specimen.mapping import *
from d3b_api_client_cli.fhir.entity_builders.specimen_common import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
    extension_age_at_event,
)


class ChildSpecimen(FhirResourceBuilder):
    """
    Build FHIR Specimen from Dataservice Biospecimen. Link this Specimen to
    the parent FHIR Specimen
    """

    sources = {"required": ["Biospecimen"], "optional": []}
    resource_type = "Specimen"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID
    external_id_col = CONCEPT.BIOSPECIMEN.ID
    kf_id_system = "biospecimens"
    external_id_system = f"{kf_id_system}?external_aliquot_id="

    reference_builders = ["patient", "parent_specimen"]

    def _build_resource(
        self, row, patient_reference, parent_specimen_reference
    ):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)

        consent_type = row.get(CONCEPT.BIOSPECIMEN.CONSENT_SHORT_NAME)
        dbgap_consent_code = row.get(
            CONCEPT.BIOSPECIMEN.DBGAP_STYLE_CONSENT_CODE
        )
        external_sample_id = row.get(CONCEPT.SAMPLE.ID)
        external_aliquot_id = row.get(CONCEPT.BIOSPECIMEN.ID)
        analyte_type = row[CONCEPT.BIOSPECIMEN.ANALYTE]
        event_age_days = row.get(CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS)
        volume_ul = row.get(CONCEPT.BIOSPECIMEN.VOLUME_UL)
        specimen_status = row.get(CONCEPT.BIOSPECIMEN.STATUS)
        tissue_type = row.get(CONCEPT.BIOSPECIMEN.TISSUE_TYPE)
        has_matched_normal_sample = row.get(
            CONCEPT.BIOSPECIMEN.HAS_MATCHED_NORMAL_SAMPLE
        )

        preservation_method = row.get(CONCEPT.BIOSPECIMEN.PRESERVATION_METHOD)

        # Add references
        participant_kf_id = row[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
        parent_kf_id = row[CONCEPT.SAMPLE.TARGET_SERVICE_ID]
        resource["subject"] = patient_reference(participant_kf_id)
        if parent_kf_id:
            resource["parent"] = [parent_specimen_reference(parent_kf_id)]

        # Add other content
        resource["status"] = status_mapping.get(specimen_status, "unavailable")

        # meta.tag
        tags = []

        # tissue_type
        if tissue_type:
            tags.append(
                {"system": "urn:kids_first_tissue_type", "code": tissue_type}
            )

        # has_paired_normal
        if has_matched_normal_sample is not None:
            tags.append(
                {
                    "system": "urn:kids_first_has_paired_normal",
                    "code": has_matched_normal_sample,
                }
            )
        # external_sample_id
        if external_sample_id:
            tags.append(
                {
                    "system": "urn:kids_first_external_collection_id",
                    "code": external_sample_id,
                }
            )

        if tags:
            resource["meta"]["tag"].extend(tags)

        # meta.security
        if consent_type:
            resource["meta"].setdefault("security", []).append(
                {
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/biospecimens?consent_type=",
                    "code": consent_type,
                }
            )
        if dbgap_consent_code:
            resource["meta"].setdefault("security", []).append(
                {
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/biospecimens?dbgap_consent_code=",
                    "code": dbgap_consent_code,
                }
            )

        # type
        if analyte_type == constants.COMMON.NOT_APPLICABLE:
            analyte_type = row.get(CONCEPT.BIOSPECIMEN.COMPOSITION)
        specimen_type = {"text": analyte_type}
        if type_coding.get(analyte_type):
            specimen_type["coding"] = type_coding[analyte_type]
        resource["type"] = specimen_type

        # collection
        collection = {}

        # collection.collectedDateTime
        try:
            collection["_collectedDateTime"] = extension_age_at_event(
                patient_reference(participant_kf_id), event_age_days
            )
            # collection.quantity
            collection["quantity"] = {
                "value": float(volume_ul),
                "unit": "microliter",
                "system": "http://unitsofmeasure.org",
                "code": "uL",
            }
        except (ValueError, TypeError):
            pass

        if collection:
            resource["collection"] = collection

        # processing
        processing = {}

        # preservation_method
        if preservation_method_coding.get(preservation_method):
            procedure = {
                "coding": preservation_method_coding[preservation_method]
            }
            processing["procedure"] = procedure

        if processing:
            resource["processing"] = processing

        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        self.source_tables = self.import_source_data(source_dir)
        df = list(self.source_tables.values())[0]

        builders = self.import_reference_builders()
        patient_reference = builders["patient"].fhir_reference
        parent_specimen_reference = builders["parent_specimen"].fhir_reference

        resources = []
        total = df.shape[0]
        for i, row in df.iterrows():
            resource = self._build_resource(
                row, patient_reference, parent_specimen_reference
            )
            resources.append(resource)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
