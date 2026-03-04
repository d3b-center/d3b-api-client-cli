# noqa

import logging

import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.drs_document_reference_index.mapping import *
from d3b_api_client_cli.fhir.entity_builders.drs_doc_ref_common import (
    DocumentReferenceBase,
)

logger = logging.getLogger(__name__)


class DrsDocumentReferenceIndex(DocumentReferenceBase):
    """
    Build FHIR DocumentReference from Dataservice GenomicFile index files
    (e.g. .crai, .bai)
    """

    def _modify_resource(self, resource, row):
        """
        Add reference to DrsDocumentReferenceIndex
        """
        index_file_kf_id = row.get(CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID)
        non_index_kf_id = row.get(
            CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID + "_REF"
        )

        if pandas.notnull(index_file_kf_id) and pandas.notnull(non_index_kf_id):
            resource.setdefault("relatesTo", []).append(
                {
                    "code": "transforms",
                    "target": self.fhir_reference(non_index_kf_id),
                }
            )

        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        return super()._build(source_dir, only_index_files=True)
