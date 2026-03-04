# noqa

import logging
import pandas

from d3b_api_client_cli import utils
from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.drs_document_reference.mapping import *
from d3b_api_client_cli.fhir.entity_builders.drs_doc_ref_common import (
    DocumentReferenceBase,
)

logger = logging.getLogger(__name__)


class DrsDocumentReference(DocumentReferenceBase):
    """
    Build FHIR DocumentReference from Dataservice GenomicFile
    """

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        return super()._build(source_dir, only_index_files=False)
