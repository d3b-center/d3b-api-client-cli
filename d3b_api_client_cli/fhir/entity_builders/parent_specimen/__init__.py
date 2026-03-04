# noqa
"""
Module responsible for building FHIR JSON that captures the patient parent
specimen info
"""

import logging
import pandas
from numpy import nan

from d3b_api_client_cli import utils
from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.parent_specimen.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
    extension_age_at_event,
)
from d3b_api_client_cli.fhir.entity_builders.parent_specimen import (
    tree,
)


PID = tree.PID
CID = tree.CID


class ParentSpecimen(FhirResourceBuilder):
    """
    Build FHIR Specimen from Dataservice Biospecimen and Sample
    """

    sources = {
        "required": ["Biospecimen", "Sample"],
        "optional": ["SampleRelationship"],
    }
    resource_type = "Specimen"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.SAMPLE.TARGET_SERVICE_ID
    external_id_col = CONCEPT.SAMPLE.ID
    kf_id_system = "samples"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["patient"]

    def _build_resource(self, row, patient_reference):
        """
        Build FHIR JSON from csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)

        # Add references
        participant_kf_id = row[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
        resource["subject"] = patient_reference(participant_kf_id)

        kf_id = row[self.kf_id_col]
        sample_type = str(row[CONCEPT.SAMPLE.SAMPLE_TYPE])

        # TODO Where is the consent going to come from?!
        consent_type = row.get(CONCEPT.BIOSPECIMEN.CONSENT_SHORT_NAME)
        dbgap_consent_code = row.get(
            CONCEPT.BIOSPECIMEN.DBGAP_STYLE_CONSENT_CODE
        )

        tissue_type = row.get(CONCEPT.SAMPLE.TISSUE_TYPE)
        has_matched_normal_sample = row.get(
            CONCEPT.SAMPLE.HAS_MATCHED_NORMAL_SAMPLE
        )
        external_collection_id = row.get(CONCEPT.SAMPLE.EXTERNAL_COLLECTION_ID)

        # ncit_id_tissue_type = row.get(CONCEPT.SAMPLE.NCIT_TISSUE_TYPE_ID)
        event_age_days = row.get(CONCEPT.SAMPLE.EVENT_AGE_DAYS)
        volume_ul = row.get(CONCEPT.SAMPLE.VOLUME_UL)
        sample_procurement = row.get(CONCEPT.SAMPLE.SAMPLE_PROCUREMENT)
        anatomy_site = row.get(CONCEPT.SAMPLE.ANATOMY_SITE)
        uberon_anatomy_site_id = row.get(
            CONCEPT.BIOSPECIMEN.UBERON_ANATOMY_SITE_ID
        )
        ncit_anatomy_site_id = row.get(CONCEPT.BIOSPECIMEN.NCIT_ANATOMY_SITE_ID)

        preservation_method = row.get(CONCEPT.SAMPLE.PRESERVATION_METHOD)

        # Add other content
        resource["status"] = "unavailable"

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

        # external_collection_id
        if external_collection_id:
            tags.append(
                {
                    "system": "urn:kids_first_external_collection_id",
                    "code": external_collection_id,
                }
            )

        if tags:
            resource["meta"]["tag"].extend(tags)

        # meta.security
        if consent_type:
            resource["meta"].setdefault("security", []).append(
                {
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/samples?consent_type=",
                    "code": consent_type,
                }
            )
        if dbgap_consent_code:
            resource["meta"].setdefault("security", []).append(
                {
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/samples?dbgap_consent_code=",
                    "code": dbgap_consent_code,
                }
            )

        # type
        specimen_type = {"text": sample_type}
        if type_coding.get(sample_type):
            specimen_type["coding"] = type_coding[sample_type]
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

        # method
        method = {}
        if sample_procurement:
            method["text"] = sample_procurement
            # if collection_method_coding.get(sample_procurement):
            #     method.setdefault("coding", []).append(
            #         collection_method_coding[sample_procurement]
            #     )
        if method:
            collection["method"] = method

        # bodySite
        body_site = {}
        if anatomy_site:
            body_site["text"] = anatomy_site
        if uberon_anatomy_site_id:
            body_site_coding = {"code": uberon_anatomy_site_id}
            if uberon_anatomy_site_id.startswith("UBERON:"):
                body_site_coding["system"] = (
                    "http://purl.obolibrary.org/obo/uberon.owl"
                )
            elif uberon_anatomy_site_id.startswith("EFO:"):
                body_site_coding["system"] = "http://www.ebi.ac.uk/efo/efo.owl"
            body_site.setdefault("coding", []).append(body_site_coding)
        if ncit_anatomy_site_id and ncit_anatomy_site_id.startswith("NCIT:"):
            body_site.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/ncit.owl",
                    "code": ncit_anatomy_site_id,
                }
            )
        if body_site:
            collection["bodySite"] = body_site

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

    def _transform(self, source_tables):
        """
        Do necessary transformations to source tables before FHIR resource
        creation
        """
        # Now we cannot inner merge bc not all Samples have linked Biospecimens
        # Left merge Biospecimens into Samples so that we can get
        # some of the sample info from Biospecimens (consent, etc)
        self.logger.info(
            f"🏭 Start {self.entity_type} pre-processing/transformation ..."
        )
        biospecimens = source_tables["Biospecimen"]
        samples = source_tables["Sample"]
        df = utils.merge_wo_duplicates(
            samples,
            biospecimens[
                [
                    c
                    for c in biospecimens.columns
                    if c.startswith(CONCEPT.BIOSPECIMEN._CONCEPT_NAME)
                ]
                + [CONCEPT.SAMPLE.TARGET_SERVICE_ID]
            ],
            how="left",
            on=CONCEPT.SAMPLE.TARGET_SERVICE_ID,
        )
        df = (
            df.drop_duplicates(CONCEPT.SAMPLE.TARGET_SERVICE_ID)
            .replace({nan: None})
            .reset_index()
        )

        self.logger.info(
            f"Completed pre-processing of {df.shape[0]} {self.entity_type}  ✅"
        )

        return df

    def _create_specimens(self, sample_df):
        """
        Create FHIR Specimens from rows of Samples

        :returns: dict of specimens keyed by FHIR IDs
        """
        self.logger.info(f"🏭 Starting {self.entity_type} resource build ...")
        builders = self.import_reference_builders()
        patient_reference = builders["patient"].fhir_reference

        specimen_dict = {}
        total = sample_df.shape[0]
        for i, row in sample_df.iterrows():
            resource = self._build_resource(row, patient_reference)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )
            specimen_dict[resource["id"]] = resource

        self.logger.info(
            f"Completed building {total} {self.entity_type} resources ✅"
        )

        return specimen_dict

    def _link_specimens(self, specimen_dict, sample_relationship_df):
        """
        Update the FHIR Specimens with references to their appropriate
        specimens, using the sample_relationships table

        :returns: dict of specimens keyed by FHIR IDs
        """
        self.logger.info(f"🏭 Start linking {len(specimen_dict)} specimens ...")
        # Iterate over child specimens in the relationships
        for _, row in sample_relationship_df.iterrows():
            child_id = row.get(
                CONCEPT.SAMPLE_RELATIONSHIP.CHILD.TARGET_SERVICE_ID
            )
            # Look up child FHIR specimen
            child_fhir_id = self.fhir_id(child_id)
            child_specimen = specimen_dict[child_fhir_id]

            # Link each child FHIR specimen to its parent FHIR specimen
            parent_id = row.get(
                CONCEPT.SAMPLE_RELATIONSHIP.PARENT.TARGET_SERVICE_ID
            )
            if parent_id:
                child_specimen["parent"] = [self.fhir_reference(parent_id)]

        self.logger.info(f"Completed specimen linking ✅")

        return specimen_dict

    def _order_specimens(self, specimen_dict, sample_relationship_df):
        """
        Determine load order of Specimens so that references are respected

        Iterate over sample_relationships table to build Specimen trees
        Traverse Specimen trees using BFS/level order traversal
        Add the Specimens from each traversal to the output list of Specimens

        Return the list of ordered Specimens

        :returns: list of dicts
        """
        self.logger.info(
            f"🏭 Start order operation for {len(specimen_dict)} specimens ..."
        )
        ordered_specimens = []
        ordered_ids = []

        # kf_id -> fhir id
        for c in [PID, CID]:
            sample_relationship_df[c] = sample_relationship_df[c].apply(
                lambda sid: self.fhir_id(sid) if sid else sid
            )

        # Collect Specimens that are not part of Specimen trees
        specimens_in_relationships = set(
            sample_relationship_df[PID].values
        ).union(set(sample_relationship_df[CID].values))

        all_specimens = set(specimen_dict.keys())

        # Add non-tree Specimens to output
        ordered_ids.extend(all_specimens - specimens_in_relationships)

        self.logger.debug(
            f"Found {len(ordered_ids)} specimens not in relationships"
        )
        self.logger.debug(
            f"Found {len(specimens_in_relationships)} specimens"
            " that are part of a sample tree"
        )

        # Specimens that are part of Specimen trees need to be ordered
        # in a way such that references are respected
        if utils.df_exists(sample_relationship_df):
            self.logger.info(
                "🧪 Detected sample relationships table of m x n: "
                f" {sample_relationship_df.shape}."
                " Starting tree processing ..."
            )
            ordered_ids.extend(
                tree.level_ordered_specimens(sample_relationship_df)
            )

        # Populate payloads list
        ordered_specimens = [specimen_dict[sid] for sid in ordered_ids]

        if len(ordered_specimens) < len(all_specimens):
            raise tree.MissingDataException(
                "❌ Detected problems in specimen data during "
                "ordering operation. This is likely due to a missing "
                "sample relationship that designates a sample is the "
                "root of a sample tree. Each root sample must have a row "
                "in the sample_relationship like this: parent_id=null "
                "child_id=<sample id of root>"
            )
        self.logger.info(
            f"Completed ordering {len(ordered_specimens)} specimens ✅"
        )

        return ordered_specimens

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        # Import source tables
        source_tables = self.import_source_data(source_dir)

        # Sample pre-processing
        sample_df = self._transform(source_tables)

        # Build specimens
        specimen_dict = self._create_specimens(sample_df)

        # If sample relationships exist, link specimens to each other
        if "SampleRelationship" in source_tables:
            relationship_df = source_tables["SampleRelationship"]
            # -- Link specimens to each other --
            specimen_dict = self._link_specimens(specimen_dict, relationship_df)

            # -- Determine Specimen load order -- #
            resources = self._order_specimens(specimen_dict, relationship_df)
        else:
            resources = list(specimen_dict.values())

        return resources
