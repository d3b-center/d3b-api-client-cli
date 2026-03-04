# noqa

import os
import ast
import logging
from pprint import pformat
from urllib.parse import urlparse

import pandas

from d3b_api_client_cli.config import config
from d3b_api_client_cli import utils
from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.constants import MISSING_DATA_VALUES
from d3b_api_client_cli.fhir.entity_builders.drs_doc_ref_common.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)
from d3b_api_client_cli.fhir.constants import COMMON

logger = logging.getLogger(__name__)


def _build_content_list(
    resource, file_name, urls, drs_uri, hash_dict, file_format, size
):
    """
    Build the content list with file metadata for the resource
    """
    # content
    content_list = []

    # DRS content
    content = {}

    # format
    if file_format and (file_format not in MISSING_DATA_VALUES):
        content["format"] = {
            "system": file_format_coding["system"],
            "code": file_format,
            "display": file_format,
        }

    # attachment
    attachment = {}

    # size
    try:
        attachment.setdefault("extension", []).append(
            {
                "url": "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/file-size",
                "valueDecimal": int(size),
            }
        )
    except:
        pass

    # hash
    if hash_dict:
        if isinstance(hash_dict, str):
            hash_dict = ast.literal_eval(hash_dict)
        for algorithm, hash_value in hash_dict.items():
            attachment.setdefault("extension", []).append(
                {
                    "url": "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/hashes",
                    "valueCodeableConcept": {
                        "coding": [{"display": algorithm}],
                        "text": hash_value,
                    },
                }
            )

    # DRS URI
    if drs_uri:
        attachment["url"] = drs_uri

        # Add tag for file source
        tag = {
            "system": "urn:drs_hostname",
            "code": urlparse(drs_uri).netloc,
        }
        resource["meta"]["tag"].append(tag)

    # File name
    s3_url = urls["s3_url"]
    if not file_name:
        if s3_url:
            file_name = os.path.split(s3_url)[-1]

    if file_name:
        attachment["title"] = file_name

    # s3 url
    if s3_url:
        content_list.append({"attachment": {"url": s3_url}})
    # Additional urls
    other_urls = urls["others"]
    if other_urls:
        for url in other_urls:
            if url != drs_uri:
                content_list.append({"attachment": {"url": url}})

    if attachment:
        content["attachment"] = attachment

    if content:
        content_list.append(content)

    return content_list


def _get_external_id(resource):
    """
    Extract the KF external id for genomic files
    """
    identifiers = resource["identifier"]
    external_id = None
    for identifier in identifiers:
        if (
            identifier["system"]
            == "https://kf-api-dataservice.kidsfirstdrc.org/genomic-files?external_id="
        ):
            external_id = identifier["value"]
    return external_id


def _extract_urls(row, resource):
    """
    Extract urls and mark the s3_url separately

    Try finding s3_url from url list first. If not there try external_id
    """
    urls = {"s3_url": None, "others": []}
    url_list = row.get(CONCEPT.GENOMIC_FILE.URL_LIST)
    if url_list:
        if isinstance(url_list, str):
            url_list = ast.literal_eval(url_list)

        for url in url_list:
            if url.startswith("s3://"):
                urls["s3_url"] = url
            else:
                urls["others"].append(url)

    if not urls["s3_url"]:
        external_id = _get_external_id(resource)
        if external_id and external_id.startswith("s3://"):
            urls["s3_url"] = external_id

    return urls


def set_authorization(row):
    """
    Create the auth codes for the security label from genomic_file authz
    or acl field

    - authz takes precedence over acl
    - Ensure all values are in "old" acl format
    """
    authz = row.get(CONCEPT.GENOMIC_FILE.ACL)
    acl = row.get(CONCEPT.GENOMIC_FILE.ACL)

    if isinstance(acl, str):
        acl = ast.literal_eval(acl)

    if isinstance(authz, str):
        authz = ast.literal_eval(authz)

    if authz:
        new_codes = []
        for code in authz:
            new_code = code.split("/")[-1].strip()
            if new_code == "open":
                new_code = "*"
            new_codes.append(new_code)
    else:
        new_codes = acl or []

    return new_codes


class DocumentReferenceBase(FhirResourceBuilder):
    """
    Base class for FHIR DocumentReference entity builders. Common functionality
    """

    sources = {"required": ["SequencingExperimentGenomicFile"], "optional": []}
    resource_type = "DocumentReference"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID
    external_id_col = CONCEPT.GENOMIC_FILE.ID
    kf_id_system = "genomic-files"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["patient", "child_specimen"]

    def _mark_index_files(self, df):
        """
        Mark which rows are index files in the table

        Also save a dict of index file KF IDs mapped to non-index file KF IDs.
        This dict will be used in the drs_document_reference_index builder to
        create the reference to the drs_document_reference
        """
        gf_kf_id = CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID
        gf_filename = CONCEPT.GENOMIC_FILE.FILE_NAME
        gf_ref = CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID + "_REF"

        # Drop duplicate gfs
        df = df.drop_duplicates(gf_kf_id)

        # Fix filenames if needed
        def fix_file_name(row):
            file_name = row.get(gf_filename)
            url_list = row.get(CONCEPT.GENOMIC_FILE.URL_LIST)

            # Try getting file name from s3 url
            if (not file_name) and url_list:
                if isinstance(url_list, str):
                    url_list = ast.literal_eval(url_list)
                file_path = None
                for url in url_list:
                    if url.startswith("s3://"):
                        file_path = url
                        break
                if file_path:
                    file_name = os.path.split(".")[0]

            return file_name

        df[gf_filename] = df.apply(fix_file_name, axis=1)

        # If we don't have file metadata from Indexd then we cannot
        # figure out which index files go with which genomic files since
        # we need the genomic filename. Mark all the files as normal files
        if gf_filename not in df.columns:
            df[CONCEPT.GENOMIC_INDEX_FILE.TARGET_SERVICE_ID] = None
            df["is_index"] = False
            return df

        # Mark index files
        logger.info("Mark index files")
        df["is_index"] = df[gf_filename].apply(
            lambda file_name: (
                file_name.split(".")[-1].endswith("i") if file_name else False
            )
        )

        # Get file name without index extension
        logger.info("Get filename without extension")
        df["without_extension"] = df.apply(
            lambda row: (
                ".".join(row[CONCEPT.GENOMIC_FILE.FILE_NAME].split(".")[0:-1])
                if row[CONCEPT.GENOMIC_FILE.FILE_NAME] and row["is_index"]
                else row[CONCEPT.GENOMIC_FILE.FILE_NAME]
            ),
            axis=1,
        )
        # Mark non-index file KF_ID column as something different so
        # we can merge it into the original df. After merge we should
        # see that index file rows have a referenced non-index file KF ID
        logger.info("Collect non-index files")
        non_index_files = (
            df[df["is_index"] == False][
                [
                    gf_kf_id,
                    "without_extension",
                ]
            ]
            .rename(
                columns={
                    gf_kf_id: gf_ref,
                }
            )
            .drop_duplicates(gf_ref)
        )

        logger.info(
            f"Left merging files together. All gfs {df.shape}, non-index"
            f" gfs {non_index_files.shape}"
        )
        logger.info("Set index")
        df = df.set_index("without_extension")
        logger.info("Complete set index")

        df = pandas.merge(
            df, non_index_files, how="left", on="without_extension"
        ).drop_duplicates(gf_kf_id)

        logger.info("Finished merge")

        return df

    def _mark_participant_specimen_lists(self, df):
        """
        Add two columns to the source table to capture each genomic file's
        list of associated biospecimens and list of participants

        Each row in the source table represents a genomic files. You will notice
        that genomic files repeat since there is a row for every file and
        its:
            - linked participant info
            - linked biospecimen info

        This method essentially reduces the source table so that there is
        one row per genomic file and that row will have:
            - genomic file metadata
            - list of linked participants (comma delimited str of IDs)
            - list of linked biospecimen (comma delimited str of IDs)
            )

        The resulting table is returned as a list of dicts where each dict
        represents a row in the table
        """
        rows = []
        total = df.shape[0]
        for i, (genomic_file_id, group) in enumerate(
            df.groupby(CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID)
        ):
            logger.info(
                f"Collecting pts, biospec for gf {genomic_file_id}: {i+1}/{total}"
            )
            row = {
                "PARTICIPANT|LIST": group[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
                .unique()
                .tolist(),
                "BIOSPECIMEN|LIST": group[CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID]
                .unique()
                .tolist(),
            }
            rest = group.drop_duplicates(
                CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID
            ).to_dict(orient="records")[0]

            row.update(rest)
            rows.append(row)

        return pandas.DataFrame(rows)

    def _transform(self, source_tables, only_index_files=False):
        """
        Do necessary transformations to source tables before FHIR resource
        creation. Return a resulting table as a list of dicts/rows.

        See _mark_index_files and _mark_participant_specimen_lists for
        details
        """
        genomic_file_df = source_tables["SequencingExperimentGenomicFile"]

        logger.info("Create participant, specimen groups")
        df = self._mark_participant_specimen_lists(genomic_file_df)
        logger.info("Finished grouping participants and specimens")

        logger.info("Start marking genomic index files ...")
        df = self._mark_index_files(df)
        logger.info(f"Finished marking index files: {df.shape}")

        df = df[df["is_index"] == only_index_files]
        logger.info(f"Get files by is_index files: {df.shape}")

        return df

    def _build_resource(self, row, patient_reference, specimen_reference):
        """
        Build a DRS DocumentReference resource from a CSV row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)
        kf_id = row.get(CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID)
        strategy = row.get(CONCEPT.SEQUENCING.STRATEGY)
        controlled_access = row.get(CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS)
        data_type = row.get(CONCEPT.GENOMIC_FILE.DATA_TYPE)
        file_format = row.get(CONCEPT.GENOMIC_FILE.FILE_FORMAT)
        file_name = row.get(CONCEPT.GENOMIC_FILE.FILE_NAME)
        hash_dict = row.get(CONCEPT.GENOMIC_FILE.HASH_DICT)
        size = row.get(CONCEPT.GENOMIC_FILE.SIZE)
        acl_list = set_authorization(row)

        drs_uri = row.get(CONCEPT.GENOMIC_FILE.DRS_URI)
        urls = _extract_urls(row, resource)

        resource["meta"]["profile"] = [
            "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/ncpi-drs-document-reference"
        ]
        # TEMPORARY: Impute data_type
        if (
            data_type
            in {
                "Simple Nucleotide Variations",
                constants.GENOMIC_FILE.DATA_TYPE.SOMATIC_STRUCTURAL_VARIATIONS,
            }
            and file_format == "tbi"
        ):
            data_type = f"{data_type} Index"

        #  -- Add references --
        # Patient Reference
        # NOTE - This approach only works for DRS document references that
        # that link to 1 participant. We must find a better way in the
        # future
        participant_kf_id = row.get("PARTICIPANT|LIST")[0]
        resource["subject"] = patient_reference(participant_kf_id)

        # Biospecimen Reference
        resource.setdefault("context", {})["related"] = [
            specimen_reference(biospecimen_kf_id)
            for biospecimen_kf_id in row.get("BIOSPECIMEN|LIST")
        ]

        # Add other content
        resource["status"] = "current"
        resource["docStatus"] = "final"

        # type
        if data_type:
            doc_type = {"text": data_type}
            if type_coding.get(data_type):
                doc_type.setdefault("coding", []).append(type_coding[data_type])
            resource["type"] = doc_type

        # category
        category = []
        if strategy:
            # Experimental strategy
            experimental_strategy = {"text": strategy}
            if experimental_strategy_coding.get(strategy):
                experimental_strategy.setdefault("coding", []).append(
                    experimental_strategy_coding[strategy]
                )
            category.append(experimental_strategy)

            # Data Category
            data_cateogry = {"text": strategy}
            if data_cateogry_coding.get(strategy):
                data_cateogry.setdefault("coding", []).append(
                    data_cateogry_coding[strategy]
                )
            category.append(data_cateogry)
        if category:
            resource["category"] = category

        # securityLabel
        controlled_access = str(
            controlled_access or COMMON.NOT_REPORTED
        ).lower()
        security_label_list = []
        security_label = {"text": controlled_access}
        security_label.setdefault("coding", []).append(
            data_access_coding.get(str(controlled_access))
            or data_access_coding.get("default")
        )
        security_label_list.append(security_label)
        if acl_list:
            for acl in acl_list:
                security_label = {"text": acl}
                if len(acl.split(".")) > 1:
                    security_label.setdefault("coding", []).append(
                        {
                            "code": acl.split(".")[1],
                            "system": "urn:dbgap_consent_code",
                        }
                    )
                security_label_list.append(security_label)
        if security_label_list:
            resource["securityLabel"] = security_label_list

        # content
        content_list = _build_content_list(
            resource, file_name, urls, drs_uri, hash_dict, file_format, size
        )
        if content_list:
            resource["content"] = content_list

        return resource

    def _modify_resource(self, resource, **kwargs):
        """
        Modify the FHIR JSON in any way after the base resource has been built

        This gets called directly after _build_resource in the _build method

        Optional: To be implemented by subclasses needing to exec custom
        business logic
        """
        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        # Import source data
        self.source_tables = self.import_source_data(source_dir)

        # Import reference entity builders
        builders = self.import_reference_builders()
        patient_reference = builders["patient"].fhir_reference
        specimen_reference = builders["child_specimen"].fhir_reference

        # Transform the source tables before processing to collect
        # genomic_file members in each genomic_file row
        only_index_files = kwargs.get("only_index_files", False)
        logger.info("*** Begin transforming rows for document references ****")
        df = self._transform(self.source_tables, only_index_files)

        resources = []
        total = df.shape[0]
        for i, (_, row) in enumerate(df.iterrows()):
            # Build base drs document reference resource
            resource = self._build_resource(
                row, patient_reference, specimen_reference
            )
            # Apply hook for subclasses to perform any custom logic here
            resource = self._modify_resource(resource, row=row)

            resources.append(resource)

            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
