# noqa
"""
Module responsible for building FHIR JSON to capture the reseearch study info
"""

import logging
import pandas

from d3b_api_client_cli.config.concept_schema import CONCEPT
from d3b_api_client_cli.fhir.entity_builders.research_study.mapping import *
from d3b_api_client_cli.fhir.entity_builders.base import (
    FhirResourceBuilder,
    set_id_prefix,
)
from d3b_api_client_cli import utils


class ResearchStudy(FhirResourceBuilder):
    """
    Build FHIR ResearchStudy from Dataservice Study
    """

    sources = {"required": ["Study"], "optional": []}
    resource_type = "ResearchStudy"
    id_prefix = set_id_prefix(resource_type)

    kf_id_col = CONCEPT.STUDY.TARGET_SERVICE_ID
    external_id_col = CONCEPT.STUDY.ID
    kf_id_system = "studies"
    external_id_system = f"{kf_id_system}?external_id="

    reference_builders = ["practitioner_role"]

    def _build_resource(self, row, practitioner_role_reference):
        """
        Build FHIR JSON from a csv row
        """
        # Initalize resource with basic content
        resource = self.init_resource(row)

        external_id = row.get(CONCEPT.STUDY.ID)
        version = row.get(CONCEPT.STUDY.VERSION)
        study_name = row.get(CONCEPT.STUDY.NAME)
        domain = row.get(CONCEPT.STUDY.DOMAIN)
        program = row.get(CONCEPT.STUDY.PROGRAM)
        short_code = row.get(CONCEPT.STUDY.SHORT_CODE)
        biobank_name = row.get(CONCEPT.STUDY.BIOBANK_NAME)
        biobank_email = row.get(CONCEPT.STUDY.BIOBANK_EMAIL)
        biobank_request_link = row.get(CONCEPT.STUDY.BIOBANK_REQUEST_LINK)
        biobank_request_instructions = row.get(
            CONCEPT.STUDY.BIOBANK_REQUEST_INSTRUCTIONS
        )
        institution = row.get(CONCEPT.INVESTIGATOR.INSTITUTION)
        investigator_name = row.get(CONCEPT.INVESTIGATOR.NAME)

        # NOTE - Uncomment when Dewrangle supports multi-study pr
        # # Add references
        # investigator_kf_id = row.get(CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID)
        # if investigator_kf_id:
        #     resource["principalInvestigator"] = practitioner_role_reference(
        #         investigator_kf_id
        #     )

        # Additional payload content
        resource["status"] = "completed"

        # identifier
        if external_id and external_id.startswith("phs"):
            accession = external_id.split(".")[0].strip()
            if version and version.startswith("v"):
                accession = ".".join([accession, version.strip()])
            resource["identifier"].append(
                {
                    "use": "secondary",
                    "system": "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=",
                    "value": accession,
                }
            )

        # title
        if study_name:
            resource["title"] = study_name

        # Contacts
        contact_list = []

        # Biobank contact
        biobank_contact = {}
        if biobank_name:
            biobank_contact["name"] = biobank_name
        if biobank_email:
            biobank_contact["telecom"] = [
                {"system": "email", "value": biobank_email}
            ]
            if biobank_request_link:
                biobank_contact["telecom"].append(
                    {"system": "url", "value": biobank_request_link}
                )

        if biobank_contact:
            contact_list = [biobank_contact]

        # Investigator info
        investigator_contact = {}
        if institution:
            investigator_contact["extension"] = [
                {
                    "url": "https://include-dcc.github.io/include-model-forge/StructureDefinition/contact-detail-institution",
                    "valueString": institution,
                }
            ]
        if investigator_name:
            investigator_contact["name"] = investigator_name

        if investigator_contact:
            contact_list.append(investigator_contact)

        resource["contact"] = contact_list

        # VBR Request README
        if biobank_request_instructions:
            resource["note"] = [{"text": biobank_request_instructions}]

        # cateogry
        category = {}
        if domain:
            category["text"] = domain
            if category_coding.get(domain):
                category.setdefault("coding", []).append(
                    category_coding[domain]
                )
            elif domain == "CANCERANDBIRTHDEFECT":
                category["coding"] = [
                    category_coding["CANCER"],
                    category_coding["BIRTHDEFECT"],
                ]
        if category:
            resource.setdefault("category", []).append(category)

        # keyword
        if program:
            resource.setdefault("keyword", []).append(
                {
                    "coding": [
                        {
                            "system": "https://kf-api-dataservice.kidsfirstdrc.org/studies?program=",
                            "code": program,
                            "display": program,
                        }
                    ],
                    "text": program,
                }
            )
        if short_code:
            resource.setdefault("keyword", []).append(
                {
                    "coding": [
                        {
                            "system": "https://kf-api-dataservice.kidsfirstdrc.org/studies?short_code=",
                            "code": short_code,
                            "display": short_code,
                        }
                    ],
                    "text": short_code,
                }
            )
        return resource

    def _build(self, source_dir, **kwargs):
        """
        See d3b_api_client_cli.fhir.entity_builders.base
        """
        self.source_tables = self.import_source_data(source_dir)

        builders = self.import_reference_builders()
        practitioner_role_reference = builders[
            "practitioner_role"
        ].fhir_reference

        df = list(self.source_tables.values())[0]
        resources = []
        total = df.shape[0]
        for i, row in df.iterrows():
            resource = self._build_resource(row, practitioner_role_reference)
            resources.append(resource)
            self.logger.info(
                f"Built {self.resource_type} {i+1}/{total}: {resource['id']}"
            )

        return resources
