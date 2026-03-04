"""
Base class for Kids First FHIR entity builders. Defines abstract interface 
for th entity builder subclasses

Entity builders take in one or more source tabular files from Dataservice
and build FHIR JSON
"""

from pprint import pformat, pprint
import logging
import pandas

from d3b_api_client_cli.config import config, KidsFirstFhirEntity
from d3b_api_client_cli import utils
from d3b_api_client_cli.fhir.common import import_builders
from d3b_api_client_cli.fhir import constants
from d3b_api_client_cli.dataservice.transformer import common

dataservice_url = config["dataservice"]["api_url"]
dataservice_types = list(k for k in config["dataservice"]["seeder"])
fhir_resource_types = config["fhir"]["resource_types"]
fhir_entity_builders = [et.value for et in KidsFirstFhirEntity]

logger = logging.getLogger(__name__)


def extension_age_at_event(reference, event_age_days):
    """
    Helper to create JSON that for a FHIR extension that captures a
    relative age in days since an event took place
    """
    return {
        "extension": [
            {
                "extension": [
                    {"url": "target", "valueReference": reference},
                    {
                        "url": "targetPath",
                        "valueString": "birthDate",
                    },
                    {
                        "url": "relationship",
                        "valueCode": "after",
                    },
                    {
                        "url": "offset",
                        "valueDuration": {
                            "value": int(event_age_days),
                            "unit": "day",
                            "system": "http://unitsofmeasure.org",
                            "code": "d",
                        },
                    },
                ],
                "url": "http://hl7.org/fhir/StructureDefinition/cqf-relativeDateTime",
            }
        ]
    }


def set_id_prefix(resource_type):
    """
    Given a FHIR resource type return the two char prefix for resource IDs
    having that resource type
    """
    return fhir_resource_types.get(resource_type, "none")


class MissingSourceDataError(Exception):
    """
    Error when source tables in entity builder cannot be read or loaded
    """

    pass


class FhirResourceBuilder:
    """
    Abstract class defining interface for FHIR resource builders for Kids
    First FHIR entities
    """

    sources = None
    resource_type = None
    kf_id_col = None
    kf_id_system = None
    external_id_col = None
    external_id_system = None
    reference_builders = None
    id_prefix = None

    def __init__(self, study_id, parent_study_id=None):
        self.study_id = study_id
        self.parent_study_id = parent_study_id
        self.entity_type = type(self).__name__

        n = ".".join(__name__.split(".")[:-1])
        self.logger = logging.getLogger(f"{n}.{self.entity_type}")

        for attr in [
            "sources",
            "resource_type",
            "kf_id_col",
            "kf_id_system",
            "external_id_col",
            "external_id_system",
            "id_prefix",
        ]:
            self.validate_cls_attribute(attr, "")

    def validate_cls_attribute(
        self, cls_attribute_name, msg, validate_method=None
    ):
        """
        Ensure that the class attribute is implemented and valid
        """
        cls_attribute = getattr(self, cls_attribute_name)
        if not cls_attribute:
            raise NotImplementedError(
                f"Must implement {self.entity_type}.{cls_attribute_name}."
                f" {msg}"
            )
        if validate_method:
            validate_method()

    @classmethod
    def fhir_id(cls, kf_id):
        """
        Create the resource FHIR ID from the Dataservice Kids First ID
        """
        return utils.kf_id_to_global_id(kf_id, replace_prefix=cls.id_prefix)

    @classmethod
    def fhir_reference(cls, kf_id):
        """
        Create a dict representing a reference to this resource
        """
        ref_id = cls.fhir_id(kf_id)
        return {"reference": f"{cls.resource_type}/{ref_id}"}

    def import_source_data(self, source_dir):
        """
        Read source tables from file
        """
        # Ensure class attribute is implemented
        msg = (
            "Must be a list of Dataservice entity types:"
            f" {pformat(dataservice_types)}"
        )
        sources = self.sources.get("required", []) + self.sources.get(
            "optional", []
        )
        assert set(sources) <= set(dataservice_types), msg

        dfs = common.read_dfs(source_dir, sources)
        missing = {"required": [], "optional": []}
        for source in sources:
            if source not in dfs:
                if source in self.sources["required"]:
                    missing["required"].append(source)
                else:
                    missing["optional"].append(source)

        msg = (
            f"⚠️  One or more source tables do not exist: {pformat(missing)}"
            f" during build of {self.resource_type}/{self.entity_type}!"
        )

        if missing["required"]:
            logger.error(msg)
            raise MissingSourceDataError(msg)

        if missing["optional"]:
            logger.warning(msg)

        return dfs

    def import_reference_builders(self):
        """
        Import entity builders for referenced FHIR resources.

        We need these to translate KF IDs to FHIR resource IDs using the
        builder.fhir_id method
        """
        msg = (
            "Must be one of the Kids First FHIR entity builders:"
            f" {pformat(fhir_entity_builders)}"
        )
        self.validate_cls_attribute("reference_builders", msg)
        assert set(self.reference_builders) <= set(fhir_entity_builders), msg

        return import_builders(entity_types=self.reference_builders)

    def init_resource(self, row):
        """
        Create the initial FHIR resource w content common to all resource types
        """
        # Ensure class attribute is implemented
        msg = (
            "Must be one of FHIR resource types:"
            f" {pformat(fhir_resource_types.keys())}"
        )
        assert self.resource_type in set(fhir_resource_types.keys()), msg

        kf_id = row[self.kf_id_col]
        kf_id_system = self.kf_id_system

        external_id = str(row.get(self.external_id_col))
        if external_id == "None" or pandas.isnull(external_id):
            external_id = constants.COMMON.NOT_REPORTED
        external_id_system = self.external_id_system

        # Default tags for every resource
        tags = [
            {
                "system": f"{dataservice_url}/studies/",
                "code": self.study_id,
            },
            {
                "system": "urn:kids_first_fhir_type",
                "code": utils.camel_to_snake(self.entity_type),
            },
        ]
        # If this study has a parent study, tag all resources with both
        # the child study ID and the parent study ID
        if self.parent_study_id:
            tags.append(
                {
                    "system": f"{dataservice_url}/studies?parent_study_id=",
                    "code": self.parent_study_id,
                },
            )

        return {
            "resourceType": self.resource_type,
            "id": self.fhir_id(kf_id),
            "meta": {
                "profile": [
                    f"http://hl7.org/fhir/StructureDefinition/{self.resource_type}"
                ],
                "tag": tags,
            },
            "identifier": [
                {
                    "use": "official",
                    "system": f"{dataservice_url}/{kf_id_system.lstrip('/').rstrip('/')}/",
                    "value": kf_id,
                },
                {
                    "use": "secondary",
                    "system": (
                        f"{dataservice_url}/{external_id_system.lstrip('/')}"
                    ),
                    "value": external_id,
                },
            ],
        }

    def _build(self, source_dir, output_dir, **kwargs):
        """
        Implemented by subclasses. Called by build
        """
        raise NotImplementedError(
            "All entity builders must implement this method. This should "
            "return a list of dicts representing the list of FHIR JSON"
        )

    def build(self, source_dir, output_dir, **kwargs):
        """
        The main method to build Kids First FHIR resources
        :param source_dir: Directory of source tables
        :type source_dir: str
        :param output_dir: Directory FHIR JSON will be written
        :type output_dir: str
        """
        self.logger.info(f"🧱 Building {self.entity_type} FHIR entities")

        try:
            resources = self._build(source_dir, **kwargs)
        except MissingSourceDataError:
            return []

        return resources
