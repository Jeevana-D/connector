import xml.etree.ElementTree as ET
import traceback
from pathlib import Path
from typing import Optional, List
from typing import Optional, List, Iterable, Dict, Any

# from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from pydantic import BaseModel, ValidationError, validator
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.table import Column
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import CustomDatabaseConnection
# from metadata.generated.schema.services.ingestionPipelines.status import StackTraceError
from metadata.generated.schema.entity.services.ingestionPipelines.status import StackTraceError
from metadata.generated.schema.metadataIngestion.workflow import Source as WorkflowSource
from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import Source, InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class InvalidXmlConnectorException(Exception):
    """
    Raised when XML data is not valid for ingestion.
    """


class Person(BaseModel):
    age: int
    gender: str
    name: str

    @staticmethod
    def parse_person_element(person_elem: ET.Element):
        try:
            age = int(person_elem.find("age").text)
            gender = person_elem.find("gender").text
            name = person_elem.find("name").text
            return {"age": age, "gender": gender, "name": name}
            # return Person(age=age, gender=gender, name=name)
        except (AttributeError, ValueError) as e:
            logger.warning(f"Error parsing person element {person_elem}: {e}. Skipping it.")
            return None

    @validator("age")
    def validate_age(cls, value):
        if value < 0:
            raise ValueError("Age cannot be negative")
        return value

    @validator("gender")
    def validate_gender(cls, value):
        if value.lower() not in {"m", "f"}:
            raise ValueError("Gender must be 'M' or 'F'")
        return value

    @validator("name")
    def validate_name(cls, value):
        if len(value) < 3:
            raise ValueError("Name must be at least 3 characters long")
        return value


class People(BaseModel):
    people: List[Person]

    @staticmethod
    def parse_xml_data(xml_file: str) -> List[Dict[str, str]]:
    # def parse_xml_data(xml_file: str):
        tree = ET.parse(xml_file)
        root = tree.getroot()
        people_list = []
        for person_elem in root.findall("person"):
            person = Person.parse_person_element(person_elem)
            if person:
                people_list.append(person)
        return people_list
        #     person = Person.parse_person_element(person_elem)
        #     if person_data:
        #         people_list.append(person_data)
        #     if person:
        #         people_list.append(person)
        # return People(people=people_list)


class XmlConnector(Source):
    """
    Custom connector to ingest XML data into the metadata system.
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        self.config = config
        self.metadata = metadata

        self.service_connection = config.serviceConnection.__root__.config

        self.source_directory: Optional[str] = self.service_connection.connectionOptions.__root__.get("source_directory")
        if not self.source_directory:
            raise InvalidXmlConnectorException("Missing source_file connection option")

        self.business_unit: str = (
            self.service_connection.connectionOptions.__root__.get("business_unit")
        )
        if not self.business_unit:
            raise InvalidXmlConnectorException(
                "Missing business_unit connection option"
            )

        self.xml_data: Optional[People] = None
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, metadata_config: OpenMetadataConnection
    ) -> "XmlConnector":
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: CustomDatabaseConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, CustomDatabaseConnection):
            raise InvalidSourceException(
                f"Expected CustomDatabaseConnection, but got {connection}"
            )
        return cls(config, metadata_config)

    def prepare(self):
        # Validate that the file exists
        if not self.source_directory:
            raise InvalidXmlConnectorException("Missing source_file connection option")

        xml_file_path = Path(self.source_directory)
        if not xml_file_path.exists():
            raise InvalidXmlConnectorException("XML file does not exist")

        try:
            self.xml_data = People.parse_xml_data(self.source_directory)
        except Exception as exc:
            logger.error("Error parsing XML data")
            raise exc

    def yield_create_request_database_service(self):
        yield Either(
            right=self.metadata.get_create_service_from_source(
                entity=DatabaseService, config=self.config
            )
        )

    def yield_business_unit_db(self):
        # Pick up the service we just created (if not UI)
        service_entity: DatabaseService = self.metadata.get_by_name(
            entity=DatabaseService, fqn=self.config.serviceName
        )

        yield Either(
            right=CreateDatabaseRequest(
                name=self.business_unit,
                service=service_entity.fullyQualifiedName,
            )
        )

    def yield_default_schema(self):
        database_entity: Database = self.metadata.get_by_name(
            entity=Database, fqn=f"{self.config.serviceName}.{self.business_unit}"
        )
        yield Either(
            right=CreateDatabaseSchemaRequest(
                database=database_entity.fullyQualifiedName,
                name="default"
            )
        )

    def yield_data(self):
        """
        Iterate over the XML data to create Person entities
        """
        database_schema: DatabaseSchema = self.metadata.get_by_name(
            entity=DatabaseSchema,
            fqn=f"{self.config.serviceName}.{self.business_unit}.default",
        )

        for person in self.xml_data.people:
            yield Either(right=CreateTableRequest(
                name=person.name,
                databaseSchema=database_schema.fullyQualifiedName,
                columns=[
                    Column(name='age', dataType='INT'),
                    Column(name='gender', dataType='STRING'),
                    Column(name='name', dataType='STRING'),
                ]
            ))

    def _iter(self) -> Iterable[Entity]:
        yield from self.yield_create_request_database_service()
        yield from self.yield_business_unit_db()
        yield from self.yield_default_schema()
        yield from self.yield_data()

    def test_connection(self) -> None:
        pass

    def close(self):
        pass
