import xml.etree.ElementTree as ET
from typing import Optional, List, Iterable, Dict, Any
from pathlib import Path
import traceback
 
from pydantic import BaseModel, ValidationError, validator
from metadata.generated.schema.metadataIngestion.workflow import (Source as WorkflowSource,)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.table import Column
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import CustomDatabaseConnection
from metadata.generated.schema.entity.services.ingestionPipelines.status import StackTraceError
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


class People(BaseModel):
    person: List[Person]

    @staticmethod
    def parse_xml_data(xml_file: str) -> List[Person]:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        people_list = []
        for person_elem in root.findall("person"):
            person_data = {
                "age": int(person_elem.find("age").text),
                "gender": person_elem.find("gender").text,
                "name": person_elem.find("name").text,
            }
            people_list.append(Person(**person_data))
        return people_list


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
        
        # self.business_unit: str = (
        #     self.service_connection.connectionOptions.__root__.get("business_unit")
        # )
        # if not self.business_unit:
        #     raise InvalidXmlConnectorException(
        #         "Missing business_unit connection option"
        #     )

        self.xml_data: Optional[List[Person]] = None
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
        
        self.xml_data = People.parse_xml_data(self.source_directory)

    def yield_create_request_database_service(self):
        yield Either(
            right=self.metadata.get_create_service_from_source(
                entity=DatabaseService, config=self.config
            )
        )

    # def yield_business_unit_db(self):
    #     # Pick up the service we just created (if not UI)
    #     service_entity: DatabaseService = self.metadata.get_by_name(
    #         entity=DatabaseService, fqn=self.config.serviceName
    #     )

    #     yield Either(
    #         right=CreateDatabaseRequest(
    #             name=self.business_unit,
    #             service=service_entity.fullyQualifiedName,
    #         )
    #     )

    # def yield_default_schema(self):
    #     service_entity: DatabaseService = self.metadata.get_by_name(
    #         entity=DatabaseService, fqn=self.config.serviceName
    #     )
    #     yield Either(
    #         right=CreateDatabaseRequest(
    #             name="default",
    #             service=service_entity.fullyQualifiedName,
    #         )
    #     )


    def yield_default_schema(self):
        service_entity: DatabaseService = self.metadata.get_by_name(
            entity=DatabaseService, fqn=self.config.serviceName
        )
        yield Either(
            right=CreateDatabaseRequest(
                name="default",
                service=service_entity.fullyQualifiedName,
            )
        )



    # def yield_default_schema(self):
    #     database_entity: Database = self.metadata.get_by_name(
    #         entity=Database, fqn=f"{self.config.serviceName}.{self.business_unit}"
    #     )
    #     yield Either(
    #         right=CreateDatabaseSchemaRequest(
    #             service=database_entity.fullyQualifiedName,
    #             name="default",
    #         )
    #     )

    def yield_data(self):
        """
        Iterate over the XML data to create Person entities
        """
        database_schema: DatabaseSchema = self.metadata.get_by_name(
            entity=DatabaseSchema,
            fqn=f"{self.config.serviceName}.default",
            # 
        )

        try:
            1 / 0
        except Exception:
            yield Either(
                left=StackTraceError(
                    name="My Error",
                    error="Demoing one error",
                    stackTrace=traceback.format_exc(),
                )
            )

        for person_data in self.xml_data:
            try:
                person = person_data.dict()
                yield Either(right=CreateTableRequest(
                    name=person["name"],  # Assuming 'name' is used as table name for simplicity
                    databaseSchema=database_schema.fullyQualifiedName,
                    columns=[
                        Column(
                            name='age',
                            dataType='INT'  # Assuming 'age' is always an integer
                        ),
                        Column(
                            name='gender',
                            dataType='STRING'  # Assuming 'gender' is always a string
                        ),
                        Column(
                            name='name',
                            dataType='STRING'  # Assuming 'name' is always a string
                        )
                    ]
                ))
            except Exception as e:
                logger.error(f"Error creating Person entity: {e}")

    def _iter(self) -> Iterable[Entity]:
        yield from self.yield_create_request_database_service()
        # yield from self.yield_business_unit_db()
        yield from self.yield_default_schema()
        yield from self.yield_data()

    def test_connection(self) -> None:
        pass

    def close(self):
        pass
