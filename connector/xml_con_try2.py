import xml.etree.ElementTree as ET
import traceback

from pydantic import BaseModel, ValidationError
from typing import Iterable, Optional, List, Dict, Any

from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.table import Column
from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseService
from metadata.generated.schema.metadataIngestion.workflow import WorkflowSource
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import CustomDatabaseConnection
from metadata.generated.schema.services.databaseService import DatabaseService
from metadata.generated.schema.ingestionPipelines.status import StackTraceError
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

class XmlConnector(Source):
    """
    Custom connector to ingest XML data into the metadata system.
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        self.config = config
        self.metadata = metadata

        self.service_connection = config.serviceConnection.__root__.config

        self.source_data: str = self.service_connection.connectionOptions.__root__.get("source_data")
        if not self.source_data:
            raise InvalidXmlConnectorException("Missing source_data connection option")

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
        # Validate that the data is provided
        if not self.source_data:
            raise InvalidXmlConnectorException("Missing source_data connection option")

    def yield_create_request_database_service(self):
        yield Either(
            right=self.metadata.get_create_service_from_source(
                entity=DatabaseService, config=self.config
            )
        )

    def yield_default_schema(self):
        # Create a database named "people"
        yield Either(
            right=CreateDatabaseRequest(
                name="people",
                service=self.config.serviceName,
            )
        )

    def yield_data(self):
        # Parse the XML data
        root = ET.fromstring(self.source_data)

        # Get the database schema entity for the "people" database
        database_schema: DatabaseSchema = self.metadata.get_by_name(
            entity=DatabaseSchema, fqn=self.config.serviceName + ".people.default"
        )

        # Create a table named "person" inside the "people" database
        yield Either(
            right=CreateTableRequest(
                name="person",
                databaseSchema=database_schema.fullyQualifiedName,
                columns=[
                    Column(name='age', dataType='int', description='Age of the person'),
                    Column(name='gender', dataType='string', description='Gender of the person'),
                    Column(name='name', dataType='string', description='Name of the person'),
                ],
                description="Table for storing person data",
            )
        )

        # Insert data into the "person" table
        for person in root.findall('person'):
            try:
                age = int(person.find('age').text)
                gender = person.find('gender').text
                name = person.find('name').text

                # Insert each person's data into the table
                yield Either(
                    right=InsertIntoTableRequest(
                        tableName="person",
                        databaseSchema=database_schema.fullyQualifiedName,
                        data=[{'age': age, 'gender': gender, 'name': name}],
                    )
                )
            except Exception as e:
                logger.warning(f"Error parsing XML data: {e}")

    def _iter(self) -> Iterable[Entity]:
        yield from self.yield_create_request_database_service()
        yield from self.yield_default_schema()
        yield from self.yield_data()

    def test_connection(self) -> None:
        pass

    def close(self):
        pass
