import xml.etree.ElementTree as ET
from typing import Optional, List, Iterable, Dict, Any

from pydantic import BaseModel, ValidationError
from metadata.generated.schema.metadataIngestion.workflow import WorkflowSource
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

class XmlModel(BaseModel):
    age: int
    gender: str
    name: str

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
            raise InvalidXmlConnectorException("Missing source_directory connection option")
        
        self.business_unit: str = (
            self.service_connection.connectionOptions.__root__.get("business_unit")
        )
        if not self.business_unit:
            raise InvalidXmlConnectorException(
                "Missing business_unit connection option"
            )


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
            raise InvalidXmlConnectorException("Missing source_directory connection option")

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
        service_entity: DatabaseService = self.metadata.get_by_name(
            entity=DatabaseService, fqn=self.config.serviceName
        )
        yield Either(
            right=CreateDatabaseRequest(
                name="default",
                service=service_entity.fullyQualifiedName,
            )
        )

    

    def yield_data(self):
        tree = ET.parse(self.source_directory)
        root = tree.getroot()

        # Define columns based on XML structure
        columns = [
            Column(name="age", dataType="int"),
            Column(name="gender", dataType="string"),
            Column(name="name", dataType="string"),
        ]

        # Create table request with defined columns
        table_request = CreateTableRequest(
            name="people_table",
            databaseSchema=self.config.serviceName + ".default",
            columns=columns,
        )

        yield Either(right=table_request)

        # Iterate over XML data and yield rows as dictionaries
        for person in root.findall("person"):
            age = int(person.find("age").text)
            gender = person.find("gender").text
            name = person.find("name").text

            # Create dictionary representing a row of data
            row_data = {
                "age": age,
                "gender": gender,
                "name": name,
            }

            # Create table row request and yield it
            yield Either(right=CreateTableRequest(
                name=f"person_{name}",
                databaseSchema=self.config.serviceName + ".default",
                columns=[Column(name=col_name, dataType="string") for col_name in row_data.keys()],
                rows=[row_data],
            ))

# #with xml model
#     def yield_data(self):
#         tree = ET.parse(self.source_directory)
#         root = tree.getroot()

#         for person in root.findall('person'):
#             try:
#                 xml_data = XmlModel(
#                     age=int(person.find('age').text),
#                     gender=person.find('gender').text,
#                     name=person.find('name').text
#                 )
#                 yield xml_data.dict()
#             except ValidationError as e:
#                 logger.warning(f"Error parsing XML data: {e}")



    # def yield_data(self):
    #     tree = ET.parse(self.source_directory)
    #     root = tree.getroot()

    #     for person in root.findall('person'):
    #         age = person.find('age').text
    #         gender = person.find('gender').text
    #         name = person.find('name').text

    #         yield Either(
    #             right=CreateTableRequest(
    #                 name=f"person_{name}",
    #                 databaseSchema=self.config.serviceName + '.default',
    #                 columns=[
    #                     Column(
    #                         name='age',
    #                         dataType='int',
    #                     ),
    #                     Column(
    #                         name='gender',
    #                         dataType='string',
    #                     ),
    #                     Column(
    #                         name='name',
    #                         dataType='string',
    #                     )
    #                 ],
    #             )
    #         )

    def _iter(self) -> Iterable[Entity]:
        yield from self.yield_create_request_database_service()
        yield from self.yield_default_schema()
        yield from self.yield_data()

    def test_connection(self) -> None:
        pass

    def close(self):
        pass


# # Example usage
# config_dict = {
#     "serviceName": "MyXmlService",
#     "serviceConnection": {
#         "sourceType": "xml",
#         "config": {
#             "source_directory": "path/to/your/xml/file.xml",
#             "business_unit": "MyBusinessUnit"
#         }
#     }
# }
# metadata_config = OpenMetadataConnection()
# xml_connector = XmlConnector.create(config_dict, metadata_config)

# # Prepare and ingest data
# xml_connector.prepare()
# entities = list(xml_connector.iter())
# print(entities)
