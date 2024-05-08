import xml.etree.ElementTree as ET

from pydantic import BaseModel, ValidationError, validator
from pathlib import Path
from typing import Iterable, Optional, List, Dict, Any



from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.models import Either
from metadata.generated.schema.entity.services.ingestionPipelines.status import StackTraceError
from metadata.ingestion.api.steps import Source, InvalidSourceException
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
)
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseService,
)
from metadata.generated.schema.entity.data.table import (
    Column,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()
# from typing import Optional, List, Iterable, Dict, Any
 
# from pydantic import BaseModel, ValidationError, validator
# from metadata.generated.schema.metadataIngestion.workflow import (Source as WorkflowSource,)
# from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
# from metadata.generated.schema.entity.data.database import Database
# from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
# from metadata.generated.schema.api.data.createDatabaseSchema import (
#     CreateDatabaseSchemaRequest,
# )
# # from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
# from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
# from metadata.generated.schema.api.data.createTable import CreateTableRequest
# from metadata.generated.schema.entity.data.table import Column
# from metadata.generated.schema.entity.services.databaseService import DatabaseService
# from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import CustomDatabaseConnection
# from metadata.generated.schema.entity.services.ingestionPipelines.status import StackTraceError
# from metadata.ingestion.api.common import Entity
# from metadata.ingestion.api.models import Either
# from metadata.ingestion.api.steps import Source, InvalidSourceException
# from metadata.ingestion.ometa.ometa_api import OpenMetadata
# from metadata.utils.logger import ingestion_logger

# logger = ingestion_logger()


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

    @validator('person', pre=True)
    def convert_to_person(cls, v):
        return Person(**v)


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
        
        # self.xml_data: Optional[List[People]] = None

        self.xml_data = []  # Initialize xml_data as None


        super().__init__()


    person_datatype=[int,str,str]


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
    #     database_entity: Database = self.metadata.get_by_name(
    #         entity=Database, fqn=f"{self.config.serviceName}.{self.business_unit}"
    #     )
    #     yield Either(
    #         right=CreateDatabaseSchemaRequest(
    #             name="default",
    #             database=database_entity.fullyQualifiedName,
    #         )
    #     )

    # def yield_default_schema(self):
    #     database_entity: Database = self.metadata.get_by_name(
    #         entity=Database, fqn=f"{self.config.serviceName}.{self.business_unit}"
    #     )

    #     if not database_entity.fullyQualifiedName:
    #         raise ValueError("Fully qualified name of database entity is empty or None.")

    #     yield Either(
    #         right=CreateDatabaseSchemaRequest(
    #             name="default",
    #             database=database_entity.fullyQualifiedName,
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
        # Pick up the service we just created (if not UI)
        database_entity: Database = self.metadata.get_by_name(
            entity=Database, fqn=f"{self.config.serviceName}.{self.business_unit}"
        )

        yield Either(
            right=CreateDatabaseSchemaRequest(
                name="default",
                database=database_entity.fullyQualifiedName,
            )
        )
    def yield_data(self):
        database_schema: DatabaseSchema = self.metadata.get_by_name(
            entity=DatabaseSchema,
            fqn=f"{self.config.serviceName}.{self.business_unit}.default",
            #fqn=f"{self.config.serviceName}.default",                   # 
        )
        tree = ET.parse(self.source_directory)
        root = tree.getroot()

        

        
        try:
            people_list = []
            person_elem = root.find('person')
            if person_elem is not None:  # Check if 'person' element exists
                child_tags = [child.tag for child in person_elem]  # Extract child tag names

                person_obj = Person(**dict.fromkeys(child_tags))  # Create Person object with child tag names as attributes
                people_list.append(person_obj)

                # self.xml_data = People(person=people_list)
                self.xml_data.append(People(person=people_list))
                yield self.xml_data
            else:
                print("hi")
                logger.warning("No 'person' element found in XML.")
        except ValidationError as e:
            logger.warning(f"Error parsing XML data: {e}")

        #Output: Person(age=None, gender=None, name=None)

        if self.xml_data is not None:
            for row in self.xml_data:
                yield Either(
                    right=CreateTableRequest(
                        name="Person",
                        databaseSchema=database_schema.fullyQualifiedName,
                        columns=[
                            Column(
                                name=row.age,
                                dataType=int
                            ),
                            Column(
                                name=row.gender,
                                dataType=str
                            ),
                            Column(
                                name=row.name,
                                dataType=str
                            ),
                        ],
                    )
                )

        else:
            print("heres the error")

    def _iter(self) -> Iterable[Entity]:
        yield from self.yield_create_request_database_service()
        yield from self.yield_business_unit_db()
        yield from self.yield_default_schema()
        yield from self.yield_data()

    def test_connection(self) -> None:
        pass

    def close(self):
        pass


