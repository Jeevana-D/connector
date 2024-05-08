import xml.etree.ElementTree as ET
from openmetadata.client import OpenMetadata
from openmetadata.common import OpenMetadataObject

from pydantic import BaseModel

class Person(BaseModel):
    # age: int
    # gender: str
    # name: str

    def __init__(self, age, gender, name):
        self.age = age
        self.gender = gender
        self.name = name

tree = ET.parse('C:\Users\DJeevana\Desktop\Connector\openmetadata-demo\custom-connector\result_for_marhsall.xml')  # Parse XML file
root = tree.getroot()

people = []
for person_elem in root.findall('person'):
    person_data = {
        'age': int(person_elem.find('age').text),
        'gender': person_elem.find('gender').text,
        'name': person_elem.find('name').text
    }
    people.append(Person(**person_data))

metadata_client = OpenMetadata(base_url='http://localhost:8080/openmetadata', api_token='eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MDQ4NjQwNDIsImV4cCI6bnVsbH0.aPJ-ZrDBzBi09XEL62cbKWUFhcktLj9SjB0Fr8wnr3lm6Tqzd71ATKfIHJqerzT7JjNmU-AClQvZn2LZBgFdzoEK6ECwjuKD5LDhqWG8s5XPzhYrHNIWlbQ56X0suPCxDk_JBhwZVuKC2u2pZ4TCOpDuC-VzvXSZoIf5CwPTW98_fdBNZZ-9EQHk2xbr1WN4_AEJPSB86JCtGC9gZnt_jt4Yuj8Q24xmCllTlG3ec-sriuDEt83ZqKzbwbFKSSZ6ifvOHKw_PVa_Aa_1v7Z_bzJhW66HX1Af9tWBYDiVRMZg8SyrG4DdXE5GTColUbGUTtc2C5uBAvhgpL_fUritYA')

for person in people:
    # Example: Create a Person entity in OpenMetadata
    person_entity = metadata_client.create_entity(
        type='Person',
        properties={
            'age': person.age,
            'gender': person.gender,
            'name': person.name
        }
    )
    #print(f"Created Person entity with ID: "{person_entity.id})
    print("Created Person entity with ID: {}".format(person_entity.id))


