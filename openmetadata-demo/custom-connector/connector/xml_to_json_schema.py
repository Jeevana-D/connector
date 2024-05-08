import xmltodict
from jsonschema import Draft7Validator, RefResolver, draft7_format_checker, validate

# Sample XML data
xml_data = """
<people>
    <person>
        <age>21</age>
        <gender>M</gender>
        <name>Charan</name>
    </person>
    <person>
        <age>21</age>
        <gender>F</gender>
        <name>Jeevana</name>
    </person>
    <person>
        <age>120</age>
        <gender>f</gender>
        <name>charu</name>
    </person>
    <person>
        <age>10</age>
        <gender>f</gender>
        <name>divya</name>
    </person>
    <person>
        <age>30</age>
        <gender>f</gender>
        <name>swathi</name>
    </person>
    <person>
        <age>11</age>
        <gender>f</gender>
        <name>dd</name>
    </person>
    <person>
        <age>2</age>
        <gender>f</gender>
        <name>rebecca</name>
    </person>
    <person>
        <age>29</age>
        <gender>f</gender>
        <name>akshara</name>
    </person>
    <person>
        <age>109</age>
        <gender>f</gender>
        <name>sathya</name>
    </person>
    <person>
        <age>11</age>
        <gender>f</gender>
        <name>sundari</name>
    </person>
    <person>
        <age>19</age>
        <gender>f</gender>
        <name>deepa</name>
    </person>
</people>
"""

# Convert XML to OrderedDict (JSON-like structure)
json_data = xmltodict.parse(xml_data)

# Function to recursively convert OrderedDict to JSON schema
def convert_to_json_schema(data):
    if isinstance(data, dict):
        properties = {}
        for key, value in data.items():
            properties[key] = convert_to_json_schema(value)
        return {"type": "object", "properties": properties}
    elif isinstance(data, list):
        items = [convert_to_json_schema(item) for item in data]
        return {"type": "array", "items": items}
    elif isinstance(data, str):
        return {"type": "string"}
    elif isinstance(data, int):
        return {"type": "integer"}
    elif isinstance(data, float):
        return {"type": "number"}
    elif isinstance(data, bool):
        return {"type": "boolean"}
    else:
        return {}

# Convert OrderedDict to JSON schema
json_schema = convert_to_json_schema(json_data)

# Print the generated JSON schema
print(json_schema)
