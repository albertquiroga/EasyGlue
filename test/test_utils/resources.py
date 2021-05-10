from test.test_utils import glue_client

DATABASE_NAME = "test"
TABLE_NAME = "easyglue_test"
TABLE_BUCKET = "bertolb"
TABLE_PREFIX = "test/easyglue/outputs/sampletable/"

table_def = {
    "Name": TABLE_NAME,
    "StorageDescriptor": {
        "Columns": [
            {
                "Name": "first_name",
                "Type": "string"
            },
            {
                "Name": "last_name",
                "Type": "string"
            },
            {
                "Name": "email",
                "Type": "string"
            },
            {
                "Name": "gender",
                "Type": "string"
            },
            {
                "Name": "ip_address",
                "Type": "string"
            },
            {
                "Name": "birth_date",
                "Type": "string"
            },
            {
                "Name": "employee_number",
                "Type": "string"
            },
            {
                "Name": "app_bundle_id",
                "Type": "string"
            },
            {
                "Name": "app_version",
                "Type": "string"
            },
            {
                "Name": "image_url",
                "Type": "string"
            },
            {
                "Name": "buzzword",
                "Type": "string"
            },
            {
                "Name": "car_brand",
                "Type": "string"
            },
            {
                "Name": "car_model",
                "Type": "string"
            },
            {
                "Name": "country",
                "Type": "string"
            },
            {
                "Name": "country_code",
                "Type": "string"
            },
            {
                "Name": "guid",
                "Type": "string"
            },
            {
                "Name": "isbn",
                "Type": "string"
            },
            {
                "Name": "mac_address",
                "Type": "string"
            },
            {
                "Name": "money",
                "Type": "string"
            },
            {
                "Name": "username",
                "Type": "string"
            },
            {
                "Name": "password",
                "Type": "string"
            },
            {
                "Name": "phone_number",
                "Type": "string"
            },
            {
                "Name": "postcode",
                "Type": "string"
            },
            {
                "Name": "sha256",
                "Type": "string"
            },
            {
                "Name": "time_12",
                "Type": "string"
            },
            {
                "Name": "time_24",
                "Type": "string"
            },
            {
                "Name": "timezone",
                "Type": "string"
            },
            {
                "Name": "website",
                "Type": "string"
            },
            {
                "Name": "domain_name",
                "Type": "string"
            },
            {
                "Name": "id",
                "Type": "string"
            },
            {
                "Name": "random_number",
                "Type": "string"
            },
            {
                "Name": "married",
                "Type": "string"
            },
            {
                "Name": "car_year",
                "Type": "string"
            },
            {
                "Name": "home_latitude",
                "Type": "string"
            },
            {
                "Name": "home_longitude",
                "Type": "string"
            }
        ],
        "Location": f"s3://{TABLE_BUCKET}/{TABLE_PREFIX}",
        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "Compressed": False,
        "NumberOfBuckets": -1,
        "SerdeInfo": {
            "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
            "Parameters": {
                "paths": "app_bundle_id,app_version,birth_date,buzzword,car_brand,car_model,car_year,country,country_code,domain_name,email,employee_number,first_name,gender,guid,home_latitude,home_longitude,id,image_url,ip_address,isbn,last_name,mac_address,married,money,password,phone_number,postcode,random_number,sha256,time_12,time_24,timezone,username,website"
            }
        }
    },
    "TableType": "EXTERNAL_TABLE",
}


def create_sample_table():
    delete_sample_table()
    _create_table(DATABASE_NAME, table_def)


def delete_sample_table():
    if _table_exists(DATABASE_NAME, TABLE_NAME):
        _delete_table(DATABASE_NAME, TABLE_NAME)


def _table_exists(database_name: str, table_name: str):
    tables = glue_client.get_tables(DatabaseName=database_name).get('TableList', [])
    table_names = list(map(lambda t: t['Name'], tables))
    return table_name in table_names


def _create_table(database_name: str, table_input: dict):
    glue_client.create_table(DatabaseName=database_name, TableInput=table_input)


def _delete_table(database_name: str, table_name: str):
    glue_client.delete_table(DatabaseName=database_name, Name=table_name)
