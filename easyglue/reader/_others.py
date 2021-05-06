from typing import Any

from awsglue.dynamicframe import DynamicFrame


def dynamodb(self, table_name: str, transformation_ctx: str = "", **kwargs: Any) -> DynamicFrame:
    self.connection_options_dict.clear()  # TODO do this for all reader methods with decorators
    self.connection_options_dict['dynamodb.input.tableName'] = table_name

    return self.glue_context.create_dynamic_frame_from_options(connection_type='dynamodb',
                                                               connection_options=self.connection_options_dict,
                                                               transformation_ctx=transformation_ctx,
                                                               kwargs=kwargs
                                                               )


def ddb(self, table_name: str, transformation_ctx: str = "", **kwargs: Any) -> DynamicFrame:
    return dynamodb(self, table_name, transformation_ctx, kwargs=kwargs)
