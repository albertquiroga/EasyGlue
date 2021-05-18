from awsglue.context import GlueContext

from easyglue.utils import reader_method


class JDBCMixin:
    glue_context: GlueContext
    connection_options_dict: dict

    @reader_method
    def jdbc(self, connection_options: dict = None, transformation_ctx: str = "", push_down_predicate: str = ""):
        """
        Reads a dataset from a JDBC source by calling create_dynamic_frame.from_options with the right configuration
        :param connection_options: Connection options dictionary specifying JDBC parameters
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: Push down predicate to be applied
        :return: DynamicFrame representing the JDBC table
        """
        if connection_options:
            self.connection_options_dict = connection_options
        url = self.connection_options_dict.get('url')
        db_type = url.split(':')[1]

        return self.glue_context.create_dynamic_frame.from_options(connection_type=db_type,
                                                                   connection_options=self.connection_options_dict,
                                                                   transformation_ctx=transformation_ctx,
                                                                   push_down_predicate=push_down_predicate)
