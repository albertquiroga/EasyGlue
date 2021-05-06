from awsglue.context import GlueContext


class EasyDynamicFrameReader:

    connection_options_dict = {}
    format_options_dict = {}
    additional_options_dict = {}
    data_format = ''

    def __init__(self, glue_context: GlueContext):
        """
        Initializes the EasyDynamicFrameReader with the glueContext provided by the application
        :param glue_context: GlueContext object
        """
        self.glue_context = glue_context

    from easyglue.reader._s3 import csv, json, avro, ion, groklog, orc, parquet, glueparquet, xml
    from easyglue.reader._catalog import catalog, table
    from easyglue.reader._jdbc import jdbc
    from easyglue.reader._rdd import rdd
    from easyglue.reader._others import dynamodb, ddb

    def format_option(self, key: str, value: str):
        """
        Stores a format option for later use when reading
        :param key: Format option key
        :param value: Format option value
        :return: None
        """
        self.format_options_dict.update({key: value})
        return self

    def format_options(self, options: dict):
        """
        Stores a dictionary of format options for later use when reading
        :param options: Format options dictionary
        :return: None
        """
        self.format_options_dict = options
        return self

    def connection_option(self, key: str, value: str):
        """
        Stores a connection option for later use when reading
        :param key: Connection option key
        :param value: Connection option value
        :return: None
        """
        self.format_options_dict.update({key: value})
        return self

    def connection_options(self, options: dict):
        """
        Stores a dictionary of connection options for later use when reading
        :param options: Connection options dictionary
        :return: None
        """
        self.connection_options_dict = options
        return self

    def additional_option(self, key: str, value: str):
        """
        Stores an additional option for later use when reading
        :param key: Additional option key
        :param value: Additional option value
        :return: None
        """
        self.additional_options_dict.update({key: value})
        return self

    def additional_options(self, options: dict):
        """
        Stores a dictionary of additional options for later use when reading
        :param options: Additional options dictionary
        :return: None
        """
        self.additional_options_dict = options
        return self

    def option(self, key: str, value: str):
        """
        Method added to comply with Spark's DataframeReader 'option' method. Routes the option as a connection option
        :param key: Connection option key
        :param value: Connection option value
        :return: None
        """
        return self.connection_option(key, value)

    def options(self, options: dict):
        """
        Method added to comply with Spark's DataframeReader 'options' method. Routes the options as connection options
        :param options: Connection options dictionary
        :return: None
        """
        return self.connection_options(options)
