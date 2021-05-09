from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

from easyglue.writer._S3Mixin import S3Mixin


class EasyDynamicFrameWriter(S3Mixin):
    data_format = ""  # TODO implement this, with a save() method
    connection_options_dict = {}
    format_options_dict = {}

    def __init__(self, glue_context: GlueContext, dynamicframe: DynamicFrame):
        self.glue = glue_context
        self.dyf = dynamicframe

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
        self.connection_options_dict.update({key: value})
        return self

    def connection_options(self, options: dict):
        """
        Stores a dictionary of connection options for later use when reading
        :param options: Connection options dictionary
        :return: None
        """
        self.connection_options_dict = options
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