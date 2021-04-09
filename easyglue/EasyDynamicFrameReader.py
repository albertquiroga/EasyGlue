import sys
from typing import Any, Union

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame


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

    def csv(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
            **kwargs: Any) -> DynamicFrame:
        """
        Reads a CSV dataset by calling the _read_from_s3 method with the right configuration
        :param s3_paths: Paths to read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the dataset
        """
        return self._read_from_s3(data_format='csv', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                                  push_down_predicate=push_down_predicate, kwargs=kwargs)

    def json(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
             **kwargs: Any) -> DynamicFrame:
        """
        Reads a JSON dataset by calling the _read_from_s3 method with the right configuration
        :param s3_paths: Paths to read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the dataset
        """
        return self._read_from_s3(data_format='json', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                                  push_down_predicate=push_down_predicate, kwargs=kwargs)

    def avro(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
             **kwargs: Any) -> DynamicFrame:
        """
        Reads an Avro dataset by calling the _read_from_s3 method with the right configuration
        :param s3_paths: Paths to read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the dataset
        """
        return self._read_from_s3(data_format='avro', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                                  push_down_predicate=push_down_predicate, kwargs=kwargs)

    def ion(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
            **kwargs: Any) -> DynamicFrame:
        """
        Reads an Ion dataset by calling the _read_from_s3 method with the right configuration
        :param s3_paths: Paths to read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the dataset
        """
        return self._read_from_s3(data_format='ion', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                                  push_down_predicate=push_down_predicate, kwargs=kwargs)

    def groklog(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
                **kwargs: Any) -> DynamicFrame:
        """
        Reads a Grok-parseable dataset by calling the _read_from_s3 method with the right configuration
        :param s3_paths: Paths to read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the dataset
        """
        return self._read_from_s3(data_format='grokLog', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                                  push_down_predicate=push_down_predicate, kwargs=kwargs)

    def orc(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
            **kwargs: Any) -> DynamicFrame:
        """
        Reads an ORC dataset by calling the _read_from_s3 method with the right configuration
        :param s3_paths: Paths to read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the dataset
        """
        return self._read_from_s3(data_format='orc', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                                  push_down_predicate=push_down_predicate, kwargs=kwargs)

    def parquet(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
                **kwargs: Any) -> DynamicFrame:
        """
        Reads a Parquet dataset by calling the _read_from_s3 method with the right configuration
        :param s3_paths: Paths to read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the dataset
        """
        return self._read_from_s3(data_format='parquet', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                                  push_down_predicate=push_down_predicate, kwargs=kwargs)

    def glueparquet(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
                    **kwargs: Any) -> DynamicFrame:
        """
        Reads a GlueParquet dataset by calling the _read_from_s3 method with the right configuration
        :param s3_paths: Paths to read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the dataset
        """
        return self._read_from_s3(data_format='glueparquet', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                                  push_down_predicate=push_down_predicate, kwargs=kwargs)

    def xml(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
            **kwargs: Any) -> DynamicFrame:
        """
        Reads an XML dataset by calling the _read_from_s3 method with the right configuration
        :param s3_paths: Paths to read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the dataset
        """
        return self._read_from_s3(data_format='xml', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                                  push_down_predicate=push_down_predicate, kwargs=kwargs)

    def _read_from_s3(self, data_format: str, s3_paths: Union[str, list] = "", transformation_ctx: str = "",
                      push_down_predicate: str = "", **kwargs: Any) -> DynamicFrame:
        """
        Reads a dataset from S3 by calling create_dynamic_frame.from_options with the right configuration
        :param data_format: Format of the underlying dataset
        :param s3_paths: S3 paths to be read from
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param kwargs: Keyword arguments
        :return: DyanamicFrame representing the dataset
        """
        if s3_paths:
            if isinstance(s3_paths, str):
                self.connection_options_dict['paths'] = [s3_paths]
            elif isinstance(s3_paths, list):
                self.connection_options_dict['paths'] = s3_paths
            else:
                self.glue_context.get_logger().error(f'Attribute "s3_paths" must be either str or list, '
                                                     f'{type(s3_paths)} was provided instead')
                sys.exit(1)

        return self.glue_context.create_dynamic_frame.from_options(connection_type='s3',
                                                                   connection_options=self.connection_options_dict,
                                                                   format=data_format,
                                                                   format_options=self.format_options_dict,
                                                                   transformation_ctx=transformation_ctx,
                                                                   push_down_predicate=push_down_predicate,
                                                                   kwargs=kwargs
                                                                   )

    def catalog(self, database_name: str, table_name: str, redshift_tmp_dir: str = "", transformation_ctx: str = "",
                push_down_predicate: str = "", catalog_id: int = None, **kwargs: Any) -> DynamicFrame:
        """
        Reads a dataset from Catalog by calling create_dynamic_frame.from_catalog with the right configuration
        :param database_name: Name of the Data Catalog database containing the table
        :param table_name: Name of the Data Catalog table
        :param redshift_tmp_dir: Temporary path to be used when reading/writing from/to Redshift
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: S3 push down predicate to be applied
        :param catalog_id: Data Catalog ID containing the referenced database and table names
        :param kwargs: Keyword arguments
        :return: DynamicFrame representing the Data Catalog table
        """
        return self.glue_context.create_dynamic_frame.from_catalog(database=database_name,
                                                                   table_name=table_name,
                                                                   redshift_tmp_dir=redshift_tmp_dir,
                                                                   transformation_ctx=transformation_ctx,
                                                                   push_down_predicate=push_down_predicate,
                                                                   additional_options=self.additional_options_dict,
                                                                   catalog_id=catalog_id,
                                                                   kwargs=kwargs)

    def jdbc(self, dbtable: str, url: str, user: str, password: str,
             redshift_tmp_dir: str = "", custom_jdbc_driver_s3_path: str = "", custom_jdbc_driver_class_name: str = "",
             database_type: str = "", transformation_ctx: str = "", push_down_predicate: str = ""):
        """
        Reads a dataset from a JDBC source by calling create_dynamic_frame.from_options with the right configuration
        :param dbtable: Name of the JDBC database
        :param url: JDBC URL to connect to
        :param user: Username to authenticate with when connecting to the JDBC database
        :param password: Password to authenticate with when connecting to the JDBC database
        :param redshift_tmp_dir: Temporary path to be used when reading/writing from/to Redshift
        :param custom_jdbc_driver_s3_path: Path to the S3 object containing the custom JDBC driver to be used
        :param custom_jdbc_driver_class_name: Class name to be used when using a custom driver
        :param database_type: JDBC database type
        :param transformation_ctx: Glue transformation context
        :param push_down_predicate: Push down predicate to be applied
        :return: DynamicFrame representing the JDBC table
        """

        self.connection_options({
            'url': url,
            'dbtable': dbtable,
            'redshiftTmpDir': redshift_tmp_dir,
            'user': user,
            'password': password,
        })

        if custom_jdbc_driver_s3_path and custom_jdbc_driver_class_name:
            self.connection_options({
                'customJdbcDriverS3Path': custom_jdbc_driver_s3_path,
                'customJdbcDriverClassName': custom_jdbc_driver_class_name
            })

        db_type = database_type if database_type else url.split(':')[1]

        return self.glue_context.create_dynamic_frame.from_options(connection_type=db_type,
                                                                   connection_options=self.connection_options_dict,
                                                                   transformation_ctx=transformation_ctx,
                                                                   push_down_predicate=push_down_predicate)

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
