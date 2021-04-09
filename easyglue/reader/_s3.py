import sys
from typing import Any, Union

from awsglue.dynamicframe import DynamicFrame


def _read_from_s3(self, data_format: str, s3_paths: Union[str, list] = "", transformation_ctx: str = "",
                  push_down_predicate: str = "", **kwargs: Any) -> DynamicFrame:
    """
    Reads a dataset from S3 by calling create_dynamic_frame.from_options with the right configuration
    :param data_format: Format of the underlying dataset
    :param s3_paths: S3 paths to be read from
    :param transformation_ctx: Glue transformation context
    :param push_down_predicate: S3 push down predicate to be applied
    :param kwargs: Keyword arguments
    :return: DynamicFrame representing the dataset
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

    return self.glue_context.create_dynamic_frame_from_options(connection_type='s3',
                                                               connection_options=self.connection_options_dict,
                                                               format=data_format,
                                                               format_options=self.format_options_dict,
                                                               transformation_ctx=transformation_ctx,
                                                               push_down_predicate=push_down_predicate,
                                                               kwargs=kwargs
                                                               )


def csv(self, s3_paths, transformation_ctx: str = "", push_down_predicate: str = "",
        **kwargs: Any) -> DynamicFrame:
    """
    Reads a CSV dataset by calling the _read_from_s3 method with the right configuration
    :param self: Self reference to the EasyDynamicFrameReader class
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
    :param self: Self reference to the EasyDynamicFrameReader class
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
    :param self: Self reference to the EasyDynamicFrameReader class
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
    :param self: Self reference to the EasyDynamicFrameReader class
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
    Reads a Grok-parsable dataset by calling the _read_from_s3 method with the right configuration
    :param self: Self reference to the EasyDynamicFrameReader class
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
    :param self: Self reference to the EasyDynamicFrameReader class
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
    :param self: Self reference to the EasyDynamicFrameReader class
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
    :param self: Self reference to the EasyDynamicFrameReader class
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
    :param self: Self reference to the EasyDynamicFrameReader class
    :param s3_paths: Paths to read from
    :param transformation_ctx: Glue transformation context
    :param push_down_predicate: S3 push down predicate to be applied
    :param kwargs: Keyword arguments
    :return: DynamicFrame representing the dataset
    """
    return self._read_from_s3(data_format='xml', s3_paths=s3_paths, transformation_ctx=transformation_ctx,
                              push_down_predicate=push_down_predicate, kwargs=kwargs)
