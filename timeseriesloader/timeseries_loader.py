import typing
import os
import csv
import collections

import frozendict  # type: ignore
import pandas as pd  # type: ignore

from d3m import container, exceptions, utils as d3m_utils
from d3m.metadata import base as metadata_base, hyperparams
from d3m.primitive_interfaces import base, transformer

__all__ = ('TimeSeriesLoaderPrimitive',)

Inputs = container.DataFrame
Outputs = container.DataFrame


class Hyperparams(hyperparams.Hyperparams):
    file_col_index = hyperparams.Hyperparameter[int](
        default=0,
        semantic_types=['https://metadata.datadrivendiscovery.org/types/ControlParameter'],
        description='Index of column in input dataframe containing time series file names'
    )
    time_col_index = hyperparams.Hyperparameter[int](
        default=0,
        semantic_types=['https://metadata.datadrivendiscovery.org/types/ControlParameter'],
        description='Index of column in loaded time series files containing the timestamps'
    )
    value_col_index = hyperparams.Hyperparameter[int](
        default=1,
        semantic_types=['https://metadata.datadrivendiscovery.org/types/ControlParameter'],
        description='Index of column in loaded time series files containing the values'
    )


class TimeSeriesLoaderPrimitive(transformer.TransformerPrimitiveBase[Inputs, Outputs, Hyperparams]):
    """
    Reads the time series files from a given column in an input dataframe into a new M x N dataframe,
    where each timeseries occupies one of M rows, and each of the row's N entries represents a timestamp.
    The loading process assumes that each series file has an identical set of timestamps.
    """

    _supported_media_types = (
        'text/csv',
    )

    _semantic_types = (

    )

    __author__ = 'Uncharted Software',
    metadata = metadata_base.PrimitiveMetadata(
        {
            'id': '1689aafa-16dc-4c55-8ad4-76cadcf46086',
            'version': '0.1.0',
            'name': 'Time series loader',
            'python_path': 'd3m.primitives.data.TimeSeriesLoader',
            'keywords': ['series', 'reader', 'csv'],
            'source': {
                'name': 'Uncharted Software',
                'contact': 'mailto:chris.bethune@uncharted.software'
            },
            'installation': [{
                'type': metadata_base.PrimitiveInstallationType.PIP,
                'package_uri': 'git+https://gitlab.com/unchartedsoftware/distil-timeseries-loader.git@{git_commit}#egg=distil-timeseries-loader'.format(
                    git_commit=d3m_utils.current_git_commit(os.path.dirname(__file__)),
                ),
            }],
            'algorithm_types': [
                metadata_base.PrimitiveAlgorithmType.FILE_MANIPULATION,
            ],
            'supported_media_types': _supported_media_types,
            'primitive_family': metadata_base.PrimitiveFamily.DATA_PREPROCESSING,
        }
    )

    @classmethod
    def _can_use_column(cls, inputs_metadata: metadata_base.DataMetadata, column_index: int) -> bool:
        column_metadata = inputs_metadata.query((metadata_base.ALL_ELEMENTS, column_index))

        if not column_metadata or column_metadata['structural_type'] != str:
            return False

        semantic_types = column_metadata.get('semantic_types', [])
        media_types = set(column_metadata.get('media_types', []))

        if 'https://metadata.datadrivendiscovery.org/types/FileName' in semantic_types and media_types <= set(cls._supported_media_types):
            return True

        return False

    def produce(self, *, inputs: Inputs, timeout: float = None, iterations: int = None) -> base.CallResult[Outputs]:
        # make sure the column at the specified index exists and that it is a timeseries column
        file_index = self.hyperparams['file_col_index']
        if not self._can_use_column(inputs.metadata, file_index):
            raise exceptions.InvalidArgumentValueError('column idx=' + str(file_index) + ' from ' + str(inputs.columns) + ' does not contain file names')

        value_index = self.hyperparams['value_col_index']
        time_index = self.hyperparams['time_col_index']

        # load each time series file, transpose, and append
        base_path = inputs.metadata.query((metadata_base.ALL_ELEMENTS, file_index))['location_base_uris'][0]
        timeseries_dataframe: pd.DataFrame
        for idx, file_path in enumerate(inputs.iloc[:, file_index]):
            csv_path = os.path.join(base_path, file_path)
            timeseries_row = pd.read_csv(csv_path).transpose()
            # use the time values as the column headers
            if idx is 0:
                timeseries_dataframe = pd.DataFrame(columns=timeseries_row.iloc[time_index])

            timeseries_dataframe = timeseries_dataframe.append(timeseries_row.iloc[value_index])

        # get the index to use a range of ints rather than the value col name
        timeseries_dataframe = timeseries_dataframe.reset_index(drop=True)

        # wrap as a D3M container - metadata should be auto generated
        return base.CallResult(container.DataFrame(data=timeseries_dataframe))

    @classmethod
    def can_accept(cls, *, method_name: str, arguments: typing.Dict[str, typing.Union[metadata_base.Metadata, type]], hyperparams: Hyperparams) -> typing.Optional[metadata_base.DataMetadata]:
        output_metadata = super().can_accept(method_name=method_name, arguments=arguments, hyperparams=hyperparams)

        # If structural types didn't match, don't bother.
        if output_metadata is None:
            return None

        if method_name != 'produce':
            return output_metadata

        if 'inputs' not in arguments:
            return output_metadata

        inputs_metadata = typing.cast(metadata_base.DataMetadata, arguments['inputs'])

        can_use_column = cls._can_use_column(inputs_metadata, hyperparams['file_col_index'])
        if not can_use_column:
            return None

        # we don't have access to the data at this point so there's not much that we can
        # do to figure out the resulting shape etc.
        return inputs_metadata
