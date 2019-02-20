"""
   Copyright © 2019 Uncharted Software Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import typing
import os
import csv
import collections

import frozendict  # type: ignore
import pandas as pd  # type: ignore

from d3m import container, exceptions, utils as d3m_utils
from d3m.metadata import base as metadata_base, hyperparams
from d3m.primitive_interfaces import base, transformer
from common_primitives import utils

__all__ = ('TimeSeriesFormatterPrimitive',)


class Hyperparams(hyperparams.Hyperparams):
    file_col_index = hyperparams.Hyperparameter[typing.Union[int, None]](
        default=None,
        semantic_types=['https://metadata.datadrivendiscovery.org/types/ControlParameter'],
        description='Index of column in input dataset containing time series file names.' +
                    'If set to None, will use the first csv filename column found.'
    )
    ref_resource_index = hyperparams.Hyperparameter[typing.Union[string, None]](
        default='0',
        semantic_types=['https://metadata.datadrivendiscovery.org/types/ControlParameter'],
        description='Index of data resource in input dataset containing the timeseries data.'
    )
    main_resource_index = hyperparams.Hyperparameter[typing.Union[string, None]](
        default='1',
        semantic_types=['https://metadata.datadrivendiscovery.org/types/ControlParameter'],
        description='Index of data resource in input dataset containing the reference to timeseries data.'
    )


class TimeSeriesFormatterPrimitive(transformer.TransformerPrimitiveBase[container.DataFrame,
                                                                     container.DataFrame,
                                                                     Hyperparams]):
    """
    Reads the time series files from a given column in an input dataset resource into a new M x N data resource,
    where each value in timeseries occupies one of M rows. Each row has N columns, representing the union of
    the fields found in the timeseries files and in the main data resource.
    The loading process assumes that each series file has an identical set of timestamps.
    """

    _semantic_types = ('https://metadata.datadrivendiscovery.org/types/FileName',
                       'https://metadata.datadrivendiscovery.org/types/Timeseries')
    _media_types = ('text/csv',)

    __author__ = 'Uncharted Software',
    metadata = metadata_base.PrimitiveMetadata(
        {
            'id': '1689aafa-16dc-4c55-8ad4-76cadcf46086',
            'version': '0.1.0',
            'name': 'Time series formatter',
            'python_path': 'd3m.primitives.distil.TimeSeriesFormatter',
            'keywords': ['series', 'reader', 'csv'],
            'source': {
                'name': 'Uncharted Software',
                'contact': 'mailto:chris.bethune@uncharted.software'
            },
            'installation': [{
                'type': metadata_base.PrimitiveInstallationType.PIP,
                'package_uri': 'git+https://gitlab.com/unchartedsoftware/distil-timeseries-loader.git@' +
                               '{git_commit}#egg=distil-timeseries-loader'
                               .format(git_commit=d3m_utils.current_git_commit(os.path.dirname(__file__)),),
            }],
            'algorithm_types': [
                metadata_base.PrimitiveAlgorithmType.FILE_MANIPULATION,
            ],
            'supported_media_types': _media_types,
            'primitive_family': metadata_base.PrimitiveFamily.DATA_PREPROCESSING,
        }
    )

    @classmethod
    def _find_csv_file_column(cls, inputs_metadata: metadata_base.DataMetadata) -> typing.Optional[int]:
        indices = utils.list_columns_with_semantic_types(inputs_metadata, cls._semantic_types)
        for i in indices:
            if cls._is_csv_file_column(inputs_metadata, i):
                return i
        return None

    @classmethod
    def _is_csv_file_column(cls, inputs_metadata: metadata_base.DataMetadata, column_index: int) -> bool:
        # check to see if a given column is a file pointer that points to a csv file
        column_metadata = inputs_metadata.query((metadata_base.ALL_ELEMENTS, column_index))

        if not column_metadata or column_metadata['structural_type'] != str:
            return False

        semantic_types = column_metadata.get('semantic_types', [])
        media_types = column_metadata.get('media_types', [])

        return set(cls._semantic_types).issubset(semantic_types) and set(cls._media_types).issubset(media_types)

    def produce(self, *,
                inputs: container.Dataset,
                timeout: float = None,
                iterations: int = None) -> base.CallResult[container.DataFrame]:

        file_index = self.hyperparams['file_col_index']
        if file_index is not None:
            if not self._is_csv_file_column(inputs.metadata, file_index):
                raise exceptions.InvalidArgumentValueError('column idx=' + str(file_index) + ' from '
                                                           + str(inputs.columns) + ' does not contain csv file names')
        else:
            file_index = self._find_csv_file_column(inputs.metadata)
            if file_index is None:
                raise exceptions.InvalidArgumentValueError('no column from '
                                                           + str(inputs.columns) + ' contains csv file names')

        # generate the long form timeseries data
        base_path = inputs.metadata.query((metadata_base.ALL_ELEMENTS, file_index))['location_base_uris'][0]
        timeseries_dataframe: pd.DataFrame
        for idx, tRow in inputs[main_resource_index].iterrows()):
            # read the timeseries data
            csv_path = os.path.join(base_path, tRow[file_index])
            timeseries_row = pd.read_csv(csv_path)

            # add the timeseries id
            tRow = tRow.append([idx])

            for vIdx, vRow in timeseries_row.iterrows():
                # combine the timeseries data with the value row
                combined_data = tRow.append(vRow)

                # add the timeseries index
                timeseries_dataframe = timeseries_dataframe.append(combined_data)

        # add the metadata for the new timeseries id

        # join the metadata from the 2 data resources
        metadata = utils.append_columns_metadata(inputs[main_resource_index].metadata, inputs[ref_resource_index].metadata)

        # wrap as a D3M container
        return base.CallResult(container.Dataset(timeseries_dataframe, metadata))

    @classmethod
    def can_accept(cls, *,
                   method_name: str,
                   arguments: typing.Dict[str, typing.Union[metadata_base.Metadata, type]],
                   hyperparams: Hyperparams) -> typing.Optional[metadata_base.DataMetadata]:
        output_metadata = super().can_accept(method_name=method_name, arguments=arguments, hyperparams=hyperparams)

        # If structural types didn't match, don't bother.
        if output_metadata is None:
            return None

        if method_name != 'produce':
            return output_metadata

        if 'inputs' not in arguments:
            return output_metadata

        inputs_metadata = typing.cast(metadata_base.DataMetadata, arguments['inputs'])

        # make sure there's a file column that points to a csv (search if unspecified)
        file_col_index = hyperparams['file_col_index']
        if file_col_index is not None:
            can_use_column = cls._is_csv_file_column(inputs_metadata, file_col_index)
            if not can_use_column:
                return None
        else:
            inferred_index = cls._find_csv_file_column(inputs_metadata)
            if inferred_index is None:
                return None

        # we don't have access to the data at this point so there's not much that we can
        # do to figure out the resulting shape etc
        return inputs_metadata
