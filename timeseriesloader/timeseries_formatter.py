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
    main_resource_index = hyperparams.Hyperparameter[typing.Union[str, None]](
        default='1',
        semantic_types=['https://metadata.datadrivendiscovery.org/types/ControlParameter'],
        description='Index of data resource in input dataset containing the reference to timeseries data.'
    )


class TimeSeriesFormatterPrimitive(transformer.TransformerPrimitiveBase[container.Dataset,
                                                                     container.Dataset,
                                                                     Hyperparams]):
    """
    Reads the time series files from a given column in an input dataset resource into a new M x N data resource,
    where each value in timeseries occupies one of M rows. Each row has N columns, representing the union of
    the fields found in the timeseries files and in the main data resource.
    The loading process assumes that each series file has an identical set of timestamps.
    """

    _semantic_types = ('https://metadata.datadrivendiscovery.org/types/FileName',
                       'https://metadata.datadrivendiscovery.org/types/Timeseries',
                       'http://schema.org/Text',
                       'https://metadata.datadrivendiscovery.org/types/Attribute')
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
    def _find_csv_file_column(cls, inputs_metadata: metadata_base.DataMetadata, res_id: int) -> typing.Optional[int]:
        indices = utils.list_columns_with_semantic_types(inputs_metadata, cls._semantic_types, at=(res_id,))
        for i in indices:
            if cls._is_csv_file_column(inputs_metadata, res_id, i):
                return i
        return None

    @classmethod
    def _is_csv_file_column(cls, inputs_metadata: metadata_base.DataMetadata, res_id: int, column_index: int) -> bool:
        # check to see if a given column is a file pointer that points to a csv file
        column_metadata = inputs_metadata.query((res_id, metadata_base.ALL_ELEMENTS, column_index))

        if not column_metadata or column_metadata['structural_type'] != str:
            return False

        # check if a foreign key exists
        if column_metadata['foreign_key'] is None:
            return False

        ref_col_index = column_metadata['foreign_key']['column_index']
        ref_res_id = column_metadata['foreign_key']['resource_id']

        return cls._is_csv_file_reference(inputs_metadata, ref_res_id, ref_col_index)

    @classmethod
    def _is_csv_file_reference(cls, inputs_metadata: metadata_base.DataMetadata, res_id: int, column_index: int) -> bool:
        # check to see if the column is a csv resource
        column_metadata = inputs_metadata.query((res_id, metadata_base.ALL_ELEMENTS, column_index))

        if not column_metadata or column_metadata['structural_type'] != str:
            return False

        semantic_types = column_metadata.get('semantic_types', [])
        media_types = column_metadata.get('media_types', [])

        semantic_types_set = set(semantic_types)
        _semantic_types_set = set(cls._semantic_types)

        return bool(semantic_types_set.intersection(_semantic_types_set)) and set(cls._media_types).issubset(media_types)

    def produce(self, *,
                inputs: container.Dataset,
                timeout: float = None,
                iterations: int = None) -> base.CallResult[container.DataFrame]:

        main_resource_index = self.hyperparams['main_resource_index']
        if main_resource_index is None:
            raise exceptions.InvalidArgumentValueError('no main resource specified')

        file_index = self.hyperparams['file_col_index']
        if file_index is not None:
            if not self._is_csv_file_column(inputs.metadata, main_resource_index, file_index):
                raise exceptions.InvalidArgumentValueError('column idx=' + str(file_index) + ' from does not contain csv file names')
        else:
            file_index = self._find_csv_file_column(inputs.metadata)
            if file_index is None:
                raise exceptions.InvalidArgumentValueError('no column from contains csv file names')

        # generate the long form timeseries data
        base_path = self._get_base_path(inputs.metadata, main_resource_index, file_index)
        timeseries_dataframe = pd.DataFrame()
        for idx, tRow in inputs[main_resource_index].iterrows():
            # read the timeseries data
            csv_path = os.path.join(base_path, tRow[file_index])
            timeseries_row = pd.read_csv(csv_path)

            # add the timeseries id
            tRow = tRow.append(pd.Series(idx))

            for vIdx, vRow in timeseries_row.iterrows():
                # combine the timeseries data with the value row
                combined_data = tRow.append(vRow)

                # add the timeseries index
                timeseries_dataframe = timeseries_dataframe.append(combined_data, ignore_index=True)

        # add the metadata for the new timeseries id

        # join the metadata from the 2 data resources
        ref_res_id = self._get_ref_resource(inputs.metadata, main_resource_index, file_index)
        main_metadata = metadata_base.Metadata()
        main_metadata = utils.copy_metadata(inputs.metadata, main_metadata, (main_resource_index,))
        ref_metadata = metadata_base.Metadata()
        ref_metadata = utils.copy_metadata(inputs.metadata, ref_metadata, (ref_res_id,))

        metadata = utils.append_columns_metadata(main_metadata, ref_metadata)

        # wrap as a D3M container
        #return base.CallResult(container.Dataset({'0': timeseries_dataframe}, metadata))
        return base.CallResult(container.Dataset({'0': timeseries_dataframe}, generate_metadata=True))

    def _get_base_path(self,
                   inputs_metadata: metadata_base.DataMetadata,
                   res_id: str,
                   column_index: int) -> str:
        # get the base uri from the referenced column
        column_metadata = inputs_metadata.query((res_id, metadata_base.ALL_ELEMENTS, column_index))

        ref_col_index = column_metadata['foreign_key']['column_index']
        ref_res_id = column_metadata['foreign_key']['resource_id']

        return inputs_metadata.query((ref_res_id, metadata_base.ALL_ELEMENTS, ref_col_index))['location_base_uris'][0]

    def _get_ref_resource(self,
                   inputs_metadata: metadata_base.DataMetadata,
                   res_id: str,
                   column_index: int) -> str:
        # get the referenced resource from the referenced column
        column_metadata = inputs_metadata.query((res_id, metadata_base.ALL_ELEMENTS, column_index))
        ref_res_id = column_metadata['foreign_key']['resource_id']

        return ref_res_id

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

        main_resource_index = hyperparams['main_resource_index']
        if main_resource_index is None:
            return None

        # make sure there's a file column that points to a csv (search if unspecified)
        file_col_index = hyperparams['file_col_index']
        if file_col_index is not None:
            can_use_column = cls._is_csv_file_column(inputs_metadata, main_resource_index, file_col_index)
            if not can_use_column:
                return None
        else:
            inferred_index = cls._find_csv_file_column(inputs_metadata, main_resource_index)
            if inferred_index is None:
                return None

        # we don't have access to the data at this point so there's not much that we can
        # do to figure out the resulting shape etc
        return inputs_metadata
