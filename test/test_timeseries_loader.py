"""
   Copyright © 2018 Uncharted Software Inc.

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

import unittest
from os import path
import csv

from d3m import container
from d3m.primitives.data_preprocessing.timeseries_loader import DistilTimeSeriesLoader as TimeSeriesLoader
from d3m.metadata import base as metadata_base


class TimeSeriesLoaderPrimitiveTestCase(unittest.TestCase):

    _dataset_path = path.abspath(path.join(path.dirname(__file__), 'dataset'))

    def test_basic(self) -> None:
        dataframe = self._load_timeseries()

        # create the time series dataframe
        hyperparams_class = \
            TimeSeriesLoader.metadata.query()['primitive_code']['class_type_arguments']['Hyperparams']
        hyperparams = hyperparams_class.defaults().replace(
            {
                'file_col_index': 0,
                'value_col_index': 1,
                'time_col_index': 0
            }
        )
        ts_loader = TimeSeriesLoader(hyperparams=hyperparams)
        timeseries_dataframe = ts_loader.produce(inputs=dataframe).value

        # verify that we have the expected shape
        self.assertEqual(timeseries_dataframe.shape[0], 4)
        self.assertEqual(timeseries_dataframe.shape[1], 166)

        times = []
        values = []

        file_path = path.join(path.dirname(__file__), 'dataset', 'timeseries', '0000_train_ts.csv')
        file_path = path.abspath(file_path)
        with open(file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                times.append(int(row[0]))
                values.append(float(row[1]))

        # check that column headers are the times
        self.assertListEqual(times, list(timeseries_dataframe.columns.values))

        # check that the first row in the dataframe matches the values from the file
        ts_values = list(timeseries_dataframe.iloc[0])
        self.assertEqual(len(ts_values), len(values))

    @classmethod
    def _load_timeseries(cls) -> container.DataFrame:
        dataset_doc_path = path.join(cls._dataset_path, 'datasetDoc.json')

        # load the dataset and convert resource 0 to a dataframe
        dataset = container.Dataset.load('file://{dataset_doc_path}'.format(dataset_doc_path=dataset_doc_path))
        dataframe = dataset['0']

        # Add the metadata that the execution of the DatasetToDataframe primitive would typically
        # have done.  We don't include the common_primitives package that would allow us to the call
        # that primitive as a dependency because it transitively requires Tensorflow, Torch and Keras.
        base_file_path = 'file://' + path.join(cls._dataset_path, 'timeseries')
        dataframe.metadata = dataframe.metadata.generate(dataframe)
        dataframe.metadata = dataframe.metadata. \
            add_semantic_type((metadata_base.ALL_ELEMENTS, 0),
                              'https://metadata.datadrivendiscovery.org/types/FileName')
        dataframe.metadata = dataframe.metadata. \
            add_semantic_type((metadata_base.ALL_ELEMENTS, 0),
                              'https://metadata.datadrivendiscovery.org/types/Timeseries')

        dataframe.metadata = dataframe.metadata.update((metadata_base.ALL_ELEMENTS, 0),
                                                       {'media_types': ('text/csv',)})

        dataframe.metadata = dataframe.metadata.update((metadata_base.ALL_ELEMENTS, 0),
                                                       {'location_base_uris': (base_file_path,)})

        return dataframe


if __name__ == '__main__':
    unittest.main()
