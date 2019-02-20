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
from d3m.primitives.distil import TimeSeriesFormatter
from d3m.metadata import base as metadata_base


class TimeSeriesFormatterPrimitiveTestCase(unittest.TestCase):

    _dataset_path = path.abspath(path.join(path.dirname(__file__), 'dataset'))

    def test_basic(self) -> None:
        dataset = self._load_timeseries()

        # create the time series dataset
        hyperparams_class = \
            TimeSeriesFormatter.metadata.query()['primitive_code']['class_type_arguments']['Hyperparams']
        hyperparams = hyperparams_class.defaults().replace(
            {
                'file_col_index': 0,
                'value_col_index': 1,
                'time_col_index': 0
            }
        )
        ts_formatter = TimeSeriesFormatter(hyperparams=hyperparams)
        timeseries_dataset = ts_formatter.produce(inputs=dataset).value

        # verify that we have the expected shape
        self.assertEqual(timeseries_dataset.shape[0], 4)
        self.assertEqual(timeseries_dataset.shape[1], 166)

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
        self.assertListEqual(times, list(timeseries_dataset.columns.values))

        # check that the first row in the dataframe matches the values from the file
        ts_values = list(timeseries_dataset.iloc[0])
        self.assertEqual(len(ts_values), len(values))

    def test_can_accept_success(self) -> None:
        dataset = self._load_timeseries()

        # instantiate the primitive and check acceptance
        hyperparams_class = TimeSeriesFormatter.metadata.query()['primitive_code']['class_type_arguments']['Hyperparams']
        hyperparams = hyperparams_class.defaults().replace(
            {
                'file_col_index': 0,
                'value_col_index': 1,
                'time_col_index': 0
            }
        )

        ts_reader = TimeSeriesFormatter(hyperparams=hyperparams)
        metadata = ts_reader.can_accept(arguments={'inputs': dataset.metadata},
                                        hyperparams=hyperparams_class.defaults(), method_name='produce')
        self.assertIsNotNone(metadata)

    def test_can_accept_success_inferred(self) -> None:
        dataset = self._load_timeseries()

        # instantiate the primitive and check acceptance
        hyperparams_class = TimeSeriesFormatter.metadata.query()['primitive_code']['class_type_arguments']['Hyperparams']
        ts_reader = TimeSeriesFormatter(hyperparams=hyperparams_class.defaults())
        metadata = ts_reader.can_accept(arguments={'inputs': dataset.metadata},
                                        hyperparams=hyperparams_class.defaults(), method_name='produce')
        self.assertIsNotNone(metadata)

    def test_can_accept_bad_column(self) -> None:
        dataset = self._load_timeseries()

        # instantiate the primitive and check acceptance
        hyperparams_class = TimeSeriesFormatter.metadata.query()['primitive_code']['class_type_arguments']['Hyperparams']
        hyperparams = hyperparams_class.defaults().replace({'file_col_index': 4})
        ts_reader = TimeSeriesFormatter(hyperparams=hyperparams_class.defaults())
        metadata = ts_reader.can_accept(arguments={'inputs': dataset.metadata},
                                        hyperparams=hyperparams, method_name='produce')
        self.assertIsNone(metadata)

    @classmethod
    def _load_timeseries(cls) -> container.Dataset:
        dataset_doc_path = path.join(cls._dataset_path, 'datasetDoc.json')

        # load the dataset and convert resource 0 to a dataframe
        dataset = container.Dataset.load('file://{dataset_doc_path}'.format(dataset_doc_path=dataset_doc_path))

        return dataset


if __name__ == '__main__':
    unittest.main()
