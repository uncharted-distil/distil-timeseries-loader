import unittest
from os import path
import csv

from d3m import container
from d3m.primitives.distil import TimeSeriesLoader


class TimeSeriesLoaderPrimitiveTestCase(unittest.TestCase):

    _dataset_path = path.abspath(path.join(path.dirname(__file__), 'data', 'datasets', 'timeseries_dataset_2'))

    def test_basic(self) -> None:
        dataframe = self._load_timeseries()

        # create the time series dataframe
        hyperparams_class = \
            TimeSeriesLoaderPrimitive.metadata.query()['primitive_code']['class_type_arguments']['Hyperparams']
        hyperparams = hyperparams_class.defaults().replace(
            {
                'file_col_index': 0,
                'value_col_index': 1,
                'time_col_index': 0
            }
        )
        ts_reader = timeseries_reader.TimeSeriesReaderPrimitive(hyperparams=hyperparams)
        timeseries_dataframe = ts_reader.produce(inputs=dataframe).value

        # verify that we have the expected shape
        self.assertEqual(timeseries_dataframe.shape[0], 4)
        self.assertEqual(timeseries_dataframe.shape[1], 167)

        times = []
        values = []

        file_path = path.join(
            path.dirname(__file__),
            'data', 'datasets', 'timeseries_dataset_2', 'timeseries', '0000_train_ts.csv')
        file_path = path.abspath(file_path)
        with open(file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                times.append(int(row[0]))
                values.append(float(row[1]))

        # check that column headers are the times
        self.assertListEqual(times, list(timeseries_dataframe.columns.values[1:]))

        # check that the first row in the dataframe matches the values from the file
        ts_values = list(timeseries_dataframe.iloc[0])[1:]
        self.assertEqual(len(ts_values), len(values))

    def test_can_accept_success(self) -> None:
        dataframe = self._load_timeseries()

        # instantiate the primitive and check acceptance
        hyperparams_class = timeseries_reader.TimeSeriesReaderPrimitive.metadata.query()['primitive_code']['class_type_arguments']['Hyperparams']
        ts_reader = timeseries_reader.TimeSeriesReaderPrimitive(hyperparams=hyperparams_class.defaults())
        metadata = ts_reader.can_accept(arguments={'inputs': dataframe.metadata},
                                        hyperparams=hyperparams_class.defaults(), method_name='produce')
        self.assertIsNotNone(metadata)

    def test_can_accept_bad_column(self) -> None:
        dataframe = self._load_timeseries()

        # instantiate the primitive and check acceptance
        hyperparams_class = timeseries_reader.TimeSeriesReaderPrimitive.metadata.query()['primitive_code']['class_type_arguments']['Hyperparams']
        hyperparams = hyperparams_class.defaults().replace({'file_col_index': 4})
        ts_reader = timeseries_reader.TimeSeriesReaderPrimitive(hyperparams=hyperparams_class.defaults())
        metadata = ts_reader.can_accept(arguments={'inputs': dataframe.metadata},
                                        hyperparams=hyperparams, method_name='produce')
        self.assertIsNone(metadata)

    @classmethod
    def _load_timeseries(cls) -> container.DataFrame:
        dataset_doc_path = path.join(cls._dataset_path, 'datasetDoc.json')

        # load the dataset and convert resource 0 to a dataframe
        dataset = container.Dataset.load('file://{dataset_doc_path}'.format(dataset_doc_path=dataset_doc_path))
        return dataset['0']


if __name__ == '__main__':
    unittest.main()
