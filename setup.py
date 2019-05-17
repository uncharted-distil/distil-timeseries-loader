from distutils.core import setup

setup(
    name='DistilTimeSeriesLoader',
    version='0.2.0',
    description='loads time series stored in csv files into rows of a Dataframe',
    packages=['timeseriesloader'],
    keywords=['d3m_primitive'],
    install_requires=[
        'pandas >= 0.23.4',
        'frozendict==1.2',
        'd3m==2019.4.4'
        ],
    entry_points={
        'd3m.primitives': [
            'data_preprocessing.timeseries_loader.DistilTimeSeriesLoader = timeseriesloader.timeseries_loader:TimeSeriesLoaderPrimitive',
            'data_preprocessing.timeseries_formatter.DistilTimeSeriesFormatter = timeseriesloader.timeseries_formatter:TimeSeriesFormatterPrimitive'
        ],
    }
)
