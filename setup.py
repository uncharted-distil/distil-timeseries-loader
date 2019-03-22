from distutils.core import setup

setup(
    name='DistilTimeSeriesLoader',
    version='0.1.1',
    description='loads time series stored in csv files into rows of a Dataframe',
    packages=['timeseriesloader'],
    keywords=['d3m_primitive'],
    install_requires=[
        'pandas >= 0.23.4',
        'frozendict==1.2'
        ],
    entry_points={
        'd3m.primitives': [
            'distil.TimeSeriesLoader = timeseriesloader.timeseries_loader:TimeSeriesLoaderPrimitive',
            'distil.TimeSeriesFormatter = timeseriesloader.timeseries_formatter:TimeSeriesFormatterPrimitive'
        ],
    }
)
