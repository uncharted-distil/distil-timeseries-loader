from distutils.core import setup

setup(
    name='DistilTimeSeriesLoader',
    version='0.1.0',
    description='loads time series stored in csv files into rows of a Dataframe',
    packages=['timeseriesloader'],
    keywords=['d3m_primitive'],
    install_requires=[
        'pandas>=0.22.0',
        'frozendict>=1.2',
        'd3m'
    ],
    entry_points={
        'd3m.primitives': [
            'distil.TimeSeriesLoader = timeseriesloader.timeseries_loader:TimeSeriesLoaderPrimitive'
        ],
    }
)
