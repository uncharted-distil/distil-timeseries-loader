# distil-timeseries-loader

A glue priimitive that reads the time series files from a given column in an input dataframe into a new M x N dataframe, where each timeseries occupies one of M rows, and each of the row's N entries represents a timestamp. The loading process assumes that each series file has an identical set of timestamps.

Deployment:

```shell
pip install -e git+ssh://git@github.com/unchartedsoftware/distil-timeseries-loader.git#egg=DistilTimeSeriesLoader --process-dependency-links
```

Development:

```shell
pip install -r requirements.txt
```