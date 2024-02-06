## Week 2 Homework


### Assignment

The goal will be to construct an ETL pipeline that loads the data, performs some transformations, and writes the data to a database (and Google Cloud!).

- Create a new pipeline, call it `green_taxi_etl`
- Add a data loader block and use Pandas to read data for the final quarter of 2020 (months `10`, `11`, `12`).
  - You can use the same datatypes and date parsing methods shown in the course.
  - `BONUS`: load the final three months using a for loop and `pd.concat`
- Add a transformer block and perform the following:
  - Remove rows where the passenger count is equal to 0 _or_ the trip distance is equal to zero.
  - Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
  - Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.
  - Add three assertions:
    - `vendor_id` is one of the existing values in the column (currently)
    - `passenger_count` is greater than 0
    - `trip_distance` is greater than 0
- Using a Postgres data exporter (SQL or Python), write the dataset to a table called `green_taxi` in a schema `mage`. Replace the table if it already exists.
- Write your data as Parquet files to a bucket in GCP, partioned by `lpep_pickup_date`. Use the `pyarrow` library!
- Schedule your pipeline to run daily at 5AM UTC.

### Questions

## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

* 266,855 rows x 20 columns
* 544,898 rows x 18 columns
* 544,898 rows x 20 columns
* 133,744 rows x 20 columns

> Answer: 266,855 rows x 20 columns


#### Code for data loader with answer for print

```python

import io
import pandas as pd
import requests


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here

    base_url='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_'
    needed_months=['2020-10','2020-11','2020-12']
    file_extension='csv.gz'

    taxi_dtypes={
    'VendorID': pd.Int64Dtype(),
    'store_and_fwd_flag': str,
    'RatecodeID': pd.Int64Dtype(),
    'PULocationID': pd.Int64Dtype(),
    'DOLocationID': pd.Int64Dtype(),
    'passenger_count': pd.Int64Dtype(),
    'trip_distance': float,
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'tip_amount': float,
    'tolls_amount': float,
    'ehail_fee': float,
    'improvement_surcharge': float,
    'total_amount': float,
    'payment_type': pd.Int64Dtype(),
    'trip_type': pd.Int64Dtype(),
    'congestion_surcharge': float
    }

    parse_dates=['lpep_pickup_datetime','lpep_dropoff_datetime']

    df=pd.DataFrame()

    for mth in needed_months:
        url=f'{base_url}{mth}.{file_extension}'
        df_temp=pd.read_csv(url,sep=",",compression='gzip',dtype=taxi_dtypes,parse_dates=parse_dates)
        print(url)
        print(f'{mth} data loaded!!!')

        df=pd.concat([df,df_temp])

    print('Shape of data:', df.shape)

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


```

## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 _and_ the trip distance is greater than zero, how many rows are left?

* 544,897 rows
* 266,855 rows
* 139,370 rows
* 266,856 rows

> Answer: 139,370 rows

#### Code for transformers

```python

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    print("Rows with zero passenger or trip distance:",data.query('passenger_count==0 or trip_distance==0').shape[0] )

    df_transform= data.query('passenger_count>0 and trip_distance>0')

    # For Q2 Answer
    print('Shape after filtering:', df_transform.shape)


    # For Q3 Answer
    df_transform['lpep_pickup_date']=df_transform['lpep_pickup_datetime'].dt.date

    # For Q4 Answer
    print('Existing values of VendorID: ',df_transform.VendorID.unique().tolist())

    # For Q5 Answer
    print('Number of Columns with Camelcase: ',df_transform.columns[df_transform.columns
        .str.count('[a-z][A-Z]')>0].shape[0]
        )

    

    df_transform.columns=(df_transform.columns
                            .str.replace('(?<=[a-z])(?=[A-Z])','_',regex=True)
                            .str.lower()
    )


    return df_transform


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert output.vendor_id.isin([1,2]).sum()>0, 'Vendor ID is something other than 1 or 2'
    assert output.passenger_count.isin([0]).sum()==0, 'There are rodes with zero passenger'
    assert output.trip_distance.isin([0]).sum()==0, 'There are rides with zero trip distance'

```

## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

* `data = data['lpep_pickup_datetime'].date`
* `data('lpep_pickup_date') = data['lpep_pickup_datetime'].date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()`


> Answer: `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`

#### Check Q2 code block for relevant answer

## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

* 1, 2, or 3
* 1 or 2
* 1, 2, 3, 4
* 1

> Answer: 1 or 2

#### Check Q2 code block for relevant answer


## Question 5. Data Transformation

How many columns need to be renamed to snake case?

* 3
* 6
* 2
* 4

> Answer: 4

#### Check Q2 code block for relevant answer


## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

* 96
* 56
* 67
* 108

> Answer: 96


#### Code for data exporter to PostGres

```sql
-- Settings are Schema=mage and table_name=green_taxi in Mage UI

SELECT * FROM {{ df_1 }}

```

#### Code for data exporter to GCP

```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/home/src/de-zoomcamp-2024-gcp-412014-73053d9a6b66.json'

bucket_name = 'mage-zoomcamp-saif'
project_id = 'de-zoomcamp-2024-gcp-412014'

table_name='green_taxi_data'

root_path=f'{bucket_name}/{table_name}'


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here

    table=pa.Table.from_pandas(data)

    gcs=pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )

```
