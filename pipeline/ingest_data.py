#!/usr/bin/env python
# coding: utf-8

# In[12]:


import pandas as pd
from sqlalchemy import create_engine

pg_user = 'root'
pg_pass = 'root'
pg_host = 'localhost'
pg_port = 5432
pg_db = 'ny_taxi'

year=2025
month=11

prefix='/workspaces/data-engineering-zoomcamp/pipeline'
url = f'{prefix}/green_tripdata_{year}-{month:02d}.parquet'

green_tripdata_2025_11 = pd.read_parquet(url)
# Display first rows
green_tripdata_2025_11.head()


# In[13]:


# Check data types
green_tripdata_2025_11.dtypes
# Check data shape
green_tripdata_2025_11.shape


# In[15]:


taxi_zone_lookup = pd.read_csv(prefix + '/taxi_zone_lookup.csv')

taxi_zone_lookup.dtypes
taxi_zone_lookup.head()
len(taxi_zone_lookup)


engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

green_tripdata_2025_11.to_sql(
    name="green_tripdata_2025_11",
    con=engine,
    if_exists="replace",
    index=False
)

taxi_zone_lookup.to_sql(
    name="taxi_zone_lookup",
    con=engine,
    if_exists="replace",
    index=False
)


