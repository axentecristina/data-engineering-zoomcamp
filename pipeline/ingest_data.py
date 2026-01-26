#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database')
@click.option('--year', default=2025, type=int, help='Year for the data')
@click.option('--month', default=11, type=int, help='Month for the data')
@click.option('--prefix', default='/workspaces/data-engineering-zoomcamp/pipeline', help='Prefix path for data files')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, prefix):
    """Ingest green taxi and taxi zone lookup data into PostgreSQL."""
    
    url = f'{prefix}/green_tripdata_{year}-{month:02d}.parquet'

    green_tripdata_2025_11 = pd.read_parquet(url)
    # Display first rows
    click.echo("Green tripdata head:")
    click.echo(green_tripdata_2025_11.head())

    # Check data types
    click.echo("\nGreen tripdata dtypes:")
    click.echo(green_tripdata_2025_11.dtypes)
    # Check data shape
    click.echo(f"\nGreen tripdata shape: {green_tripdata_2025_11.shape}")

    taxi_zone_lookup = pd.read_csv(prefix + '/taxi_zone_lookup.csv')

    click.echo("\nTaxi zone lookup dtypes:")
    click.echo(taxi_zone_lookup.dtypes)
    click.echo("\nTaxi zone lookup head:")
    click.echo(taxi_zone_lookup.head())
    click.echo(f"\nTaxi zone lookup length: {len(taxi_zone_lookup)}")

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    click.echo("\nInserting green_tripdata_2025_11 into database...")
    green_tripdata_2025_11.to_sql(
        name="green_tripdata_2025_11",
        con=engine,
        if_exists="replace",
        index=False
    )

    click.echo("Inserting taxi_zone_lookup into database...")
    taxi_zone_lookup.to_sql(
        name="taxi_zone_lookup",
        con=engine,
        if_exists="replace",
        index=False
    )
    
    click.echo("Data ingestion completed successfully!")


if __name__ == '__main__':
    main()


