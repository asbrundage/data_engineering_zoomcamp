from time import time
import argparse
import pandas as pd
import os
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # parquet_name = "output.parquet"
    # csv_name = "output.csv.gz"
    if url.endswith(".csv.gz"):
        csv_name = "output.csv.gz"
    else:
        csv_name = "output.csv"

    # os.system(f"wget {url} -O {parquet_name}")
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # df = pd.read_parquet("yellow_tripdata_2021-01.parquet")
    # df = pd.read_parquet(parquet_name)
    # df_iter = pd.read_csv(
    #     csv_name,
    #     encoding="utf-8",
    #     compression="gzip",
    #     iterator=True,
    #     chunksize=100000,
    #     low_memory=False,
    # )

    if url.endswith(".csv.gz"):
        df_iter = pd.read_csv(
            csv_name,
            encoding="utf-8",
            compression="gzip",
            iterator=True,
            chunksize=100000,
            low_memory=False,
        )
    else:
        df_iter = pd.read_csv(
            csv_name,
            encoding="utf-8",
            iterator=True,
            chunksize=100000,
            low_memory=False,
        )

    df = next(df_iter)

    if "tripdata" not in url:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

        df.to_sql(name=table_name, con=engine, if_exists="append")
    else:
        df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

        df.to_sql(name=table_name, con=engine, if_exists="append")

    # df_iter = pd.read_csv(
    #     "yellow_tripdata_2021-01.csv", iterator=True, chunksize=100000
    # )

    # get_ipython().run_line_magic(
    #     "time", "df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')"
    # )

    # while True:
    #     if "tripdata" not in url:
    #         t_start = time()

    #         df.to_sql(name=table_name, con=engine, if_exists="append")

    #         t_end = time()

    #     else:
    #         t_start = time()

    #         df = next(df_iter)
    #         # df = pd.read_parquet(parquet_name)

    #         df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists="append")

    #         t_end = time()

    #         print("inserted another chunk, took %.3f second" % (t_end - t_start))

    try:
        while True:
            t_start = time()

            df = next(df_iter)

            if "tripdata" in url:
                df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()
            print("inserted another chunk, took %.3f second" % (t_end - t_start))
    except StopIteration:
        print("Finished ingesting data.")

    # get_ipython().system(
    #     "wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    # )

    # df_zones = pd.read_csv("taxi+_zone_lookup.csv")

    # df_zones.to_sql(name="zones", con=engine, if_exists="replace")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Parquet data to Postgres")

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument(
        "--table_name", help="name of the table where we will write the results to"
    )
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()

    main(args)
