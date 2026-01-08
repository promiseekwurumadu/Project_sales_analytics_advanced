import pandas as pd
from prefect import task

from src.db import get_engine
from src.extract_clean import extract_and_clean


@task(name="Load fact_sales", retries=2, retry_delay_seconds=5)
def load_fact(chunk_size: int = 50000, truncate_first: bool = False) -> int:
    engine = get_engine()
    df = extract_and_clean()

    df["date_key"] = pd.to_datetime(df["invoicedate"]).dt.strftime("%Y%m%d").astype(int)

    if truncate_first:
        with engine.begin() as conn:
            conn.exec_driver_sql("TRUNCATE TABLE analytics.fact_sales;")

    dim_customer = pd.read_sql("SELECT customer_key, customer_id FROM analytics.dim_customer", engine)
    dim_product = pd.read_sql("SELECT product_key, stockcode, description FROM analytics.dim_product", engine)
    dim_country = pd.read_sql("SELECT country_key, country FROM analytics.dim_country", engine)

    df = df.merge(dim_customer, left_on="customerid", right_on="customer_id", how="left")
    df = df.merge(dim_country, left_on="country", right_on="country", how="left")
    df = df.merge(dim_product, on=["stockcode", "description"], how="left")

    missing = df[["customer_key", "country_key", "product_key"]].isna().sum()
    if missing.any():
        raise ValueError(f"Missing dimension keys found:\n{missing}")

    fact = df[[
        "invoiceno", "customer_key", "product_key", "country_key",
        "date_key", "quantity", "unitprice", "revenue"
    ]].copy()

    loaded = 0
    for start in range(0, len(fact), chunk_size):
        chunk = fact.iloc[start:start + chunk_size]
        chunk.to_sql(
            "fact_sales",
            con=engine,
            schema="analytics",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        loaded += len(chunk)

    return int(loaded)

