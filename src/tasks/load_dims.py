import pandas as pd
from prefect import task
from sqlalchemy import text

from src.db import get_engine
from src.extract_clean import extract_and_clean


def _build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    d = pd.to_datetime(df["invoicedate"]).dt.date
    dim = pd.DataFrame({"date_value": d}).drop_duplicates()

    dim["date_key"] = pd.to_datetime(dim["date_value"]).dt.strftime("%Y%m%d").astype(int)
    dim["year"] = pd.to_datetime(dim["date_value"]).dt.year
    dim["month"] = pd.to_datetime(dim["date_value"]).dt.month
    dim["month_name"] = pd.to_datetime(dim["date_value"]).dt.strftime("%B")
    dim["day"] = pd.to_datetime(dim["date_value"]).dt.day
    dim["day_of_week"] = pd.to_datetime(dim["date_value"]).dt.dayofweek + 1

    return dim[["date_key", "date_value", "year", "month", "month_name", "day", "day_of_week"]]


@task(name="Load dimensions", retries=2, retry_delay_seconds=5)
def load_dimensions() -> dict:
    engine = get_engine()
    df = extract_and_clean()

    dim_customer = df[["customerid"]].drop_duplicates().rename(columns={"customerid": "customer_id"})
    dim_product = df[["stockcode", "description"]].drop_duplicates()
    dim_country = df[["country"]].drop_duplicates()
    dim_date = _build_dim_date(df)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO analytics.dim_customer (customer_id)
                SELECT customer_id FROM (SELECT :customer_id AS customer_id) s
                ON CONFLICT (customer_id) DO NOTHING
            """),
            [{"customer_id": v} for v in dim_customer["customer_id"].tolist()],
        )

        conn.execute(
            text("""
                INSERT INTO analytics.dim_product (stockcode, description)
                SELECT stockcode, description
                FROM (SELECT :stockcode AS stockcode, :description AS description) s
                ON CONFLICT (stockcode, description) DO NOTHING
            """),
            [{"stockcode": r.stockcode, "description": r.description} for r in dim_product.itertuples(index=False)],
        )

        conn.execute(
            text("""
                INSERT INTO analytics.dim_country (country)
                SELECT country FROM (SELECT :country AS country) s
                ON CONFLICT (country) DO NOTHING
            """),
            [{"country": v} for v in dim_country["country"].tolist()],
        )

        conn.execute(
            text("""
                INSERT INTO analytics.dim_date (date_key, date_value, year, month, month_name, day, day_of_week)
                VALUES (:date_key, :date_value, :year, :month, :month_name, :day, :day_of_week)
                ON CONFLICT (date_key) DO NOTHING
            """),
            dim_date.to_dict(orient="records"),
        )

    return {
        "customers": int(len(dim_customer)),
        "products": int(len(dim_product)),
        "countries": int(len(dim_country)),
        "dates": int(len(dim_date)),
    }

