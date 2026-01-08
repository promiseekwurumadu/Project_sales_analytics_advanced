from prefect import task
from src.db import get_engine


@task(name="Data quality checks")
def run_checks(expected_rows: int = 397884) -> None:
    engine = get_engine()
    with engine.connect() as conn:
        fact_rows = conn.exec_driver_sql("SELECT COUNT(*) FROM analytics.fact_sales;").scalar_one()
        cust_rows = conn.exec_driver_sql("SELECT COUNT(*) FROM analytics.dim_customer;").scalar_one()

    if fact_rows != expected_rows:
        raise ValueError(f"fact_sales rowcount check failed: got {fact_rows}, expected {expected_rows}")

    if cust_rows <= 0:
        raise ValueError("dim_customer is empty - check dimension load")

    print(f"Checks passed: fact_sales={fact_rows}, dim_customer={cust_rows}")

