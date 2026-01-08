from prefect import flow

from src.tasks.load_dims_task import load_dimensions
from src.tasks.load_fact_task import load_fact
from src.tasks.checks_task import run_checks


@flow(name="sales_analytics_pipeline")
def sales_pipeline(truncate_fact_first: bool = False):
    dims_info = load_dimensions()
    loaded_rows = load_fact(truncate_first=truncate_fact_first)
    run_checks()
    return {"dims": dims_info, "fact_rows_loaded": loaded_rows}


if __name__ == "__main__":
    result = sales_pipeline(truncate_fact_first=True)
    print(result)

