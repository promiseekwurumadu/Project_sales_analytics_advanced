
import pandas as pd

RAW_XLSX_PATH = "data/Online Retail.xlsx"


def extract_and_clean() -> pd.DataFrame:
    df = pd.read_excel(RAW_XLSX_PATH)

    df = df.dropna(subset=["CustomerID"]).copy()
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)].copy()

    df["Revenue"] = df["Quantity"] * df["UnitPrice"]
    df["CustomerID"] = df["CustomerID"].astype(int).astype(str)

    # standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # normalize text fields to prevent join mismatches
    df["stockcode"] = df["stockcode"].astype(str).str.strip()
    df["description"] = df["description"].fillna("").astype(str).str.strip()
    df["country"] = df["country"].astype(str).str.strip()

    return df
