import os
from dataclasses import dataclass

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    name: str
    user: str
    password: str


def load_config() -> DBConfig:
    load_dotenv()
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise ValueError("DB_PASSWORD missing in .env")

    return DBConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        name=os.getenv("DB_NAME", "sales_analytics"),
        user=os.getenv("DB_USER", "postgres"),
        password=password,
    )


def get_engine() -> Engine:
    cfg = load_config()
    url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.name}"
    return create_engine(url, future=True)

