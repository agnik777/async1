import os
from sqlalchemy.ext.asyncio import (create_async_engine, async_sessionmaker,
                                    AsyncAttrs)
from sqlalchemy.orm import DeclarativeBase, MappedColumn, mapped_column
from sqlalchemy import String, Integer


POSTGRES_USER = os.getenv("POSTGRES_USER", "agnik")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "12345")
POSTGRES_DB = os.getenv("POSTGRES_DB", "swapi_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5431")

PG_DSN = (f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
          f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

engine = create_async_engine(PG_DSN)
DbSession = async_sessionmaker(bind=engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass


class SwapiPeople(Base):
    __tablename__ = "swapi_people"
    id: MappedColumn[int] = mapped_column(Integer, primary_key=True)
    uid_people: MappedColumn[str] = mapped_column(String)
    id_people: MappedColumn[str] = mapped_column(String)
    name: MappedColumn[str] = mapped_column(String)
    birth_year: MappedColumn[str] = mapped_column(String)
    gender: MappedColumn[str] = mapped_column(String)
    eye_color: MappedColumn[str] = mapped_column(String)
    hair_color: MappedColumn[str] = mapped_column(String)
    mass: MappedColumn[str] = mapped_column(String)
    skin_color: MappedColumn[str] = mapped_column(String)
    homeworld:MappedColumn[str] = mapped_column(String)


async def open_orm():
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

async def close_orm():
    await engine.dispose()
