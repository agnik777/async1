import asyncio
from db import open_orm, close_orm


async def migrate():
    await open_orm()
    await close_orm()
    print("Migration complete.")

if __name__ == "__main__":
    asyncio.run(migrate())