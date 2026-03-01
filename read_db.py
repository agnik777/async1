import asyncio
import datetime
from sqlalchemy import select
from db import DbSession, SwapiPeople, open_orm, close_orm


async def fetch_and_print_all():
    async with DbSession() as session:
        try:
            stmt = select(SwapiPeople)
            result = await session.execute(stmt)
            swapi_people = result.scalars().all()
            for person in swapi_people:
                print(f"id={person.id}, "
                      f"uid_people={person.uid_people}, "
                      f"id_people={person.id_people}, "
                      f"name={person.name}, "
                      f"birth_year={person.birth_year}, "
                      f"gender={person.gender}, "
                      f"eye_color={person.eye_color}, "
                      f"hair_color={person.hair_color}, "
                      f"mass={person.mass}, "
                      f"skin_color={person.skin_color}, "
                      f"homeworld={person.homeworld}")
        except Exception as e:
            print(f"Ошибка чтения данных: {e}")

async def main():
    await open_orm()
    await fetch_and_print_all()
    await close_orm()

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
