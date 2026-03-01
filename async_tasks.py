import asyncio
import aiohttp
import datetime
import requests
from itertools import batched
from db import DbSession, SwapiPeople, open_orm, close_orm


start = datetime.datetime.now()
NUMBER_REQUESTS = 90
MAX_ASYNC_REQUESTS = 20

async def get_people(person_id: int, http_session: aiohttp.ClientSession):
    url = f'https://www.swapi.tech/api/people/{person_id}/'
    response = await http_session.get(url)
    json_data = await response.json()
    return json_data

async def insert_results(results: list[dict]):
    async with DbSession() as session:
        person_results = [
            person["result"] for person in results
            if person.get("result") is not None
        ]
        try:
            peoples_list =[]
            for result in person_results:
                properties = result["properties"]
                people = SwapiPeople(
                    uid_people=result['uid'],
                    id_people=result['_id'],
                    name=properties['name'],
                    birth_year=properties['birth_year'],
                    gender=properties['gender'],
                    eye_color=properties['eye_color'],
                    hair_color=properties['hair_color'],
                    mass=properties['mass'],
                    skin_color=properties['skin_color'],
                    homeworld=properties['homeworld']
                )
                peoples_list.append(people)
            session.add_all(peoples_list)
            await session.commit()
        except Exception as e:
            print(f"Ошибка вставки результатов: {e}")
            raise

async def main():
    await open_orm()
    async with aiohttp.ClientSession() as http_session:
        for batch in batched(range(1, NUMBER_REQUESTS + 1), MAX_ASYNC_REQUESTS):
            coros = [get_people(i, http_session) for i in batch]
            results = await asyncio.gather(*coros)
            insert_results_task = asyncio.create_task(insert_results(results))
        tasks = asyncio.all_tasks()
        current_task = asyncio.current_task()
        tasks.remove(current_task)
        for task in tasks:
            await task
    await close_orm()

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
