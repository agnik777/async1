import asyncio
import aiohttp
import datetime

from sqlalchemy.exc import IntegrityError
from db import DbSession, SwapiPeople, open_orm, close_orm
from itertools import islice


MAX_ASYNC_REQUESTS = 5
URL_API = "https://www.swapi.tech/api/"

def batched(iterable, n):
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch

async def get_total_count(http_session: aiohttp.ClientSession):
    async with http_session.get(f'{URL_API}people/') as response:
        if response.status == 200:
            data = await response.json()
            return data['total_records']

async def get_peoples_uid(http_session: aiohttp.ClientSession, total_records: int):
    async with http_session.get(f'{URL_API}people?page=1&limit={total_records}') as response:
        if response.status == 200:
            data = await response.json()
            all_peoples = data['results']
            all_peoples_uid = []
            for person in all_peoples:
                all_peoples_uid.append(int(person['uid']))
            return all_peoples_uid

async def get_planet_name(person_result: dict, http_session: aiohttp.ClientSession):
    url = person_result['result']['properties']['homeworld']
    async with http_session.get(url) as response:
        if response.status == 200:
            json_data = await response.json()
            person_result['result']['properties']['homeworld'] = json_data['result']['properties']['name']
        return person_result

async def get_people(person_id: int, http_session: aiohttp.ClientSession):
    url = f'{URL_API}people/{person_id}/'
    async with http_session.get(url) as response:
        if response.status == 200:
            json_data = await response.json()
            return json_data

async def insert_results(results: list[dict]):
    async with DbSession() as session:
        peoples_list =[]
        for person in results:
            result = person["result"]
            properties = result["properties"]
            people = SwapiPeople(
                person_uid=result['uid'],
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
        try:
            session.add_all(peoples_list)
            await session.commit()
        except IntegrityError:
            print(f"Persons have already been added")

async def process_batch(batch: list, http_session: aiohttp.ClientSession):
    coros = [get_people(i, http_session) for i in batch]
    results = await asyncio.gather(*coros)
    person_coros = [get_planet_name(person_result, http_session) for person_result in results]
    person_results = await asyncio.gather(*person_coros)
    await insert_results(person_results)

async def main():
    await open_orm()
    async with aiohttp.ClientSession() as http_session:
        number_requests = await get_total_count(http_session)
        all_uid = await get_peoples_uid(http_session, int(number_requests))
        print(f"Total uids: {len(all_uid)}", all_uid)
        tasks = [asyncio.create_task(process_batch(batch, http_session)) for batch in batched(all_uid, MAX_ASYNC_REQUESTS)]
        await asyncio.gather(*tasks)
    await close_orm()

if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
