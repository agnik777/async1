import sys
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
    try:
        async with http_session.get(f'{URL_API}people/') as response:
            if response.status == 200:
                data = await response.json()
                return data['total_records']
            else:
                print(f'Ошибка: {response.status}')
                sys.exit(1)
    except Exception as e:
        print(f'Ошибка получения данных: {e}')
        sys.exit(1)

async def get_peoples_uid(http_session: aiohttp.ClientSession,
                          total_records: int):
    url = f'{URL_API}people?page=1&limit={total_records}'
    try:
        async with http_session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                all_peoples = data['results']
                all_peoples_uid = [int(person['uid']) for person in all_peoples]
                return all_peoples_uid
            else:
                print(f'Ошибка: {response.status}')
                sys.exit(1)
    except Exception as e:
        print(f'Ошибка получения данных: {e}')
        sys.exit(1)

async def get_planet_name(person_result: dict,
                          http_session: aiohttp.ClientSession):
    if person_result is None:
        return None
    url = person_result['result']['properties']['homeworld']
    try:
        async with (http_session.get(url) as response):
            if response.status == 200:
                json_data = await response.json()
                planet_name = json_data['result']['properties']['name']
                person_result['result']['properties']['homeworld'] = planet_name
            return person_result
    except Exception as e:
        print(f'Ошибка получения данных: {e}')
        return None

async def get_people(person_id: int, http_session: aiohttp.ClientSession):
    url = f'{URL_API}people/{person_id}/'
    try:
        async with http_session.get(url) as response:
            if response.status == 200:
                json_data = await response.json()
                return json_data
            else:
                print(f'Ошибка: {response.status}')
                return None
    except Exception as e:
        print(f'Ошибка получения данных: {e}')
        return None

async def insert_results(results: list[dict]):
    if not results:
        return
    async with DbSession() as session:
        peoples_list =[]
        for person in results:
            if person is None:
                continue
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
            print(f"Персонажи уже добавлены в базу данных")

async def process_batch(batch: list, http_session: aiohttp.ClientSession):
    coros = [get_people(i, http_session) for i in batch]
    results = await asyncio.gather(*coros)
    person_coros = [get_planet_name(person_result, http_session)
                    for person_result in results]
    person_results = await asyncio.gather(*person_coros)
    await insert_results(person_results)

async def main():
    await open_orm()
    async with aiohttp.ClientSession() as http_session:
        number_requests = await get_total_count(http_session)
        all_uid = await get_peoples_uid(http_session, int(number_requests))
        print(f"Total uids: {len(all_uid)}", all_uid)
        tasks = [asyncio.create_task(process_batch(batch, http_session))
                 for batch in batched(all_uid, MAX_ASYNC_REQUESTS)]
        await asyncio.gather(*tasks)
    await close_orm()

if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
