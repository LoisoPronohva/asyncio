import aiohttp
import asyncio
import sys


async def test_api():
    async with aiohttp.ClientSession() as session:
        #Тест 1: Запись
        print("Тест записи")
        try:
            async with session.get("https://www.swapi.tech/api/people/1", timeout=10) as response:
                print(f"Тест 1: HTTP {response.status}")
                if response.status == 200:
                    data = await response.json()
                    print(f"Данные получены: {data.get('result', {}).get('properties', {}).get('name')}")
                else:
                    print(f"Ошибка: {response.status}")
        except Exception as e:
            print(f"Ошибка подключения: {e}")

        #Тест 2: Страница
        print("\nТест страницы")
        try:
            async with session.get("https://www.swapi.tech/api/people?page=1", timeout=10) as response:
                print(f"Тест 2: HTTP {response.status}")
                if response.status == 200:
                    data = await response.json()
                    print(f"Записей на странице: {len(data.get('results', []))}")
                else:
                    print(f"Ошибка: {response.status}")
        except Exception as e:
            print(f"Ошибка подключения: {e}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test_api())