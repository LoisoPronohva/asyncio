import aiohttp
import asyncio
import aiosqlite
import logging
from typing import List, Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

API_URL = "https://www.swapi.tech/api/people"


async def create_table(db: aiosqlite.Connection):

    await db.execute("""
        CREATE TABLE IF NOT EXISTS characters (
            id INTEGER PRIMARY KEY,
            name TEXT,
            birth_year TEXT,
            eye_color TEXT,
            gender TEXT,
            hair_color TEXT,
            homeworld TEXT,
            mass TEXT,
            skin_color TEXT
        )
    """)
    await db.commit()


async def get_character_ids(session: aiohttp.ClientSession) -> List[int]:
    all_ids = []
    page = 1
    max_retries = 3

    while True:
        try:
            url = f"{API_URL}?page={page}&limit=10"
            for attempt in range(max_retries):
                try:
                    async with session.get(url, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "results" not in data:
                                logger.info(f"Нет результатов на странице {page}")
                                break

                            results = data["results"]
                            logger.info(f"Загружена страница {page}, найдено {len(results)} персонажей.")

                            for person in results:
                                uid = int(person["uid"])
                                if uid not in all_ids:
                                    all_ids.append(uid)

                            if not data.get("next"):
                                logger.info(f"Всего найдено {len(all_ids)} персонажей.")
                                return all_ids

                            page += 1
                            break

                        else:
                            logger.warning(
                                f"Страница {page}: HTTP {response.status}. Попытка {attempt + 1}/{max_retries}")
                            if attempt == max_retries - 1:
                                logger.error(f"Ошибка на странице {page} после {max_retries} попыток.")
                                return all_ids

                except Exception as e:
                    logger.warning(f"Попытка {attempt + 1}/{max_retries} для страницы {page} не удалась: {e}")
                    if attempt == max_retries - 1:
                        logger.error(f"Превышено число попыток для страницы {page}.")
                        return all_ids
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Критическая ошибка на странице {page}: {e}")
            return all_ids


async def fetch_character(session: aiohttp.ClientSession, character_id: int) -> Optional[Dict]:
    url = f"{API_URL}/{character_id}"
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()

                #Структура ответа
                if "result" not in data or "properties" not in data["result"]:
                    logger.warning(f"Неверный формат данных для персонажа {character_id}")
                    return None

                result = data["result"]["properties"]
                return {
                    "id": int(character_id),
                    "birth_year": result.get("birth_year"),
                    "eye_color": result.get("eye_color"),
                    "gender": result.get("gender"),
                    "hair_color": result.get("hair_color"),
                    "homeworld": result.get("homeworld"),
                    "mass": result.get("mass"),
                    "name": result.get("name"),
                    "skin_color": result.get("skin_color"),
                }
            else:
                logger.warning(f"Персонаж {character_id}: HTTP {response.status}")
                return None
    except Exception as e:
        logger.error(f"Ошибка загрузки персонажа {character_id}: {e}")
        return None


async def save_to_db(db: aiosqlite.Connection, character: Dict):
    #Save
    try:
        await db.execute("""
            INSERT OR REPLACE INTO characters 
            (id, name, birth_year, eye_color, gender, hair_color, homeworld, mass, skin_color)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            character["id"],
            character["name"],
            character["birth_year"],
            character["eye_color"],
            character["gender"],
            character["hair_color"],
            character["homeworld"],
            character["mass"],
            character["skin_color"]
        ))
        await db.commit()
    except Exception as e:
        logger.error(f"Ошибка сохранения персонажа {character.get('id')}: {e}")

async def load_all_characters():
    async with aiohttp.ClientSession() as session:
        character_ids = await get_character_ids(session)
        if not character_ids:
            logger.error("Не удалось получить список персонажей.")
            return

        logger.info(f"Найдено {len(character_ids)} персонажей. Начинаю загрузку...")

        async with aiosqlite.connect("sw_characters.db") as db:
            #Таблица
            await create_table(db)

            tasks = [fetch_character(session, uid) for uid in character_ids]
            results = await asyncio.gather(*tasks)

            #Соханение
            saved_count = 0
            for character in results:
                if character:  # Пропускаем None результаты
                    await save_to_db(db, character)
                    saved_count += 1

            logger.info(f"Сохранено {saved_count} персонажей из {len(results)} загруженных.")


async def check_db():

    try:
        async with aiosqlite.connect("sw_characters.db") as db:
            #Записи
            async with db.execute("SELECT COUNT(*) FROM characters") as cursor:
                count = await cursor.fetchone()
                logger.info(f"Всего персонажей в базе данных: {count[0]}")

            #Провека записей
            async with db.execute("SELECT id, name FROM characters LIMIT 5") as cursor:
                rows = await cursor.fetchall()
                logger.info("Главные персонажи:")
                for row in rows:
                    logger.info(f"  ID: {row[0]}, Имя: {row[1]}")
    except Exception as e:
        logger.error(f"Ошибка при проверке базы данных: {e}")


if __name__ == "__main__":
    try:
        #Данные
        asyncio.run(load_all_characters())
        #Результат
        asyncio.run(check_db())
    except KeyboardInterrupt:
        logger.info("Скрипт прерван пользователем.")
    except Exception as e:
        logger.critical(f"Непредвиденная ошибка: {e}")