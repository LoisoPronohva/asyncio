import aiohttp
import asyncio
import aiosqlite
import logging
from typing import List, Dict, Optional
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

API_URL = "https://www.swapi.tech/api/people"
MAX_CONCURRENT_REQUESTS = 3


async def test_api_availability() -> bool:
    #Проверка API
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"{API_URL}/1", timeout=5) as response:
                return response.status == 200
    except:
        return False


async def load_from_local_backup(db: aiosqlite.Connection) -> int:
    #Брать из local_data
    logger.info("Используются локальные данные")

    try:
        #Импорт
        from local_data import get_local_characters

        characters = get_local_characters(20)
        saved_count = 0

        for character in characters:
            try:
                await db.execute("""
                    INSERT OR REPLACE INTO characters 
                    (id, name, birth_year, eye_color, gender, hair_color, homeworld_name, mass, skin_color)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    character["id"],
                    character["name"],
                    character["birth_year"],
                    character["eye_color"],
                    character["gender"],
                    character["hair_color"],
                    character["homeworld_name"],
                    character["mass"],
                    character["skin_color"]
                ))
                saved_count += 1
                logger.debug(f"Локально: {character['name']}")
            except Exception as e:
                logger.error(f"Ошибка локального сохранения: {e}")

        await db.commit()
        logger.info(f"📊 Локально загружено: {saved_count} персонажей")
        return saved_count

    except ImportError:
        logger.error("❌ Не найден файл local_data.py")
        return 0
    except Exception as e:
        logger.error(f"❌ Ошибка локальной загрузки: {e}")
        return 0


async def fetch_homeworld_name(session: aiohttp.ClientSession, url: str) -> str:
    """Получаем название планеты по URL"""
    if not url or url == "Unknown":
        return "Unknown"

    try:
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("result", {}).get("properties", {}).get("name", "Unknown")
    except:
        pass
    return "Unknown"


async def get_character_ids_from_api(session: aiohttp.ClientSession) -> List[int]:
    #Получение ID по API
    logger.info("Попытка получить ID из API")

    #Получение через пагинацию
    try:
        url = f"{API_URL}?page=1&limit=100"
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()

                if "results" in data and data["results"]:
                    ids = []
                    for person in data["results"]:
                        try:
                            ids.append(int(person["uid"]))
                        except:
                            continue

                    logger.info(f"API вернуло {len(ids)} ID")
                    return ids
    except:
        pass

    #Пагинация не сработала
    logger.info("Проверка диапазона")

    #Первые 20
    key_ids = list(range(1, 21))

    async def check_id(char_id: int) -> Optional[int]:
        try:
            url = f"{API_URL}/{char_id}"
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    return char_id
        except:
            pass
        return None


    tasks = [check_id(cid) for cid in key_ids]
    results = await asyncio.gather(*tasks)

    found_ids = [cid for cid in results if cid is not None]
    logger.info(f"📊 Найдено {len(found_ids)} ID через проверку")

    return found_ids


async def fetch_character_data(
        session: aiohttp.ClientSession,
        character_id: int
) -> Optional[Dict]:
    #Загрузка данных
    url = f"{API_URL}/{character_id}"

    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()

                if "result" not in data or "properties" not in data["result"]:
                    return None

                props = data["result"]["properties"]

                #Планета
                homeworld_url = props.get("homeworld")
                homeworld_name = "Unknown"

                if homeworld_url:
                    homeworld_name = await fetch_homeworld_name(session, homeworld_url)

                character = {
                    "id": character_id,
                    "name": props.get("name", "").strip() or f"Персонаж {character_id}",
                    "birth_year": props.get("birth_year", "").strip() or "Unknown",
                    "eye_color": props.get("eye_color", "").strip() or "Unknown",
                    "gender": props.get("gender", "").strip() or "Unknown",
                    "hair_color": props.get("hair_color", "").strip() or "Unknown",
                    "homeworld_name": homeworld_name,
                    "mass": props.get("mass", "").strip() or "Unknown",
                    "skin_color": props.get("skin_color", "").strip() or "Unknown",
                }

                return character
    except:
        pass

    return None


async def save_character(db: aiosqlite.Connection, character: Dict) -> bool:
    #Сохранение в БД
    try:
        await db.execute("""
            INSERT OR REPLACE INTO characters 
            (id, name, birth_year, eye_color, gender, hair_color, homeworld_name, mass, skin_color)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            character["id"],
            character["name"],
            character["birth_year"],
            character["eye_color"],
            character["gender"],
            character["hair_color"],
            character["homeworld_name"],
            character["mass"],
            character["skin_color"]
        ))
        await db.commit()
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения ID {character.get('id')}: {e}")
        return False


async def create_table(db: aiosqlite.Connection):
    #Создание таблицы, если нет
    try:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS characters (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                birth_year TEXT,
                eye_color TEXT,
                gender TEXT,
                hair_color TEXT,
                homeworld_name TEXT,
                mass TEXT,
                skin_color TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_name ON characters(name)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_homeworld ON characters(homeworld_name)")

        await db.commit()
        logger.info("Таблица создана/проверена")
    except Exception as e:
        logger.error(f"Ошибка создания таблицы: {e}")
        raise


async def load_from_api():
    #Загружает API
    logger.info("Загрузка API")

    timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_read=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Получаем ID
        character_ids = await get_character_ids_from_api(session)

        if not character_ids:
            logger.warning("Ошибка ID из API")
            return 0

        logger.info(f"Будет загружено: {len(character_ids)} персонажей")

        async with aiosqlite.connect("starwars_characters.db") as db:
            await create_table(db)

            total_saved = 0

            #Группы для стабильности
            group_size = 5
            groups = [character_ids[i:i + group_size] for i in range(0, len(character_ids), group_size)]

            for i, group in enumerate(groups, 1):
                logger.info(f"Группа {i}/{len(groups)}: {len(group)} персонажей")

                #Загрузка персонажей
                tasks = []
                for char_id in group:
                    task = fetch_character_data(session, char_id)
                    tasks.append(task)

                characters = await asyncio.gather(*tasks)

                #Сохранение
                for character in characters:
                    if character:
                        if await save_character(db, character):
                            total_saved += 1

                logger.info(f"Прогресс: {total_saved}/{len(character_ids)} сохранено")

                #Пауза
                if i < len(groups):
                    await asyncio.sleep(2)

            return total_saved


async def main():

    print("=" * 70)
    print("ЗАГРУЗКА ДАННЫХ")
    print("=" * 70)

    #Проверяка API
    logger.info("Проверка  API")
    api_available = await test_api_availability()

    if not api_available:
        logger.warning("API недоступно, локальные данные")

        #Создаем
        async with aiosqlite.connect("starwars_characters.db") as db:
            await create_table(db)

        #Загружаем локально
        async with aiosqlite.connect("starwars_characters.db") as db:
            saved = await load_from_local_backup(db)

            if saved > 0:
                logger.info(f"Локальная загрузка завершена: {saved} персонажей")
            else:
                logger.error("Не удалось загрузить данные")

        return

    #Загрузка из API
    logger.info("Загружаем из API")

    saved_count = await load_from_api()

    if saved_count == 0:
        logger.warning("Не удалось загрузить из API,  локальные данные")

        async with aiosqlite.connect("starwars_characters.db") as db:
            saved = await load_from_local_backup(db)

            if saved > 0:
                logger.info(f"Локальная загрузка завершена: {saved} персонажей")
            else:
                logger.error("Не удалось загрузить данные")
    else:
        logger.info(f"Загрузка из API завершена: {saved_count} персонажей")


async def show_summary():
    #Промежуточное отображение
    try:
        async with aiosqlite.connect("starwars_characters.db") as db:
            async with db.execute("SELECT COUNT(*) FROM characters") as cursor:
                total = await cursor.fetchone()
                print(f"\nВСЕГО ПЕРСОНАЖЕЙ В БАЗЕ: {total[0]}")

            if total[0] > 0:
                print("\nПЕРВЫЕ 10 ПЕРСОНАЖЕЙ:")
                print("-" * 80)

                async with db.execute("""
                    SELECT id, name, homeworld_name, birth_year, gender 
                    FROM characters 
                    ORDER BY id 
                    LIMIT 10
                """) as cursor:
                    chars = await cursor.fetchall()
                    for char in chars:
                        print(
                            f"  ID: {char[0]:3} | {char[1]:25} | Планета: {char[2]:15} | Род.: {char[3]:10} | Пол: {char[4]}")

                #Проверяем
                print("\nПРОВЕРКА ВЫПОЛНЕНИЯ ТРЕБОВАНИЙ:")
                print("-" * 50)

                checks = [
                    ("Таблица создана", "SELECT 1 FROM sqlite_master WHERE type='table' AND name='characters'"),
                    ("Есть данные", "SELECT COUNT(*) > 0 FROM characters"),
                    ("Поле homeworld_name не содержит ссылок",
                     "SELECT COUNT(*) = 0 FROM characters WHERE homeworld_name LIKE 'http%'"),
                    ("Все обязательные поля присутствуют",
                     "SELECT COUNT(*) = 9 FROM pragma_table_info('characters') WHERE name IN ('id', 'name', 'birth_year', 'eye_color', 'gender', 'hair_color', 'homeworld_name', 'mass', 'skin_color')")
                ]

                all_passed = True
                for check_name, query in checks:
                    async with db.execute(query) as cursor:
                        result = await cursor.fetchone()
                        passed = result[0] == 1 if isinstance(result[0], int) else bool(result[0])

                        if passed:
                            print(f"{check_name}")
                        else:
                            print(f"{check_name}")
                            all_passed = False

                if all_passed:
                    print(f"\nТРЕБОВАНИЯ ЗАДАЧИ ВЫПОЛНЕНЫ!")
                else:
                    print(f"\nНе все требования выполнены")

    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:

        asyncio.run(main())

        print("\n" + "=" * 70)
        print("ИТОГИ ЗАГРУЗКИ")
        print("=" * 70)

        asyncio.run(show_summary())

        print("\n" + "=" * 70)
        print("ПРОГРАММА РАБОТАЕТ")
        print("=" * 70)

    except KeyboardInterrupt:
        print("\nПрервано")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")