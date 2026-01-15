import aiohttp
import asyncio
import aiosqlite
import logging
from typing import List, Dict, Optional, Set
import sys
import time

#Ошибки
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

API_BASE_URL = "https://www.swapi.tech/api"
MAX_CONCURRENT_REQUESTS = 10  # Семафор как в ТЗ


class ProgressTracker:

    def __init__(self):
        self.start_time = time.time()
        self.current_stage = ""
        self.stage_start_time = 0
        self.total_api_characters = 0
        self.found_characters = 0
        self.existing_in_db = 0
        self.missing_characters = 0
        self.loaded_characters = 0
        self.total_in_db = 0
        self.current_page = 0
        self.total_pages = 0

    def start_stage(self, stage_name: str):
        self.current_stage = stage_name
        self.stage_start_time = time.time()
        print(f"\n{'=' * 100}")
        print(f"{stage_name}")
        print(f"{'=' * 100}")

    def end_stage(self):
        if self.current_stage:
            duration = time.time() - self.stage_start_time
            print(f"\n{self.current_stage} завершен за {duration:.1f} сек")
            self.current_stage = ""
            self.current_page = 0
            self.total_pages = 0

    def show_progress_bar(self, stage: str, current: int, total: int, description: str = ""):
        #Прогресс-бар
        if total > 0:
            percentage = (current / total) * 100 if total > 0 else 0
            bar_length = 40
            filled = int(bar_length * current // total) if total > 0 else 0
            bar = '█' * filled + '░' * (bar_length - filled)

            stage_icons = {
                "search": "",
                "loading": "",
                "complete": "",
                "final": ""
            }

            icon = stage_icons.get(stage, "")
            stage_name = {
                "search": "ПОИСК ПЕРСОНАЖЕЙ",
                "loading": "ЗАГРУЗКА ПЕРСОНАЖЕЙ",
                "complete": "Завершение",
                "final": "Финальная статистика"
            }.get(stage, stage)

            print(f"\n{icon} ПРОГРЕСС: {stage_name}")
            print(f"[{bar}] {current}/{total} ({percentage:.1f}%)")

    def show_search_progress(self, found: int, total: int, page: int = 0):
        #Поиск
        self.show_progress_bar("search", found, total)

    def show_loading_progress(self, loaded: int, total: int):
        #Загрузка
        self.show_progress_bar("loading", loaded, total)

    def show_final_summary(self):
        #Показ
        total_time = time.time() - self.start_time

        print(f"\n{'=' * 100}")
        print("ЗАВЕРШЕНО!")
        print(f"{'=' * 100}")
        print("ФИНАЛЬНАЯ СТАТИСТИКА:")
        print(f"Общее время: {total_time:.1f} сек")
        print(f"Найдено в API: {self.total_api_characters} персонажей")
        print(f"Было в базе: {self.existing_in_db} персонажей")
        print(f"Загружено новых: {self.loaded_characters} персонажей")
        print(f"Всего в базе: {self.total_in_db} персонажей")
        print(f"{'=' * 100}")


#Прогресс
progress_tracker = ProgressTracker()


class ResourceCache:
    #Кэш

    def __init__(self):
        self.cache = {}

    async def get_name(self, session: aiohttp.ClientSession, url: str, resource_type: str) -> str:
        if not url:
            return "Unknown"

        # Проверяем кэш
        if url in self.cache:
            return self.cache[url]

        try:
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()

                    if resource_type == "planets":
                        name = data.get("result", {}).get("properties", {}).get("name", "Unknown")
                    elif resource_type == "films":
                        name = data.get("result", {}).get("properties", {}).get("title", "Unknown")
                    else:
                        name = data.get("result", {}).get("properties", {}).get("name", "Unknown")

                    #Кэш
                    self.cache[url] = name
                    return name
        except:
            pass

        return "Unknown"

    async def get_names_from_urls(self, session: aiohttp.ClientSession, urls: List[str], resource_type: str) -> str:
        if not urls:
            return ""

        tasks = [self.get_name(session, url, resource_type) for url in urls]
        names = await asyncio.gather(*tasks)

        #Пустышка
        valid_names = [name for name in names if name and name != "Unknown"]
        return ", ".join(valid_names) if valid_names else ""


async def get_all_character_ids_with_next_check(session: aiohttp.ClientSession) -> List[int]:
   #Получение персонажей
    all_ids = []
    current_url = f"{API_BASE_URL}/people?page=1"
    page_num = 1
    max_pages = 50
    max_retries = 3

    while current_url and page_num <= max_pages:
        progress_tracker.current_page = page_num

        for attempt in range(max_retries):
            try:
                async with session.get(current_url, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()

                        if "results" not in data:
                            break

                        results = data.get("results", [])

                        if not results:
                            #Прогресс
                            progress_tracker.show_search_progress(
                                len(all_ids),
                                progress_tracker.total_api_characters
                            )
                            return sorted(all_ids)

                        #ID страницы
                        page_ids = []
                        for person in results:
                            try:
                                uid = int(person["uid"])
                                if uid not in all_ids:
                                    page_ids.append(uid)
                                    all_ids.append(uid)
                            except (KeyError, ValueError):
                                continue

                        #Прогресс
                        progress_tracker.show_search_progress(
                            len(all_ids),
                            progress_tracker.total_api_characters
                        )

                        #Седующая страница
                        next_url = data.get("next")

                        if not next_url:
                            #Поиск
                            progress_tracker.show_search_progress(
                                len(all_ids),
                                progress_tracker.total_api_characters
                            )
                            return sorted(all_ids)

                        #Отличия
                        if next_url == current_url:
                            progress_tracker.show_search_progress(
                                len(all_ids),
                                progress_tracker.total_api_characters
                            )
                            return sorted(all_ids)

                        #Следующая
                        current_url = next_url
                        page_num += 1
                        break

                    elif response.status == 404:
                        progress_tracker.show_search_progress(
                            len(all_ids),
                            progress_tracker.total_api_characters
                        )
                        return sorted(all_ids)
                    else:
                        if attempt == max_retries - 1:
                            return sorted(all_ids)
                        await asyncio.sleep(2 ** attempt)

            except asyncio.TimeoutError:
                if attempt == max_retries - 1:
                    return sorted(all_ids)
                await asyncio.sleep(2 ** attempt)

            except Exception:
                if attempt == max_retries - 1:
                    return sorted(all_ids)
                await asyncio.sleep(2 ** attempt)

    progress_tracker.show_search_progress(
        len(all_ids),
        progress_tracker.total_api_characters
    )
    return sorted(all_ids)


async def check_id_range(session: aiohttp.ClientSession, start: int, end: int) -> List[int]:
    #ID проверка
    existing_ids = []
    ids_to_check = list(range(start, end + 1))

    #ID
    async def check_single_id(char_id: int) -> Optional[int]:
        try:
            url = f"{API_BASE_URL}/people/{char_id}"
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    return char_id
                return None
        except:
            return None

    #Одновременно
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def check_with_semaphore(char_id: int) -> Optional[int]:
        async with semaphore:
            return await check_single_id(char_id)

    batch_size = 20
    for i in range(0, len(ids_to_check), batch_size):
        batch = ids_to_check[i:i + batch_size]
        tasks = [check_with_semaphore(cid) for cid in batch]
        results = await asyncio.gather(*tasks)

        batch_existing = [cid for cid in results if cid is not None]
        existing_ids.extend(batch_existing)

        if i + batch_size < len(ids_to_check):
            await asyncio.sleep(1)

    return existing_ids


async def get_existing_ids_from_db() -> List[int]:
    #Персонажи из базы
    try:
        async with aiosqlite.connect("starwars_characters.db") as db:
            async with db.execute("SELECT id FROM characters") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    except:
        return []


async def get_all_available_ids(session: aiohttp.ClientSession) -> List[int]:
    all_ids = set()

    #Текст
    pagination_ids = await get_all_character_ids_with_next_check(session)
    all_ids.update(pagination_ids)

    #Диапазон
    if len(all_ids) < 30:
        range_ids = await check_id_range(session, 1, 150)
        all_ids.update(range_ids)

    #из API
    try:
        info_url = f"{API_BASE_URL}/people?page=1"
        async with session.get(info_url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                total_records = data.get("total_records", 0)

                if total_records > 0:
                    progress_tracker.total_api_characters = total_records

                    #Прогресс
                    progress_tracker.show_search_progress(0, total_records)
    except:
        pass

    result = sorted(list(all_ids))
    progress_tracker.found_characters = len(result)

    return result


async def get_missing_ids(session: aiohttp.ClientSession) -> List[int]:
    #ID персонажей
    progress_tracker.start_stage("АНАЛИЗ БАЗЫ ДАННЫХ")

    #Доступные ID из API
    api_ids = await get_all_available_ids(session)

    #Получаем ID
    db_ids = await get_existing_ids_from_db()

    #Поиск недостающего
    db_ids_set = set(db_ids)
    missing_ids = [cid for cid in api_ids if cid not in db_ids_set]

    progress_tracker.existing_in_db = len(db_ids)
    progress_tracker.missing_characters = len(missing_ids)

    progress_tracker.end_stage()
    return missing_ids


async def fetch_character_full_data(
        session: aiohttp.ClientSession,
        character_id: int,
        cache: ResourceCache,
        semaphore: asyncio.Semaphore
) -> Optional[Dict]:
    #Загрузка данных
    url = f"{API_BASE_URL}/people/{character_id}"

    # Нагрузка
    async with semaphore:
        for attempt in range(3):
            try:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()

                        if "result" not in data or "properties" not in data["result"]:
                            return None

                        props = data["result"]["properties"]

                        # Получаем название родной планеты
                        homeworld_url = props.get("homeworld")
                        homeworld_name = await cache.get_name(session, homeworld_url, "planets")

                        # Получаем названия связанных сущностей через запятую
                        films = await cache.get_names_from_urls(session, props.get("films", []), "films")
                        species = await cache.get_names_from_urls(session, props.get("species", []), "species")
                        starships = await cache.get_names_from_urls(session, props.get("starships", []), "starships")
                        vehicles = await cache.get_names_from_urls(session, props.get("vehicles", []), "vehicles")

                        character = {
                            "id": character_id,
                            "name": props.get("name", "").strip() or f"Character {character_id}",
                            "birth_year": props.get("birth_year", "").strip() or "Unknown",
                            "eye_color": props.get("eye_color", "").strip() or "Unknown",
                            "gender": props.get("gender", "").strip() or "Unknown",
                            "hair_color": props.get("hair_color", "").strip() or "Unknown",
                            "homeworld_name": homeworld_name,
                            "mass": props.get("mass", "").strip() or "Unknown",
                            "skin_color": props.get("skin_color", "").strip() or "Unknown",
                            "films": films,
                            "species": species,
                            "starships": starships,
                            "vehicles": vehicles,
                        }

                        return character

                    elif response.status == 404:
                        return None
                    else:
                        if attempt < 2:
                            await asyncio.sleep(1)
                            continue
                        return None

            except asyncio.TimeoutError:
                if attempt < 2:
                    await asyncio.sleep(2)
                    continue
                return None
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(1)
                    continue
                return None

    return None


async def save_character_full(db: aiosqlite.Connection, character: Dict) -> bool:
    #Сохранение персонажей
    try:
        await db.execute("""
            INSERT OR REPLACE INTO characters 
            (id, name, birth_year, eye_color, gender, hair_color, homeworld_name, 
             mass, skin_color, films, species, starships, vehicles)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            character["id"],
            character["name"],
            character["birth_year"],
            character["eye_color"],
            character["gender"],
            character["hair_color"],
            character["homeworld_name"],
            character["mass"],
            character["skin_color"],
            character["films"],
            character["species"],
            character["starships"],
            character["vehicles"],
        ))
        await db.commit()
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения ID {character['id']}: {e}")
        return False


async def create_table_full(db: aiosqlite.Connection):
    #Таблица
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
                films TEXT,
                species TEXT,
                starships TEXT,
                vehicles TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_name ON characters(name)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_homeworld ON characters(homeworld_name)")

        await db.commit()
    except Exception as e:
        logger.error(f"Ошибка создания таблицы: {e}")
        raise


async def load_missing_characters():
    #Недостающие
    progress_tracker.start_stage("ЗАГРУЗКА ПЕРСОНАЖЕЙ")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    #Кэш
    cache = ResourceCache()

    timeout = aiohttp.ClientTimeout(total=300, connect=30, sock_read=60)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        #Поиск
        missing_ids = await get_missing_ids(session)

        if not missing_ids:
            progress_tracker.end_stage()
            return 0, 0

        #Начало
        progress_tracker.show_loading_progress(0, len(missing_ids))

        async with aiosqlite.connect("starwars_characters.db") as db:
            await create_table_full(db)

            total_saved = 0

            batch_size = 10
            batches = [missing_ids[i:i + batch_size] for i in range(0, len(missing_ids), batch_size)]

            for batch_num, batch in enumerate(batches, 1):
                tasks = []
                for char_id in batch:
                    task = fetch_character_full_data(session, char_id, cache, semaphore)
                    tasks.append(task)

                characters = await asyncio.gather(*tasks)

                #Сохранение
                batch_saved = 0
                for character in characters:
                    if character:
                        if await save_character_full(db, character):
                            total_saved += 1
                            batch_saved += 1
                            progress_tracker.loaded_characters = total_saved

                progress_tracker.show_loading_progress(
                    total_saved,
                    len(missing_ids)
                )

                if batch_num < len(batches):
                    await asyncio.sleep(2)

            progress_tracker.end_stage()
            return total_saved, len(missing_ids)


async def show_final_report():
    #Отчетность
    progress_tracker.start_stage("ФИНАЛЬНАЯ СТАТИСТИКА")

    try:
        async with aiosqlite.connect("starwars_characters.db") as db:
            #Статистика
            async with db.execute("SELECT COUNT(*) FROM characters") as cursor:
                total = await cursor.fetchone()
                progress_tracker.total_in_db = total[0]
                print(f"\nВСЕГО ПЕРСОНАЖЕЙ В БАЗЕ: {total[0]}")

            if total[0] > 0:
                # ID
                async with db.execute("SELECT MIN(id), MAX(id) FROM characters") as cursor:
                    min_max = await cursor.fetchone()
                    print(f"Диапазон ID: {min_max[0]} - {min_max[1]}")

                # Примеры данных
                print("\nПРИМЕРЫ ДАННЫХ С НОВЫМИ ПОЛЯМИ:")
                print("-" * 100)

                async with db.execute("""
                    SELECT name, homeworld_name, films, species, starships 
                    FROM characters 
                    WHERE films != '' OR species != '' OR starships != ''
                    ORDER BY RANDOM()
                    LIMIT 3
                """) as cursor:
                    examples = await cursor.fetchall()
                    if examples:
                        for ex in examples:
                            print(f"\n{ex[0]}:")
                            print(f"Планета: {ex[1]}")
                            if ex[2]:
                                print(f"Фильмы: {ex[2]}")
                            if ex[3]:
                                print(f"Виды: {ex[3]}")
                            if ex[4]:
                                print(f"Звездолеты: {ex[4]}")
                    else:
                        print("  Нет данных в новых полях (возможно API не возвращает их)")

            # Проверка целостности данных
            print("\nПРОВЕРКА ЦЕЛОСТНОСТИ ДАННЫХ:")
            print("-" * 100)

            checks = [
                ("Персонажей в базе", "SELECT COUNT(*) FROM characters"),
                ("С уникальными именами", "SELECT COUNT(DISTINCT name) FROM characters"),
                ("С известными планетами", "SELECT COUNT(*) FROM characters WHERE homeworld_name != 'Unknown'"),
                ("Со связанными сущностями",
                 "SELECT COUNT(*) FROM characters WHERE films != '' OR species != '' OR starships != '' OR vehicles != ''"),
            ]

            for check_name, query in checks:
                async with db.execute(query) as cursor:
                    count = await cursor.fetchone()
                    print(f"  {check_name:25}: {count[0]}")

            progress_tracker.end_stage()
            progress_tracker.show_final_summary()

    except Exception as e:
        print(f"Ошибка при создании отчета: {e}")
        progress_tracker.end_stage()


async def main():

    print("=" * 100)
    print("ЗАГРУЗКА ДАННЫХ")
    print("=" * 70)

    #Загрузка
    saved, total = await load_missing_characters()

    if saved > 0:
        print(f"\nЗагружено {saved} новых персонажей из {total} недостающих")

    print("\n" + "=" * 100)

    #Результат
    await show_final_report()

    print("\n" + "=" * 100)
    print("ПРОГРАММА ЗАВЕРШЕНА!")
    print("=" * 100)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
        progress_tracker.end_stage()
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        progress_tracker.end_stage()