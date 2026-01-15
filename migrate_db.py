import asyncio
import aiosqlite
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


async def create_database():
    #Создание таблицы
    try:
        async with aiosqlite.connect("starwars_characters.db") as db:
            #Удаление таблицы
            await db.execute("DROP TABLE IF EXISTS characters")

            #Создание таблицы
            await db.execute("""
                CREATE TABLE characters (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    birth_year TEXT,
                    eye_color TEXT,
                    gender TEXT,
                    hair_color TEXT,
                    homeworld_name TEXT,
                    mass TEXT,
                    skin_color TEXT,
                    -- НОВЫЕ ПОЛЯ: связанные сущности как строки через запятую
                    films TEXT,
                    species TEXT,
                    starships TEXT,
                    vehicles TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            #Оптимизация
            await db.execute("""
                CREATE INDEX idx_characters_name 
                ON characters(name)
            """)

            await db.execute("""
                CREATE INDEX idx_characters_homeworld 
                ON characters(homeworld_name)
            """)

            await db.commit()

            print("=" * 100)
            print("БАЗА ДАННЫХ СОЗДАНА")
            print("=" * 100)

            # Показываем полную структуру
            print("\nПОЛНАЯ СТРУКТУРА ТАБЛИЦЫ 'characters':")
            print("=" * 100)

            async with db.execute("PRAGMA table_info(characters)") as cursor:
                columns = await cursor.fetchall()

                required_fields = [
                    ('id', 'ID персонажа'),
                    ('name', 'Имя'),
                    ('birth_year', 'Год рождения'),
                    ('eye_color', 'Цвет глаз'),
                    ('gender', 'Пол'),
                    ('hair_color', 'Цвет волос'),
                    ('homeworld_name', 'Название родной планеты'),
                    ('mass', 'Масса'),
                    ('skin_color', 'Цвет кожи'),
                    ('films', 'Фильмы (через запятую)'),
                    ('species', 'Виды (через запятую)'),
                    ('starships', 'Звездолеты (через запятую)'),
                    ('vehicles', 'Транспорт (через запятую)'),
                ]

                for col in columns:
                    field_name = col[1]
                    field_type = col[2]
                    is_nullable = "NOT NULL" if col[3] == 1 else "NULL"
                    is_primary = "PRIMARY KEY" if col[5] == 1 else ""

                    print(f"  {field_name:20} {field_type:15} {is_nullable:10} {is_primary}")

            print("\nВСЕ ПОЛЯ ДОБАВЛЕНЫ:")
            print("=" * 100)
            for field, description in required_fields:
                print(f"{field:20} - {description}")

    except Exception as e:
        logger.error(f"Ошибка при создании базы данных: {e}")
        raise


async def check_database():
    #База
    try:
        async with aiosqlite.connect("starwars_characters.db") as db:
            print("=" * 100)
            print("🔍 ПРОВЕРКА СОСТОЯНИЯ БАЗЫ ДАННЫХ")
            print("=" * 100)

            # Проверка таблицы
            async with db.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='characters'
            """) as cursor:
                exists = await cursor.fetchone()

                if not exists:
                    print("Таблица 'characters' не существует!")
                    return

            #Таблица
            async with db.execute("PRAGMA table_info(characters)") as cursor:
                columns = await cursor.fetchall()
                print(f"\nТаблица содержит {len(columns)} полей")

            #Записи
            async with db.execute("SELECT COUNT(*) FROM characters") as cursor:
                count = await cursor.fetchone()
                print(f"Записей в таблице: {count[0]}")

            if count[0] > 0:
                #Новые
                print("\n🔍 ПРОВЕРКА НОВЫХ ПОЛЕЙ:")
                print("=" * 100)

                new_fields = ['films', 'species', 'starships', 'vehicles']
                for field in new_fields:
                    async with db.execute(f"""
                        SELECT COUNT(*) FROM characters 
                        WHERE {field} IS NOT NULL AND {field} != ''
                    """) as cursor:
                        has_data = await cursor.fetchone()
                        print(f"  {field:15}: {has_data[0]} записей с данными")

    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    import argparse

    parser = argparse.ArgumentParser(description='Управление базой данных Star Wars')
    parser.add_argument('--create', action='store_true', help='Создать/пересоздать таблицу')
    parser.add_argument('--check', action='store_true', help='Проверить состояние базы')

    args = parser.parse_args()

    try:
        if args.check:
            asyncio.run(check_database())
        else:
            asyncio.run(create_database())

    except KeyboardInterrupt:
        print("\nОперация прервана")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")