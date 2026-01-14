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
    """Создает базу данных и таблицу"""
    try:
        async with aiosqlite.connect("starwars_characters.db") as db:
            # Удаляем старую таблицу если существует
            await db.execute("DROP TABLE IF EXISTS characters")

            # Создаем новую таблицу с ВСЕМИ необходимыми полями
            await db.execute("""
                CREATE TABLE characters (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    birth_year TEXT,
                    eye_color TEXT,
                    gender TEXT,
                    hair_color TEXT,
                    homeworld_name TEXT,  -- НАЗВАНИЕ планеты, не ссылка!
                    mass TEXT,
                    skin_color TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Индексы для оптимизации
            await db.execute("""
                CREATE INDEX idx_characters_name 
                ON characters(name)
            """)

            await db.execute("""
                CREATE INDEX idx_characters_homeworld 
                ON characters(homeworld_name)
            """)

            await db.commit()

            print("=" * 60)
            print("✅ БАЗА ДАННЫХ УСПЕШНО СОЗДАНА")
            print("=" * 60)
            print(f"📁 Файл: starwars_characters.db")
            print(f"📊 Таблица: characters")

            # Показываем структуру
            print("\n📋 СТРУКТУРА ТАБЛИЦЫ:")
            print("-" * 50)

            async with db.execute("PRAGMA table_info(characters)") as cursor:
                columns = await cursor.fetchall()

                # Проверяем обязательные поля
                required_fields = [
                    ('id', 'INTEGER', 'PRIMARY KEY'),
                    ('name', 'TEXT', 'NOT NULL'),
                    ('birth_year', 'TEXT', ''),
                    ('eye_color', 'TEXT', ''),
                    ('gender', 'TEXT', ''),
                    ('hair_color', 'TEXT', ''),
                    ('homeworld_name', 'TEXT', ''),  # Важно для задачи!
                    ('mass', 'TEXT', ''),
                    ('skin_color', 'TEXT', '')
                ]

                for col in columns:
                    field_name = col[1]
                    field_type = col[2]
                    is_nullable = "NOT NULL" if col[3] == 1 else "NULL"
                    is_primary = "PRIMARY KEY" if col[5] == 1 else ""

                    print(f"  {field_name:20} {field_type:10} {is_nullable:10} {is_primary}")

                # Проверка соответствия требованиям
                print("\n✅ ПРОВЕРКА ТРЕБОВАНИЙ ЗАДАЧИ:")
                print("-" * 50)

                actual_fields = {col[1]: col[2] for col in columns}

                requirements = [
                    ("id", "ID персонажа"),
                    ("birth_year", "Год рождения"),
                    ("eye_color", "Цвет глаз"),
                    ("gender", "Пол"),
                    ("hair_color", "Цвет волос"),
                    ("homeworld_name", "Название родной планеты"),  # Не ссылка!
                    ("mass", "Масса"),
                    ("name", "Имя"),
                    ("skin_color", "Цвет кожи")
                ]

                all_good = True
                for field, description in requirements:
                    if field in actual_fields:
                        print(f"  ✅ {field:20} - {description}")
                    else:
                        print(f"  ❌ {field:20} - ОТСУТСТВУЕТ: {description}")
                        all_good = False

                if all_good:
                    print(f"\n🎉 Все требования задачи выполнены!")
                else:
                    print(f"\n⚠️  Не все требования выполнены!")

            print("\n" + "=" * 60)
            print("🚀 БАЗА ГОТОВА К ЗАГРУЗКЕ ДАННЫХ")
            print("=" * 60)

    except Exception as e:
        logger.error(f"❌ Ошибка при создании базы данных: {e}")
        raise


async def check_database():
    """Проверяет состояние базы данных"""
    try:
        async with aiosqlite.connect("starwars_characters.db") as db:
            print("=" * 60)
            print("🔍 ПРОВЕРКА СОСТОЯНИЯ БАЗЫ ДАННЫХ")
            print("=" * 60)

            # Проверяем существует ли таблица
            async with db.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='characters'
            """) as cursor:
                exists = await cursor.fetchone()

                if not exists:
                    print("❌ Таблица 'characters' не существует!")
                    print("\n💡 Совет: запустите команду:")
                    print("    python migrate_db.py --create")
                    return

            # Количество записей
            async with db.execute("SELECT COUNT(*) FROM characters") as cursor:
                count = await cursor.fetchone()
                print(f"\n📊 Записей в таблице: {count[0]}")

            if count[0] > 0:
                # Пример данных
                print("\n👥 ПРИМЕР ДАННЫХ (первые 5 записей):")
                print("-" * 70)

                async with db.execute("""
                    SELECT id, name, homeworld_name 
                    FROM characters 
                    ORDER BY id 
                    LIMIT 5
                """) as cursor:
                    rows = await cursor.fetchall()
                    for row in rows:
                        print(f"  ID: {row[0]:3} | Имя: {row[1]:25} | Планета: {row[2]}")

                # Проверяем что homeworld_name содержит названия, а не ссылки
                print("\n🔍 ПРОВЕРКА ПОЛЯ homeworld_name:")
                print("-" * 50)

                async with db.execute("""
                    SELECT homeworld_name 
                    FROM characters 
                    WHERE homeworld_name LIKE 'http%' 
                    LIMIT 3
                """) as cursor:
                    url_results = await cursor.fetchall()

                    if url_results:
                        print("⚠️  Обнаружены ссылки в поле homeworld_name!")
                        for result in url_results:
                            print(f"  ❌ {result[0]}")
                        print("\n💡 Проблема: в поле должны быть названия планет, а не ссылки!")
                    else:
                        print("✅ В поле homeworld_name нет ссылок - только названия планет")

                # Статистика по планетам
                print("\n🌍 СТАТИСТИКА ПО ПЛАНЕТАМ:")
                print("-" * 50)

                async with db.execute("""
                    SELECT homeworld_name, COUNT(*) as count 
                    FROM characters 
                    WHERE homeworld_name != 'Unknown'
                    GROUP BY homeworld_name 
                    ORDER BY count DESC 
                    LIMIT 3
                """) as cursor:
                    planets = await cursor.fetchall()
                    if planets:
                        for planet in planets:
                            print(f"  {planet[0]}: {planet[1]} персонажей")
                    else:
                        print("  Нет данных о планетах")
            else:
                print("\n📭 Таблица пуста")
                print("\n💡 Совет: запустите загрузку данных:")
                print("    python load_data.py")

            print("\n" + "=" * 60)
            print("✅ ПРОВЕРКА ЗАВЕРШЕНА")
            print("=" * 60)

    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    import argparse

    parser = argparse.ArgumentParser(
        description='Управление базой данных Star Wars',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python migrate_db.py                     # Создать/пересоздать таблицу
  python migrate_db.py --create           # Создать таблицу
  python migrate_db.py --check            # Проверить состояние базы
        """
    )

    parser.add_argument('--create', action='store_true', help='Создать/пересоздать таблицу')
    parser.add_argument('--check', action='store_true', help='Проверить состояние базы')

    args = parser.parse_args()

    try:
        if args.check:
            asyncio.run(check_database())
        else:
            # По умолчанию создаем таблицу
            asyncio.run(create_database())

    except KeyboardInterrupt:
        print("\n⏹️  Операция прервана пользователем")
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")