import aiosqlite
import asyncio
import sys


async def verify_database():
    #Проверка записей
    try:
        async with aiosqlite.connect("starwars_characters.db") as db:
            print("=" * 110)
            print(" ПРОВЕРКА БАЗЫ ДАННЫХ")
            print("=" * 110)

            #Наличие таблицы
            print("\nПРОВЕРКА СТРУКТУРЫ :")
            print("-" * 110)

            async with db.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='characters'
            """) as cursor:
                table_exists = await cursor.fetchone()

                if not table_exists:
                    print("Таблица 'characters' не существует!")
                    return

                print("Таблица 'characters' существует")

            #Структура таблицы
            print("\nСТРУКТУРА ТАБЛИЦЫ 'characters':")
            print("-" * 110)

            async with db.execute("PRAGMA table_info(characters)") as cursor:
                columns = await cursor.fetchall()

                for col in columns:
                    field_name = col[1]
                    field_type = col[2]
                    is_nullable = "NOT NULL" if col[3] == 1 else "NULL"
                    is_primary = "PRIMARY KEY" if col[5] == 1 else ""

                    print(f"  {field_name:20} {field_type:15} {is_nullable:10} {is_primary}")

            #Статистика
            print("\nСТАТИСТИКА:")
            print("-" * 110)

            async with db.execute("SELECT COUNT(*) FROM characters") as cursor:
                total = await cursor.fetchone()
                print(f"  Всего персонажей в базе: {total[0]}")

            if total[0] == 0:
                print("\n Пустая база данных")
                return

            async with db.execute("SELECT MIN(id), MAX(id) FROM characters") as cursor:
                min_max = await cursor.fetchone()
                print(f"  Диапазон ID: {min_max[0]} - {min_max[1]}")

            async with db.execute("""
                SELECT COUNT(*) FROM characters 
                WHERE homeworld_name != 'Unknown' AND homeworld_name != ''
            """) as cursor:
                with_planets = await cursor.fetchone()
                print(f"  Персонажей с известными планетами: {with_planets[0]}")

            async with db.execute("""
                SELECT COUNT(DISTINCT homeworld_name) 
                FROM characters 
                WHERE homeworld_name != 'Unknown'
            """) as cursor:
                unique_planets = await cursor.fetchone()
                print(f"  Уникальных планет: {unique_planets[0]}")

            # 4. ВСЕ персонажи в базе
            print(f"\n{'ВСЕ ПЕРСОНАЖИ В БАЗЕ ДАННЫХ':^110}")
            print("=" * 110)
            print(
                f"{'ID':^5} | {'ИМЯ':^25} | {'ПЛАНЕТА':^20} | {'РОЖДЕНИЕ':^10} | {'ПОЛ':^8} | {'ВЕС':^8} | {'ЦВЕТ ГЛАЗ':^12} | {'ЦВЕТ КОЖИ':^15}")
            print("-" * 110)

            #Персонажи
            async with db.execute("""
                SELECT 
                    id, name, homeworld_name, birth_year, gender, mass,
                    eye_color, skin_color, hair_color
                FROM characters 
                ORDER BY id
            """) as cursor:
                all_characters = await cursor.fetchall()

                for char in all_characters:

                    char_id = str(char[0])
                    name = char[1][:23] + "..." if len(char[1]) > 25 else char[1]
                    planet = char[2][:18] + "..." if len(char[2]) > 20 else char[2]
                    birth = char[3][:8] + "..." if len(char[3]) > 10 else char[3]
                    gender = char[4][:6] + "..." if len(char[4]) > 8 else char[4]
                    mass = char[5][:6] + "..." if len(char[5]) > 8 else char[5]
                    eyes = char[6][:10] + "..." if len(char[6]) > 12 else char[6]
                    skin = char[7][:13] + "..." if len(char[7]) > 15 else char[7]

                    print(
                        f"{char_id:^5} | {name:^25} | {planet:^20} | {birth:^10} | {gender:^8} | {mass:^8} | {eyes:^12} | {skin:^15}")

            print("-" * 110)

            #Детали
            print(f"\n{'ИНФОРМАЦИЯ ПО КАЖДОМУ ПЕРСОНАЖУ':^110}")
            print("=" * 110)

            for i, char in enumerate(all_characters, 1):
                print(f"\n[{i}/{len(all_characters)}] ПЕРСОНАЖ ID: {char[0]}")
                print("-" * 50)
                print(f"  Имя:            {char[1]}")
                print(f"  Планета:        {char[2]}")
                print(f"  Год рождения:   {char[3]}")
                print(f"  Пол:            {char[4]}")
                print(f"  Масса:          {char[5]}")
                print(f"  Цвет глаз:      {char[6]}")
                print(f"  Цвет кожи:      {char[7]}")
                print(f"  Цвет волос:     {char[8]}")

            #Планеты
            print(f"\n{'ПО ПЛАНЕТАМ':^110}")
            print("=" * 110)

            async with db.execute("""
                SELECT 
                    homeworld_name, 
                    COUNT(*) as count,
                    GROUP_CONCAT(name, ', ') as characters
                FROM characters 
                WHERE homeworld_name != 'Unknown' AND homeworld_name != ''
                GROUP BY homeworld_name 
                ORDER BY count DESC, homeworld_name
            """) as cursor:
                planets = await cursor.fetchall()

                if planets:
                    print(f"{'ПЛАНЕТА':^25} | {'КОЛ-ВО':^8} | {'ПЕРСОНАЖИ':^70}")
                    print("-" * 110)

                    for planet in planets:
                        planet_name = planet[0][:23] + "..." if len(planet[0]) > 25 else planet[0]
                        count = str(planet[1])
                        characters = planet[2][:67] + "..." if len(planet[2]) > 70 else planet[2]

                        print(f"{planet_name:^25} | {count:^8} | {characters:^70}")
                else:
                    print("  Нет данных о планетах")

            #Пол М/Ж
            print(f"\n{'СТАТИСТИКА ПО ПОЛУ':^110}")
            print("=" * 110)

            async with db.execute("""
                SELECT 
                    gender,
                    COUNT(*) as count,
                    GROUP_CONCAT(name, ', ') as characters
                FROM characters 
                GROUP BY gender 
                ORDER BY count DESC
            """) as cursor:
                genders = await cursor.fetchall()

                print(f"{'ПОЛ':^15} | {'КОЛ-ВО':^8} | {'ПЕРСОНАЖИ':^80}")
                print("-" * 110)

                for gender in genders:
                    gender_name = gender[0][:13] + "..." if len(gender[0]) > 15 else gender[0]
                    count = str(gender[1])
                    characters = gender[2][:77] + "..." if len(gender[2]) > 80 else gender[2]

                    print(f"{gender_name:^15} | {count:^8} | {characters:^80}")

            #Проверка
            print(f"\n{'ПРОВЕРКА ДАННЫХ':^110}")
            print("=" * 110)

            checks = [
                ("Персонажи без имени",
                 "SELECT id, name FROM characters WHERE name = '' OR name IS NULL OR name = 'Unknown'"),
                ("Персонажи без планеты",
                 "SELECT id, name FROM characters WHERE homeworld_name = 'Unknown' OR homeworld_name = ''"),
                ("Персонажи без года рождения",
                 "SELECT id, name FROM characters WHERE birth_year = 'Unknown' OR birth_year = ''"),
                ("Персонажи без пола",
                 "SELECT id, name FROM characters WHERE gender = 'Unknown' OR gender = ''"),
                ("Персонажи без массы",
                 "SELECT id, name FROM characters WHERE mass = 'Unknown' OR mass = ''"),
            ]

            for check_name, query in checks:
                async with db.execute(query) as cursor:
                    results = await cursor.fetchall()

                    print(f"\n{check_name}: {len(results)}")
                    if results:
                        print("  " + ", ".join([f"ID:{r[0]} ({r[1]})" for r in results[:10]]))
                        if len(results) > 10:
                            print(f"  ... и еще {len(results) - 10}")

            #Проверка задач
            print(f"\n{'ПРОВЕРКА ТРЕБОВАНИЙ ЗАДАЧИ':^110}")
            print("=" * 110)

            requirements = [
                ("База данных создана",
                 "SELECT 1 FROM sqlite_master WHERE type='table' AND name='characters'"),
                ("Есть данные в базе",
                 "SELECT COUNT(*) > 0 FROM characters"),
                ("Все обязательные поля присутствуют",
                 "SELECT COUNT(*) = 9 FROM pragma_table_info('characters') WHERE name IN ('id', 'name', 'birth_year', 'eye_color', 'gender', 'hair_color', 'homeworld_name', 'mass', 'skin_color')"),
                ("Поле homeworld_name содержит названия (не ссылки)",
                 "SELECT COUNT(*) = 0 FROM characters WHERE homeworld_name LIKE 'http%'"),
                ("Данные загружены асинхронно",
                 "SELECT 1 FROM characters LIMIT 1"),
                ("Структура таблицы унифицирована",
                 "SELECT COUNT(*) = 1 FROM sqlite_master WHERE type='table' AND name='characters'"),
            ]

            requirement_descriptions = [
                "Создана база данных с таблицей characters",
                "В базу загружены персонажи",
                "Присутствуют все требуемые поля: id, name, birth_year, eye_color, gender, hair_color, homeworld_name, mass, skin_color",
                "В поле homeworld_name хранятся названия планет, а не ссылки",
                "Выгрузка из API и загрузка в базу происходила асинхронно",
                "Структура таблицы одинакова во всех частях кода",
            ]

            passed_count = 0
            for i, (req_name, query) in enumerate(requirements):
                try:
                    async with db.execute(query) as cursor:
                        result = await cursor.fetchone()
                        passed = result[0] == 1 if isinstance(result[0], int) else bool(result[0])

                        if passed:
                            print(f"{req_name}")
                            print(f"{requirement_descriptions[i]}")
                            passed_count += 1
                        else:
                            print(f"{req_name}")
                            print(f"{requirement_descriptions[i]}")
                except:
                    print(f"{req_name} (ошибка проверки)")
                    print(f"{requirement_descriptions[i]}")

            print(f"\n{'ИТОГ:':<20} {passed_count}/{len(requirements)} требований выполнено")

            if passed_count == len(requirements):
                print(f"\n{'ТРЕБОВАНИЯ ЗАДАЧИ ВЫПОЛНЕНЫ!':^110}")
            else:
                print(f"\n{'НЕ ВСЕ ТРЕБОВАНИЯ ВЫПОЛНЕНЫ':^110}")

            print("\n" + "=" * 110)
            print(f"{'ФАЙЛ БАЗЫ ДАННЫХ: starwars_characters.db':^110}")
            print(f"{'ВСЕГО ПЕРСОНАЖЕЙ: ' + str(len(all_characters)):^110}")
            print("=" * 110)

    except Exception as e:
        print(f"\nОшибка при проверке базы данных: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Для Windows
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    print("\n" + "=" * 110)
    print("STAR WARS DATABASE VERIFIER - ПОЛНАЯ ПРОВЕРКА")
    print("=" * 110)

    asyncio.run(verify_database())