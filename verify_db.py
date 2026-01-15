import aiosqlite
import asyncio
import sys


async def verify_full_database():
    #Проверка базы данных
    try:
        async with aiosqlite.connect("starwars_characters.db") as db:
            print("=" * 100)
            print("ПРОВЕРКА БАЗЫ ДАННЫХ")
            print("=" * 100)

            #Проверка
            print("\nСТРУКТУРА ТАБЛИЦЫ:")
            print("=" * 100)

            async with db.execute("PRAGMA table_info(characters)") as cursor:
                columns = await cursor.fetchall()

                required_fields = [
                    'id', 'name', 'birth_year', 'eye_color', 'gender', 'hair_color',
                    'homeworld_name', 'mass', 'skin_color', 'films', 'species',
                    'starships', 'vehicles'
                ]

                for col in columns:
                    field_name = col[1]
                    is_required = "ОБЯЗАТЕЛЬНОЕ" if field_name in required_fields else "ДОПОЛНИТЕЛЬНОЕ"
                    print(f"  {field_name:20} {col[2]:15} {is_required}")

            #Записи
            async with db.execute("SELECT COUNT(*) FROM characters") as cursor:
                total = await cursor.fetchone()
                print(f"\nВсего персонажей: {total[0]}")

            if total[0] > 0:
                #Персонажи
                print("\nВСЕ ПЕРСОНАЖИ:")
                print("=" * 100)

                async with db.execute("""
                    SELECT 
                        id, name, homeworld_name, birth_year, gender, mass,
                        eye_color, hair_color, skin_color,
                        films, species, starships, vehicles
                    FROM characters 
                    ORDER BY id
                """) as cursor:
                    all_chars = await cursor.fetchall()

                    for char in all_chars:
                        print(f"\n{'=' * 100}")
                        print(f"ID: {char[0]} |{char[1]}")
                        print(f"{'=' * 100}")

                        #Информация
                        print(f"Планета:      {char[2]}")
                        print(f"Рождение:     {char[3]}")
                        print(f"Пол:          {char[4]}")
                        print(f"Масса:        {char[5]}")
                        print(f"Цвет глаз:    {char[6]}")
                        print(f"Цвет волос:   {char[7]}")
                        print(f"Цвет кожи:    {char[8]}")

                        if char[9]:
                            films_list = char[9].split(', ')
                            print(f"Фильмы ({len(films_list)}):")
                            for film in films_list:
                                print(f"{film}")

                        if char[10]:
                            species_list = char[10].split(', ')
                            print(f"Виды ({len(species_list)}):")
                            for species in species_list:
                                print(f"{species}")

                        if char[11]:
                            starships_list = char[11].split(', ')
                            print(f"Звездолеты ({len(starships_list)}):")
                            for ship in starships_list:
                                print(f"{ship}")

                        if char[12]:
                            vehicles_list = char[12].split(', ')
                            print(f"Транспорт ({len(vehicles_list)}):")
                            for vehicle in vehicles_list:
                                print(f"      • {vehicle}")

                print(f"\n{'=' * 100}")

    except Exception as e:
        print(f"\nОшибка: {e}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(verify_full_database())