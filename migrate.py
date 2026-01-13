import aiosqlite
import asyncio

async def create_table():
    async with aiosqlite.connect("sw_characters.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS characters (
                id INTEGER PRIMARY KEY,
                birth_year TEXT,
                eye_color TEXT,
                gender TEXT,
                hair_color TEXT,
                homeworld TEXT,
                mass TEXT,
                name TEXT NOT NULL,
                skin_color TEXT
            )
        """)  # ← Здесь была ошибка: не закрыта тройная кавычка
        await db.commit()
        print("Таблица 'characters' создана или уже существует.")

if __name__ == "__main__":
    asyncio.run(create_table())
