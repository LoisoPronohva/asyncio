LOCAL_CHARACTERS = [
    #Основные
    {
        "id": 1,
        "name": "Luke Skywalker",
        "birth_year": "19BBY",
        "eye_color": "blue",
        "gender": "male",
        "hair_color": "blond",
        "homeworld_name": "Tatooine",
        "mass": "77",
        "skin_color": "fair"
    },
    {
        "id": 2,
        "name": "C-3PO",
        "birth_year": "112BBY",
        "eye_color": "yellow",
        "gender": "n/a",
        "hair_color": "n/a",
        "homeworld_name": "Tatooine",
        "mass": "75",
        "skin_color": "gold"
    },
    {
        "id": 3,
        "name": "R2-D2",
        "birth_year": "33BBY",
        "eye_color": "red",
        "gender": "n/a",
        "hair_color": "n/a",
        "homeworld_name": "Naboo",
        "mass": "32",
        "skin_color": "white, blue"
    },
    {
        "id": 4,
        "name": "Darth Vader",
        "birth_year": "41.9BBY",
        "eye_color": "yellow",
        "gender": "male",
        "hair_color": "none",
        "homeworld_name": "Tatooine",
        "mass": "136",
        "skin_color": "white"
    },
    {
        "id": 5,
        "name": "Leia Organa",
        "birth_year": "19BBY",
        "eye_color": "brown",
        "gender": "female",
        "hair_color": "brown",
        "homeworld_name": "Alderaan",
        "mass": "49",
        "skin_color": "light"
    },
    {
        "id": 6,
        "name": "Owen Lars",
        "birth_year": "52BBY",
        "eye_color": "blue",
        "gender": "male",
        "hair_color": "brown, grey",
        "homeworld_name": "Tatooine",
        "mass": "120",
        "skin_color": "light"
    },
    {
        "id": 7,
        "name": "Beru Whitesun lars",
        "birth_year": "47BBY",
        "eye_color": "blue",
        "gender": "female",
        "hair_color": "brown",
        "homeworld_name": "Tatooine",
        "mass": "75",
        "skin_color": "light"
    },
    {
        "id": 8,
        "name": "R5-D4",
        "birth_year": "unknown",
        "eye_color": "red",
        "gender": "n/a",
        "hair_color": "n/a",
        "homeworld_name": "Tatooine",
        "mass": "32",
        "skin_color": "white, red"
    },
    {
        "id": 9,
        "name": "Biggs Darklighter",
        "birth_year": "24BBY",
        "eye_color": "brown",
        "gender": "male",
        "hair_color": "black",
        "homeworld_name": "Tatooine",
        "mass": "84",
        "skin_color": "light"
    },
    {
        "id": 10,
        "name": "Obi-Wan Kenobi",
        "birth_year": "57BBY",
        "eye_color": "blue-gray",
        "gender": "male",
        "hair_color": "auburn, white",
        "homeworld_name": "Stewjon",
        "mass": "77",
        "skin_color": "fair"
    },
    {
        "id": 11,
        "name": "Anakin Skywalker",
        "birth_year": "41.9BBY",
        "eye_color": "blue",
        "gender": "male",
        "hair_color": "blond",
        "homeworld_name": "Tatooine",
        "mass": "84",
        "skin_color": "fair"
    },
    {
        "id": 12,
        "name": "Wilhuff Tarkin",
        "birth_year": "64BBY",
        "eye_color": "blue",
        "gender": "male",
        "hair_color": "auburn, grey",
        "homeworld_name": "Eriadu",
        "mass": "unknown",
        "skin_color": "fair"
    },
    {
        "id": 13,
        "name": "Chewbacca",
        "birth_year": "200BBY",
        "eye_color": "blue",
        "gender": "male",
        "hair_color": "brown",
        "homeworld_name": "Kashyyyk",
        "mass": "112",
        "skin_color": "unknown"
    },
    {
        "id": 14,
        "name": "Han Solo",
        "birth_year": "29BBY",
        "eye_color": "brown",
        "gender": "male",
        "hair_color": "brown",
        "homeworld_name": "Corellia",
        "mass": "80",
        "skin_color": "fair"
    },
    {
        "id": 15,
        "name": "Greedo",
        "birth_year": "44BBY",
        "eye_color": "black",
        "gender": "male",
        "hair_color": "n/a",
        "homeworld_name": "Rodia",
        "mass": "74",
        "skin_color": "green"
    },
    {
        "id": 16,
        "name": "Jabba Desilijic Tiure",
        "birth_year": "600BBY",
        "eye_color": "orange",
        "gender": "hermaphrodite",
        "hair_color": "n/a",
        "homeworld_name": "Nal Hutta",
        "mass": "1,358",
        "skin_color": "green-tan, brown"
    },
    {
        "id": 17,
        "name": "Wedge Antilles",
        "birth_year": "21BBY",
        "eye_color": "hazel",
        "gender": "male",
        "hair_color": "brown",
        "homeworld_name": "Corellia",
        "mass": "77",
        "skin_color": "fair"
    },
    {
        "id": 18,
        "name": "Jek Tono Porkins",
        "birth_year": "unknown",
        "eye_color": "blue",
        "gender": "male",
        "hair_color": "brown",
        "homeworld_name": "Bestine IV",
        "mass": "110",
        "skin_color": "fair"
    },
    {
        "id": 19,
        "name": "Yoda",
        "birth_year": "896BBY",
        "eye_color": "brown",
        "gender": "male",
        "hair_color": "white",
        "homeworld_name": "unknown",
        "mass": "17",
        "skin_color": "green"
    },
    {
        "id": 20,
        "name": "Palpatine",
        "birth_year": "82BBY",
        "eye_color": "yellow",
        "gender": "male",
        "hair_color": "grey",
        "homeworld_name": "Naboo",
        "mass": "75",
        "skin_color": "pale"
    },
]

def get_local_characters(count: int = 20) -> list:
    return LOCAL_CHARACTERS[:count]