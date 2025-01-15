from skyfield.api import Topos, load
import sqlite3

db_path = "C:/DocaSilva/astrosena/data/data_base.db"

eph = load('de421.bsp')
earth = eph['earth']
ts = load.timescale()

lat, lon = -10.55, -51.0833
topos = Topos(latitude_degrees=lat, longitude_degrees=lon)

def create_astro_table():
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS astro_positions (
            contest INTEGER PRIMARY KEY,
            date DATE,
            mercury TEXT,
            venus TEXT,
            mars TEXT,
            jupiter TEXT,
            saturn TEXT,
            uranus TEXT,
            neptune TEXT,
            pluto TEXT,
            sun TEXT,
            moon TEXT
        )
        ''')
        print("Table created/verified successfully.")
        conn.commit()
    except Exception as e:
        print(f"Error creating/verifying table: {e}")
    finally:
        conn.close()

def get_astrological_data(date):
    t = ts.utc(date['year'], date['month'], date['day'])
    planets = {
        "Mercury": eph['mercury'],
        "Venus": eph['venus'],
        "Mars": eph['mars'],
        "Jupiter": eph['jupiter barycenter'],
        "Saturn": eph['saturn barycenter'],
        "Uranus": eph['uranus barycenter'],
        "Neptune": eph['neptune barycenter'],
        "Pluto": eph['pluto barycenter'],
        "Sun": eph['sun'],
        "Moon": eph['moon'],
    }
    positions = {}
    for name, planet in planets.items():
        astrometric = (earth + topos).at(t).observe(planet)
        alt, az, distance = astrometric.apparent().ecliptic_latlon()
        positions[name] = f"{alt.degrees:.2f}°"
    return positions

def save_astrological_data(contest, date, positions):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        sql_insert = '''
        INSERT OR REPLACE INTO astro_positions (
            contest, date, mercury, venus, mars, jupiter, saturn, uranus, neptune, pluto, sun, moon
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(sql_insert, (
            contest, date,
            positions["Mercury"], positions["Venus"], positions["Mars"],
            positions["Jupiter"], positions["Saturn"], positions["Uranus"],
            positions["Neptune"], positions["Pluto"], positions["Sun"], positions["Moon"]
        ))
        conn.commit()
        # print(f"Data successfully inserted for contest {contest}.")
    except Exception as e:
        print(f"Error importing data: {e}")
    finally:
        conn.close()

def process_astrological_data():
    create_astro_table()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT contest, draw_date FROM raw_lottery_results")
    draws = cursor.fetchall()
    conn.close()

    for contest, draw_date in draws:
        # print(f"Processing draw {contest} - Date: {draw_date}")
        day, month, year = map(int, draw_date.split('/'))
        date = {'day': day, 'month': month, 'year': year}
        positions = get_astrological_data(date)
        save_astrological_data(contest, draw_date, positions)
    
    print('data successfully inserted')


if __name__ == "__main__":
    process_astrological_data()
