# import pandas as pd
import sqlite3
from skyfield.api import Topos, load


# # Conectar aos bancos de dados
# lottery_db = "C:/DocaSilva/astrosena/data/lottery_results.db"
# astro_db = "C:/DocaSilva/astrosena/data/astro_positions.db"

# # Carregar dados das tabelas
# with sqlite3.connect(lottery_db) as conn:
#     lottery_df = pd.read_sql_query("SELECT * FROM lottery_results", conn)

# with sqlite3.connect(astro_db) as conn:
#     astro_df = pd.read_sql_query("SELECT * FROM astro_positions", conn)

# # Mesclar os dados com base no contest
# merged_df = pd.merge(lottery_df, astro_df, on="contest")

# # Exemplo: Calcular correlação entre posição de Mercúrio e a soma dos números sorteados
# merged_df['sum_balls'] = merged_df[['ball1', 'ball2', 'ball3', 'ball4', 'ball5', 'ball6']].sum(axis=1)
# merged_df['mercury_float'] = merged_df['mercury'].str.replace('°', '').astype(float)

# correlation = merged_df[['sum_balls', 'mercury_float']].corr()
# print("Correlação entre a soma dos números e Mercúrio:", correlation)

db_path = "C:/DocaSilva/astrosena/data/data_base.db"
eph = load('de421.bsp')
earth = eph['earth']
ts = load.timescale()

lat, lon = -10.55, -51.0833
topos = Topos(latitude_degrees=lat, longitude_degrees=lon)


def get_astrological_data(date):
    """Returns planetary positions and signs for a specific date."""
    t = ts.utc(date['year'], date['month'], date['day'])  # Create a Time object
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
    data = {}
    for name, planet in planets.items():
        astrometric = (earth + topos).at(t).observe(planet)
        lat, lon, distance = astrometric.apparent().ecliptic_latlon()
        sign = get_sign_from_longitude(lon.degrees)
        data[name] = {"longitude": lon.degrees, "sign": sign}
    return data

def get_sign_from_longitude(longitude):
    """Maps a longitude in degrees to a zodiac sign."""
    signs = [
        "Áries", "Touro", "Gêmeos", "Câncer", "Leão", "Virgem",
        "Libra", "Escorpião", "Sagitário", "Capricórnio", "Aquário", "Peixes"
    ]
    index = int(longitude // 30)  # Each sign spans 30 degrees
    return signs[index % 12]

def correlate_numbers_to_signs():
    """Maps lottery numbers to astrological signs based on draw dates."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT contest, draw_date, ball1, ball2, ball3, ball4, ball5, ball6 FROM raw_lottery_results")
    draws = cursor.fetchall()
    conn.close()

    correlations = []
    for contest, draw_date, *balls in draws:
        print(f"Processing draw {contest} - Date: {draw_date}")

        # Convert date to dictionary format
        day, month, year = map(int, draw_date.split('/'))
        date = {'day': day, 'month': month, 'year': year}

        # Get astrological data
        astro_data = get_astrological_data(date)

        # Map numbers to signs
        for ball in balls:
            planet, sign = sorted(astro_data.items())[ball % len(astro_data)]
            correlations.append((contest, ball, planet, sign))

    return correlations

# Example usage
if __name__ == "__main__":
    correlations = correlate_numbers_to_signs()
    for correlation in correlations:
        print(correlation)
