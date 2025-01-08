from skyfield.api import Topos, load
import sqlite3

# Path to the SQLite database
db_path = "C:/DocaSilva/astrosena/data/lottery_results.db"

# Load Skyfield ephemeris
eph = load('de421.bsp')  # Ephemeris file (use de430.bsp for higher precision)
earth = eph['earth']
ts = load.timescale()  # Load Skyfield timescale

# Coordinates of the central point in Brazil
lat = -10.55   # Latitude of 10° 33' S
lon = -51.0833 # Longitude of 51° 05' W

# Define the observation point
topos = Topos(latitude_degrees=lat, longitude_degrees=lon)

def get_astrological_data(date):
    """Returns planetary positions for a specific date."""
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
    positions = {}
    for name, planet in planets.items():
        astrometric = (earth + topos).at(t).observe(planet)
        alt, az, distance = astrometric.apparent().ecliptic_latlon()
        positions[name] = f"{alt.degrees:.2f}°"
    return positions

def process_astrological_data():
    """Connects to the database, reads dates, and inserts astrological data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query the draw dates
    cursor.execute("SELECT contest, draw_date FROM lottery_results")
    draws = cursor.fetchall()

    for contest, draw_date in draws:
        print(f"Processing draw {contest} - Date: {draw_date}")

        # Convert the date to the correct format
        day, month, year = map(int, draw_date.split('/'))  # Split the date in dd/mm/yyyy format
        date = {'day': day, 'month': month, 'year': year}

        # Get astrological data
        positions = get_astrological_data(date)
        print(f"Planetary positions: {positions}")

        # Update the database with astrological information
        cursor.execute(
            """
            UPDATE lottery_results
            SET observation = ?
            WHERE contest = ?
            """,
            (str(positions), contest)
        )
    
    conn.commit()
    conn.close()
    print("Astrological data processed and saved to the database.")

if __name__ == "__main__":
    process_astrological_data()
