import sqlite3
import pandas as pd

db_file = 'C:/DocaSilva/astrosena/data/lottery_results.db'

excel_file = 'C:/DocaSilva/astrosena/data/lottery_results.xlsx'

def create_table():
    """Creates or verifies the lottery results table in the database."""
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS lottery_results (
            contest INTEGER PRIMARY KEY,
            draw_date TEXT,
            ball1 INTEGER,
            ball2 INTEGER,
            ball3 INTEGER,
            ball4 INTEGER,
            ball5 INTEGER,
            ball6 INTEGER,
            winners_6 INTEGER,
            city_state TEXT,
            payout_6 REAL,
            winners_5 INTEGER,
            payout_5 REAL,
            winners_4 INTEGER,
            payout_4 REAL,
            accumulated_6 INTEGER,
            total_revenue REAL,
            estimated_prize REAL,
            rollover_accumulated REAL,
            observation TEXT
        )
        ''')
        print("Table created/verified successfully.")
        conn.commit()
    except Exception as e:
        print(f"Error creating/verifying table: {e}")
    finally:
        conn.close()

def import_results_data():
    """Imports data from an Excel file into the lottery results table."""
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        print("Loading spreadsheet...")
        df = pd.read_excel(excel_file)

        print("Spreadsheet loaded successfully!")
        print(f"Number of rows and columns: {df.shape}")
        print("Preview of the data:")
        print(df.head())

        sql_insert = '''
        INSERT INTO lottery_results (
            contest, draw_date, ball1, ball2, ball3, ball4, ball5, ball6,
            winners_6, city_state, payout_6, winners_5, payout_5,
            winners_4, payout_4, accumulated_6, total_revenue,
            estimated_prize, rollover_accumulated, observation
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''

        for _, row in df.iterrows():
            cursor.execute(sql_insert, tuple(row))

        conn.commit()
        print(f"Data successfully inserted: {len(df)} records.")
    except Exception as e:
        print(f"Error importing data: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("Starting the process...")
    create_table()
    import_results_data()
    print("Process completed.")
