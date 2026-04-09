import statsapi
import psycopg2
import os
from datetime import datetime, timedelta

# Railway Variable
DATABASE_URL = os.getenv('postgresql://postgres:VEjGWyYJZisCNESFaFUzcRDCnusMlKoP@postgres.railway.internal:5432/railway')

def run_pipeline():
    if not DATABASE_URL:
        print("DATABASE_URL not found. Ensure Postgres is linked in Railway.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # --- STEP 1: TRUNCATE STAGING ---
    print("Emptying staging table...")
    cur.execute("TRUNCATE TABLE staging_pitching_logs;")

    # --- STEP 2: LOAD DATA FROM API ---
    # For daily updates, use 3 days. For historical, change start_date to '2021-03-01'
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=3)
    
    print(f"Scraping MLB data from {start_date} to {end_date}...")
    schedule = statsapi.schedule(start_date=start_date, end_date=end_date)

    for game in schedule:
        if game['status'] != 'Final': continue
            
        gid = game['game_id']
        try:
            data = statsapi.boxscore_data(gid)
            for side in ['home', 'away']:
                if not data[side]['pitchers']: continue
                
                p_id = data[side]['pitchers'][0]
                player = data[side]['players'][f'ID{p_id}']
                s = player['stats']['pitching']
                opp_side = 'away' if side == 'home' else 'home'

                cur.execute("""
                    INSERT INTO staging_pitching_logs (
                        game_id, game_date, pitcher_name, team, opponent, home_away, venue,
                        innings_pitched, hits, earned_runs, runs, walks, strikeouts, home_runs, pitches, strikes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    gid, game['game_date'], player['person']['fullName'],
                    data[side]['teamName'], data[opp_side]['teamName'],
                    side.capitalize(), game['venue_name'],
                    s.get('inningsPitched'), s.get('hits'), s.get('earned_runs'),
                    s.get('runs'), s.get('baseOnBalls'), s.get('strikeOuts'),
                    s.get('homeRuns'), s.get('pitchesThrown'), s.get('strikes')
                ))
        except Exception as e:
            print(f"Error on game {gid}: {e}")

    # --- STEP 3: MERGE INTO PRODUCTION ---
    print("Merging staging into production table...")
    cur.execute("""
        INSERT INTO pitching_logs 
        SELECT * FROM staging_pitching_logs
        ON CONFLICT (game_id, pitcher_name) DO UPDATE SET
            innings_pitched = EXCLUDED.innings_pitched,
            hits = EXCLUDED.hits,
            earned_runs = EXCLUDED.earned_runs,
            runs = EXCLUDED.runs,
            walks = EXCLUDED.walks,
            strikeouts = EXCLUDED.strikeouts,
            pitches = EXCLUDED.pitches,
            strikes = EXCLUDED.strikes;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Pipeline execution successful.")

if __name__ == "__main__":
    run_pipeline()
