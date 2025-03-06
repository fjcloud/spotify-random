import csv
import json
import sqlite3
import os
import re

def create_spotify_tracks_db(csv_file, db_file):
    # Connect to SQLite database (will create it if it doesn't exist)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Create table for tracks with just URI and decade
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tracks (
        uri TEXT PRIMARY KEY,
        decade INTEGER
    )
    ''')
    
    # Read the CSV file
    with open(csv_file, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        tracks_data = []
        
        for row in csv_reader:
            uri = row['uri']
            
            # Parse the album_date - convert single quotes to double quotes for proper JSON
            try:
                # Replace single quotes with double quotes
                date_str = row['album_date'].replace("'", '"')
                date_info = json.loads(date_str)
                
                if 'year' in date_info:
                    year = date_info['year']
                    # Calculate the decade (e.g., 2015 -> 2010)
                    decade = (year // 10) * 10
                else:
                    decade = None
            except Exception:
                # Fallback: try regex extraction if JSON parsing fails
                match = re.search(r"'year':\s*(\d+)", row['album_date'])
                if match:
                    year = int(match.group(1))
                    decade = (year // 10) * 10
                else:
                    decade = None
            
            # Only add tracks where we could determine the decade
            if decade is not None:
                tracks_data.append((uri, decade))
    
    # Insert data into the database
    cursor.executemany(
        'INSERT OR REPLACE INTO tracks (uri, decade) VALUES (?, ?)',
        tracks_data
    )
    
    # Commit changes and close connection
    conn.commit()
    print(f"Database created with {len(tracks_data)} tracks inserted")
    
    # Show a sample of the data
    cursor.execute('SELECT * FROM tracks LIMIT 5')
    print("\nSample data:")
    for row in cursor.fetchall():
        print(f"URI: {row[0]}, Decade: {row[1]}")
    
    # Count tracks by decade
    cursor.execute('SELECT decade, COUNT(*) FROM tracks GROUP BY decade ORDER BY decade')
    print("\nTracks by decade:")
    for decade, count in cursor.fetchall():
        print(f"{decade}s: {count} tracks")
    
    conn.close()

if __name__ == "__main__":
    csv_file = "popular_tracks.csv"
    db_file = "spotify_tracks_by_decade.db"
    
    if os.path.exists(csv_file):
        create_spotify_tracks_db(csv_file, db_file)
    else:
        print(f"Error: CSV file '{csv_file}' not found.")
