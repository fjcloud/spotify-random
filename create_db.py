import csv
import json
import sqlite3
import sys
import os
import re

def create_spotify_tracks_db(csv_file, db_file="tracks.db", min_records=100):
    # Connect to SQLite database (will create it if it doesn't exist)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    print(f"Processing CSV file: {csv_file}")
    print(f"Only creating tables for combinations with at least {min_records} tracks")
    print(f"Creating minimal database structure for space efficiency")
    
    # Dictionary to store URIs for each decade and language combination
    decade_language_tracks = {}
    
    # Read the CSV file
    with open(csv_file, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            uri = row['uri']
            
            # Parse the album_date
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
            
            # Extract language(s)
            try:
                # The language field seems to be a list in string format
                lang_str = row['language'].replace("'", '"')
                languages = json.loads(lang_str)
                
                # If it's empty or couldn't be parsed as a list, set as 'unknown'
                if not languages or not isinstance(languages, list):
                    languages = ['unknown']
            except Exception:
                # Fallback: try regex extraction if JSON parsing fails
                match = re.findall(r"'([a-z]{2})'", row['language'])
                languages = match if match else ['unknown']
            
            # Only process tracks where we could determine the decade
            if decade is not None:
                for lang in languages:
                    # Create a key for this decade/language combination
                    key = f"{decade}_{lang}"
                    
                    # Initialize the list for this combination if it doesn't exist
                    if key not in decade_language_tracks:
                        decade_language_tracks[key] = []
                    
                    # Add the URI to the appropriate list
                    decade_language_tracks[key].append(uri)
    
    # Create tables and insert data for each decade/language combination with enough tracks
    tables_created = 0
    track_counts = {}
    
    # Use WAL mode for better performance during bulk inserts
    cursor.execute("PRAGMA journal_mode = WAL")
    
    # Begin a transaction for faster inserts
    cursor.execute("BEGIN TRANSACTION")
    
    try:
        for key, uris in decade_language_tracks.items():
            # Only create table if there are enough tracks
            if len(uris) >= min_records:
                decade, lang = key.split('_')
                table_name = f"tracks_{key}"
                
                # Create table (with no primary key constraint to save space)
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (uri TEXT)")
                
                # Insert URIs into this table as a batch
                cursor.executemany(f"INSERT INTO {table_name} (uri) VALUES (?)", 
                                 [(uri,) for uri in uris])
                
                # Store the count for reporting
                track_counts[key] = len(uris)
                tables_created += 1
        
        # Commit the transaction
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    
    # Switch back to delete mode after bulk inserts
    cursor.execute("PRAGMA journal_mode = DELETE")
    
    # Create indices for faster random selection
    for key in track_counts.keys():
        table_name = f"tracks_{key}"
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{key} ON {table_name} (uri)")
    
    # Analyze the database to optimize query planning
    cursor.execute("ANALYZE")
    
    # Vacuum to reclaim unused space
    cursor.execute("VACUUM")
    
    # Print summary information
    print(f"\nDatabase '{db_file}' created successfully!")
    print(f"Created {tables_created} tables for decade/language combinations with at least {min_records} tracks")
    
    # Summary of included combinations
    print("\nIncluded decade/language combinations:")
    print("-" * 50)
    print(f"{'Decade':<10} {'Language':<10} {'Count':<10}")
    print("-" * 50)
    
    # Sort by decade, then by count in descending order
    for key in sorted(track_counts.keys(), 
                     key=lambda k: (int(k.split('_')[0]), -track_counts[k])):
        decade, lang = key.split('_')
        count = track_counts[key]
        print(f"{decade}s{'':<5} {lang:<10} {count:<10}")
    
    # Get database file size
    db_size = os.path.getsize(db_file)
    print("-" * 50)
    print(f"Database file size: {db_size / (1024*1024):.2f} MB")
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    # Default minimum records threshold
    min_records = 100
    
    # Check for command-line arguments
    if len(sys.argv) < 2:
        print("Usage: python script_name.py path_to_csv_file [min_records]")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    # Check if minimum records threshold is provided
    if len(sys.argv) >= 3:
        try:
            min_records = int(sys.argv[2])
        except ValueError:
            print(f"Warning: Invalid min_records value '{sys.argv[2]}'. Using default ({min_records}).")
    
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found.")
        sys.exit(1)
    
    create_spotify_tracks_db(csv_file, min_records=min_records)
