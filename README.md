# Spotify Random Track Selector

A simple web application that lets users discover random Spotify tracks by selecting decades and languages.

## Features

- Select tracks from specific decades (1950s-2020s)
- Filter by major European languages or instrumental music
- Browser caching to avoid repeated downloads
- Direct integration with Spotify Web Player and mobile app

## Setup

1. Create a SQLite database with your track data:
   ```python
   python create_db.py popular_tracks.csv
   ```

2. Place the `tracks.db` file in the same directory as the HTML file

3. Open `index.html` in a web browser or host it on a web server

## Database Structure

The database contains tables for each decade/language combination with at least 100 tracks, using the naming pattern `tracks_DECADE_LANGUAGE` (e.g., `tracks_1980_en`).

## Usage

1. On first visit, users will download and cache the database
2. Select desired decades and languages
3. Click "Find Random Track" to discover music
4. The app will redirect to Spotify to play the selected track

## Technical Notes

- Uses SQLite.js for database operations
- Utilizes IndexedDB for browser caching
- No server-side processing required
- Compatible with modern browsers
