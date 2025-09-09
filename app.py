from flask import Flask, render_template, request, jsonify
import sqlite3

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('anime.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search')
def search():
    query = request.args.get('query', '').lower()
    conn = get_db_connection()
    anime = conn.execute("""
        SELECT id, COALESCE(English_Name, Name) AS Name, Rating, Genres, Image_source 
        FROM anime 
        WHERE LOWER(COALESCE(English_Name, Name)) LIKE ? 
        ORDER BY Popularity ASC
        LIMIT 10
    """, (query + '%',)).fetchall()
    conn.close()
    return jsonify([dict(row) for row in anime])

@app.route('/recommendations/<int:anime_id>')
def recommendations(anime_id):
    conn = get_db_connection()
    recs = conn.execute("""
        SELECT a.id, COALESCE(a.English_Name, a.Name) AS Name, a.Rating, a.Genres, a.Image_source, a.Synopsis
        FROM recommendations r
        JOIN anime a ON r.recommended_anime_id = a.id
        WHERE r.anime_id = ?
        ORDER BY r.similarity DESC
        LIMIT 15
    """, (anime_id,)).fetchall()
    conn.close()
    return jsonify([dict(row) for row in recs])

@app.route('/top-anime')
def top_anime():
    conn = get_db_connection()
    anime = conn.execute("""
        SELECT id, COALESCE(English_Name, Name) AS Name, Rating, Genres, Image_source, Synopsis
        FROM anime
        ORDER BY Popularity ASC
        LIMIT 12
    """).fetchall()
    conn.close()
    return jsonify([dict(row) for row in anime])

if __name__ == '__main__':
    app.run(debug=True)
