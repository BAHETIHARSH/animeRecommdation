import sqlite3
import ast
import pandas as pd

# Load your CSV into DataFrame
file_path = "anime.csv"
df = pd.read_csv(file_path)

# Connect to SQLite database
conn = sqlite3.connect("anime.db")
cursor = conn.cursor()

# Create anime table (excluding last column)
cursor.execute("""
CREATE TABLE IF NOT EXISTS anime (
    id INTEGER PRIMARY KEY,
    Name TEXT,
    English_Name TEXT,
    Image_source TEXT,
    Synopsis TEXT,
    Rating REAL,
    Rated_by TEXT,
    Rank REAL,
    Popularity REAL,
    Release_time TEXT,
    Number_of_episodes REAL,
    Duration TEXT,
    Status TEXT,
    Aired TEXT,
    Producers TEXT,
    Studios TEXT,
    Genres TEXT,
    Theme TEXT,
    Demographic TEXT,
    Aired_Year REAL
)
""")

# Create recommendations table
cursor.execute("""
CREATE TABLE IF NOT EXISTS recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER,
    recommended_anime_id INTEGER,
    similarity REAL,
    FOREIGN KEY(anime_id) REFERENCES anime(id)
)
""")


# Drop the 'Recommendations' column for anime table
anime_data = df.drop(columns=["Recommendations"])
print(anime_data.columns)
# Insert anime data
for _, row in anime_data.iterrows():
    cursor.execute("""
    INSERT INTO anime (
        id, Name, English_Name, Image_source, Synopsis, Rating, Rated_by, Rank, Popularity,
        Release_time, Number_of_episodes, Duration, Status, Aired, Producers, Studios,
        Genres, Theme, Demographic,  Aired_Year
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        row['Unnamed: 0'],
        row['Name'],
        row['English Name'],
        row['Image source'],
        row['Synopsis'],
        row['Rating'],
        row['Rated by(number of users)'],
        row['Rank'],
        row['Popularity'],
        row['Release time'],
        row['Number of episodes'],
        row['Duration'],
        row['Status'],
        row['Aired'],
        row['Producers'],
        row['Studio(s)'],
        row['Genres'],
        row['Theme'],
        row['Demographic'],
        row['Aired_Year']
    ))



# Iterate over Recommendations column
for idx, row in df.iterrows():
    anime_id = row['Unnamed: 0']
 # since anime table has AUTOINCREMENT id
    recs = row["Recommendations"]

    if pd.notna(recs):
        recs_list = ast.literal_eval(recs)  # Convert string to list of tuples
        for rec in recs_list:
            recommended_anime_id = rec[0]
            similarity = rec[1]
            cursor.execute("""
            INSERT INTO recommendations (anime_id, recommended_anime_id, similarity)
            VALUES (?, ?, ?)
            """, (anime_id, recommended_anime_id, similarity))

conn.commit()
conn.close()
