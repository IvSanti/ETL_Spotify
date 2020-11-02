import spotipy
import psycopg2

from spotipy.oauth2 import SpotifyClientCredentials

client_id = '01309df0f5bb4abc8e49fa2c8579875f'
client_secret = 'd2a7c4c3ff9b4c7f95b46d5b6cd851dc'

client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

connection = psycopg2.connect(user = "postgres",
                                password = "santi1014",
                                host = "127.0.0.1",
                                port = "2430",
                                database = "spotify")
cursor = connection.cursor()

procedure ="""
CREATE TABLE artists (
artist_id SERIAL PRIMARY KEY,
name_artist  varchar,
popularity varchar, 
tipo varchar,
uri varchar,
followers varchar
);
"""
cursor.execute(procedure)
connection.commit()
print("Table created successfully in PostgreSQL ");

procedure ="""
CREATE TABLE tracks (
track_id SERIAL PRIMARY KEY,
artist_id integer,
name_track varchar,
tipo_track  varchar,
album  varchar,
popularity_track varchar,
number_track varchar,
id_t varchar,
uri varchar,
launch  varchar
);
"""

cursor.execute(procedure)
connection.commit()
print("Table created successfully in PostgreSQL ")

procedure = """
ALTER TABLE "tracks"
   ADD CONSTRAINT fk_artist
   FOREIGN KEY (artist_id)
   REFERENCES "artists"(artist_id);
"""
cursor.execute(procedure)
connection.commit()
print("Alter_Table successfully in PostgreSQL")


procedure ="""
CREATE TABLE genres (
genre_id SERIAL PRIMARY KEY,
artist_id integer,
name_genre varchar
);
"""
cursor.execute(procedure)
connection.commit()
print("Table created successfully in PostgreSQL ")

procedure = """
ALTER TABLE "genres"
   ADD CONSTRAINT fk_artist
   FOREIGN KEY (artist_id)
   REFERENCES "artists"(artist_id);
"""
cursor.execute(procedure)
connection.commit()
print("Alter_Table successfully in PostgreSQL")



def getTrackIDs(user, playlist_id):
    ids = []
    playlist = sp.user_playlist(user, playlist_id)
    for item in playlist['tracks']['items']:
        track = item['track']
        ids.append(track['id'])
    return ids

ids = getTrackIDs('santiago', '37i9dQZF1DX08jcQJXDnEQ')

def getTrackFeatures(id):

    track = sp.track(id)
    artist = sp.artist(track['album']['artists'][0]['id'])

    name_artist = artist['name']
    popularity = artist["popularity"]
    tipo = artist["type"]
    uri = artist["uri"]
    followers = artist["followers"]["total"]
    uri_t = track["uri"]
    pop_track = track["popularity"]
    id_t = track["id"]
    album = track['album']['name']
    name_track = track['name']
    tipo_track = track["type"]
    number_track = track = ["track_number"]
    ## launch = track["release_date"]
    ## name_genre = artist["genres"]

    insert_query = """INSERT INTO artists (name_artist, popularity, tipo, uri, followers) VALUES (%s,%s,%s,%s,%s)"""
    record_to_insert = (name_artist, popularity, tipo, uri, followers)
    cursor.execute(insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    print(count, "Record inserted successfully")

    insert_query = """INSERT INTO tracks (name_track, tipo_track, album,
    popularity_track, number_track, id_t, uri) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (name_track, tipo_track, album, pop_track, number_track, id_t, uri_t)
    cursor.execute(insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    print(count, "Record inserted successfully")


for id in range(len(ids)):
  track = getTrackFeatures(ids[id])

print("FINALIZO EL PROGRAMA")
if (connection):
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")