import spotipy, requests, pytz, json, os, time
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime
from hashlib import md5

load_dotenv()

start = time.perf_counter()


def now():
    return datetime.now(pytz.timezone("Asia/Kolkata")).replace(tzinfo=None)


SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

print(f"\n{now()}---> Authenticating with Spotify")

scope = [
    "user-library-read",
    "user-read-playback-state",
    "app-remote-control",
    "user-modify-playback-state",
    "playlist-read-private",
    "playlist-read-collaborative",
    "playlist-modify-private",
    "playlist-modify-public",
    "user-top-read",
    "user-read-recently-played",
]

spotify = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        scope=scope,
        redirect_uri="http://127.0.0.1:9090",
    )
)

time.sleep(1)


saved_df = pd.read_excel("history.xlsx")
max_timestamp = saved_df.played_on.max().to_pydatetime()

tracks = []
response = None

while True:
    payload = {
        "api_key": os.getenv("LASTFM_API_KEY"),
        "method": "user.getRecentTracks",
        "format": "json",
        "user": "cynicalszn",
        "page": int(response["recenttracks"]["@attr"]["page"]) + 1
        if response and len(response["recenttracks"]["@attr"].keys()) > 0
        else 1,
        "from": int(time.mktime(max_timestamp.timetuple())) - 1,
    }

    response = requests.get(f"http://ws.audioscrobbler.com/2.0/", params=payload)

    response = json.loads(response.text)

    [tracks.append(x) for x in response["recenttracks"]["track"]]

    if (
        response["recenttracks"]["@attr"]["page"]
        == response["recenttracks"]["@attr"]["totalPages"]
    ):
        break

    time.sleep(0.5)


lastfm_df = pd.DataFrame.from_records(tracks)

lastfm_df = lastfm_df.drop(columns=["streamable", "mbid"])
lastfm_df = lastfm_df.rename(
    mapper={"@attr": "now_playing", "date": "played_on"}, axis="columns"
)
lastfm_df.insert(loc=0, column="entry_id", value=None)

lastfm_df.artist = lastfm_df.artist.apply(lambda x: x["#text"])

lastfm_df.image = lastfm_df.image.apply(
    lambda x: "".join(
        [x["#text"] for x in x if len(x) > 0 and x["size"] == "extralarge"]
    )
)

lastfm_df.album = lastfm_df.album.apply(lambda x: x["#text"])

lastfm_df["now_playing"] = (
    lastfm_df.now_playing.apply(
        lambda x: True
        if isinstance(x, dict)
        and "nowplaying" in x.keys()
        and x["nowplaying"] == "true"
        else False
    )
    if "now_playing" in lastfm_df.columns.to_list()
    else False
)

lastfm_df.played_on = lastfm_df.played_on.apply(
    lambda x: datetime.fromtimestamp(int(x["uts"]))
    if isinstance(x, dict) and "uts" in x.keys()
    else now()
)

lastfm_df = lastfm_df.loc[lastfm_df.now_playing == False].copy()

lastfm_df["etl_datetime"] = now()

lastfm_df.entry_id = range(1, len(lastfm_df) + 1)


def spotify_search(query: str):
    return spotify.search(q=query, type="track", limit=1)


spotify_df = lastfm_df[
    [
        "entry_id",
        "name",
        "artist",
    ]
].copy()

spotify_df["search_string"] = (
    "track:"
    + spotify_df["name"].str.strip().str.replace("['()]", "", regex=True)
    + " artist:"
    + spotify_df["artist"].str.strip()
)


spotify_df["spotify_track_info"] = spotify_df.search_string.apply(
    lambda x: spotify_search(query=x)
)

spotify_df["spotify_album_url"] = spotify_df.spotify_track_info.apply(
    lambda x: x["tracks"]["items"][0]["album"]["external_urls"]["spotify"]
    if len(x["tracks"]["items"]) > 0
    else None
)

spotify_df["album_image_spotify"] = spotify_df.spotify_track_info.apply(
    lambda x: x["tracks"]["items"][0]["album"]["images"][0]["url"]
    if len(x["tracks"]["items"]) > 0
    and len(x["tracks"]["items"][0]["album"]["images"]) > 0
    else None
)

spotify_df["album_release_date"] = spotify_df.spotify_track_info.apply(
    lambda x: np.datetime64(x["tracks"]["items"][0]["album"]["release_date"])
    if len(x["tracks"]["items"]) > 0
    else None
)

spotify_df["spotify_song_url"] = spotify_df.spotify_track_info.apply(
    lambda x: x["tracks"]["items"][0]["external_urls"]["spotify"]
    if len(x["tracks"]["items"]) > 0
    else None
)

spotify_df["spotify_song_id"] = spotify_df.spotify_track_info.apply(
    lambda x: x["tracks"]["items"][0]["id"] if len(x["tracks"]["items"]) > 0 else None
)

spotify_df["song_artists_all"] = spotify_df.spotify_track_info.apply(
    lambda x: x["tracks"]["items"][0]["artists"]
    if len(x["tracks"]["items"]) > 0
    else None
).apply(lambda x: ", ".join([a["name"] for a in x]) if isinstance(x, list) else None)


spotify_df["track_duration"] = spotify_df.spotify_track_info.apply(
    lambda x: x["tracks"]["items"][0]["duration_ms"]
    if len(x["tracks"]["items"]) > 0
    else None
)

spotify_df["track_popularity"] = spotify_df.spotify_track_info.apply(
    lambda x: x["tracks"]["items"][0]["popularity"]
    if len(x["tracks"]["items"]) > 0
    else None
)

spotify_df["processed_flag"] = True

df = pd.merge(
    right=lastfm_df, left=spotify_df, on=["entry_id", "name", "artist"], validate="1:1"
)

df = df[
    ["entry_id"] + sorted([x for x in df.columns.tolist() if x not in ["entry_id"]])
]


df = pd.merge(
    right=lastfm_df, left=spotify_df, on=["entry_id", "name", "artist"], validate="1:1"
)

if df.columns.tolist() == saved_df.columns.tolist():
    print(f"{now()} ---> {len(df)} records appended")
    df = pd.concat([df, saved_df]).sort_values(by="played_on", ascending=False)
    df = df.drop_duplicates(subset=["played_on"])
    df.entry_id = range(1, len(df) + 1)

    df.to_pickle("history")
    df.to_excel("history.xlsx", index=False, engine="xlsxwriter", sheet_name="Tracker")

end = time.perf_counter()

print(f"\n--------- Finished executing in {round(end-start, 1)} seconds ---------\n")
