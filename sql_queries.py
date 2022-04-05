"""This module keeps the SQL queries for the ETL pipeline."""

from aws_params import get_params
from aws_iam_roles import get_iam_client
from aws_iam_roles import get_role_arn

# GET CONFIG PARAMS
awsParams = get_params()

iam_client = get_iam_client(
        region_name=awsParams.REGION,
        aws_access_key_id=awsParams.KEY,
        aws_secret_access_key=awsParams.SECRET,
    )

role_arn = get_role_arn(iam_client, awsParams.IAM_ROLE_NAME)

# DROP TABLES

drop_if_exists = """DROP TABLE IF EXISTS {};"""

staging_events_table_drop = drop_if_exists.format('"staging_events"')
staging_songs_table_drop = drop_if_exists.format('"staging_songs"')
songplay_table_drop = drop_if_exists.format('"songplays"')
user_table_drop = drop_if_exists.format('"users"')
song_table_drop = drop_if_exists.format('"songs"')
artist_table_drop = drop_if_exists.format('"artists"')
time_table_drop = drop_if_exists.format('"time"')

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE "staging_events" (
    "artist"            TEXT,
    "auth"              TEXT,
    "firstName"         TEXT,
    "gender"            CHAR,
    "itemInSession"     INT,
    "lastName"          TEXT,
    "length"            FLOAT,
    "level"             TEXT,
    "location"          TEXT,
    "method"            TEXT,
    "page"              TEXT,
    "registration"      DOUBLE PRECISION,
    "sessionId"         INT,
    "song"              TEXT,
    "status"            INT,
    "ts"                BIGINT,
    "userAgent"         TEXT,
    "userId"            INT
);
"""

staging_songs_table_create = """
CREATE TABLE "staging_songs" (
    "num_songs"         INT,
    "artist_id"         TEXT,
    "artist_latitude"   FLOAT,
    "artist_longitude"  FLOAT,
    "artist_location"   TEXT,
    "artist_name"       TEXT,
    "song_id"           TEXT,
    "title"             TEXT,
    "duration"          FLOAT,
    "year"              INT
);
"""

songplay_table_create = """
CREATE TABLE "songplays"
(
    "songplay_id"   INT IDENTITY(0,1) PRIMARY KEY,
    "start_time"    BIGINT,
    "user_id"       INT,
    "level"         TEXT,
    "song_id"       TEXT,
    "artist_id"     TEXT,
    "session_id"    INT,
    "location"      TEXT,
    "user_agent"    TEXT
);
"""

user_table_create = """
CREATE TABLE "users"
(
    "user_id"       INT     PRIMARY KEY,
    "first_name"    TEXT    NOT NULL,
    "last_name"     TEXT    NOT NULL,
    "gender"        TEXT,
    "level"         TEXT    NOT NULL
);
"""

song_table_create = """
CREATE TABLE "songs"
(
    "song_id"   TEXT    PRIMARY KEY,
    "title"     TEXT    NOT NULL,
    "artist_id" TEXT    NOT NULL,
    "year"      INT,
    "duration"  FLOAT
);
"""

artist_table_create = """
CREATE TABLE "artists"
(
    "artist_id" TEXT    PRIMARY KEY,
    "name"      TEXT    NOT NULL,
    "location"  TEXT,
    "latitude"  FLOAT,
    "longitude" FLOAT
);
"""

time_table_create = """
CREATE TABLE "time"
(
    "start_time"    BIGINT      PRIMARY KEY,
    "hour"          INT         NOT NULL,
    "day"           INT         NOT NULL,
    "week"          INT         NOT NULL,
    "month"         INT         NOT NULL,
    "year"          INT         NOT NULL,
    "weekday"       INT         NOT NULL
);
"""

# STAGING TABLES

staging_events_copy = (
    f"COPY staging_events FROM {awsParams.LOG_DATA}"
    f"CREDENTIALS 'aws_iam_role={role_arn}' "
    f"json {awsParams.LOG_JSONPATH};"
)

staging_songs_copy = (
    f"COPY staging_songs FROM {awsParams.SONG_DATA} "
    f"CREDENTIALS 'aws_iam_role={role_arn}' json 'auto';"
)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays
(
    start_time, user_id, level, session_id, location, user_agent,
    artist_id,
    song_id
)
VALUES
(
    %s, %s, %s, %s, %s, %s,
    (
        SELECT max(artist_id)
        FROM artists
        WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))
        GROUP BY LOWER(TRIM(name))
    ),
    (
        SELECT song_id
        FROM songs
        WHERE LOWER(TRIM(title)) = LOWER(TRIM(%s))
            AND artist_id = (
                SELECT artist_id
                FROM artists
                WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))
            )
    )
)
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
"""

song_table_insert = """
INSERT INTO songs  (song_id, title, artist_id, year, duration)
(
    select song_id, title, artist_id, year, duration
    from staging_songs
);
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
(
    select artist_id, min(artist_name), max(artist_location),
        max(artist_latitude), max(artist_longitude)
    from staging_songs
    group by artist_id
);
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
