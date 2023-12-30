# DataWarehouse On Amazon RedShift

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The aim of the this project is  to build an ETL pipeline for a Datawarehouse hosted on Redshift. Here, I loaded data from S3 to staging tables on Redshift and executed SQL statements that create the analytics tables from these staging tables.

## Sparkify Dataset

Currently, Data is stored for songs available and user log activities that resides in S3, they are in the JSON format.

`Song data`: s3://udacity-dend/song_data
`Log data`: s3://udacity-dend/log_data

### Song dataset structure

```json
{
     "num_songs": 1, 
     "artist_id": "ARD842G1187B997376", 
     "artist_latitude": 43.64856, 
     "artist_longitude": -79.38533, 
     "artist_location": "Toronto, Ontario, Canada", 
     "artist_name": "Blue Rodeo", 
     "song_id": "SOHUOAP12A8AE488E9", 
     "title": "Floating", 
     "duration": 491.12771, 
     "year": 1987}
```

# Log dataset structure

```json
{
     "artist":"Des'ree",
     "auth":"Logged In",
     "firstName":"Kaylee",
     "gender":"F",
     "itemInSession":1,
     "lastName":"Summers",
     "length":246.30812,
     "level":"free",
     "location":"Phoenix-Mesa-Scottsdale, AZ",
     "method":"PUT",
     "page":"NextSong",
     "registration":1540344794796.0,
     "sessionId":139,
     "song":"You Gotta Be",
     "status":200,"ts":1541106106796,
     "userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"",
     "userId":"8"
}
```

## Database Schema Design

### Fact Table

#### `songplay_table`

Records in log data associated with user song plays.

|   Columns    |            Type        | Nullable |
| -----------  | ---------------------  | -------- |
| songplay_id  | serial                 | not null | (Primary Key)
| start_time   | timestamp              | not null |
| user_id      | int                    | not null |
| level        | varchar                | not null |
| song_id      | varchar                |          |
| artist_id    | varchar                |          |
| session_id   | integer                | not null |
| location     | varchar                | not null |
| user_agent   | varchar                | not null |

<p align="center">
<img src="images/songplay_table.png"  >
</p>
    
### Dimensions Table

#### `user_table`

Users of the sparkify

|   Column   |       Type        | Nullable |
| ---------- | ----------------- | -------- |
| user_id    | int               | not null | (Primary Key)
| first_name | varchar           | not null |
| last_name  | varchar           | not null |
| gender     | char(1)           | not null |
| level      | varchar           | not null |
    
<p align="center">
<img src="images/user_table.png">
</p>
 
#### `song_table`

Songs in sparkify database.

|  Column   |         Type       | Nullable |
| --------- | ------------------ | -------- |
| song_id   | varchar            | not null | (Primary Key)
| title     | varchar            | not null |
| artist_id | varchar            | not null |
| year      | int                | not null |
| duration  | float              | not null |

<p align="center">
<img src="images/song_table.png">
</p>
    
#### `artist_table`

Artists in sparkify database.

|  Column   |         Type      | Nullable |
| --------- | ----------------- | -------- |
| artist_id | varchar           | not null | (Primary Key)
| name      | varchar           | not null |
| location  | varchar           | not null |
| latitude  | float             |          |
| longitude | float             |          |
    
<p align="center">
<img src="images/artist_table.png">
</p>
 
Primary key: artist_id

#### `time_tab;e`

Timestamps of records in songplays broken down into specific time units.

|   Column   |            Type          | Nullable |
| ---------- | ------------------------ | -------- |
| start_time | timestamp                | not null | (Primary Key)
| hour       | int                      | not null |
| day        | int                      | not null |
| week       | int                      | not null |
| month      | int                      | not null |
| year       | int                      | not null |
| weekday    | int                      | not null |
    
<p align="center">
<img src="images/time_table.png">
</p>
 
## Running

To run the project, follow the below steps:-
    
#### 1. Run `create_tables.py` to create your database and tables
``` sh
python create_tables.py
```
    
#### 2. Run `etl.py` to process the entire datasets and load data into database
``` sh
python etl.py
```

#### 3. Run `analyze_queries.py` for analyzing queries and also to check whether the datasets is loaded Redshift.
``` sh
python analyze_queries.py
```