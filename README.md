# ETL Pipeline Project
The project is to create a ETL pipeline which initializes a database and be loaded with data extracting and transforming from songs megadata and users activities log json files. The database serves for Sparkify,a startup company, to easily access and analyze songs and users activities on their music streaming app and help them understanding what songs users are listening to.

### Table of Contents

1. [Installation](#installation)
2. [Project Motivation](#motivation)
3. [File Descriptions](#files)
4. [Database Descriptions](#Database)
5. [Results](#results)
6. [Licensing, Authors, and Acknowledgements](#licensing)


# Installation<a name="installation"></a>
The python file should be run with python 3.X .

# Project Motivation<a name="motivation"></a>
Built a ETL pipeline automation by SQL and phthon and created a database and tables which stored ETL results. Facilitated data analysis process with the database and tables.


# File Descriptions<a name="files"></a>
There are 4 python files implement the product.

create_tables.py--create a new database and tables.

etl.py--etl pipeline extracting and transforming data from json files and loading them into tables.

sql_queries.py--DLL,DML and queries for creating tables and inserting data into tables.

test.ipynb--connect the database and test queries of tables.

# Database Descriptions<a name="Database"></a>
The database has 5 tables, which are song_play, songs, users,time, artists. 

Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, userId, level, song_id, artist_id, sessionId, location, userAgent

Dimension Tables
users - users in the app
userId, first_name, last_name, gender, level
![users table](/images/users.png)
songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, artist_name, artist_location, artist_latitude, artist_longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday


# Results<a name="results"></a>
The database and tables was establised and were loaded with data extracted and transformed from original json files. 

# Licensing, Authors, Acknowledgements<a name="licensing"></a>
Must give credit to  [Udacity](http://www.udacity.com) for project design and instructions.


