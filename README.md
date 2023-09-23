# Lacrosse-Game-Stream---ETL-pipeline

## The environment:
The environment is a docker-compose.yaml file that simulates a distributed environment. It consists of the following services:

Databases:

mssql - A Microsoft SQL Server database that stores the player and team reference data. The database is called sidearmdb and the tables are players and teams.
minio - An S3 compatible object store that contains the live game stream. The game stream is stored in the minio/gamestreams bucket.
mongodb - A mongodb database that stores the game stream's real-time box score so the web developers can create a page from the data. The box score is written to the mongodb/sidearm/boxscores collection.
Tools:

drill - An instance of Apache Drill that can be used to query the databases. The drill-storage-plugins folder contains the configuration files for the databases. You will need to modify these with specifics for them to work.
jupyter - An instance of Jupyter Lab that can be used to write PySpark code. The work folder contains the Start.ipynb that demonstrates the base spark configuration.
Scripts:

gamestream - A python script that simulates a lacrosse game stream. It writes the game stream to a file in the minio/gamestreams S3 bucket.

## The Problem
The objective is to create a data pipeline that processes a simplified, in game stream from a simulated a lacrosse game. The game stream has been simplified to only process goals scored. There are two parts to this problem:

At any point while the game is in progress, the game stream should be converted into a JSON format so the web developers can use it to create a box score page. This JSON should be written to the mongodb/sidearm/boxscores collection, and should contain all the data necessary to display the box score page from a single query to the database.
When the game is over, the player and team reference should be updated to reflect the team records and player statistics. Normally you would update the mssql tables, but for this exam you will create new tables with the updated data, players2 and teams2 respectively. This is mostly because spark does not support row-level updates. In a real world scenario, you would write an SQL script on mssql to update the tables. from the changes in the players2 and teams2 tables, but that is outside the scope of this exam.

## Game Stream
While the game is going on, there is a file called gamestream.txt located in the minio/gamestreams S3 bucket. Each time an in-game event happens, the event is appended to this file. To simplify things, the game stream only reports shots on goal. Here is the format of the file each line is an event and the fields are separated by a space:

<img width="170" alt="image" src="https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/ccdd2a5f-539b-4905-a79f-4e9cb108b197">


## Data Dictionary for gamestream.txt
The first column is the event ID. These are sequential. An event ID of -1 means the game is over.
The second column is the timestamp of the event in the format mm:ss. This counts down to 00:00. For example the first event occurred 9 seconds into the game.
The third column is the team ID, indicating team took the shot on goal. In the simulation there are only two teams, 101 and 205.
the fourth colum is the jersey number of the player who took the shot.
the final column is a 1 if the shot was a goal, 0 if it was a miss.

## Player and Team Reference Data
The player and team reference data is stored in a Microsoft SQL Server database. The database is called sidearmdb . The database has two tables, players and teams with the following schemas, respectively:

<img width="508" alt="image" src="https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/0eab5c33-9ac1-4d9c-8c5b-7c164f167191">

The teams table, only has two teams, 101 = syracuse and 205 = johns hopkins. Each team has a conference affiliation, and a current win / loss record.

The players table has 10 players for each team. Each player has a name, jersey number, shots taken, goals scored, along with their team id.

## Questions

1. Write a drill SQL query to list the team and player data. Specifically display team name, team wins, team losses player name, player shots and player goals.

2. Write a drill SQL query to display the gamestream. Label each of the columns in the gamestream with their appropriate columns names from the data dictionary.

3. Write pyspark code (in SQL or DataFrame API) to display the gamestream. Label each of the columns in the gamestream with their appropriate columns names from the data dictionary.

4. Write pyspark code (in SQL or DataFrame API) to group the gamestream by team/player jersey number adding up the shots and goals. Specifically:

    - Values dependent on team and player: total shots and goals for each player.
    - Value dependent on only team: total goals (this should repeat for every row with the same team id)
    - The last event id and timestamp for that point in time in the game (every row should have the same event ID and as timestamp as these         represent the point in time when the stats were compiled)
    For example (sample - not the actual data):

        <img width="681" alt="image" src="https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/7a49c4c9-e4c7-454f-8b91-8b4fc55e09aa">


5. Write pyspark code (in SQL or DataFrame API) to join the output from question 4 with the player and team reference data mssql so that you have the data necessary for the box score.

6. Write pyspark code (in SQL or DataFrame API) to transform the output from question 5 into the box score document structure shown in part 3.1.

7. Write pyspark code (in SQL or DataFrame API) to write the box score completed in question 6 to the mongo.sidearm.boxscores collection.

8. Combine parts 4-7 into a single pyspark script that will run the entire process of creating the box score document. Make sure to run this a couple of times while the game stream is going on.

9. Write a drill SQL query to display all the box scores.

10. Write a drill SQL query to display the latest box score.

11. When the game is complete, write pyspark code (in SQL or DataFrame API) update the wins and losses for the teams in the teams table. Specifically, load the teams table and update it, then display the updated data frame.

12. Write pyspark code (in SQL or DataFrame API) to write the updated in question 11 to a new mssql.sidearmdb.teams2 table.

13. When the game is complete, write pyspark code (in SQL or DataFrame API) update the shots and goals for the players in the players table. Specifically, load the players table and update it, then display the updated data frame.

14. Write pyspark code (in SQL or DataFrame API) to write the updated in question 11 to a new mssql.sidearmdb.players table.

15. Re-write drill SQL query from question 1 to use the updated players2 and teams2 tables.
