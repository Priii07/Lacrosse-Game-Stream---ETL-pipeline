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
    - The first column is the event ID. These are sequential. An event ID of -1 means the game is over.
    - The second column is the timestamp of the event in the format mm:ss. This counts down to 00:00. For example the first event occurred        9 seconds into the game.
    - The third column is the team ID, indicating team took the shot on goal. In the simulation there are only two teams, 101 and 205.
    - The fourth colum is the jersey number of the player who took the shot.
    - The final column is a 1 if the shot was a goal, 0 if it was a miss.

## Player and Team Reference Data
The player and team reference data is stored in a Microsoft SQL Server database. The database is called sidearmdb . The database has two tables, players and teams with the following schemas, respectively:

<img width="508" alt="image" src="https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/0eab5c33-9ac1-4d9c-8c5b-7c164f167191">

The teams table, only has two teams, 101 = syracuse and 205 = johns hopkins. Each team has a conference affiliation, and a current win / loss record.

The players table has 10 players for each team. Each player has a name, jersey number, shots taken, goals scored, along with their team id.

## Questions

1. Write a drill SQL query to list the team and player data. Specifically display team name, team wins, team losses player name, player shots and player goals.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/c723f1db-773b-4c53-b2d0-dcf4434ab465)

2. Write a drill SQL query to display the gamestream. Label each of the columns in the gamestream with their appropriate columns names from the data dictionary.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/c79e3021-e8b0-41f6-a5c7-3ed9191da452)

3. Write pyspark code (in SQL or DataFrame API) to display the gamestream. Label each of the columns in the gamestream with their appropriate columns names from the data dictionary.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/84ddb06b-ec50-4a3d-9f8e-988d39cf35f0)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/41f89c00-db9e-4ca6-81b4-03fefad4f0dd)


4. Write pyspark code (in SQL or DataFrame API) to group the gamestream by team/player jersey number adding up the shots and goals. Specifically:

    - Values dependent on team and player: total shots and goals for each player.
    - Value dependent on only team: total goals (this should repeat for every row with the same team id)
    - The last event id and timestamp for that point in time in the game (every row should have the same event ID and as timestamp as these         represent the point in time when the stats were compiled)
    For example (sample - not the actual data):

        <img width="681" alt="image" src="https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/7a49c4c9-e4c7-454f-8b91-8b4fc55e09aa">

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/76d67077-289f-4b54-9dad-f0117976e5ce)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/54421c84-8902-480f-82ae-223165ade821)


5. Write pyspark code (in SQL or DataFrame API) to join the output from question 4 with the player and team reference data mssql so that you have the data necessary for the box score.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/65653d9d-2543-4f76-92f1-6218c592483d)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/5d08a8fc-d629-4870-91ba-f80d04ea1dcd)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/174cf11b-c134-48fb-afec-6cbe02b4d09a)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/b14b576c-dca9-4845-b0d1-943da97590e2)


6. Write pyspark code (in SQL or DataFrame API) to transform the output from question 5 into the box score document structure shown in part 3.1.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/80930ba4-51c2-4623-b5e2-53ce45e8330a)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/62c50c15-8cdf-44f7-bc26-a885fe0e91bd)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/bede4537-a591-4c1b-adbe-afacb0d7e45c)


7. Write pyspark code (in SQL or DataFrame API) to write the box score completed in question 6 to the mongo.sidearm.boxscores collection.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/24b99ab0-b70f-4d0c-8c19-2c76d1d4c252)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/0c3802e9-8ac9-46a2-b4e3-27d0605f597b)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/3abbb62a-041b-4187-af8c-c4c6ae020f03)


8. Combine parts 4-7 into a single pyspark script that will run the entire process of creating the box score document. Make sure to run this a couple of times while the game stream is going on.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/6d1ab0a3-5e33-4b9c-b016-1255233d7bbf)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/c6ac290c-caac-4faa-a7b4-8baaef22be46)


9. Write a drill SQL query to display all the box scores.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/12cd5c15-711d-491b-9a5d-87e0d1375b38)


10. Write a drill SQL query to display the latest box score.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/b9adc8a5-d445-4c28-be30-6824afb29214)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/65bd236d-bbfc-4337-a672-281ef226c809)


11. When the game is complete, write pyspark code (in SQL or DataFrame API) update the wins and losses for the teams in the teams table. Specifically, load the teams table and update it, then display the updated data frame.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/a685a0e8-20ea-4f3d-aa39-d3c100e35de0)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/da1b8760-783f-4366-b633-cc0b2aa8c1b9)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/77904276-e6bd-42c0-9cf8-e3ef5c88d8b8)


12. Write pyspark code (in SQL or DataFrame API) to write the updated in question 11 to a new mssql.sidearmdb.teams2 table.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/427b5d80-2173-4b77-bd82-792864bf9044)


13. When the game is complete, write pyspark code (in SQL or DataFrame API) update the shots and goals for the players in the players table. Specifically, load the players table and update it, then display the updated data frame.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/797b7a6b-0ac4-4dcd-a09e-038426a8058c)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/8277181c-6ef1-41c9-bb2a-6bde8a01c9b0)


14. Write pyspark code (in SQL or DataFrame API) to write the updated in question 11 to a new mssql.sidearmdb.players table.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/fac35b63-d0b2-4ab4-9762-74ce45d42fc5)


15. Re-write drill SQL query from question 1 to use the updated players2 and teams2 tables.

![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/c3c26941-2e63-489d-9f13-371b19b207b3)
![image](https://github.com/Priii07/Lacrosse-Game-Stream---ETL-pipeline/assets/50296254/8928a9fc-2a8e-4f80-98e0-f47e0622efa6)


