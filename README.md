
# A Simple ETL for sanitizing data before loading into MySQL

## Main
ETL is automatically run upon start up of the docker container.
No execution is required. 
Just navigate to any database tools and connect to the database and view the data

## Installation
```
cd python-etl

# Start up the docker
docker-compose build
docker-compose up

# List docker options
docker ps

# Check docker logs
docker logs python-etl_python-etl_1

# Enter to the docker container
docker exec -it python-etl_python-etl_1 bash

# Exit docker container
exit

# to view the data:
go to browser and type localhost:8084
System = mysql
Server = python-etl_database_1
Username = root
Password = password
Database = testdb

# Once done, remove the docker container
docker-compose down
```