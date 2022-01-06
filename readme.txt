DavisBase Project Submission :

Team LONDON

The goal of this project is to implement a (very) rudimentary database engine that is based on a simplified file-per-table variation on the SQLite file format, which we call DavisBase.

Running the program :

To run the program execute the run bash script using ./run . Note that this will create a directory named data/ within the 
bin/ folder (if it doesn’t already exist). The bin/data/ directory contains all the .tbl files. 

#!/bin/bash
# Build
cd src
javac -g DavisBasePrompt.java
mv *.class ../bin/

# Run
cd ../bin
java DavisBasePrompt


Summary of Supported Commands :

Our database engine supports the following DDL, DML, and DQL commands. 
All commands are terminated by a semicolon (;). 

DDL (Data Definition Language) :
Show tables – displays a list of all tables in DavisBase. 
Create table – creates a new table file, its associated meta-data, and indexes (if they exist). 
Drop table – removes a table file, its associated meta-data, and indexes (if they exist).
Create index – creates an index file that is associated with a table file. Note that DavisBase only allows indexes to be created on single columns. 
Exit – Cleanly exits DavisBase and saves all table, index, and meta-data information to disk in nonvolatile files. 

The database catalog (i.e. meta-data) shall be stored in two special tables that should exist by default: davisbase_tables and davisbase_columns. 

DML (Data Manipulation Language) :
Insert – inserts a new record into a table file and updates any associated meta-data and indexes. 
Delete – removes a record from a table file and updates any associated meta-data and indexes. 
Update – modifies an existing record in a table file and updates any associated meta-data and indexes. 
INSERT INTO table_name [(column_list)] VALUES (value_list); - Inserts a single record into a table.
DELETE FROM table_name [WHERE condition]; - Deletes one or more records from a a table.
UPDATE table_name SET column_name = value [WHERE condition]; - Modifies one or more records in a table.


Command implementation syntax: 

show tables;

create table person(id int, name text);

insert into person (id, name) values (1001, vivek);

select * from person;

update person set name = abc where id = 1003;

delete from table person where id = 1001;

create index on person(id);

drop table person;

exit;