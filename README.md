MedlineXmlToDatabase
====================

This is a Java application for loading MEDLINE XML files into a relational database (currently supporting SQL Server and PostgreSQL). The application was designed with two goals in mind:

1. Everything in the XML files needs to go into the database.

2. Any changes in the XML structure that occur over the years should not require changing the program.

The application is run in two phases:

1. During *analysis*, the structure and contents of a large set of XML files is analysed, and a database structure is build to accomodate the data. This is typically done only once a year.

2. During *parse*, all XML files in a folder are parsed and their contents are inserted into the database. This is typically done every time new XML files are available from MEDLINE.

This is still very much work in progress. See the MainClass to see how one could run this Java project. If you have Ant installed on your system, you might be able to load MEDLINE using Ant targets (see below). Note that you would need  your customized 'ini' file to be called 'MedlineXmlToDatabase.ini' and be located in this folder.

Note that the application works directly of the GZipped XML files, so no need to unzip them.

How to use
==========

- Download all xml.gz files from MEDLINE (see http://www.nlm.nih.gov/databases/license/license.html for licensing information)

- Create an ini file according to the example in the iniFileExamples folder, pointing to the folder containing the xml.gz files, and the server and schema where the data should be uploaded

<<<<<<< HEAD
- Compile all java classes. If you have Ant installed you can run the 'compile' target

- Run the MainClass with parameters ```-analyze -ini <path to ini file>``` to create the database structure. If you have Ant installed you can run the 'run-analyzer' target. On Linux, run it using nohup so that the process is not dependent on the terminal session that starts it: 'nohup ant -noinput run-analyzer &'

- Run the MainClass with parameters ```-parse -ini <path to ini file>``` to load the data from the xml files into the database. If you have Ant installed you can run the 'run-parse' target. On Linux, run it using nohup so that the process is not dependent on the terminal session that starts it: 'nohup ant -noinput run-parse &'


-----------------------------------------------------------------
Additional notes
-----------------------------------------------------------------

-- Creating the postgres user and database on Ubuntu 14
$ sudo -i -u postgres
postgres$ createuser --interactive
# create the medline user
postgres$ createdb medline
postgres$ psql
postgres: ALTER ROLE medline WITH PASSWORD '<password>'
postgres: \q

-- Deleting all tables from the schema (works for postgres)
-- 1) run the following query as the database user from within psql
-- 2) copy and paste the output within psql
SELECT 'drop table if exists "' || tablename || '" cascade;'
FROM pg_tables
WHERE schemaname = 'public';

-- Backup of the 'medline' database can be done from pgadmin. The
-- backup file for 'medline' can then be moved to a different database
-- and restored using the following commands as the postgres super-user:
$ sudo -i -u postgres
postgres$ createuser --interactive
# create the medline user
postgres$ createdb -T template0 medline
postgres$ pg_restore -d medline <path to the dump file>
=======
- Run the MainClass with parameters ```-analyse -ini <path to ini file>``` to create the database structure

- Run the MainClass with parameters ```-parse -ini <path to ini file>``` to load the data from the xml files into the database



>>>>>>> 20a83acf5eab2a59eb2e9e16947666ee5e7a5ebf


