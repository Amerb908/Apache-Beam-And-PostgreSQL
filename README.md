# Data Transfer Pipeline: PostgreSQL to MySQL

## Overview

#### This README describes a data transfer pipeline designed to move data from a PostgreSQL database to a MySQL database. The pipeline utilizes Apache Beam, a distributed data processing framework, to extract data from PostgreSQL and insert it into MySQL. By orchestrating this process, the pipeline enables seamless migration or synchronization of data between different database systems.

#### MySQL.py defines a pipeline that reads data from a CSV file and transfers it to a MySQL database. The pipeline uses Apache Beam to create a PCollection from the CSV file, and then uses ParDo to write the data to a MySQL table. If a record already exists in the table with the same primary key, the pipeline updates the record with the new data. Otherwise, it inserts a new record.

### transfer_pipeline.py defines a pipeline that reads data from a PostgreSQL database and transfers it to a MySQL database. The pipeline uses Apache Beam to create a PCollection from the PostgreSQL table, and then uses ParDo to write the data to a MySQL table. If a record already exists in the table with the same primary key, the pipeline updates the record with the new data. Otherwise, it inserts a new record.


