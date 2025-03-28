# Sparkify Data Warehouse Project

## Project Overview
Sparkify, a music streaming startup, has expanded its user base and music database and aims to migrate its processes and data to the cloud. The data is stored in S3, with JSON log files documenting user activity in the application and JSON metadata files about the app's music catalog.

This project develops an ETL pipeline that extracts data from S3, processes it, and stores it in a set of dimensional tables in a Redshift data warehouse, enabling the analytics team to gain valuable insights into user music listening patterns.

![image](https://github.com/user-attachments/assets/71e87483-36ee-4e7f-b3bd-41ebd0f2478b)

## Database Schema Design

### Staging Tables
- **staging_events**: Stores raw event data from log files
- **staging_songs**: Stores raw song metadata

### Analytical Tables (Star Schema)

#### Fact Table
- **songplays**: Records associated with music playbacks
  - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
- **users**: Users in the application
  - *user_id, first_name, last_name, gender, level*
- **songs**: Songs in the database
  - *song_id, title, artist_id, year, duration*
- **artists**: Artists in the database
  - *artist_id, name, location, latitude, longitude*
- **time**: Timestamps from songplays records broken down into specific time units
  - *start_time, hour, day, week, month, year, weekday*

### Data Model Diagram
  
![Diagram](dbdiagram.png)

The diagram illustrates the star schema structure and relationships between tables:
- The fact table `songplays` is centrally located, connected to all dimension tables
- Arrows show foreign key relationships between tables
- Staging tables (`staging_events` and `staging_songs`) feed the analytical model
- `songplays` filters only events where `page = 'NextSong'`, ensuring only music playback events are recorded

### Schema Design Rationale
This project implements a star schema optimized for music playback analysis. The schema includes:

1. A centralized fact table (**songplays**) focusing on user music playbacks
2. Four dimension tables (**users**, **songs**, **artists**, **time**) providing descriptive attributes

This design offers the following benefits:
- Simple and intuitive structure for business users
- Fast aggregations across all dimensions
- Efficiency for known query patterns
- Reduced need for complex joins
- Performance optimized with Redshift distribution and sort keys

## ETL Pipeline

1. Loads configurations from `dwh.cfg` using the `get_config()` utility function
2. Displays detailed information about available S3 files for processing
3. Processes individual music files, extracting data for `songs` and `artists` tables
4. Processes individual log files, extracting data for `time`, `users`, and `songplays` tables
5. Provides real-time feedback on processing progress, including performance metrics

## Project Files

- **utils.py**: Central module with shared utility functions
- **sql_queries.py**: Contains all SQL queries used in ETL and analysis processes, now using dictionaries for better organization
- **create_tables.py**: Creates database tables with detailed feedback
- **etl.py**: Implements ETL process with individual file processing and monitoring
- **run_analytics.py**: Executes predefined analytical queries using the query dictionary
- **dwh.cfg**: Configuration file for AWS credentials and S3 file paths
- **requirements.txt**: List of dependencies required to run the project

## Analytical Queries

The project now uses a dictionary to organize analytical queries, providing:

1. **Better Organization**: Each query is directly associated with its descriptive name
2. **Simplified Maintenance**: Easy to add, remove, or modify queries
3. **Cleaner Code**: Elimination of separate lists for queries and their names

Available analytical queries:
- **Top 10 Most Popular Songs**: Identifies songs with the highest number of playbacks
- **User Activity by Hour of Day**: Analyzes usage patterns throughout the day
- **Free vs. Paid User Distribution**: Compares the number of users in each subscription level
- **Top 5 Locations by User Count**: Identifies regions with the most users
- **Most Active Users**: Lists users who listen to the most music
- **Music Playbacks by Day of Week**: Analyzes usage patterns across the week

## How to Execute

Redshift Cluster Management Tool

A Python script that uses boto3 to manage an Amazon Redshift cluster. It sets up an IAM role for S3 access, creates and configures the cluster, and updates 'dwh.cfg'. It can also delete the cluster and role. Config uses 'dwh.cfg' and '.env', with prompts for missing values.

   ```
   python create_tables.py
   ```

Options:
  1: Set up cluster (prompts for config, updates 'dwh.cfg', opens ports)
  2: Delete cluster and IAM role


1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure your AWS credentials in `dwh.cfg`

3. Create database tables:
   ```
   python create_tables.py
   ```

4. Run the complete ETL process:
   ```
   python etl.py
   ```


5. Execute predefined analytical queries:
   ```
   python run_analytics.py
   ```
