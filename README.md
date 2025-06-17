# School Safety Analysis

the datasets we are going to use are :

## Crime Dataset

https://www.kaggle.com/datasets/ishajangir/crime-data 

Description:
This dataset contains detailed records of crimes reported across various regions from 2020 to the present. It provides valuable insights into crime trends, patterns, and changes in crime rates over time. The data is suitable for researchers, data analysts, law enforcement agencies, and policymakers looking to analyze crime dynamics or develop predictive models to enhance public safety measures.

The link to download the data is provided below: \
(you don’t need to download it manually when executing the pipeline, as this process is already included in our spark-job.py)

wget http://d3jieva0erjrjq.cloudfront.net/Crime_Data.csv

## School Dataset

https://www.kaggle.com/datasets/andrewmvd/us-schools-dataset/data

Description:
This dataset, taken from the US Department of Homeland Security, contains information on all public and private schools with attributes regarding their geographical distribution.

The link to download the data is provided below: \
(you don’t need to download it manually when executing the pipeline, as this process is already included in our spark-job.py)

wget http://d3jieva0erjrjq.cloudfront.net/Private_Schools.csv

wget http://d3jieva0erjrjq.cloudfront.net/Public_Schools.csv

## Our Goal

Our project will analyze U.S. private and public school data alongside crime data to identify patterns, correlations, and insights related to education and crime trends. While schools are relatively well-distributed, our focus will be on exploring how the type of school (public vs. private), their concentration in certain areas, and the socio-economic profiles of those areas might influence crime rates. The goal is to create an interactive dashboard that allows users to explore these relationships, particularly how income levels and the presence of different types of schools correlate with crime rates across various geographic regions. Based on insights from our visualizations, we will also provide actionable recommendations to improve safety around schools, helping to reduce the risk of crime and create a more secure environment for students.

 ## Tableau Visualization

 https://public.tableau.com/views/405_final/Dashboard2?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

## Pipeline Process

This project uses a data pipeline that integrates Spark and DuckDB to analyze the relationship between schools and crime data in California. The pipeline is run by the pipeline.sh bash script and consists of the following steps: 

1. `spark-job.py`  – Data Cleaning and Joining

This **Spark job** handles the data downloading, preprocessing, and spatial join: 

 •	Downloads crime, public school, and private school datasets from Kaggle using kagglehub. \
	•	Uses H3 spatial indexing to convert latitude/longitude into hexagonal index for spatial join. \
	•	Cleans and processes crime data (e.g., date formatting, victim info, crime severity). \
	•	Processes school data, categorizes by level (Elementary/Middle/High), and calculates faculty size. \
	•	Joins crime data with nearby schools using H3 neighbors. \
	•	Outputs the joined data as Parquet files into the output/ folder. 

2. `queries.sql` – Analysis with DuckDB

The **DuckDB SQL script** performs the following operations:

 •	Reads the generated Parquet files into a DuckDB database (final.db). \
	•	Creates an indexed table crime_near_schools. \
	•	Creates a view crime_vs_faculty to calculate and compare crime counts per school and normalize it by faculty size.  

3. Running the Pipeline

To execute the full pipeline, simply run: 

`bash pipeline.sh`

This script will: \
	1.	Remove any previous outputs and database. \
	2.	Run the Spark job to generate the transformed data. \
	3.	Load the data into DuckDB and run the analysis queries.
