import sys
import h3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, to_timestamp, lit, udf, explode
from pyspark.sql.types import StringType, IntegerType, ArrayType
import pandas as pd
import kagglehub
import os

# Define H3 resolution for spatial indexing(street level)
H3_RESOLUTION = 11

# convert latitude and longitude to an H3 index
def lat_lon_to_h3(lat, lon):
    return h3.latlng_to_cell(lat, lon, H3_RESOLUTION)

h3_udf = udf(lat_lon_to_h3, StringType())

#get neighboring H3 indexes
def get_h3_neighbors(h3_index):
    return list(h3.grid_disk(h3_index, 1)) #one layer

h3_neighbors_udf = udf(get_h3_neighbors, ArrayType(StringType()))

# school level based on end grade
def categorize_level(end_grade):
    try:
        grade = int(end_grade) 
        if grade <= 5:
            return "ELEMENTARY"
        elif 6 <= grade <= 8:
            return "MIDDLE"
        elif grade >= 9:
            return "HIGH"
    except ValueError:
        pass  
    return "OTHER"

categorize_level_udf = udf(categorize_level, StringType())

# crime_data_path = "../data/Crime_Data.csv"
# public_schools_path = "../data/Public_Schools.csv"
# private_schools_path = "../data/Private_Schools.csv"

# Download kaggle datasets
crime = kagglehub.dataset_download("ishajangir/crime-data")
schools = kagglehub.dataset_download("andrewmvd/us-schools-dataset")
house_price = kagglehub.dataset_download("fratzcan/usa-house-prices")

# Convert paths to absolute paths for Spark
crime_p = os.path.abspath(crime)
schools_p = os.path.abspath(schools)
house_price_p = os.path.abspath(house_price)

crime_data_path = os.path.join(crime_p, os.listdir(crime_p)[0])
public_schools_path = os.path.join(schools_p, os.listdir(schools_p)[1])
private_schools_path = os.path.join(schools_p, os.listdir(schools_p)[0])

def main():
    print(sys.argv)
    if len(sys.argv) != 2:
        print("Usage: spark-submit spark-job.py [output_path]")
        sys.exit(1)
    # output_path = sys.argv[1]
    output_path = "../output"
    os.makedirs(output_path, exist_ok=True)

    spark = SparkSession.builder \
			 .master("local[*]") \
			 .config("spark.driver.memory", "700m") \
       .config("spark.executor.memory", "700m") \
       .config("spark.sql.shuffle.partitions", "2") \
       .getOrCreate() 

    # Load and preprocess crime data
    crime = spark.read.csv(crime_data_path, header=True, inferSchema=True)

    crime = (
        crime.withColumn("DATE OCC", to_date(to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a")))
             .withColumn("Vict Sex", when(col("Vict Sex").isin(["M", "F"]), col("Vict Sex")).otherwise("Other"))
             .withColumn("Crime Severity", when(col("Part 1-2") == 1, "Serious Crime")
                                          .when(col("Part 1-2") == 2, "Non-Serious Crime"))
             .withColumnRenamed("Crm Cd Desc", "Crime Type")
             .withColumn("h3_index", h3_udf(col("LAT"), col("LON"))) \
             .withColumnRenamed("h3_index", "crime_h3_index") \
             .select("DATE OCC", "LAT", "LON", "crime_h3_index", "Vict Sex", "Vict Age", "Crime Severity", "Crime Type" )
    )
    # Load and preprocess public and private school data
    public = spark.read.csv(public_schools_path, header=True, inferSchema=True)
    private = spark.read.csv(private_schools_path, header=True, inferSchema=True)

    public = (
        public.withColumn("school_type", lit("Public"))
              .withColumn("level", when(col("LEVEL_").isin(["ELEMENTARY", "HIGH", "MIDDLE"]), col("LEVEL_"))
                                  .otherwise("OTHER"))
              .withColumn("faculty", col("POPULATION") - col("ENROLLMENT"))
              .withColumn("h3_index", h3_udf(col("LATITUDE"), col("LONGITUDE")))
	          .filter(col("STATE") == "CA")
    )

    private = (
        private.withColumn("school_type", lit("Private"))
               .withColumn("level", categorize_level_udf(col("END_GRADE")))
               .withColumn("faculty", col("POPULATION") - col("ENROLLMENT"))
               .withColumn("h3_index", h3_udf(col("LATITUDE"), col("LONGITUDE")))
	           .filter(col("STATE") == "CA")
    )

    # Combine public and private school data
    school = private.unionByName(public, allowMissingColumns=True)

    # Generate H3 neighbors for spatial analysis
    school_h3 = (
        school.withColumn("h3_neighbors", h3_neighbors_udf(col("h3_index")))
              .selectExpr("*", "explode(h3_neighbors) as h3_join")
              .select("NAME", "ADDRESS", "CITY", "STATE", "ZIP", "LATITUDE", "LONGITUDE",
                      "h3_index", "h3_join", "school_type", "POPULATION", "ENROLLMENT", "level", "faculty")
    )
    
    #join two data
    crime_near_school = school_h3.join(crime, crime['crime_h3_index'] == school_h3['h3_join'], 'inner')

    # Write final output in Parquet format with a max of 100,000 records per file
    crime_near_school.write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(output_path)
        #.parquet(output_path)
if __name__ == "__main__":
    main()
