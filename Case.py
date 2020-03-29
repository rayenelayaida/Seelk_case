# Main entry point for DataFrame and SQL functionality.
from pyspark.sql import SparkSession
 
# To interact with various spark’s functionality (create DataFrame, register DataFrame as tables, execute SQL over tables, cache tables, and read parquet files).
spark = SparkSession.builder \
	.master("local") \
	.appName("My Test") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()


#################################################### Original #################################################################


# Read the csv file "winemag-data-130k-v2.csv" into a Spark DataFrame "df"
df = spark.read.csv("winemag-data-130k-v2.csv",header=True,sep=",");

# Get first rows of the Spark DataFrame "df" ( remove '#' below)
#df.toPandas().head()

# Save the contents of the Spark DataFrame "df" as a Parquet file named "Original.parquet", preserving the schema.
df.write.parquet("Original.parquet")

# Read back "Original.parquet" file in a Spark DataFrame named "Original_parquet_format".
Original_parquet_format  = spark.read.parquet("Original.parquet")

# Get first rows of the Spark DataFrame "Original_parquet_format" ( remove '#' below)
#Original_parquet_format.toPandas().head()


#################################################### Cleaned ###################################################################

Text_columns_list= ['country','description','designation','province','region_1','region_2',\
             'taster_name','taster_twitter_handle','title','variety','winery']

# Copy the content of the Spark DataFrame "Original_parquet_format" into a new spark DataFrame "df_clean"
df_clean = Original_parquet_format

# Drop all the text columns of "Text_columns_list" from the Spark DataFrame "df_clean" 
for col in Text_columns_list:
	df_clean =  df_clean.drop(col)  


# Change the column name from "_c0" to "Id".
df_clean = df_clean.withColumnRenamed("_c0", "Id")

# Get first rows of the Spark DataFrame "df_clean" (only id, points, price) ( remove '#' below)
#df_clean.toPandas().head()

# Save the contents of the Spark DataFrame "df_clean" as a Parquet file named "Cleaned.parquet", preserving the schema.
df_clean.write.parquet("Cleaned.parquet")

#  Read back "Cleaned.parquet" file in a Spark DataFrame named "Cleaned_parquet_format".
Cleaned_parquet_format  = spark.read.parquet("Cleaned.parquet")

# Get first rows of the Spark DataFrame "Cleaned_parquet_format". (only id, points, price) ( remove '#' below)
#Cleaned_parquet_format.toPandas().head()


#################################################### Aggregated ################################################################


# Aggregate the data by country, and calculate the standard deviation of points and store the result into a Spark dataFrame "df_Std"..
df_Std = Original_parquet_format.groupby("country").agg({"points" :"stddev"})

# Aggregate the data by country, and calculate the average of points and store the result into a Spark dataFrame "df_Mean".
df_Mean = Original_parquet_format.groupby("country").agg({"points" : "mean"})

# Change the column name from "stddev(points)" to "StandardDeviation".
df_Std = df_Std.withColumnRenamed("stddev(points)", "StandardDeviation")

# Change the column name from "avg(points)" to "Average".
df_Mean = df_Mean.withColumnRenamed("avg(points)", "Average")

# Combine the two DataFrames "df_Mean" and "df_Std".Match is performed on column( "Country").
df_Aggregation= df_Std.join(df_Mean, on=['country'], how='inner')

# Get first rows of the Spark DataFrame "df_Aggregation". (Country, StandardDeviation, Average) ( remove '#' below)
#df_Aggregation.toPandas().head()

# Save the contents of the Spark DataFrame "df_Aggregation" as a Parquet file named "Aggregated.parquet", preserving the schema.
df_Aggregation.write.parquet("Aggregated.parquet")

# Read back "Aggregated.parquet" file in a Spark DataFrame named "Aggregated_parquet_format".
Aggregated_parquet_format = spark.read.parquet("Aggregated.parquet")

# Get first rows of the Spark DataFrame "Aggregated_parquet_format". (Country, StandardDeviation, Average) ( remove '#' below)
#Aggregated_parquet_format.toPandas().head()


