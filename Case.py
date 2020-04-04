# Main entry point for DataFrame and SQL functionality.
from pyspark.sql import SparkSession
from matplotlib.pyplot import plot
from pyspark.sql.functions import desc
from pyspark.sql.types import *
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.neighbors import KNeighborsClassifier
import numpy as np
 
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
# Copy the "id","price" and "points" columns of the Spark DataFrame "Original_parquet_format" into a new spark DataFrame "df_clean"
df_clean =  Original_parquet_format.select("_c0","price","points")

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

# Drop the NaN values  from "Aggregated_parquet_format"'s columns 
Aggregated_parquet_format = Aggregated_parquet_format.dropna()

# Get first rows of the Spark DataFrame "Aggregated_parquet_format". (Country, StandardDeviation, Average) ( remove '#' below)
#Aggregated_parquet_format.toPandas().head()

############################################################# BONUS ###################################################################

# BONUS 1 : The top 5 best wines below 10 USD
top_5_best_wines_below_10_USD = Original_parquet_format.select("points","price","designation").orderBy(desc("points")).where("price <  10").limit(5)
# Show the result of the query above ( remove '#' below)
#top_5_best_wines_below_10_USD.show()


# BONUS 2 : The top 5 best wines below 30 USD from Chile
top_5_best_wines_below_30_USD_from_Chile = Original_parquet_format.select("points","price","designation","country").where("country = 'Chile'")
top_5_best_wines_below_30_USD_from_Chile= top_5_best_wines_below_30_USD_from_Chile.select("points","price","designation","country").orderBy(desc("points")).where("price < 30").limit(5)
# Show the result of the query above ( remove '#' below)
#top_5_best_wines_below_30_USD_from_Chile.show()


# BONUS 3 : Create a visualisation of "points" vs "price" from the clean dataset "Cleaned_parquet_format" 
Cleaned_parquet_format = Cleaned_parquet_format.withColumn("points", Cleaned_parquet_format["points"].cast(FloatType()))
Cleaned_parquet_format = Cleaned_parquet_format.withColumn("price", Cleaned_parquet_format["price"].cast(FloatType()))
Cleaned_parquet_format.toPandas().plot(x='points', y='price', style='o',title='Visualisation of points vs price')

# BONUS 4 : Predict the points of a wine taking as input the price and the country (using Machine Learning).


################### Prepare train and test data #####################


# Change the type of both "price" and "points" to float 
Original_parquet_format = Original_parquet_format.withColumn("points", Original_parquet_format["points"].cast(FloatType()))
Original_parquet_format = Original_parquet_format.withColumn("price", Original_parquet_format["price"].cast(FloatType()))

BONUS_4 = Original_parquet_format.select(["points","price","country"])
# delete the Null column of ( "price', "country" , "points")
BONUS_4 = BONUS_4.filter(BONUS_4.price.isNotNull())
BONUS_4 = BONUS_4.filter(BONUS_4.points.isNotNull())
BONUS_4 = BONUS_4.filter(BONUS_4.country.isNotNull())

# convert BONUS_4 to a pandas dataFrame
X = BONUS_4.select("country","price").toPandas()
Y = BONUS_4.select("points").toPandas()

# encode a part of a pandas DataFrame " X["country"] " which have a string type 
le = preprocessing.LabelEncoder()
X["country"] = le.fit_transform(X["country"])

# Split data into random train and test subsets
X_train,X_test,Y_train,Y_test = train_test_split(X,Y,test_size = 0.1,random_state = 0)


################### Accuracy of different models #####################

#  RIDGE  ####################
clf = Ridge(alpha=1.0, random_state=241)
clf.fit(X_train, Y_train) 
clf.score(X_test,Y_test)

# RANDOM FOREST CLASSIFIER  ##############
forest = RandomForestClassifier(n_estimators=50)
forest.fit(X_train,Y_train)         
forest.score(X_test,Y_test)

# LINEAR REGRESSION #################
lr = LinearRegression()
lr.fit(X_train, Y_train)
lr.score(X_test,Y_test)

#  K-NEIGHBORS CLASSIFIER ################
neigh = KNeighborsClassifier(n_neighbors = 260)
neigh.fit(X_train, Y_train)
neigh.score(X_test,Y_test)

########################### Points prediction #############################

#  Using RIDGE ####################

def predict_points_Ridge(Country,Price,X,y):
    # Split data into train and test subsets
    X_train,X_test,Y_train,Y_test = train_test_split(X,Y,test_size = 0.1,random_state = 0)
    clf = Ridge(alpha=1.0, random_state=241)
    clf.fit(X_train, Y_train)
    test =X.loc[(X['country'] == Country) & (X['price'] == Price)]
    prediction_points = clf.predict(test)
    print(prediction_points[0])

#Example: Country = 30 (encoded), price = 15
predict_points_Ridge(30,15.0,X,y)



# Using RANDOM FOREST ##############

def predict_points_RandomForest(Country,Price,X,y):
    # Split data into train and test subsets
    X_train,X_test,Y_train,Y_test = train_test_split(X,Y,test_size = 0.1,random_state = 0)
    forest = RandomForestClassifier(n_estimators=50)
    forest.fit(X_train,np.ravel(Y_train,order='C'))   
    test =X.loc[(X['country'] == Country) & (X['price'] == Price)]
    prediction_points = forest.predict(test)
    print(prediction_points[0])

#Example: Country = 30 (encoded), price = 15
predict_points_RandomForest(30,15.0,X,y)


# Using LINEAR REGRESSION  #################

def predict_points_LinearRegression(Country,Price,X,y):
    # Split data into train and test subsets
    X_train,X_test,Y_train,Y_test = train_test_split(X,Y,test_size = 0.1,random_state = 0)
    lr = LinearRegression()
    lr.fit(X_train, Y_train)  
    test =X.loc[(X['country'] == Country) & (X['price'] == Price)]
    prediction_points = lr.predict(test)
    print(prediction_points[0])

#Example: Country = 30 (encoded), price = 15
predict_points_LinearRegression(30,15.0,X,y)



#  K-NEIGHBORS CLASSIFIER   ################

def predict_points_KNeighborsClassifier(Country,Price,X,y):
    # Split data into train and test subsets
    X_train,X_test,Y_train,Y_test = train_test_split(X,Y,test_size = 0.1,random_state = 0)
    neigh = KNeighborsClassifier(n_neighbors = 260)
    neigh.fit(X_train, np.ravel(Y_train,order='C')) 
    test =X.loc[(X['country'] == Country) & (X['price'] == Price)]
    prediction_points = neigh.predict(test)
    print(prediction_points[0])

#Example: Country = 30 (encoded), price = 15
predict_points_KNeighborsClassifier(30,15.0,X,y)



