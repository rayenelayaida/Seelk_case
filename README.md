# Seelk_case
The goal of this exercise is to prepare and process a small Dataset (50 MB). The dataset was taken from <https://www.kaggle.com/zynicide/wine-reviews>.
## About the dataset 
* id
* country: The country that the wine is from description
* designation: The vineyard within the winery where the grapes that made the wine are from
* points: The number of points WineEnthusiast rated the wine on a scale of 1-100 (though they say they only post reviews for wines that score >=80)
* price: The cost for a bottle of the wine
* province: The province or state that the wine is from
* region_1: The wine growing area in a province or state (ie Napa)
* region_2: Sometimes there are more specific regions specified within a wine growing area (ie Rutherford inside the Napa Valley), but this value can sometimes be blank
* taster_name
* taster_twitter_handle
* title: The title of the wine review, which often contains the vintage if you're interested in extracting that feature
* variety: The type of grapes used to make the wine (ie Pinot Noir)
* winery: The winery that made the wine
## Installing Pyspark On Windows
### Prerequisites
* Anaconda
* GOW
* Java

If you already have anaconda and GOW installed, skip to step 5. 
### Step 1 
Download and install Gnu on windows (GOW) from the following <https://github.com/bmatzelle/gow/releases/download/v0.8.0/Gow-0.8.0.exe>. Basically, GOW allows you to use linux commands on windows. In this install, we will need curl, gzip, tar which GOW provides.
`Gow --list`
### Step 2
Download and install Anaconda. If you need help, please see this tutorial : <https://medium.com/@GalarnykMichael/install-python-anaconda-on-windows-2020-f8e188f9a63d>
### Step 3
Close and open a new command line (CMD).
### Step 4 
Go to the Apache Spark website <http://spark.apache.org/downloads.html>

* Choose a Spark release
* Choose a package type
* Choose a download type: (Direct Download)
* Download Spark. Keep in mind if you download a newer version, you will need to modify the remaining commands for the file you downloaded.
### Step 5 
 Move the file to where you want to unzip it.
 `mkdir C:\opt\spark`
 
 `mv C:\Users\mgalarny\Downloads\spark-3.0.0-preview2-bin-hadoop2.7.tgz C:\opt\spark\spark-3.0.0-preview2-bin-hadoop2.7.tgz`
 
### Step 6 
Unzip the file. Use the bolded commands below

`gzip -d spark-3.0.0-preview2-bin-hadoop2.7.tgz`

`tar xvf spark-3.0.0-preview2-bin-hadoop2.7.tar`
### Step 7
Download winutils.exe into your __spark-3.0.0-preview2-bin-hadoop2.7\bin__
`curl -k -L -o winutils.exe https://github.com/steveloughran/winutils/blob/master/hadoop-2.6.0/bin/winutils.exe?raw=true`
### Step 8 
Make sure you have Java 7+ installed on your machine.
` java -version `
### Step 9
Here, we will edit our environmental variables so we can open a spark notebook in any directory.
`setx SPARK_HOME C:\opt\spark\spark-3.0.0-preview2-bin-hadoop2.7
setx HADOOP_HOME C:\opt\spark\spark-3.0.0-preview2-bin-hadoop2.7
setx PYSPARK_DRIVER_PYTHON ipython
setx PYSPARK_DRIVER_PYTHON_OPTS notebook`

Add; __C:\opt\spark\spark-3.0.0-preview2-bin-hadoop2.7\bin__ to your path.

Notes on the setx command: <https://ss64.com/nt/set.html>
See the video if you want to update your path manually.
### Step 10 
Close your terminal and open a new one. Type the command below.
 `pyspark --master local[2]`
 
### Notes
The __PYSPARK_DRIVER_PYTHON__ parameter and the __PYSPARK_DRIVER_PYTHON_OPTS__ parameter are used to launch the PySpark shell in Jupyter Notebook. The — master parameter is used for setting the master node address. Here we launch Spark locally on 2 cores for local testing
## Install Pyspark on Ubunto
### Prerequisites
* Anaconda. 
 If you don't have Anaconda already installed please take a look on the __step 2__ of the __pyspark installation for Windows__ above
### Step 1 
Make sure you have java installed on your machine. If you don’t,the link below is useful.
<http://tecadmin.net/install-oracle-java-8-jdk-8-ubuntu-via-ppa/>
### Step 2
Go to your home directory 

`cd ~`
### Step 3
Unzip the folder in your home directory using the following command.
`tar -zxvf spark-3.0.0-preview2-bin-hadoop2.7.tgz`
### Step 4
Use the following command to see that you have a __.bashrc file__
`ls -a`
### Step 5 
Next, we will edit our __.bashrc__ so we can open a spark notebook in any directory
`nano .bashrc`
### Step 6 
Don’t remove anything in your __.bashrc file.__ Add the following to the bottom of your __.bashrc file__

`function snotebook () 
{
#Spark path (based on your computer)
SPARK_PATH=~/spark-2.0.0-bin-hadoop2.7

export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

#For python 3 users, you have to add the line below or you will get an error 
#export PYSPARK_PYTHON=python3

$SPARK_PATH/bin/pyspark --master local[2]
}`


### Step 7
Save and exit out of your __.bashrc file.__ Either close the terminal and open a new one or in your terminal type:
`source .bashrc`
### Notes
The __PYSPARK_DRIVER_PYTHON__ parameter and the __PYSPARK_DRIVER_PYTHON_OPTS__ parameter are used to launch the __PySpark shell__ in __Jupyter Notebook__. The master parameter is used for setting the master node address. Here we launch Spark locally on 2 cores for local testing.
## Running the code
### Download the dataset and the code 
* Download the CSV file of the __wine-reviews dataset__ directly from
 <https://lengow.s3-eu-west-1.amazonaws.com/winemag-data-130k-v2.csv>
* Upload the case.py file that contains the code <https://github.com/rayenelayaida/Seelk_case/blob/master/Case.py>
### Launch PySpark shell in Jupyter Notebook
 Open the command prompts and type : 
 `PYSPARK_DRIVER_PYTHON`
 `PYSPARK_DRIVER_PYTHON_OPTS`
### Download the dataset and the code 
Create a new folder on __Jupyter__:

open The folder and __Upload__ the __wine-review dataset__ and the __case.py__ file 




 
Not finished yet

