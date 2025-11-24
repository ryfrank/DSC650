from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import happybase


# Step 1: Create a Spark session with Hive Support
spark = SparkSession.builder.appName("MLlib Salary Prediction").enableHiveSupport().getOrCreate()

# Step 2: Load the data from the Hive table 'dsSalaries' into a Spark DataFrame
salaryDF = spark.sql("SELECT workYear, experienceLevel, employmentType, jobTitle, salary, salaryCurrency, "
                     "salaryInUSD, employeeResidence, remoteRatio, companyLocation, companySize "
                     "FROM dsSalaries")

print(salaryDF.head())
