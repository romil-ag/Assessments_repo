from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
import sqlite3

# Initialize Spark session
spark = SparkSession.builder.appName("SalesDataProcessing").getOrCreate()

# Read the data from the CSV files
region_a_df = spark.read.csv("path/order_region_a.csv", header=True)
region_b_df = spark.read.csv("path/order_region_b.csv", header=True)

# Adding region identifier
region_a_df = region_a_df.withColumn("region", F.lit("A"))
region_b_df = region_b_df.withColumn("region", F.lit("B"))

combined_df = region_a_df.union(region_b_df)

# Calculate total_sales and net_sale
transformed_df = combined_df.withColumn("QuantityOrdered", combined_df["QuantityOrdered"].cast("int")) \
                            .withColumn("ItemPrice", combined_df["ItemPrice"].cast("double")) \
                            .withColumn("PromotionDiscount", combined_df["PromotionDiscount"].cast("double")) \
                            .withColumn("total_sales", F.col("QuantityOrdered") * F.col("ItemPrice")) \
                            .withColumn("net_sale", F.col("total_sales") - F.col("PromotionDiscount"))

transformed_df = transformed_df.dropDuplicates(["OrderId"])

# Orders with net_sale > 0
filtered_df = transformed_df.filter(F.col("net_sale") > 0)

filtered_df.show()

#Connecttion to SQLite

db_path = "sales_data.db"
jdbc_url = f"sqlite:///{db_path}"

# Save the DataFrame to SQLite
filtered_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "sales_data") \
    .mode("overwrite") \
    .save()


conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# SQL Queries
cursor.execute("SELECT COUNT(*) from sales_data")
total_records = cursor.fetchone()[0]
print(f"Total records- {total_records}")

cursor.execute("SELECT region, SUM(net_sale) from sales_data group by region")
total_sales_by_region = cursor.fetchall()
print(f"Total sales by region- {total_sales_by_region}")

cursor.execute("SELECT AVG(net_sale) from sales_data")
average_sales_per_transaction = cursor.fetchone()[0]
print(f"Average sales amount per transaction- {average_sales_per_transaction}")

conn.close()




