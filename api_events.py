from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read JSON to DataFrame") \
    .getOrCreate()

# Path to your JSON file
json_installs = "C:/Users/mehme/Desktop/Apps_Interview/api_response/response_installs.json"
json_events = "C:/Users/mehme/Desktop/Apps_Interview/api_response/response_events.json"
json_cost = "C:/Users/mehme/Desktop/Apps_Interview/api_response/response_cost.json"

# Read Installs response file
df = spark.read.option("multiLine", True).json(json_installs)


print("Installs structure: \n")
df.printSchema()
print("Installs data example: \n")
df.show(3)

# Read Events response file
df = (spark.read
.option("multiLine", True)
.option("inferschema", True)
.json(json_events))


print("Events structure: \n")
df.printSchema()
print("Events data example: \n")
df.show(3)

# Read Cost response file
df = spark.read.option("multiLine", True).json(json_cost)


print("Cost structure: \n")
df.printSchema()
print("Cost data example: \n")
df.show(3)