from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Traffic Crashes Data Preparation").getOrCreate()

df = spark.read.csv("s3://trafficcrashesemrjob/dataset/Traffic_Crashes.csv", header=True, inferSchema=True)

df = df.drop("CRASH_DATE_EST_I", "LANE_CNT", "REPORT_TYPE", "INTERSECTION_RELATED_I",
             "NOT_RIGHT_OF_WAY_I", "HIT_AND_RUN_I", "PHOTOS_TAKEN_I", "STATEMENTS_TAKEN_I", "DOORING_I", "WORK_ZONE_I",
             "WORK_ZONE_TYPE", "WORKERS_PRESENT_I")

df = df.na.drop(subset=["MOST_SEVERE_INJURY", "INJURIES_TOTAL", "INJURIES_FATAL", "INJURIES_INCAPACITATING",
                        "INJURIES_NON_INCAPACITATING", "INJURIES_REPORTED_NOT_EVIDENT", "INJURIES_NO_INDICATION",
                        "INJURIES_UNKNOWN", "LATITUDE", "LONGITUDE", "LOCATION", "STREET_DIRECTION", "STREET_NAME",
                        "BEAT_OF_OCCURRENCE"])

columns_to_convert = ["CRASH_RECORD_ID", "CRASH_DATE", "TRAFFIC_CONTROL_DEVICE", "DEVICE_CONDITION", "WEATHER_CONDITION",
                      "LIGHTING_CONDITION", "FIRST_CRASH_TYPE", "TRAFFICWAY_TYPE", "ALIGNMENT", "ROADWAY_SURFACE_COND",
                      "ROAD_DEFECT", "CRASH_TYPE", "DAMAGE", "DATE_POLICE_NOTIFIED", "PRIM_CONTRIBUTORY_CAUSE", "SEC_CONTRIBUTORY_CAUSE",
                      "STREET_NAME", "STREET_DIRECTION"]

for col_name in columns_to_convert:
    df = df.withColumn(col_name, df[col_name].cast("string"))

df.createOrReplaceTempView("trafficCrashes")
query = "SELECT * FROM trafficCrashes where POSTED_SPEED_LIMIT<30 and WEATHER_CONDITION='CLEAR' and LIGHTING_CONDITION='DAYLIGHT'"
df_result = spark.sql(query)

df_result.write.mode("overwrite").parquet("s3://trafficcrashesemrjob/dataset/output/")
