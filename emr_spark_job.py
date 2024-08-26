from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Traffic Crashes Data Preparation").getOrCreate()

    # Read CSV from S3 bucket
    df = spark.read.csv("s3://trafficcrashesemrjob/dataset/Traffic_Crashes.csv", header=True, inferSchema=True)

    # Drop columns with too many NA values
    df = df.drop("CRASH_DATE_EST_I", "LANE_CNT", "REPORT_TYPE", "INTERSECTION_RELATED_I",
                 "NOT_RIGHT_OF_WAY_I", "HIT_AND_RUN_I", "PHOTOS_TAKEN_I", "STATEMENTS_TAKEN_I", "DOORING_I",
                 "WORK_ZONE_I",
                 "WORK_ZONE_TYPE", "WORKERS_PRESENT_I")

    # drop rows with too NA values on the following columns
    df = df.na.drop(subset=["MOST_SEVERE_INJURY", "INJURIES_TOTAL", "INJURIES_FATAL", "INJURIES_INCAPACITATING",
                            "INJURIES_NON_INCAPACITATING", "INJURIES_REPORTED_NOT_EVIDENT", "INJURIES_NO_INDICATION",
                            "INJURIES_UNKNOWN", "LATITUDE", "LONGITUDE", "LOCATION", "STREET_DIRECTION", "STREET_NAME",
                            "BEAT_OF_OCCURRENCE"])

    # Convert the data type to string
    columns_to_convert = ["CRASH_RECORD_ID", "CRASH_DATE", "TRAFFIC_CONTROL_DEVICE", "DEVICE_CONDITION",
                          "WEATHER_CONDITION",
                          "LIGHTING_CONDITION", "FIRST_CRASH_TYPE", "TRAFFICWAY_TYPE", "ALIGNMENT",
                          "ROADWAY_SURFACE_COND",
                          "ROAD_DEFECT", "CRASH_TYPE", "DAMAGE", "DATE_POLICE_NOTIFIED", "PRIM_CONTRIBUTORY_CAUSE",
                          "SEC_CONTRIBUTORY_CAUSE",
                          "STREET_NAME", "STREET_DIRECTION"]

    # Iterate over the list and typecast to string
    for col_name in columns_to_convert:
        df = df.withColumn(col_name, df[col_name].cast("string"))

    # Create a temporary view to run a query
    df.createOrReplaceTempView("trafficCrashes")
    query = "SELECT * FROM trafficCrashes where POSTED_SPEED_LIMIT<30 and WEATHER_CONDITION='CLEAR' and LIGHTING_CONDITION='DAYLIGHT'"
    df_result = spark.sql(query)

    # store the query results in s3 bucket
    df_result.write.mode("overwrite").csv("s3://trafficcrashesemrjob/dataset/output/")


if __name__ == "__main__":
    main()
