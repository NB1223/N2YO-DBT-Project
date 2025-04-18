from pyspark.sql.functions import col, when, udf, lit
from pyspark.sql.types import DoubleType
import math

# Haversine distance function
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth radius in km
    lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
    lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# UDF registration
haversine_udf = udf(haversine, DoubleType())

def calculate_overlap(df_sat_obs):
    sat_df = df_sat_obs.select(
        col("sat_id"),
        col("satname"),
        col("latitude"),
        col("longitude"),
        col("timestamp")
    )

    df1 = sat_df.alias("df1")
    df2 = sat_df.alias("df2")

    paired_df = df1.join(
        df2,
        (col("df1.timestamp") == col("df2.timestamp")) &
        (col("df1.sat_id") < col("df2.sat_id"))
    ).select(
        col("df1.timestamp").alias("timestamp"),
        col("df1.satname").alias("satname1"),
        col("df2.satname").alias("satname2"),
        col("df1.latitude").alias("lat1"),
        col("df1.longitude").alias("lon1"),
        col("df2.latitude").alias("lat2"),
        col("df2.longitude").alias("lon2")
    )

    with_distance = paired_df.withColumn(
        "distance_km",
        haversine_udf(col("lat1"), col("lon1"), col("lat2"), col("lon2"))
    )

    coverage_radius = 2500
    with_overlap = with_distance.withColumn(
        "overlap",
        col("distance_km") < (2 * coverage_radius)
    ).withColumn(
        "overlap_km",
        when(col("overlap"), 2 * coverage_radius - col("distance_km"))
        .otherwise(lit(0.0))
    )

    final_result = with_overlap.select(
        "timestamp",
        "distance_km",
        "overlap",
        "overlap_km",
        "satname1",
        "satname2",
        "lat1",
        "lon1",
        "lat2",
        "lon2"
    )

    # # ✅ Print for debugging (only works in batch mode or when called outside streaming)
    # try:
    #     final_result.show(truncate=False)
    # except:
    #     print("Note: .show() is not supported in streaming context.")

    return final_result
