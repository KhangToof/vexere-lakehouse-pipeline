from pyspark.sql import SparkSession
from spark_session.spark_config import get_spark_session

def update_charts(): 
    spark = get_spark_session("convert_to_gold")
    
    bus_ids_path = "s3a://silver/bus_ids"
    ticket_path = "s3a://silver/ticket"
    facility_path = "s3a://silver/facility"
    facility_name_path = "s3a://silver/facility_name"
    bus_review_vi_path = "s3a://silver/bus_reviews_vi"
    bus_review_en_path = "s3a://silver/bus_reviews_en"
    
    bus_ids_df = spark.read.format("delta").load(bus_ids_path)
    ticket_df = spark.read.format("delta").load(ticket_path)
    facility_df = spark.read.format("delta").load(facility_path)
    facility_name_df = spark.read.format("delta").load(facility_name_path)
    bus_review_vi_df = spark.read.format("delta").load(bus_review_vi_path)
    bus_review_en_df = spark.read.format("delta").load(bus_review_en_path)
    
    bus_ids_df.createOrReplaceTempView("bus_ids")
    ticket_df.createOrReplaceTempView("ticket")
    facility_df.createOrReplaceTempView("facility")
    facility_name_df.createOrReplaceTempView("facility_name")
    bus_review_vi_df.createOrReplaceTempView("bus_review_vi")
    bus_review_en_df.createOrReplaceTempView("bus_review_en")
    
    sql_1 = spark.sql("""
    SELECT 
        t.Route,
        t.Bus_Name,
        COUNT(*) AS Total_Trips,
        ROUND(AVG(t.Price), 0) AS Avg_Price_Per_Day,
        concat_ws(', ', collect_set(
            CASE 
                WHEN HOUR(TO_TIMESTAMP(t.Departure_Time, 'HH:mm')) BETWEEN 0 AND 5 THEN '00h-05h'
                WHEN HOUR(TO_TIMESTAMP(t.Departure_Time, 'HH:mm')) BETWEEN 6 AND 11 THEN '06h-11h'
                WHEN HOUR(TO_TIMESTAMP(t.Departure_Time, 'HH:mm')) BETWEEN 12 AND 17 THEN '12h-17h'
                WHEN HOUR(TO_TIMESTAMP(t.Departure_Time, 'HH:mm')) BETWEEN 18 AND 23 THEN '18h-23h'
            END
        )) AS depart_time_ranges
    FROM Ticket t
    GROUP BY t.Route, t.Bus_Name
    ORDER BY t.Route, Total_Trips DESC
    """)
    
    sql_2 = spark.sql("""
    WITH cheapest_price AS (
        SELECT 
            start_date,
            route,
            MIN(price) AS min_price
        FROM ticket
        GROUP BY start_date, route
    ),
    review_score AS (
        SELECT 
            bus_id,
            ROUND(AVG(pos), 2) AS avg_positive
        FROM (
            SELECT bus_id, pos FROM bus_review_vi
            UNION ALL
            SELECT bus_id, pos FROM bus_review_en
        ) AS all_reviews
        GROUP BY bus_id
    ),
    candidates AS (
        SELECT DISTINCT
            t.start_date,
            t.route,
            t.bus_name,
            t.bus_id,
            t.price,
            COALESCE(r.avg_positive, 0) AS avg_positive,
            CASE WHEN COALESCE(r.avg_positive, 0) > 0.5 THEN 1 ELSE 0 END AS is_good
        FROM ticket t
        JOIN cheapest_price c ON 
            t.start_date = c.start_date AND 
            t.route = c.route AND 
            t.price = c.min_price
        LEFT JOIN review_score r ON t.bus_id = r.bus_id
    ),
    ranked AS (
        SELECT *,
               RANK() OVER (PARTITION BY start_date, route ORDER BY is_good DESC) AS rank_in_group
        FROM candidates
    )
    SELECT 
        start_date,
        route,
        bus_name,
        price
    FROM ranked
    WHERE rank_in_group = 1
    ORDER BY start_date, route, bus_name;
    """)
    
    sql_3 = spark.sql("""
    SELECT 
        t.Route,
        COUNT(DISTINCT t.Bus_Name) AS total_bus_operators
    FROM ticket t
    GROUP BY t.Route
    ORDER BY total_bus_operators DESC
    """)
    
    sql_4 = spark.sql("""
    SELECT 
        t.Start_Date,
        ROUND(AVG(t.Price), 0) AS avg_price_per_day
    FROM ticket t
    GROUP BY t.Start_Date
    ORDER BY t.Start_Date
    """)
    
    sql_5 = spark.sql("""
    SELECT 
        t.Bus_Name,
        COUNT(*) AS total_reviews
    FROM bus_review_vi t
    GROUP BY t.Bus_Name
    ORDER BY total_reviews DESC
    """)
    
    sql_6 = spark.sql("""
    WITH combined AS (
        SELECT 
            CAST(Bus_Name AS STRING) AS bus_name,
            NEG,
            POS
        FROM bus_review_vi
        
        UNION ALL
    
        SELECT 
            CAST(Bus_Name AS STRING) AS bus_name,
            NEG,
            POS
        FROM bus_review_en
    )
    SELECT 
        ROUND(AVG(NEG * 5 + POS * 10), 2) AS avg_rating_10pt
    FROM combined
    GROUP BY bus_name
    HAVING COUNT(*) >= 50
    ORDER BY avg_rating_10pt DESC
    """)
    
    sql_7 = spark.sql("""
    WITH hours AS (
        SELECT explode(sequence(0, 23)) AS hour
    ),
    bus_hours AS (
        SELECT 
            CAST(Bus_Name AS STRING) AS bus_name,
            CAST(SUBSTRING(departure_time, 1, 2) AS INT) AS hour
        FROM ticket
        WHERE departure_time IS NOT NULL
    ),
    bus_hour_flags AS (
        SELECT DISTINCT 
            bus_name, 
            hour, 
            1 AS has_departure
        FROM bus_hours
    ),
    bus_names AS (
        SELECT DISTINCT bus_name FROM bus_hours
    )
    SELECT 
        b.bus_name,
        h.hour,
        COALESCE(f.has_departure, 0) AS has_departure
    FROM bus_names b
    CROSS JOIN hours h
    LEFT JOIN bus_hour_flags f 
        ON b.bus_name = f.bus_name AND h.hour = f.hour
    ORDER BY b.bus_name, h.hour
    """)
    
    sql_8 = spark.sql("""
    WITH source AS (
        SELECT
            CAST(bus_name AS STRING) AS bus_name,
            CAST(facility_id AS INT) AS facility_id
        FROM facility
        WHERE bus_name IS NOT NULL AND facility_id IS NOT NULL
    ),
    facilities AS (
        SELECT explode(sequence(1, 21)) AS facility_id
    ),
    bus_names AS (
        SELECT DISTINCT bus_name FROM source
    ),
    bus_facility_flag AS (
        SELECT DISTINCT bus_name, facility_id, 1 AS has_facility FROM source
    ),
    facility_name_dedup AS (
        SELECT
            facility_id,
            MIN(facility_name) AS facility_name
        FROM facility_name
        WHERE facility_name IS NOT NULL
        GROUP BY facility_id
    )
    SELECT
        b.bus_name,
        f.facility_id,
        COALESCE(bff.has_facility, 0) AS has_facility,
        fnd.facility_name
    FROM bus_names b
    CROSS JOIN facilities f
    LEFT JOIN bus_facility_flag bff 
        ON b.bus_name = bff.bus_name AND f.facility_id = bff.facility_id
    LEFT JOIN facility_name_dedup fnd 
        ON f.facility_id = fnd.facility_id
    ORDER BY b.bus_name, f.facility_id
    """)
    
    
    sql_1.write.format("delta").mode("overwrite").save("s3a://gold/cau_1/")
    sql_2.write.format("delta").mode("overwrite").save("s3a://gold/cau_2/")
    sql_3.write.format("delta").mode("overwrite").save("s3a://gold/cau_3/")
    sql_4.write.format("delta").mode("overwrite").save("s3a://gold/cau_4/")
    sql_5.write.format("delta").mode("overwrite").save("s3a://gold/cau_5/")
    sql_6.write.format("delta").mode("overwrite").save("s3a://gold/cau_6/")
    sql_7.write.format("delta").mode("overwrite").save("s3a://gold/cau_7/")
    sql_8.write.format("delta").mode("overwrite").save("s3a://gold/cau_8/")

