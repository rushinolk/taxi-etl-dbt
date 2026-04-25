WITH raw_data AS (
    SELECT * FROM read_csv_auto('/usr/local/airflow/include/data/bronze/*.csv',union_by_name=true)
)

SELECT 
    VendorID::INT AS vendor_id,
    RatecodeID::INT AS rate_code_id,
    store_and_fwd_flag::VARCHAR AS store_and_fwd_flag,
    tpep_pickup_datetime::TIMESTAMP AS pickup_time,
    tpep_dropoff_datetime::TIMESTAMP AS dropoff_time,
    passenger_count::INT AS passenger_count,
    trip_distance::DECIMAL(10,2) AS trip_distance,
    pickup_longitude::DECIMAL(10,6) AS pickup_lon,
    pickup_latitude::DECIMAL(10,6) AS pickup_lat,
    dropoff_longitude::DECIMAL(10,6) AS dropoff_lon,
    dropoff_latitude::DECIMAL(10,6) AS dropoff_lat,
    payment_type::INT AS payment_type,
    total_amount::DECIMAL(10,2) AS total_amount,
    fare_amount::DECIMAL(10,2) AS fare_amount,
    extra::DECIMAL(10,2) AS extra,
    tip_amount::DECIMAL(10,2) AS tip_amount,
    tolls_amount::DECIMAL(10,2) AS tolls_amount,
    mta_tax::DECIMAL(10,2) AS mta_tax,
    improvement_surcharge::DECIMAL(10,2) AS improvement_surcharge
FROM raw_data