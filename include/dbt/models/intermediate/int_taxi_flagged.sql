
WITH deduplicated_data AS (
    SELECT * FROM {{ ref('stg_taxi') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pickup_time, vendor_id, trip_distance, total_amount 
        ORDER BY pickup_time
    ) = 1
)

SELECT 
    *,
    -- 1. CORREÇÃO: Adicionamos a coluna "extra" (Taxas noturnas e de pico)
    CASE 
        WHEN abs(total_amount - (fare_amount + extra + tip_amount + tolls_amount + mta_tax + improvement_surcharge)) < 0.05 
        THEN 1 ELSE 0 
    END AS is_financial_valid,
    
    -- 2. CORREÇÃO: Aceitamos >= 0 para passageiros e distância, pois o taxímetro rodou e gerou receita.
    CASE 
        WHEN trip_distance >= 0 AND trip_distance < 200 
                AND passenger_count >= 0 AND passenger_count <= 6
                AND fare_amount >= 0 
        THEN 1 ELSE 0 
    END AS is_business_valid,
    
    CASE 
        WHEN dropoff_time > pickup_time 
                AND EXTRACT(YEAR FROM pickup_time) IN (2015, 2016)  
        THEN 1 ELSE 0 
    END AS is_time_valid,
    
    -- Deixei a localização igual, mas se faltar dado, podemos relaxar isso também.
    CASE 
        WHEN pickup_lat BETWEEN 40.5 AND 41.0 AND pickup_lon BETWEEN -74.3 AND -73.7
                AND dropoff_lat BETWEEN 40.5 AND 41.0 AND dropoff_lon BETWEEN -74.3 AND -73.7
        THEN 1 ELSE 0
    END AS is_location_valid
    
FROM deduplicated_data