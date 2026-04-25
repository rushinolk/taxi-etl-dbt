

SELECT * FROM {{ ref('int_taxi_flagged') }}
WHERE 
    is_financial_valid = 1 
    AND is_business_valid = 1 
    AND is_time_valid = 1
    AND is_location_valid = 1