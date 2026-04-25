WITH valid_data AS (
    SELECT * FROM {{ ref('int_taxi_validate') }}
)

SELECT 
    *,
    
    -- 1. Duração da corrida em minutos (Necessário para a rentabilidade)
    DATE_DIFF('minute', pickup_time, dropoff_time) AS duracao_minutos,

    -- 2. Turnos do dia (Facilita comparações no dashboard)
    CASE 
        WHEN EXTRACT(HOUR FROM pickup_time) BETWEEN 6 AND 11 THEN 'Manhã'
        WHEN EXTRACT(HOUR FROM pickup_time) BETWEEN 12 AND 17 THEN 'Tarde'
        WHEN EXTRACT(HOUR FROM pickup_time) BETWEEN 18 AND 23 THEN 'Noite'
        ELSE 'Madrugada'
    END AS turno_dia,

    -- 3. Faixas de distância
    CASE 
        WHEN trip_distance <= 2 THEN 'Curta'
        WHEN trip_distance > 2 AND trip_distance <= 5 THEN 'Média'
        ELSE 'Longa'
    END AS faixa_distancia,

    -- 4. Calendário: Indicador de Fim de Semana
    -- No DuckDB, DAYOFWEEK retorna 0 para Domingo e 6 para Sábado
    CASE 
        WHEN DAYOFWEEK(pickup_time) IN (0, 6) THEN 1 ELSE 0 
    END AS is_weekend,

    -- 5. Rentabilidade (Com tratamento de Divisão por Zero!)
    CASE 
        WHEN DATE_DIFF('minute', pickup_time, dropoff_time) > 0 
        THEN ROUND(total_amount / DATE_DIFF('minute', pickup_time, dropoff_time), 2)
        ELSE 0 
    END AS receita_por_minuto,
    
    CASE 
        WHEN trip_distance > 0 
        THEN ROUND(total_amount / trip_distance, 2)
        ELSE 0 
    END AS receita_por_milha

FROM valid_data