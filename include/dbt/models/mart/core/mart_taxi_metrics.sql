{{ config(
    materialized='external',
    location='/usr/local/airflow/include/data/gold/mart_taxi_metrics.parquet'
) }}

WITH transformed_data AS (
    SELECT * FROM {{ ref('int_taxi_transforms') }}
)

SELECT 
    -- 1. Dimensões (Granularidade para os filtros do Streamlit)
    CAST(pickup_time AS DATE) AS data_corrida,
    EXTRACT(HOUR FROM pickup_time) AS hora_dia,
    DAYOFWEEK(pickup_time) AS dia_semana_num,
    vendor_id,
    payment_type,
    turno_dia, -- Mantivemos para manter compatibilidade com seus gráficos anteriores
    
    -- 2. Métricas de Volume e Operação
    COUNT(*) AS total_corridas,
    SUM(passenger_count) AS total_passageiros,
    ROUND(AVG(trip_distance), 2) AS distancia_media_milhas,

    -- Métricas Operacionais (Novas)
    SUM(trip_distance) AS distancia_total_milhas,
    SUM(duracao_minutos) AS tempo_total_minutos,
    
    -- 3. Métricas Financeiras
    ROUND(SUM(total_amount), 2) AS receita_total,
    ROUND(AVG(total_amount), 2) AS ticket_medio,
    ROUND(AVG(tip_amount), 2) AS gorjeta_media,
    
    -- 4. Métricas de Rentabilidade (O que torna o projeto "Data Engineering de Elite")
    ROUND(AVG(receita_por_minuto), 2) AS rentabilidade_media_minuto

FROM transformed_data
GROUP BY 1, 2, 3, 4, 5, 6