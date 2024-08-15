{{ config(materialized='table') }}

WITH transform_table AS (
    SELECT 
        id
        trans_date,
        address,
        layout_v2,
        CASE
            WHEN REPLACE(build_area, '坪', '') IN ('-', '') THEN NULL 
            ELSE CAST(REPLACE(build_area, '坪', '') AS DECIMAL(10, 2)) 
        END AS build_area,
        CASE
            WHEN REPLACE(building_area, '坪', '') IN ('-', '') THEN NULL 
            ELSE CAST(REPLACE(building_area, '坪', '') AS DECIMAL(10, 2)) 
        END AS building_area,
        CASE
            WHEN REPLACE(building_total_price, '萬', '') IN ('-', '') THEN NULL 
            ELSE CAST(REPLACE(REPLACE(building_total_price, '萬', ''), ',', '') AS UNSIGNED) 
        END AS building_total_price,
        CASE 
            WHEN REPLACE(real_park_area, '坪', '') IN ('-', '') THEN NULL 
            ELSE CAST(REPLACE(real_park_area, '坪', '') AS DECIMAL(10, 2)) 
        END AS real_park_area,
        CASE
            WHEN REPLACE(unit_price, '萬/坪', '') IN ('-', '') THEN NULL 
            ELSE CAST(REPLACE(unit_price, '萬/坪', '') AS DECIMAL(10, 2)) 
        END AS unit_price,
        CASE
            WHEN REPLACE(total_price_v, ',', '') IN ('-', '') THEN NULL 
            ELSE CAST(REPLACE(total_price_v, ',', '') AS UNSIGNED) 
        END AS total_price_v,
        price_tips,
        context,
        is_special,
        has_park,
        unit_has_park,
        REPLACE(shift_floor, '樓', '') AS shift_floor,
        -- CASE
        --     WHEN REPLACE(shift_floor, '樓', '') IN ('-', '') THEN NULL 
        --     ELSE CAST(REPLACE(shift_floor, '樓', '') AS UNSIGNED) 
        -- END AS shift_floor,
        build_purpose_str,
        CASE 
            WHEN real_park_total_price IN ('-', '') THEN NULL 
            ELSE CAST(real_park_total_price AS UNSIGNED)
        END AS real_park_total_price,
        community,
        tags,
        park_type_str,
        park_count,
        history_trans_count,
        community_id
    FROM Real_Estate.PropertyTransactions
)

SELECT * FROM transform_table