-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).
SELECT 
    STRFTIME('%m', order_purchase_timestamp) AS month_no,
    CASE STRFTIME('%m', order_purchase_timestamp)
        WHEN '01' THEN 'Ene'
        WHEN '02' THEN 'Feb'
        WHEN '03' THEN 'Mar'
        WHEN '04' THEN 'Abr'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'Jun'
        WHEN '07' THEN 'Jul'
        WHEN '08' THEN 'Ago'
        WHEN '09' THEN 'Sep'
        WHEN '10' THEN 'Oct'
        WHEN '11' THEN 'Nov'
        WHEN '12' THEN 'Dic'
    END AS month,
    COALESCE(SUM(CASE WHEN STRFTIME('%Y', order_purchase_timestamp) = '2016' THEN i.price END), 0) AS Year2016,
    COALESCE(SUM(CASE WHEN STRFTIME('%Y', order_purchase_timestamp) = '2017' THEN i.price END), 0) AS Year2017,
    COALESCE(SUM(CASE WHEN STRFTIME('%Y', order_purchase_timestamp) = '2018' THEN i.price END), 0) AS Year2018
FROM olist_orders o
JOIN olist_order_items i ON o.order_id = i.order_id
WHERE 
    o.order_status = 'delivered' 
    AND order_purchase_timestamp IS NOT NULL
GROUP BY month_no
ORDER BY month_no ASC;

