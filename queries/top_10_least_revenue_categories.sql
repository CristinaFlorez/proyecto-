-- TODO: Esta consulta devolverá una tabla con las 10 categorías con menores ingresos
-- (en inglés), el número de pedidos y sus ingresos totales. La primera columna será
-- Category, que contendrá las 10 categorías con menores ingresos; la segunda será
-- Num_order, con el total de pedidos de cada categoría; y la última será Revenue,
-- con el ingreso total de cada categoría.
-- PISTA: Todos los pedidos deben tener un estado 'delivered' y tanto la categoría
-- como la fecha real de entrega no deben ser nulas.
SELECT 
    pc.product_category_name AS Category,
    COUNT(o.order_id) AS Num_order,
    SUM(i.price) AS Revenue
FROM olist_orders o
JOIN olist_order_items i ON o.order_id = i.order_id
JOIN olist_products p ON i.product_id = p.product_id
JOIN product_category_name_translation pc ON p.product_category_name = pc.product_category_name
WHERE 
    o.order_status = 'delivered'
    AND pc.product_category_name IS NOT NULL
    AND o.order_delivered_customer_date IS NOT NULL
GROUP BY pc.product_category_name
ORDER BY Revenue ASC
LIMIT 10;

