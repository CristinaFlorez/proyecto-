from collections import namedtuple
from enum import Enum
from typing import Callable, Dict, List

import pandas as pd
from pandas import DataFrame, read_sql
from sqlalchemy import text
from sqlalchemy.engine.base import Engine

from src.config import QUERIES_ROOT_PATH

QueryResult = namedtuple("QueryResult", ["query", "result"])


class QueryEnum(Enum):
    """This class enumerates all the queries that are available"""

    DELIVERY_DATE_DIFFERECE = "delivery_date_difference"
    GLOBAL_AMMOUNT_ORDER_STATUS = "global_ammount_order_status"
    REVENUE_BY_MONTH_YEAR = "revenue_by_month_year"
    REVENUE_PER_STATE = "revenue_per_state"
    TOP_10_LEAST_REVENUE_CATEGORIES = "top_10_least_revenue_categories"
    TOP_10_REVENUE_CATEGORIES = "top_10_revenue_categories"
    REAL_VS_ESTIMATED_DELIVERED_TIME = "real_vs_estimated_delivered_time"
    ORDERS_PER_DAY_AND_HOLIDAYS_2017 = "orders_per_day_and_holidays_2017"
    GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP = "get_freight_value_weight_relationship"


def read_query(query_name: str) -> str:
    """Read the query from the file."""
    with open(f"{QUERIES_ROOT_PATH}/{query_name}.sql", "r") as f:
        sql = f.read()
    print(f"Consulta cargada: {query_name}\n{sql}")
    return sql


def query_delivery_date_difference(database: Engine) -> QueryResult:
    query_name = QueryEnum.DELIVERY_DATE_DIFFERECE.value
    query = read_query(query_name)
    print(f"Ejecutando consulta para: {query_name}")
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_global_ammount_order_status(database: Engine) -> QueryResult:
    query_name = QueryEnum.GLOBAL_AMMOUNT_ORDER_STATUS.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_revenue_by_month_year(database: Engine) -> QueryResult:
    query_name = QueryEnum.REVENUE_BY_MONTH_YEAR.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_revenue_per_state(database: Engine) -> QueryResult:
    query_name = QueryEnum.REVENUE_PER_STATE.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_top_10_least_revenue_categories(database: Engine) -> QueryResult:
    query_name = QueryEnum.TOP_10_LEAST_REVENUE_CATEGORIES.value
    print(f"Ejecutando consulta: {query_name}")
    query = read_query(query_name)
    result = read_sql(query, database)
    print(f"Resultado de {query_name}:")
    print(result.head())
    return QueryResult(query=query_name, result=result)



def query_top_10_revenue_categories(database: Engine) -> QueryResult:
    query_name = QueryEnum.TOP_10_REVENUE_CATEGORIES.value
    query = read_query(query_name)
    print(f"Ejecutando consulta para: {query_name}")
    print(f"Consulta SQL:\n{query}")
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_real_vs_estimated_delivered_time(database: Engine) -> QueryResult:
    query_name = QueryEnum.REAL_VS_ESTIMATED_DELIVERED_TIME.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_freight_value_weight_relationship(database: Engine) -> QueryResult:
    query_name = QueryEnum.GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP.value

    try:
        # Cargar los datos desde la base de datos
        orders = read_sql("SELECT * FROM olist_orders", database)
        items = read_sql("SELECT * FROM olist_order_items", database)
        products = read_sql("SELECT * FROM olist_products", database)

        print(f"Pedidos cargados: {len(orders)} filas")
        print(f"Items cargados: {len(items)} filas")
        print(f"Productos cargados: {len(products)} filas")

        # Realizar la fusión de las tablas
        data = pd.merge(items, orders, on='order_id', how='inner')
        data = pd.merge(data, products, on='product_id', how='inner')

        # Filtrar solo pedidos entregados
        delivered = data[data['order_status'] == 'delivered']
        print(f"Pedidos entregados: {len(delivered)} filas")

        # Agrupar por order_id y calcular las sumas
        aggregations = delivered.groupby('order_id').agg({
            'freight_value': 'sum',
            'product_weight_g': 'sum'
        }).reset_index()

        print(f"Resultado de la agregación: {aggregations.head()}")

        return QueryResult(query=query_name, result=aggregations)

    except Exception as e:
        print(f"Error en query_freight_value_weight_relationship: {e}")
        raise



def query_orders_per_day_and_holidays_2017(database: Engine) -> QueryResult:
    query_name = QueryEnum.ORDERS_PER_DAY_AND_HOLIDAYS_2017.value

    holidays = read_sql("SELECT * FROM public_holidays", database)
    orders = read_sql("SELECT * FROM olist_orders", database)

    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])

    filtered_dates = orders[orders['order_purchase_timestamp'].dt.year == 2017]

    order_purchase_ammount_per_date = filtered_dates['order_purchase_timestamp'].dt.date.value_counts().reset_index()
    order_purchase_ammount_per_date.columns = ['date', 'order_count']

    holidays['date'] = pd.to_datetime(holidays['date']).dt.date
    order_purchase_ammount_per_date['holiday'] = order_purchase_ammount_per_date['date'].isin(holidays['date'])

    result_df = order_purchase_ammount_per_date

    return QueryResult(query=query_name, result=result_df)


def get_all_queries() -> List[Callable[[Engine], QueryResult]]:
    return [
        query_delivery_date_difference,
        query_global_ammount_order_status,
        query_revenue_by_month_year,
        query_revenue_per_state,
        query_top_10_least_revenue_categories,
        query_top_10_revenue_categories,
        query_real_vs_estimated_delivered_time,
        query_orders_per_day_and_holidays_2017,
        query_freight_value_weight_relationship,
    ]


def run_queries(database: Engine) -> Dict[str, DataFrame]:
    query_results = {}
    for query in get_all_queries():
        try:
            print(f"Ejecutando {query.__name__}")
            query_result = query(database)
            print(f"Consulta '{query_result.query}' ejecutada con éxito.")
            print(query_result.result.head())
            query_results[query_result.query] = query_result.result
        except Exception as e:
            print(f"Error al ejecutar la consulta '{query.__name__}': {e}")
    return query_results

