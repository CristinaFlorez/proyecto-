from typing import Dict
from pandas import DataFrame
from sqlalchemy.engine.base import Engine

def load(data_frames: Dict[str, DataFrame], database: Engine):
    """
    Load the dataframes into the SQLite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
        database (Engine): SQLAlchemy database engine.
    """


    # TODO: Implementa esta función. Por cada DataFrame en el diccionario, debes
    # usar pandas.DataFrame.to_sql() para cargar el DataFrame en la base de datos
    # como una tabla.
    # Para el nombre de la tabla, utiliza las claves del diccionario `data_frames`.

    # Para cada DataFrame en el diccionario, cargar en la base de datos usando to_sql()

    for table_name, df in data_frames.items():
        try:
            df.to_sql(table_name, database, if_exists='replace', index=False)
            print(f'Tabla "{table_name}" cargada correctamente en la base de datos.')
        except Exception as e:
            print(f'Error al cargar la tabla "{table_name}": {e}')

