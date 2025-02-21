from .database import engine
from sqlalchemy import text
try:
    # Intenta conectar y ejecutar una consulta simple
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        print("Conexión exitosa, resultado:", result.fetchone())
except Exception as e:
    print("Error en la conexión:", e)