import holidays
import mysql.connector
from datetime import datetime

# Conectar a la base de datos MySQL
conn = mysql.connector.connect(
    host='database-3.cp42228aw8qt.us-east-1.rds.amazonaws.com',
    user='admin',
    password='gamer451',  # Reemplaza 'your_password' con tu contraseña real
    database='datawarehouse_sakila'
)
cursor = conn.cursor()

# Generar los días festivos de Estados Unidos
us_holidays = holidays.US(years=range(2000, 2025))

# Crear una lista de días festivos
holiday_dates = list(us_holidays.keys())

# Actualizar los días festivos en la tabla dim_date
for holiday_date in holiday_dates:
    cursor.execute("""
        UPDATE dim_date
        SET is_holiday = TRUE
        WHERE date = %s
    """, (holiday_date,))

# Confirmar los cambios y cerrar la conexión
conn.commit()
cursor.close()
conn.close()
