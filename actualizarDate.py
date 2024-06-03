import mysql.connector
import holidays
from datetime import datetime, timedelta
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    logger.info("Lambda function has started.")
    
    # Configuración de conexión a MySQL
    mysql_config = {
        'host': 'database-3.cp42228aw8qt.us-east-1.rds.amazonaws.com',
        'user': 'admin',
        'password': 'gamer451',
        'database': 'datawarehouse_sakila'  # Base de datos correcta
    }

    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        # Determinar el rango de fechas a insertar
        start_date = datetime.now().date()
        end_date = start_date + timedelta(days=365)  # Un año de fechas

        us_holidays = holidays.US()

        # Insertar nuevas fechas en la tabla dim_date
        for single_date in (start_date + timedelta(n) for n in range((end_date - start_date).days)):
            date_key = int(single_date.strftime('%Y%m%d'))
            date = single_date.strftime('%Y-%m-%d')
            day = single_date.day
            month = single_date.month
            year = single_date.year
            day_of_week = single_date.strftime('%A')
            is_holiday = single_date in us_holidays
            is_weekend = day_of_week in ['Saturday', 'Sunday']
            quarter = (single_date.month - 1) // 3 + 1

            cursor.execute("""
                INSERT INTO dim_date (date_key, date, day, month, year, day_of_week, is_holiday, is_weekend, quarter)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    day = VALUES(day),
                    month = VALUES(month),
                    year = VALUES(year),
                    day_of_week = VALUES(day_of_week),
                    is_holiday = VALUES(is_holiday),
                    is_weekend = VALUES(is_weekend),
                    quarter = VALUES(quarter)
            """, (date_key, date, day, month, year, day_of_week, is_holiday, is_weekend, quarter))

        conn.commit()
        logger.info(f"Date dimension updated successfully from {start_date} to {end_date}")

    except mysql.connector.Error as err:
        logger.error(f"MySQL Error: {err}")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return {
        'statusCode': 200,
        'body': 'Date dimension updated successfully'
    }
