import mysql.connector
import random
from datetime import datetime, timedelta
import logging

# Configurar el registro
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Conectar a la base de datos MySQL
        logger.info("Connecting to the database...")
        conn = mysql.connector.connect(
            host='database-3.cp42228aw8qt.us-east-1.rds.amazonaws.com',
            user='admin',
            password='gamer451',
            database='sakila'
        )
        cursor = conn.cursor()

        # Obtener clientes y películas con sus frecuencias de alquiler
        logger.info("Fetching customers...")
        cursor.execute("""
            SELECT c.customer_id, COUNT(r.rental_id) AS rental_count
            FROM rental r
            JOIN customer c ON r.customer_id = c.customer_id
            GROUP BY c.customer_id
        """)
        customers = cursor.fetchall()

        logger.info("Fetching films...")
        cursor.execute("""
            SELECT f.film_id, COUNT(r.rental_id) AS rental_count
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            GROUP BY f.film_id
        """)
        films = cursor.fetchall()

        # Crear listas de clientes y películas basadas en sus probabilidades
        customer_choices = [customer[0] for customer in customers for _ in range(customer[1])]
        film_choices = [film[0] for film in films for _ in range(film[1])]

        # Simular alquileres para 100 clientes
        new_rentals = []
        for _ in range(100):
            customer_id = random.choice(customer_choices)
            film_id = random.choice(film_choices)
            rental_date = datetime.now()
            return_date = rental_date + timedelta(days=random.randint(1, 14))

            new_rentals.append((customer_id, film_id, rental_date, return_date))

        # Insertar nuevos alquileres en la base de datos
        logger.info("Inserting new rentals...")
        for rental in new_rentals:
            cursor.execute("""
                INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (rental[2], rental[1], rental[0], rental[3], 1))

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Successfully added 100 new rentals")
        return {
            'statusCode': 200,
            'body': 'Successfully added 100 new rentals'
        }

    except Exception as e:
        logger.error(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': f"Error: {e}"
        }
