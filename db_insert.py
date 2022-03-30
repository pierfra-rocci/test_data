from db_module import load_connection, insert_data
import pandas as pd
import psycopg2


if __name__ == "__main__":
    # host, database, user, password
    conn_info = load_connection("db_postgresql.ini")
    # Connect to the "places" database
    connection = psycopg2.connect(**conn_info)
    cursor = connection.cursor()

    # Insert data into the "place" table
    df_place = pd.read_csv('data/lieux.csv')
    place_query = "INSERT INTO place(commune, departement, region) VALUES %s"
    insert_data(place_query, connection, cursor, df_place, 1000)

    # Insert data into the "people" table
    df_people = pd.read_csv('data/people.csv')
    people_query = "INSERT INTO people(nom, prenom, date_naissance, commune) VALUES %s"
    insert_data(people_query, connection, cursor, df_people, 20000)

    # Close connections to the database
    connection.close()
    cursor.close()