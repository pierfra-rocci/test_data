from db_module import load_connection, create_db, create_table
import psycopg2


if __name__ == "__main__":
    # host, database, user, password
    conn_info = load_connection("db_postgresql.ini")

    # Create the database
    create_db(conn_info)

    # Connect to the database created
    connection = psycopg2.connect(**conn_info)
    cursor = connection.cursor()

    # Create the "place" table
    place_table = """
        CREATE TABLE place (
            id SERIAL PRIMARY KEY,
            commune VARCHAR(100) NOT NULL,
            departement VARCHAR(100) NOT NULL,
            region VARCHAR(100) NOT NULL
        )
    """
    create_table(place_table, connection, cursor)

    people_table = """
        CREATE TABLE people (
            id SERIAL PRIMARY KEY,
            nom VARCHAR(100) NOT NULL,
            prenom VARCHAR(100) NOT NULL,
            date_naissance VARCHAR(100) NOT NULL,
            commune VARCHAR(100) NOT NULL
        )
    """
    create_table(people_table, connection, cursor)

    # Close connections to the database
    connection.close()
    cursor.close()