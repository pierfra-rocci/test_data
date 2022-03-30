from db_module import load_connection, get_column_names, get_data_from_db
import pandas as pd
import psycopg2


if __name__ == "__main__":
    # host, database, user, password
    conn_info = load_connection("db_postgresql.ini")
    # Connect to the "houses" database
    connection = psycopg2.connect(**conn_info)
    cursor = connection.cursor()

    # These names must match the columns names returned by the SQL query
    col_names = get_column_names("place", cursor)
    # Create an empty DataFrame
    df_place = pd.DataFrame(columns=col_names)
    query = "SELECT * from place;"
    df_place = get_data_from_db(query, connection, cursor, df_place, col_names)
    # print(df_place)

    # These names must match the columns names returned by the SQL query
    col_names = get_column_names("people", cursor)
    # Create an empty DataFrame
    df_people = pd.DataFrame(columns=col_names)
    query = "SELECT * from people;"
    df_people = get_data_from_db(query, connection, cursor, df_people, col_names)
    # print(df_people)


    col_names = ["Departement", "Number_of_Born"]
    # Create an empty DataFrame
    df_people_place = pd.DataFrame(columns=col_names)
    query = """
        SELECT departement AS Departement,
        COUNT(*) AS number_of_born
        FROM place
        INNER JOIN people 
        ON people.commune = place.commune  
        GROUP BY place.departement, Departement
        ORDER BY Departement;
        """
    df_people_place = get_data_from_db(query, connection, cursor, df_people_place, col_names)
    print(df_people_place, df_people_place.sum())
    df_people_place.to_json(r'naissances_par_departement.json')
    
    col_names = ["Region", "Number_of_Born"]
    # Create an empty DataFrame
    df_people_place = pd.DataFrame(columns=col_names)
    query = """
        SELECT region AS Region,
        COUNT(*) AS number_of_born
        FROM place
        INNER JOIN people 
        ON people.commune = place.commune  
        GROUP BY place.region, Region
        ORDER BY Region;
        """
    df_people_place = get_data_from_db(query, connection, cursor, df_people_place, col_names)
    print(df_people_place, df_people_place.sum())
    df_people_place.to_json(r'naissances_par_region.json')

    # Close connections to the database
    connection.close()
    cursor.close()