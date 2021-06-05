# import psycopg2
# from psycopg2 import Error
# import pipelines.config.envConfig as env

# psql_pool = psycopg2.pool.SimpleConnectionPool(
#     user=env.psql_db_user,
#     password=env.psql_db_password,
#     host=env.psql_db_host,
#     port=env.psql_db_port,
#     database=env.psql_db_name,
# )


# class PostgreSql:
#     def __init__(self):
#         pass

#     def run(self):
#         conn = psql_pool.getconn()
#         with conn:
#             with conn.cursor() as curs, open("../sql/create_tables.sql", "r") as sql:
#                 curs.execute(sql.read())

#         conn.close()


# def connectedPg():
#     num_threads = 20
#     threads = []

#     psql_pool.closeall()


# # try:
# #     # Connect to an existing database
# #     connection = psycopg2.connect(
# #         user="postgres",
# #         password="pynative@#29",
# #         host="127.0.0.1",
# #         port="5432",
# #         database="postgres_db",
# #     )

# #     # Create a cursor to perform database operations
# #     cursor = connection.cursor()

# #     # Print PostgreSQL details
# #     print("PostgreSQL server information")
# #     print(connection.get_dsn_parameters(), "\n")
# #     # Executing a SQL query
# #     cursor.execute(open("schema.sql", "r").read())
# #     cursor.execute("SELECT version();")
# #     # Fetch result
# #     record = cursor.fetchone()
# #     print("You are connected to - ", record, "\n")

# # except (Exception, Error) as error:
# #     print("Error while connecting to PostgreSQL", error)
# # finally:
# #     if connection:
# #         cursor.close()
# #         connection.close()
# #         print("PostgreSQL connection is closed")