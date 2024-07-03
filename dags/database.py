# import psycopg2
# import os
# from dotenv import load_dotenv


# load_dotenv() 

# #Redshift connection
# url = os.environ.get('redshift_url')
# database = os.environ.get('redshift_database')
# user = os.environ.get('redshift_user')
# database_password = os.environ.get('redshift_password')

# def create_redshift_connection():
#     try:
#         conn = psycopg2.connect(
#             host = url,
#             dbname = database,
#             user = user,
#             password = database_password,
#             port='5439'
#         )
#         print("Connected successfully!")
#         return conn
        
#     except Exception as e:
#         print("It has not been able to make the connection")
#         print(e)