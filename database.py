import psycopg2

#Redshift connection
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user="julianlavie16_coderhouse"
with open("/Users/julianlavie/Desktop/password_redshift.txt",'r') as f:
    database_password= f.read()

try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password=database_password,
        port='5439'
    )
    print("Connected successfully!")
    
except Exception as e:
    print("It has not been able to make the connection")
    print(e)