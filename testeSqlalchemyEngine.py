# Funciona mas escrever usando o pandas não estã dando certo eo imagino que, já que ´epra converter para JS depois, n vai ser legal usar pandas


from sqlalchemy import create_engine

user = "root"
password = "025supergerasol"
host = "127.0.0.1"
port = 3306
db = "testes"

def get_connection():
    return create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, db
        )
    )

if __name__ == '__main__':
 
    try:
       
        # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
        engine = get_connection()
        print(
            f"Connection to the {host} for user {user} created successfully.")
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)