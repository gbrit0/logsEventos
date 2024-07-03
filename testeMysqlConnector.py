import mysql.connector
from mysql.connector import Error

try:
   cnx = mysql.connector.connect(user='root', password='025supergerasol',
                                 host='127.0.0.1',
                                 database='testes')
   
   print(f"Conexão bem sucedida")

   sql = f"""
      INSERT INTO `testelogs` (cod_equipamento, cod_tipo_equipamento, nome_event, text_event, data_cadastro) 
         VALUES ({442}, {442}, '{442}', '{157}', {2024})
   """

   cursor = cnx.cursor()
   cursor.execute(sql)

   cnx.commit()

   sql2 = """SELECT * FROM testelogs"""
   cursor.execute(sql2)
   print(cursor.fetchall())

   cursor.close()
   cnx.close()
except Error as e:
   print(e)