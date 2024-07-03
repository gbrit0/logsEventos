import struct
import socket
import time, datetime
import mysql.connector
from mysql.connector import Error


def conexao(host: str, porta: int) -> socket.socket:
   con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   con.connect((host, porta))

   return con


def gerarRequisicao(transactionId: int, unitId: int, startingAddress = 0) -> bytes:
   """tipoLog: 'E' para eventos ou 'A' para alarmes"""
   
   return struct.pack(
      '>3H2B2H',
      transactionId,
      0,
      6,
      unitId,
      67,
      startingAddress,
      84
   )


def recuperarParametrosCounicacao(codEquipamento: int) -> list:
   
   sql = f"""
      SELECT 
         `host`, `porta`, `modbus_id`, `equipamentos`.`cod_tipo_equipamento`
      FROM
         `modbus_tcp`
            LEFT JOIN
         `equipamentos` ON `modbus_tcp`.`cod_equipamento` = `equipamentos`.`codigo`
      WHERE
         `modbus_tcp`.`cod_equipamento` = {codEquipamento} AND `modbus_tcp`.`ativo` = 1   
   """
   with mysql.connector.connect( user='root', 
                                 password='025supergerasol',
                                 host='192.168.4.50',
                                 database='sup_geral') as con:
      with con.cursor() as cursor:
         cursor.execute(sql)
         result = cursor.fetchone()
         
         # host, porta, modbusId, codTipoEquipamento
         return result[0], result[1], result[2], result[3], codEquipamento


def processarResposta(resp: bytes) -> str:
   try:  
      data = struct.unpack(
            """>3H83B29H30B""",
            resp
         )
   except struct.error:
      return Exception
   

   text = [chr(x) for x in data[6:86] if x != 0]

   # Juntar a lista de caracteres em uma string
   text = ''.join(text)

   date = datetime.datetime(year=data[86], month=data[87], day=data[88], 
                            hour=data[89], minute=data[90], second=data[91], microsecond=data[92])

   return text, data, date



def escreveNoBanco(cnx, cursor, codEquipamento, codTipoEquipamento, nomeEvent, textEvent, date):
   sql = f"""
      INSERT INTO `testelogs` (cod_equipamento, cod_tipo_equipamento, nome_event, text_event, data_cadastro) 
         VALUES ({codEquipamento}, {codTipoEquipamento}, '{nomeEvent}', '{textEvent}', '{date}')
   """
   try:
      cursor.execute(sql)
      cnx.commit()
   except Error as e:
       print(e)


def main():
   inicio = time.time()

   host, porta, modbusId, codTipoEquipamento, codEquipamento = recuperarParametrosCounicacao(293) 

   
   log = []
   with conexao(host, porta) as con:
      with mysql.connector.connect(user='root', password='025supergerasol',
                                 host='127.0.0.1',
                                 database='testes') as cnx:
         with cnx.cursor() as cursor:
            try:
               for startingAddress in range(500):
                  
                  req = gerarRequisicao(startingAddress,modbusId,startingAddress) # startingAddress é sempre o mesmo número que o transactionId
                  con.send(req)
                  res = con.recv(1024)
                  try:
                     nomeEvent, textEvent, date = processarResposta(res)
                     if nomeEvent != '':
                        escreveNoBanco(cnx, cursor, codEquipamento, codTipoEquipamento, nomeEvent, textEvent, date)
                        
                     else: StopIteration
                  except Exception:
                     StopIteration
               

            except Error as e:
               print(e)

   fim = time.time()
   print(f"tempo de execução: {(fim-inicio):.2f} segundos")



if __name__ == "__main__": main()



# id, cod_equipamento, tipo_equipamento, nome_event, text_event, data_cadastro

# em text_event vc coloca a linha inteira e define no banco esse campo como sendo "text", 
# pq dai vc consegue transformar o array da linha inteira em texto e transformar de volta 
# em array quando for usar

# dai vc vai precisar só de outra tabela com id, cod_tipo_equipamento, colunas

# AGC200

# 16	GERADOR ELETRONICO COM AGC-200
# 29	GERADOR COM AGC200 SEM MOTOR ELETRONICO
# 31	MAINS COM AGC-200
# 37	GERADOR COM AGC-200 2

# AGC150

# 27	GERADOR COM AGC 150 E MOTOR ELETRONICO
# 41	GERADOR COM AGC 150 2
# 43	GERADOR COM AGC 150 BIOGAS
# 51	GERADOR COM AGC 150 HIBRIDO 34 KV
# 55	GMG CAM ENERGY AGC 150

# AGC 3/4

# 1	GERADOR COM MODULO AGC-3/4 E MOTOR ELETRONICO
# 4	GERADOR COM AGC-3 E MOTOR PERKINS ELETRONICO
# 5	MAINS COM AGC-4
# 6	MAINS AGC 4-34,5 kV
# 7	MAINS COM AGC-3 13.8kV
# 8	MAINS COM AGC-3 34kV
# 18	GERADOR COM AGC-4 - SLIM/TWININFINITY
# 22	GERADOR COM CGC400 SEM MOTOR ELETRONICO
# 40	SICES GC-400 - GERADOR
# 41	GERADOR COM AGC 150 2
# 43	GERADOR COM AGC 150 BIOGAS
# 51	GERADOR COM AGC 150 HIBRIDO 34 KV