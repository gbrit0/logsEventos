import struct
import socket
import time, datetime
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error



import pandas as pd


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
   # con = pymysql.connect(host='192.168.4.50', user='root',
   #                         password='025supergerasol', database='sup_geral')
   
   # sql = """
   #    SELECT 
   #       `host`, `porta`, `modbus_id`, `equipamentos`.`cod_tipo_equipamento`
   #    FROM
   #       `modbus_tcp`
   #          LEFT JOIN
   #       `equipamentos` ON `modbus_tcp`.`cod_equipamento` = `equipamentos`.`codigo`
   #    WHERE
   #       `modbus_tcp`.`cod_equipamento` = %s AND `modbus_tcp`.`ativo` = 1   
   # """
   # with con:
   #    with con.cursor() as cursor:
   #          cursor.execute(sql,codEquipamento)
   #          result = cursor.fetchone()
            
            
            return '10.10.63.11', 502, 1, 16, codEquipamento

def processarResposta(resp: bytes) -> str:
   try:  
      data = struct.unpack(
            """>3H83B29H30B""",
            resp
         )
   except struct.error:
      return Exception
   
   # print(data)
   text = [chr(x) for x in data[6:86] if x != 0]

   # Juntar a lista de caracteres em uma string
   text = ''.join(text)

   # Dividir a string no caractere '\xa0' e selecionar a parte antes do primeiro '\xa0'
   # text = text.split('\xa0')[0]

   # dados = {
   #       "transactionId":   data[0],
   #       "protocolId":      data [1],
   #       "unitId":          data[3],
   #       "functionCode":    data[4],
   #       "byteCount":       data[5],
   #       "text":            text,
   #       "year":            data[86],
   #       "month":           data[87],
   #       "day":             data[88],
   #       "hour":            data[89],
   #       "minute":          data[90],
   #       "second":          data[91],
   #       "milisecond":      data[92],
   #       "channel":         data[93],
   #       "ppower":          data[94],
   #       "qpower":          data[95],
   #       "pf":              data[96],
   #       "genU1":           data[97],
   #       "genU2":           data[98],
   #       "genU3":           data[99],
   #       "genI1":           data[100],
   #       "genI2":           data[101],
   #       "genI3":           data[102],
   #       "genF":            data[103],
   #       "busU1":           data[104],
   #       "busU2":           data[105],
   #       "busU3":           data[106],
   #       "busF":            data[107],
   #       "df/dt":           data[108],
   #       "vector":          data[109],
   #       "multiInput20":    data[110],
   #       "multiInput21":    data[111],
   #       "multiInput22":    data[112],
   #       "multiInput23":    data[113],
   #       "tacho":           data[114],
   #       "alarmValue":      data[115]
   #    }

   # date =    f"{data[86]}-{data[87]}-{data[88]} {data[89]}:{data[90]}:{data[91]}.{data[92]}"
   date = datetime.datetime(year=data[86], month=data[87], day=data[88], 
                            hour=data[89], minute=data[90], second=data[91], microsecond=data[92])

   return text, data, date

# def getEngine(host, port, user, password):
#     return create_engine(
#         url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
#             user, password, host, port, "testes"
#         )
#     )


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
   # id, cod_equipamento, tipo_equipamento, nome_event, text_event, data_cadastro

   host, porta, modbusId, codTipoEquipamento, codEquipamento = recuperarParametrosCounicacao(293) # host, porta, modbusId
   # print(host, porta, modbusId, codTipoEquipamento)

   
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
                        # log.append([codEquipamento, codTipoEquipamento, textEvent, nomeEvent, date])

                        # sql = f"""INSERT INTO `logs` (cod_equipamento, cod_tipo_equipamento, nome_event, text_event, data_cadastro) 
                        #             VALUES ({codEquipamento}, {codTipoEquipamento}, '{nomeEvent}', '{textEvent}', {date})
                        # """
                        # cursor.execute(sql)
                        escreveNoBanco(cnx, cursor, codEquipamento, codTipoEquipamento, nomeEvent, textEvent, date)
                        
                     else: StopIteration
                  except Exception:
                     StopIteration
               # engine = getEngine(host, porta, "root", "025supergerasol")
               # df = pd.DataFrame(log)
               # # df.to_csv('teste.csv', index=False, encoding='utf-8')
               # df.to_sql('testelogs',engine,'testes',if_exists='append')
               

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