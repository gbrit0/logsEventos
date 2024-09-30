import struct
import socket
import time, datetime
import os
import argparse
import mysql.connector # type: ignore
# from memory_profiler import profile


def conectarComModbus(idSolicitacao: str, host: str, porta: int): #  -> socket.socket
   try:
      con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      con.settimeout(100)
      con.connect((host, porta))
   except TimeoutError as e:
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'Erro de conexão com Modbus: {e}'\n")
      # print(f"Erro de conexão com Modbus: {e}")
      return
   finally:
      return con
      
# @profile

def gerarRequisicao(transactionId: int = 0, unitId: int = 1, startingAddress: int = 0, tipoLog: int = 0, codTipoEquipamento: int = 88) -> bytes:
   """tipoLog= 0 para eventos ou 1 para alarmes
      tipoLog = 3: teste de conexão e recupera modelo do controlador
   """
   
   codFuncao = 67
   codCampo = startingAddress
   
   # print(f'geraRequisicao codTipoEquipamento: {codTipoEquipamento}')

   if codTipoEquipamento == 182:
      quantidade = 3
   elif tipoLog == 3:
      codFuncao = 4
      codCampo = 59900
      quantidade = 1
   else:
      quantidade = 84
      
   # print(f'quantidade: {quantidade}')
   return struct.pack(
      '>3H2B2H',
      transactionId,
      0,
      6,
      unitId,
      codFuncao,
      codCampo,
      quantidade
   )


def recuperarParametrosCounicacao(idSolicitacao, codEquipamento: int, conexaoComBanco, cursor) -> list:
   
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
   
   # mudar host para as variáveis de ambiente
   try:
      # with connector.connect( user=os.environ['USER'], 
      #                         password=os.environ['PASSWORD'],
      #                         host='192.168.4.50',
      #                         database=os.environ['DATABASE']) as con:
      #    # print(f"user: {os.environ['USER']}")
      #    # print(f"password: {os.environ['PASSWORD']}")
      #    # print(f"database: {os.environ['DATABASE']}")

      #    with con.cursor() as cursor:
               cursor.execute(sql)
               result = cursor.fetchone()
               
               # host, porta, modbusId, codTipoEquipamento
               return result[0], result[1], result[2], result[3] #, codEquipamento
   except mysql.connector.InterfaceError as e:
      # print(f"Erro de interface MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de interface MySQL: {e}'\n")
   except mysql.connector.DatabaseError as e:
      # print(f"Erro de banco de dados MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de banco de dados MySQL: {e}'\n")
   except mysql.connector.OperationalError as e:
      # print(f"Erro operacional MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro operacional MySQL: {e}'\n")
   except mysql.connector.IntegrityError as e:
      # print(f"Erro de integridade MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de integridade MySQL: {e}'\n")
   except mysql.connector.ProgrammingError as e:
      # print(f"Erro de programação MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id{idSolicitacao}        'Erro de programação MySQL: {e}'\n")
   except mysql.connector.DataError as e:
      # print(f"Erro de dados MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de dados MySQL: {e}'\n")
   except mysql.connector.Error as e:
      # print(f"Erro de conexão MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de conexão MySQL: {e}'\n")

   

def hexParaDatetime(hex):
   diasHoras = hex[:6]  
   minutosSegundos = hex[6:] 
   
   dias = int(diasHoras, 16)  # --> somando 8 na data inicial
   diasComFracao = dias / 24
   dias = int(diasComFracao)
   horasFracionais = diasComFracao - dias
   horas = int(horasFracionais * 24) - 3360
   
   segundosComFracao = int(minutosSegundos, 16) / 10
   segundos = int(segundosComFracao)
   microsegundos = int((segundosComFracao - segundos) * 1_000_000)
   minutes = segundos // 60
   segundosRestantes = segundos % 60
   
   dataInicial = datetime.datetime(year=1781, month=8, day=7, hour=16, microsecond=1) # Adicionando 1 microssegundo para evitar 0's nesse campo e ñ bugar no banco
   
   dataFinal = dataInicial + datetime.timedelta(days=dias, hours=horas, minutes=minutes, seconds=segundosRestantes, microseconds=microsegundos)
   
   return dataFinal


def processarRespostaModbus(codTipoEquipamento, resp: bytes):
   # print(f'tamRes:{len(resp)}')
   # print(resp[:76].hex())
   # print(resp[76:152].hex())
   # print(resp[152:].hex())

   # a = struct.unpack(
   #    '>18B5L19H',
   #    resp[:76]
   # )
   # print(a)
   # b = struct.unpack(
   #    '>18B5L19H',
   #    resp[76:152]
   # )
   # print(b)
   # c = struct.unpack(
   #    '>18B5L19H',
   #    resp[152:]
   # )
   # print(c)
   if codTipoEquipamento == 182:
      
      for i in range(0,228,76):
         # print(resp[i:76+i].hex())
         text = extrair_texto(resp[0+i:19+i], codTipoEquipamento)
         data = struct.unpack(
            '>26h',
            resp[24+i:76+i]
         )
         date = resp[19+i:24+i].hex()
         date = hexParaDatetime(date)
         # print(text)
         # print(data)
         # print(date)
         if date.month == datetime.datetime.now().month:
            yield [text, data, date]  
            # print(f'{resp[19+i:24+i].hex()},{date}') 
         else:
            return


   else:
      try:
         data = struct.unpack(
            """>3H83B30h28b""",
            resp
         )
         
      except struct.error as e:
         # print(f"struct error: {e}")
         # with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         #    file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'struct error: {e}'\n")
         return (None, None, None)
      
      if data[86] == 0:
         return (None, None, None)

      text =  data[6:86]
      text = extrair_texto(text, codTipoEquipamento)

      date = datetime.datetime(year=data[86], month=data[87], day=data[88], 
                              hour=data[89], minute=data[90], second=data[91], microsecond=data[92]*1000)
      # print(text, data, date)
      return [text, data, date]
   

def extrair_texto(caracteres, codTipoEquipamento):
   texto = []
   i = 0
   if codTipoEquipamento == 182:

      while i < len(caracteres):
         try:   
            if caracteres[i] == 0x20 and caracteres[i + 1] == 0x20:
               break
            if caracteres[i] != 0x00:
               texto.append(chr(caracteres[i]))
            i += 1
         except IndexError:
            break

   else:

      while i < len(caracteres):
            if caracteres[i] == 0x00 and caracteres[i + 1] == 0x00:
               break
            if caracteres[i] != 0x00:
               texto.append(chr(caracteres[i]))
            i += 1

   return "".join(texto)



def escreverLogNoBancoLinhaALinha(conexaoComBanco, cursor, codEquipamento, codTipoEquipamento, nomeEvent, textEvent, date, tipoLog = 0):
   
   if tipoLog == 1: 
      tabela , coluna1, coluna2 = ['event_alarm', 'nome_alarm', 'text_alarm']
   else:
      tabela, coluna1, coluna2 = ['event_log', 'nome_event', 'text_event']

   sql = f"""
      INSERT INTO `{tabela}` (cod_equipamento, cod_tipo_equipamento, {coluna1}, {coluna2}, data_cadastro) 
         VALUES ({codEquipamento}, {codTipoEquipamento}, '{nomeEvent}', '{textEvent[93:]}', '{date}')    
   """
   # de text[86:93] está a data
   # de text[93:] estão os valores dos parâmetros
   try:
      cursor.execute(sql)
      conexaoComBanco.commit()
      
   except mysql.connector.Error as e:
      print(f"erro de conexao MySQL: {e}")



def escreverLogNoBanco(pool, values, tipoLog):
   if tipoLog == 1: 
      tabela , coluna1, coluna2 = ['event_alarm', 'nome_alarm', 'text_alarm']
   else:
      tabela, coluna1, coluna2 = ['event_log', 'nome_event', 'text_event']

   sql = f"""
      INSERT IGNORE INTO `{tabela}` (cod_equipamento, cod_tipo_equipamento, {coluna1}, {coluna2}, data_cadastro) 
         VALUES (%s, %s, %s, %s, %s)    
   """
   tentativas = 0
   maxTentativas = 3

   while tentativas < maxTentativas:
      try:
         with pool.get_connection() as conexaoComBanco:
               with conexaoComBanco.cursor() as cursor:
                  cursor.executemany(sql, values)
                  conexaoComBanco.commit()
         break  
      except mysql.connector.errors.InternalError as e:
         tentativas += 1
         if tentativas < maxTentativas:
            time.sleep(3)  
         else: 
            raise e
      
      



def buscarColunasPorTipoEquipamento(codTipoEquipamento: int, cursor):
   query = f"""
      SELECT colunas from colunas_por_tipo_equipamento WHERE cod_tipo_equipamento = {codTipoEquipamento};
   """

   cursor.execute(query)
   return cursor.fetchone()
   


def buscarLogsNoBanco(cursor, codEquipamento, tipoLog):
   if tipoLog == 1: 
      tabela , coluna1, coluna2 = ['event_alarm', 'nome_alarm', 'text_alarm']
   else:
      tabela, coluna1, coluna2 = ['event_log', 'nome_event', 'text_event']
   query = f"""
      SELECT 
         data_cadastro, cod_equipamento, cod_tipo_equipamento, {coluna1}, {coluna2} 
      FROM 
         {tabela}
      WHERE 
         cod_equipamento = {codEquipamento} 
      ORDER BY 
         data_cadastro 
      DESC 
      LIMIT 500
   """
   cursor.execute(query)
   return cursor.fetchall()



def expandirTextEvent(log, colunas):
   dataCadastro, codEquipamento, codTipoEquipamento, nomeEvent, textEvents  = log
   textEvents = textEvents.strip('()').split(', ')
   # print(type(textEvents))
   textEventsExpandido = {}
   for index, coluna in enumerate(colunas):
      textEventsExpandido[coluna] = textEvents[index]

   logExpandido = {
      'data_cadastro': dataCadastro,
      'cod_equipamento': codEquipamento,
      'codTipoEquipamento': codTipoEquipamento,
      'nome_event': nomeEvent,

   }

   logExpandido.update(textEventsExpandido)
   
   return logExpandido
   

def processarLogs(logs, colunas):
    todosLogs = []
    for log in logs:
        logExpandido = expandirTextEvent(log, colunas)
        todosLogs.append(logExpandido)
    return todosLogs


def buscarUltimaLinhaLog(codEquipamento, cursor, tipoLog = 0):
   if tipoLog == 1: 
      tabela , colunaNome, colunaText = ['event_alarm', 'nome_alarm', 'text_alarm']
   else:
      tabela, colunaNome, colunaText = ['event_log', 'nome_event', 'text_event']

   query = f"""
      SELECT 
         cod_equipamento, cod_tipo_equipamento, {colunaNome}, {colunaText},  data_cadastro 
      FROM {tabela} 
      WHERE cod_equipamento = {codEquipamento} 
      HAVING max(data_cadastro)
      ORDER BY data_cadastro DESC 
      LIMIT 1
   """

   cursor.execute(query)
   return cursor.fetchone()

   


# def abreConexaoComBancoEExecutaFuncao(func, **kwargs):
   
#    with mysql.connector.connect(user=os.environ['MYSQL_USER'], 
#                           password=os.environ['MYSQL_PASSWORD'], 
#                           host=os.environ['MYSQL_HOST'], 
#                           database=os.environ['MYSQL_DATABASE']) as conexaoComBanco:
#       with conexaoComBanco.cursor() as cursor:
#             func(conexaoComBanco=conexaoComBanco, cursor=cursor, **kwargs)


def testaConexaoModbusERecuperaTipoEquipamento(idSolicitacao, host, porta):
   req = gerarRequisicao(tipoLog=3)
   try:
      with conectarComModbus(idSolicitacao, host, porta) as conexaoComModbus:
         conexaoComModbus.send(req)
         return struct.unpack(
            ">3H3BH",
            conexaoComModbus.recv(1024))[6]
   except:
      return 0


def fetchLog(idSolicitacao: int,
             codEquipamento: int,
             modbusId: int,
             host: str,
             porta: int,
             tipoLog = 0):

   try:
      pool = mysql.connector.pooling.MySQLConnectionPool(
         pool_name="MySqlPool",
         pool_size=5,
         user=os.environ['MYSQL_USER'],
         password=os.environ['MYSQL_PASSWORD'],
         host=os.environ['MYSQL_HOST'],
         database=os.environ['MYSQL_DATABASE']
      )

      with pool.get_connection() as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            codTipoEquipamento = testaConexaoModbusERecuperaTipoEquipamento(idSolicitacao, host, porta)
            # print(f'codTipoEquipamento - {codTipoEquipamento}')

            if codTipoEquipamento == 0: # codTipoEquipamento == 0 quer dizer que não foi possível conectrar com o modbus
               with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
                  file.write(f"{datetime.datetime.now()}       id:{id}        'Conexão com o equipamento {codEquipamento} não estabelecida'\n")
                  return
            else:      
               ultimaLinha = buscarUltimaLinhaLog(codEquipamento, cursor, tipoLog)

               if ultimaLinha is None:
                  ultimaLinha = (0, 0, '', '', datetime.datetime(1900,1,1,0,0,0,0))
            
            # print(f"ultimaLinha: {ultimaLinha}")

         if tipoLog == 1:  # Log Alarmes
            if codTipoEquipamento == 88:
               ran = range(500, 651)
            elif codTipoEquipamento == 182:
               ran = range(500, 998, 3)
            else:
               ran = range(500, 1000)
         elif codTipoEquipamento == 88:
            ran = range(151)
         elif codTipoEquipamento == 182:
               ran = range(0, 500, 3)
         else:
            # Log Eventos
            ran = range(500)

         # print(f'ran - {ran}')

      with conectarComModbus(idSolicitacao, host, porta) as conexaoComModbus:
         try:
            values = []
            
            if tipoLog == 0:
               registerValue = 0
            elif tipoLog == 1:
               registerValue = 1
               
            req = struct.pack(
                  '>3H2B2HBH',
                  0, #transaction Id
                  0, # protocol id
                  9, # length
                  18, #unit Id
                  16, # function code (16)
                  58900, # reference number
                  1, # word count
                  2, #byte count
                  registerValue 
               )

            conexaoComModbus.send(req)
            res = conexaoComModbus.recv(1024)

            assert res == b'\x00\x00\x00\x00\x00\x06\x12\x10\xe6\x14\x00\x01'

            for startingAddress in ran:
               req = gerarRequisicao(startingAddress,modbusId,startingAddress, tipoLog,codTipoEquipamento ) # startingAddress é sempre o mesmo número que o transactionId
               # print(f'requisição {startingAddress} - {req.hex()}')
               conexaoComModbus.send(req)
               res = conexaoComModbus.recv(1024)
               # print(f'res - {res}')
               if codTipoEquipamento == 182:
                  respostas = processarRespostaModbus(codTipoEquipamento, res[9:])
                  # print(res.hex())

                  try:
                     for resposta in respostas:
                        # print(f'resposta:{resposta}\n')
                        
                        nomeEvent, textEvent, date = resposta
                        # print(f"nomeEvent - {nomeEvent}")
                        # print(f"textEvente - {textEvent}")
                        # print(f"dataEvente - {date}")
                        
                        values.append((str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent), str(date)))
                        # print(str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent), str(date))
                     
                        linha = (codEquipamento, codTipoEquipamento, nomeEvent, date)
                        if linha[3] >= ultimaLinha[4] and textEvent != ultimaLinha[3]:    #  Existem casos em que o mesmo alarme/evento se repetem com o mesmo horário (ultimaLinha[3] é a data e hora)
                                                                                          #  para esses casos vou considerar apenas um dos alarme/eventos. O que realmente importa é o nome
                                                                                          #  então exibir apenas um é o suficiente.
                           values.append((str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent[93:]), str(date)))
                           # print(str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent[93:]), str(date))
                        
                  except mysql.connector.IntegrityError as e:  # Integrity Error aconteceu durante a execução devido ao Unique adicionado nas tabelas no banco
                                                               # modificando a exceção para 'pass' para pular para a próxima iteração e ignorar os valores repetidos
                     print(f"Erro de integridade MySQL: {e}")
                     with open("logRecuperaLogs.txt", 'a') as file:
                        file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de integridade MySQL: {e}'\n") 
                  except TypeError as e: # O TypeError aqui vai indicar que a resposta do modbus foi vazia, logo, chegou ao fim do log e deve ser encerrado o fetchLog
                     # print(f"type error: {e}")
                     return 1
                  except Exception as e:
                     print(f"erro ao processar resposta modbus: {e}")
                     return 0
               
               else:
                  respostas = processarRespostaModbus(codTipoEquipamento, res)
                  # print(respostas)

                  try:
                     for resposta in respostas:
                        # print(f'resposta:{resposta}')
                        
                        nomeEvent, textEvent, date = resposta
                        # print(f"nomeEvent - {nomeEvent}")
                        # print(f"textEvente - {textEvent}")
                        # print(f"dataEvente - {date}")
                     
                        linha = (codEquipamento, codTipoEquipamento, nomeEvent, date)
                        if linha[3] >= ultimaLinha[4] and textEvent != ultimaLinha[3]:    #  Existem casos em que o mesmo alarme/evento se repetem com o mesmo horário (ultimaLinha[3] é a data e hora)
                                                                                       #  para esses casos vou considerar apenas um dos alarme/eventos. O que realmente importa é o nome
                                                                                       #  então exibir apenas um é o suficiente.
                           values.append((str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent[93:]), str(date)))
                           # print(str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent[93:]), str(date))
                     
                  except mysql.connector.IntegrityError as e:  # Integrity Error aconteceu durante a execução devido ao Unique adicionado nas tabelas no banco
                                                               # modificando a exceção para 'pass' para pular para a próxima iteração e ignorar os valores repetidos
                     print(f"Erro de integridade MySQL: {e}")
                     with open("logRecuperaLogs.txt", 'a') as file:
                        file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de integridade MySQL: {e}'\n") 
                  except TypeError as e: # O TypeError aqui vai indicar que a resposta do modbus foi vazia, logo, chegou ao fim do log e deve ser encerrado o fetchLog
                     # print(f"type error: {e}")
                     return 1
                  except Exception as e:
                     print(f"erro ao processar resposta modbus: {e.with_traceback()}")
                     return 0
               

         except mysql.connector.Error as e:
            print(f"erro na comunicacao com o banco de dados: {e}")
            return 0
         except ConnectionResetError as e:
            print(f"Erro de conexao: {e}")
            return 0
         except TimeoutError as e:
            print(f"{e}")
            return 0
         finally:
            # print(values)
            escreverLogNoBanco(pool, values, tipoLog)

   except TimeoutError as e:
      print(f"Erro de conexão com Modbus: {e}")
      return 1
   

def buscarSolicitacoes(cursor: mysql.connector.cursor):
   query = f"""SELECT
                  *
               FROM
                  solicitacao_log;
            """# WHERE
                 #  status = 0
   
   cursor.execute(query)
   return cursor.fetchall()


def main(idSolicitacao, codEquipamento, modbusId, host, porta, codTipoLog): # idSolicitacao, codEquipamento, modbusId, host, porta, codTipoLog
   inicio = time.time()

   

   fetchLog(idSolicitacao, codEquipamento, modbusId, host, porta, codTipoLog)

  

   fim = time.time()
   print(f"tempo de execução: {(fim-inicio):.2f} segundos")


if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='Recupera Logs de Equipamento')
   parser.add_argument('idSolicitacao', type=int, help='ID da Solicitação')
   parser.add_argument('codEquipamento', type=int, help='Código do Equipamento')
   parser.add_argument('modbusId', type=int, help='ID Modbus')
   parser.add_argument('host', type=str, help='Host')
   parser.add_argument('porta', type=int, help='Porta')
   parser.add_argument('codTipoLog', type=int, help='Código do Tipo de Log')

   args = parser.parse_args()
   main(args.idSolicitacao, args.codEquipamento, args.modbusId, args.host, args.porta, args.codTipoLog)
   