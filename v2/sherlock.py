import numpy as np
import pandas as pd
import requests
import json
import os

pasta = ""
        
# Classe de Conversao
class Gera_Massa:

  teste = {}
  __json_modelo = {}
  __fixed_array_of_payloads = []
  
  def __init__(self,inicial):
    self.__baseiteracao = inicial

  def get_bom(self):
    try:
      return self.__json_modelo
    except:
      print("Executar set_bom ou set_bom_arqv antes ")
  
  def set_bom_arqv(self):
    file_name = "arquivos/payload_inicial.txt"
    file_path = pasta + file_name
    with open(file_path,'r') as f:
      x = f.read()
    self.__json_modelo = json.loads(x)

    print('--------------------------------------')
    print("Payload setado com sucesso")

  def set_bom(self,json_entr):
    self.__json_modelo = json_entr
    
    print('--------------------------------------')
    print("Payload setado com sucesso")

  def converte_massa(self):
    
    massa_csv = self.__baseiteracao
    fixed_array_of_payloads = []
    json_modelo = self.__json_modelo
    variaveis = list(massa_csv.columns)
    
    for i in range(len(massa_csv)):
      y = massa_csv.loc[i]
      
      for x in variaveis:
        json_var = json_modelo
        for v in range(len(x.split('.'))):
          listaCampos = x.split('.')
          z = listaCampos[v]
          if v == len(x.split('.')) - 1:
            var = z
          else:
            if z.isnumeric() == True:
              try:
                json_var = json_var[int(z)]
              except:
                json_var.append(json.loads(json.dumps(json_var[0])))
                json_var = json_var[int(z)]
            else:
              json_var = json_var[z]

        if isinstance(json_var[var],list) == True:
          if isinstance(y[x],list) == True:
            json_var[x] = y[x]
          else:
            print("Dado Inválido")
            pass
        elif isinstance(json_var[var],bool) == True:
          try:
            json_var[var] = eval(str(y[x]))
          except:
            json_var[var] = str(y[x])
        elif isinstance(json_var[var],float) == True or isinstance(y[x],float) == True:
          try:
            json_var[var] = float(str(y[x]))
          except:
            json_var[var] = str(y[x])
        elif isinstance(json_var[var],int) == True: 
          try:
            json_var[var] = int(str(y[x]))
          except:
            json_var[var] = str(y[x])
        else:
          json_var[var] = str(y[x])

      json_modelo['idSolicitacao'] = i
      fixed_array_of_payloads.append(json.dumps(json_modelo))
    self.__fixed_array_of_payloads = fixed_array_of_payloads
    
    print('--------------------------------------')
    print ("Conversao concluida com sucesso")
    
    
  def get_massa_json(self):
    
    lista_payload = []
    
    for x in self.__fixed_array_of_payloads:
      lista_payload.append(json.loads(x))
      
    return lista_payload
    
#    return self.__fixed_array_of_payloads
  
  def gera_arqv_json(self,nome_arqv): 
    lin_arquivo = {"fixed_array_of_Payload":[]}
    for x in self.__fixed_array_of_payloads:
      payload = json.loads(x)
      lin_arquivo["fixed_array_of_Payload"].append(payload)
    nome = nome_arqv
    with open(f'{nome}.json','w') as f:
      f.write(json.dumps(lin_arquivo))
      
  def get_massa_final(self):
    return self.__baseiteracao

  def gera_cenario(self,variavel,cenarios):

    # Gera um dataframe com casos a serem forçados na massa
    caso = pd.DataFrame({variavel:cenarios})

    # Gera um dataframe sem a coluna a ser forçada e mantém o resto das variáveis
    drops = self.__baseiteracao.drop([variavel], axis=1)

    #Cria uma coluna com todos valores iguais, para criar um cartesiano
    caso['tmp'] = 1
    drops['tmp'] = 1

    # Gera a base de saída fazendo um cartesiano dos casos forçados com os existentes no arquivo inicial
    saida = pd.merge(drops, caso, on=['tmp'])

    # Remove a coluna tmp
    saida = saida.drop('tmp', axis=1)

    self.__baseiteracao = saida
    
    del drops
    del caso

  def empilha_massas(self,BASES):
    
    lista_bases = [self.__baseiteracao]
    lista_bases.extend(BASES)
    
    self.__baseiteracao = pd.concat(lista_bases,ignore_index=True,sort=False)
    
    #df = self.__baseiteracao   
    #df['payloadInput.proponente.numeroDocumento'] = 'P' + df.index.astype(str)
    #self.__baseiteracao = df
    
    self.qtdLinhas()

#    return self.__baseiteracao
  
  def qtdLinhas(self):

    print('--------------------------------------')
    print("Quantidade de Linhas da Massa:",len(self.__baseiteracao))
    
  def rename_cabecalho(self):
    
    df = self.__baseiteracao
    
    df['Data-ID'] = df.index + 1
    df['Result-State'] = ""
    df['Result-Detail'] = ""

    file_name = "arquivos/dicionario_cabecalho_completo.txt"
    file_path = pasta + file_name
    with open(file_path,'r') as f:
      x = f.read()
      
    dicionario = json.loads(x)
    
    retorno = df.rename(columns=dicionario)

    # Vars obrigatórias de formatação da massa do DM
    dataID   = retorno.pop('Data-ID')
    ResultSTATE = retorno.pop('Result-State')
    ResultDETAIL = retorno.pop('Result-Detail')
    retorno.insert(0, 'Data-ID', dataID)
    retorno.insert(1, 'Result-State', ResultSTATE)
    retorno.insert(2, 'Result-Detail', ResultDETAIL)

    self.__massadm = retorno
    
  def get_massa_dm(self):
    
    try:
      y = self.__massadm

      for x in y.columns:
        if isinstance(y[x][0],np.bool_) == True:
          y[x] = y[x].astype(str).str.lower()  
                 
      return y
    except:
      print("Executar metodo rename_cabecalho antes")

  def rename_variavel(self,variavel,cabecalho):

    try:
      df = self.__massadm
    except:
      df = self.__baseiteracao
    
    retorno = df.rename(columns= {variavel: cabecalho}) 
    
    self.__massadm = retorno

  def atribui_expected(self,payload,caminho,nome):

    lista_resultados = []
    
    for x in payload:
      
      valor = x
      for y in caminho.split("."):
        if y.isnumeric() == True:
          if isinstance(valor,list) == True and len(valor)>0 and len(valor)>int(y):
            valor = valor[int(y)]
          else:
            valor = ""
            break
        else:
          valor = valor[y]
        
      lista_resultados.append(valor)

    self.__massadm['result:'+nome] = ""
    self.__massadm['expected:'+nome] = lista_resultados 

  def atribui_expected_lista(self,payload,caminho,nome):

    dicionario_resultados = {}
    lista_resultados = []
    
    for x in payload:
      
      valor = x

      for y in caminho.split("."):
        valor = valor[y]

      i = 0
      for z in valor:  
        dicionario_resultados[nome.replace("n",str(i))] = 0
        lista_resultados.append({"nome":nome + str(i),"valor":z})
        i = i + 1

    self.__massadm['result:'+nome] = ""
    self.__massadm['expected:'+nome] = lista_resultados 

  def ajuda(self,funcao="todas"):

    descricao = {}
  
    descricao["get_massa_final"] = "Metodo que retorna a massa final em formato pandas Dataframe"
    descricao["get_massa_json"] = "Metodo que retorna a massa final em formato json"
    descricao["get_massa_dm"] = "Metodo que retorna a massa final com o cabecalho do DM"
    descricao["gera_cenario"] = "Metodo que combina a dataframe de origem da massa com os cenarios desejados"
    descricao["empilha_massas"] = "Metodo que empilha dataframes e cria a massa final em Dataframe"
    descricao["set_bom_arqv"] = "Metodo que configura o arquivo (deve estar na pasta 'arquivos' no formato 'txt') que sera usado como base da conversao Dataframe para json"
    descricao["converte_massa"] = "Metodo que converte a massa gerada em formato Dataframe para json"
    descricao["rename_cabecalho"] = "Metodo que renomeia o cabecalho da massa para adequar ao formato que o DM espera receber"
    descricao["rename_variavel"] = "Metodo que renomeia uma variavel especifica do cabecalho"
    descricao["atribui_expected"] = "Metodo que adiciona a massa do DM as colunas de result e expected"
  
    if funcao == "todas":
      print("Versao 1.3 (teste) - 12/05/2023")
      print("> get_massa_final:",descricao["get_massa_final"],"\n")
      print("> get_massa_json:",descricao["get_massa_json"],"\n")  
      print("> get_massa_dm:",descricao["get_massa_dm"],"\n")
      print("> gera_cenario:",descricao["gera_cenario"],"\n")
      print("> empilha_massas:",descricao["empilha_massas"],"\n")
      print("> set_bom_arqv:",descricao["set_bom_arqv"],"\n")
      print("> converte_massa:",descricao["converte_massa"],"\n")
      print("> rename_cabecalho:",descricao["rename_cabecalho"],"\n") 
      print("> rename_variavel:",descricao["rename_variavel"],"\n") 
      print("> atribui_expected:",descricao["atribui_expected"],"\n") 
  
    elif funcao == "get_massa_final": 
      print("> Metodo:",funcao,"\n","* Parametros:","\n"," Nao espera parametros","\n","* Exemplo:","\n"," MassaFinal.get_massa_final()","\n","* Descricao:","\n"," ",descricao[funcao])
    elif funcao == "get_massa_json":
      print("> Metodo:",funcao,"\n","* Parametros:","\n"," Nao espera parametros","\n","* Exemplo:","\n"," MassaFinal.get_massa_json()","\n","* Descricao:","\n"," ",descricao[funcao])
    elif funcao == "get_massa_dm": 
      print("> Metodo:",funcao,"\n","* Parametros:","\n"," Nao espera parametros","\n","* Exemplo:","\n"," MassaFinal.get_massa_dm()","\n","* Descricao:","\n"," ",descricao[funcao])
    elif funcao == "gera_cenario": 
      print("> Metodo:",funcao,"\n",
         "* Parametros:","\n"," [variavel:string]- Campo existente na base de entrada, que tera os valores alterados, conforme os cenarios","\n"," [cenarios:lista]- Campo com os cenarios desejados para a varivel informada","\n",
         "* Exemplo:","\n"," MassaFinal.gera_cenario('rating',['AAA','AB','C'])","\n",
         "* Descricao:","\n"," ",descricao[funcao])
    elif funcao == "empilha_massas": 
      print("> Metodo:",funcao,"\n",
         "* Parametros:","\n"," [BASES:lista]- Campo do tipo lista com os dataframes que se deseja empilhar para criar a massa final","\n",
         "* Exemplo:","\n"," MassaFinal.empilha_massas([Massa_CPF,Massa_Idade,Massa_ReclamacaoBacen])","\n",
         "* Descricao:","\n"," ",descricao[funcao])
    elif funcao == "set_bom_arqv": 
      print("> Metodo:",funcao,"\n","* Parametros:","\n"," Nao espera parametros","\n","* Exemplo:","\n"," MassaFinal.set_bom_arqv()","\n","* Descricao:","\n"," ",descricao[funcao])
    elif funcao == "converte_massa": 
      print("> Metodo:",funcao,"\n","* Parametros:","\n"," Nao espera parametros","\n","* Exemplo:","\n"," MassaFinal.converte_massa()","\n","* Descricao:","\n"," ",descricao[funcao])
    elif funcao == "rename_cabecalho": 
      print("> Metodo:",funcao,"\n","* Parametros:","\n"," Nao espera parametros","\n","* Exemplo:","\n"," MassaFinal.rename_cabecalho()","\n","* Descricao:","\n"," ",descricao[funcao])
    elif funcao == "rename_variavel":
      print("> Metodo:",funcao,"\n",
         "* Parametros:","\n"," [variavel:string]- Campo existente na massa do DM, que tera o nomealterado","\n"," [cabecalho:string]- Campo com o novo nome, no cabecalho, da variavel em questao","\n",
         "* Exemplo:","\n"," MassaFinal.rename_variavel('payloadInput.proponente.dadosFatorRisco.listaApontamentos.2.codigo','listaApontamentos#3.codigo')","\n",
         "* Descricao:","\n"," ",descricao[funcao])
    elif funcao == "atribui_expected": 
      print("> Metodo:",funcao,"\n",
         "* Parametros:","\n"," [payload:lista]- Campo do tipo lista contendo o resultado, em formato dicionario, das regras da politica","\n"," [caminho:string]- Campo string com o caminho do resultado no payload, separado por '.'","\n"," [nome:string]- Campo do tipo string com o nome que deseja-se dar ao resultado no cabecalho do DM (result e expected serao inseridos automaticamente)","\n"
         "* Exemplo:","\n"," MassaFinal.atribui_expected(lista_resultado,'payloadOutput.resultadoTeste.statusDecisao','statusDecisao')","\n",
         "* Descricao:","\n"," ",descricao[funcao])
    else:
      print("Funcao Inexistente na classe")


class Valida:
 
  __payload = {}
  __lista_validacao = {}
  __df_validacao = pd.DataFrame({})
  
  def __init__(self,payload_response,payload_request=None):

    print("Versao 1.0 - 13/04/2023")

    lista_payload = []
    lista_ids = []

    if payload_request == None:

      try:
        x = payload_response[0]['payloadOutput']
        y = payload_response[0]['payloadHomol']

        for i in range(len(payload_response)):
          lista_ids.append(payload_response[i]["payloadInput"]["proponente"]["numeroDocumento"])
           
        self.__payload = payload_response
        self.__df_validacao["id"] = lista_ids

      except:
        print("Classes fora do padrão: payloadOutput e payloadHomol")

    else:
      try:
        x = payload_response['fixed_array_of_Payload'][0]['payloadOutput']
        y = payload_request['fixed_array_of_Payload'][0]['payloadHomol']

        for i in range(len(payload_response['fixed_array_of_Payload'])):
          payload_request['fixed_array_of_Payload'][i]['payloadOutput'] = payload_response['fixed_array_of_Payload'][i]['payloadOutput'] 
          lista_payload.append(payload_request['fixed_array_of_Payload'][i])
          lista_ids.append(payload_request['fixed_array_of_Payload'][i]["payloadInput"]["proponente"]["numeroDocumento"])
        
        self.__payload = lista_payload
        self.__df_validacao["id"] = lista_ids
      
      except:
        print("Classes fora do padrão: fixed_array_of_Payload[x].payloadOutput e fixed_array_of_Payload[x].payloadHomol")

  def valida(self,caminho_dm,caminho_hml=None,exec=None):
    
    lista_dm = []
    lista_hml = []
    lista_validacao = []

    for caso in self.__payload: 
      try:
        id = caso["payloadInput"]["proponente"]["numeroDocumento"]
      except:
        pass

      if caminho_hml == None:
        var_dm,nome_dm = self.__get_classe(caso,caminho_dm)
        var_hml,nome_hml = self.__get_classe(caso,caminho_dm)

      else:
        var_dm,nome_dm = self.__get_classe(caso,caminho_dm)
        var_hml,nome_hml = self.__get_classe(caso,caminho_hml)

      if nome_dm == "listaMotivos":
        var_dm = self.__prepara_lista_motivos(var_dm)
      else:
        pass

      validacao = self.__compara_valores(var_dm,var_hml)

      if nome_dm == nome_hml:
        nome_hml = nome_hml + "_hml"
      else:
        pass

      json_validacao = {nome_dm:var_dm,nome_hml:var_hml,"validacao":validacao}
      
      if id in list(self.__lista_validacao.keys()):
        self.__lista_validacao[id].append(json_validacao)
      else:
        self.__lista_validacao[id] = []
        self.__lista_validacao[id].append(json_validacao)

      lista_dm.append(var_dm)
      lista_hml.append(var_hml)
      lista_validacao.append(validacao)

    self.__monta_df(lista_dm,lista_hml,lista_validacao,{"nome_dm":nome_dm,"nome_hml":nome_hml})  

  def get_lista_validacao(self):
    
    return self.__lista_validacao,self.__df_validacao
  
  def __prepara_lista_motivos(self,lista_motivos):

    lista_saida = []

    for x in lista_motivos:
      lista_saida.append(int(x["codigo"]))

    return lista_saida
  
  def __monta_df(self,lista_dm,lista_hml,lista_validacao,json_nomes):

    if json_nomes["nome_dm"] == json_nomes["nome_hml"]:
      json_nomes["nome_hml"] = json_nomes["nome_hml"] + "_hml"
    else:
      pass
    
    self.__df_validacao["flag_"+json_nomes["nome_dm"]] = lista_validacao
    self.__df_validacao[json_nomes["nome_dm"]] = lista_dm
    self.__df_validacao[json_nomes["nome_hml"]] = lista_hml

  def __get_classe(self,payload,caminho):
    
    valor = payload 
    i = 0

    while isinstance(valor,dict) == True and i <= 10:
      lista = caminho.split(".")
      valor = valor[lista[i]]
      chave = lista[i]
      i=i+1

    return valor,chave
  
  def __compara_valores(self,var_1,var_2):

    tipo_1 = self.__retorna_tipo_var(var_1)
    tipo_2 = self.__retorna_tipo_var(var_2)
    validacao = False

    if tipo_1 == tipo_2:
      if tipo_1 == "lista":
        tipo_1_0 = self.__retorna_tipo_var(var_1[0])
        tipo_2_0 = self.__retorna_tipo_var(var_2[0])
        
        if tipo_1_0 == tipo_2_0:
          validacao = self.__compara_lista(var_1,var_2)

        else:
          pass

      else:
        if tipo_1 == "int":
          if int(var_1) == int(var_2):
            validacao = True
          else:
            validacao = False

        elif tipo_1 == "float":
          if float(var_1) == float(var_2):
            validacao = True
          else:
            validacao = False

        elif tipo_1 == "string":
          if str(var_1) == str(var_2):
            validacao = True
          else:
            validacao = False

        elif tipo_1 == "dicionario":
          if var_1 == var_2:
            validacao = True
          else:
            validacao = False

        else:
          validacao = False

    elif tipo_1 in ["int","float"] and tipo_2 in ["int","float"]:
      if tipo_1 == "int":
        if int(var_1) == int(var_2):
          validacao = True
        else:
          validacao = False
      
      elif tipo_1 == "float":
        if float(var_1) == float(var_2):
          validacao = True
        else:
          validacao = False

    else:
      validacao = False

    return validacao

  def __compara_lista(self,lista_1,lista_2):
    
    lista_1.sort()
    lista_2.sort()

    validacao = np.array_equal(lista_1,lista_2)

    return validacao

  def get_payload(self):

    return self.__payload

  def __retorna_tipo_var(self,x):

    if isinstance(x,list) == True:
      variavel = "lista"
    elif isinstance(x,dict) == True:
      variavel = "dicionario"
    elif isinstance(x,int) == True:
      variavel = "int"
    elif isinstance(x,float) == True:
      variavel = "float"
    else:
      try:
        if x.replace('.','').isnumeric() == True or x.replace(',','').isnumeric() == True:
          if x.find(".") >= 0 or x.find(",") >= 0:
            variavel = "float"
          else:
            variavel = "int"
        else:
          variavel = "string"

      except:
        variavel = "outro"
      
    return variavel
  
####################################################################################################

# Criando a classe que verifica se a massa está passando por todos os cenários da regra
class Freq:
  def ProcFreq(lista_resultado, variavel_controle): 
    variavel_controle = str(variavel_controle) 
  
    i = 0 
    lista_teste = [] 
    for resultado in lista_resultado: 
      lista_teste.append(lista_resultado[i]['payloadHomol']['intermediarias'][variavel_controle]) 
      i = i + 1 
    
    lista_teste_unique = list(dict.fromkeys(lista_teste))
    lista_teste_unique.sort() 
  
    return print("Os casos da variável de controle", "*", variavel_controle, "*", "passaram pelos seguintes cenários da regra: \n", lista_teste_unique)

