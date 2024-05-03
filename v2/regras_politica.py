########################################################################################################################
#####################################       IMPORTS DE LIBS UTILIZADAS      ############################################
########################################################################################################################
import json
import pandas as pd
import numpy as np
#from datetime import datetime
from datetime import date
import multiprocessing
import concurrent.futures
import os
import datetime

# Funções em UpperCamelCase e variáveis, parametros e atributos em lowerCamelCase
########################################################################################################################
####################################       IMPORTS DE TABELAS AUXILIARES      ##########################################
########################################################################################################################
pastaTabelas = "arquivos/tabelasHomologacao/"
tabMarAberto = "Mar_Aberto_Segmentacao_codigo_motor.csv"
tabClienteAntigo = "Ja_Cliente_Segmentacao_codigo_motor.csv"

file_path = pastaTabelas + tabClienteAntigo
dftabClienteAntigo = pd.read_csv(file_path)

file_path = pastaTabelas + tabMarAberto
dftabClienteMarAberto = pd.read_csv(file_path)


########################################################################################################################
##########################################       FUNÇÕES AUXILIARES      ###############################################
########################################################################################################################

def verificaNulo(variavel):
    if variavel is None or variavel == '' or variavel == "null" or variavel == "1001-01-01" or variavel == "-99999" or variavel == -99999 or variavel == "-99999.0" or variavel == -99999.0:
        return True
    else:
        return False

def execucaoEndToEnd (payload_entrada):

    lista_payloads = payload_entrada

    lista_resultado = []

    for payload in lista_payloads:

        payload = decisionFlow(payload)
        lista_resultado.append(payload)

    return lista_resultado

def execucaoModular (payload_entrada, modulo):
    
    lista_payloads = payload_entrada

    lista_resultado = []

    for payload in lista_payloads:
        payload = modulo(payload)
        lista_resultado.append(payload)

    return lista_resultado


# DUAS SOLUÇÕES DE PARALELISMO DE EXECUÇÃO DE GRANDES QUANTIDADES DE DADO - PRECISAMOS TESTAR E VER QUAL TERÁ MELHOR PERFORMANCE.
#num_cores = os.cpu_count()
#import multiprocessing
def ExecucaoModularMultiProcessing(modulo, payload_entrada, num_cores):
    # Cria uma pool com a quantidade de workers
    pool = multiprocessing.Pool(processes=num_cores)
    # Aplica a função pra cada elemento da list payload_entrada
    listaResults = pool.map(modulo, payload_entrada)

    # Fecha a pool
    pool.close()
    pool.join()

    # Retorna os resultados tratados
    return listaResults

#import concurrent.futures
def ExecucaoModularConcurrent(modulo, payload_entrada, num_cores):
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        results = executor.map(modulo, payload_entrada)
    
    return list(results)


########################################################################################################################
##########################################       REGRAS DE POLITICA      ###############################################
########################################################################################################################

# Regra que cria a flag de direcionamento de chamadas da Raven e OPF
def DefineFlagRenda(payload):
    fonteRenda = payload['solicitante']['fonteRenda']
    ######################################################################################################################
    if fonteRenda in ['01. Fopag', '02. Funcionários Não FOPAG (J&F)', '03. Portabilidade']:
        flagRenda = "BATCH"
    elif fonteRenda in ['04. Renda OPF Confirmada']:
        flagRenda = "OPEN FINANCE"
    elif fonteRenda in ['06. Servidor Público']:
        flagRenda = "OPEN FINANCE SERVIDOR"
    else:
        flagRenda = "OPEN FINANCE MODELO"
        
    payload['payloadHomol']['saidas']['flagTipoRenda'] = flagRenda
    return payload


# 1. Hard Filters ( REGRA DE HARD FILTERS )
# 1. Hard Filters ( REGRA DE HARD FILTERS )
def HardFilters(payload):
    etapa = payload['etapa']
    #flagCadastroIrregular = payload['solicitante']['flagCadastroIrregular'] 
    #flagCadastroNegado = payload['solicitante']['flagCadastroNegado'] 
    #flagCadastroEmAndamento = payload['solicitante']['flagCadastroEmAndamento'] 
    #flagContaInativa = payload['solicitante']['flagContaInativa']
    #flagContaTeste = payload['solicitante']['flagContaTeste']
    #flagOSDefasado = payload['solicitante']['flagOSDefasado']
    statusCPF = payload['solicitante']['statusCPF'] 
    flagFraudePicPay = payload['solicitante']['flagFraudePicpay'] 
    flagFraudeCartaoPicpay = payload['solicitante']['flagFraudeCartaoPicpay']
    flagChargebackPicPay = payload['solicitante']['flagChargebackPicpay'] 
    flagHoldForced = payload['solicitante']['flagHoldForced']
    idade = payload['solicitante']['idade'] 
    qtdDiasAtrasoCartao = payload['solicitante']['atrasos']['qtdDiasAtrasoCartao']
    qtdDiasAtrasoParcelados = payload['solicitante']['atrasos']['qtdDiasAtrasoParcelados']
    qtdDiasAtrasoBNPLMova = payload['solicitante']['atrasos']['qtdDiasAtrasoBNPLMova']
    qtdDiasAtrasoP2PL = payload['solicitante']['atrasos']['qtdDiasAtrasoP2PL']    
    vlrMaxRestritivosBVSM1 = payload['solicitante']['restritivosBVS']['vlrMaxRestritivosBVSM1']
    vlrMaxRestritivosBVSM2 = payload['solicitante']['restritivosBVS']['vlrMaxRestritivosBVSM2']
    vlrMaxRestritivosBVSM3 = payload['solicitante']['restritivosBVS']['vlrMaxRestritivosBVSM3']
    vlrMaxRestritivosSerasa90Dias = payload['solicitante']['vlrMaxRestritivosSerasa90Dias']   
    flagDesenrola = payload['solicitante']['flagDesenrola'] 
    flagFNV = payload['solicitante']['flagFNV']
    flagReneg = payload['solicitante']['flagReneg']
    flagBloqueados = payload['solicitante']['flagBloqueados']

    regrasNegativas = payload['payloadHomol']['saidas']['regrasNegativas']
    ######################################################################################################################

    statusDecisao = "APROVADO"

    # Para questões de debug e teste modular, setamos o valor incial para vazio.
    payload['payloadHomol']['saidas']['mensagemFiltro'] = "99999. NAO PREENCHIDO"
    payload['payloadHomol']['saidas']['mensagemFinal'] = "99999. NAO PREENCHIDO"
    passouFlagHardFilter = 0
    passouMsgHardFilter = 0

    # PARAMETRO DE FLEXIBILIZAÇÃO DE REGRAS COM RENDA - TEREMOS UM UNICO RMA SENDO CHAMADO EM DUAS ETAPAS, CADA ETAPA UTILZIA UM RESPECTIVO %
    if etapa == "":
        percentualRenda = 50
        rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpayBatch'] # renda liquida do batch para etapa que o hard filter é flexibilizado
    else:
        percentualRenda = 20
        rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpay'] # renda liquida online para etapa que o hard filter é aplicado sem flexibilização
    pct = str(percentualRenda)

    # FLAGS DOS HARD FILTERS - UTILIZADA HOJE PELO TIME DE POLITICAS
    # Flag cadastro irregular:
    payload['payloadHomol']['saidas']['hasInvalidRegistration'] = 0
    #if (flagCadastroIrregular == 1):
    #    payload['payloadHomol']['saidas']['hasInvalidRegistration'] = 1
    #    passouFlagHardFilter = 1

    # Cadastro Negado ou Cadastro Em Andamento
    payload['payloadHomol']['saidas']['hasDeniedOngoingRegistration'] = 0
    #if (flagCadastroNegado == 1 or flagCadastroEmAndamento == 1):
    #    payload['payloadHomol']['saidas']['hasDeniedOngoingRegistration'] = 1
    #    passouFlagHardFilter = 2

    # Conta Inativa ou Conta Teste PicPay
    payload['payloadHomol']['saidas']['hasDeactivatedOrTestAccount'] = 0
    #if (flagContaInativa == 1 or flagContaTeste == 1):
    #    payload['payloadHomol']['saidas']['hasDeactivatedOrTestAccount'] = 1
    #    passouFlagHardFilter = 3

    # Sistema Operacional Defasado
    payload['payloadHomol']['saidas']['hasOutdatedOs'] = 0
    #if (flagOSDefasado == 1):
    #    payload['payloadHomol']['saidas']['hasOutdatedOs'] = 1
    #    passouFlagHardFilter = 4

    # CPF Invalido
    payload['payloadHomol']['saidas']['isInvalidCpf'] = 0
    if (statusCPF in ['PENDENTE','CANCELADO','SUSPENSA']):
        payload['payloadHomol']['saidas']['isInvalidCpf'] = 1
        passouFlagHardFilter = 5

    # Obito
    payload['payloadHomol']['saidas']['isDeadUser'] = 0
    if (statusCPF in ['TITULAR FALECIDO']):
        payload['payloadHomol']['saidas']['isDeadUser'] = 1
        passouFlagHardFilter = 6

    # Fraude PicPay
    payload['payloadHomol']['saidas']['isFraudPP'] = 0
    if (flagFraudePicPay == 1 or flagFraudeCartaoPicpay == 1 or flagHoldForced == 1):
        payload['payloadHomol']['saidas']['isFraudPP'] = 1
        passouFlagHardFilter = 7

    # Chargeback PicPay
    payload['payloadHomol']['saidas']['isChargebackPP'] = 0
    if (flagChargebackPicPay == 1):
        payload['payloadHomol']['saidas']['isChargebackPP'] = 1
        passouFlagHardFilter = 8

    # idade
    payload['payloadHomol']['saidas']['isOutAgeRange'] = 0
    if (idade < 18 or idade >= 80 or idade == -99999):
        payload['payloadHomol']['saidas']['isOutAgeRange'] = 1
        passouFlagHardFilter = 9

    # Atraso PicPay
    payload['payloadHomol']['saidas']['isDelayPP'] = 0
    if (max(qtdDiasAtrasoCartao,qtdDiasAtrasoParcelados,qtdDiasAtrasoBNPLMova,qtdDiasAtrasoP2PL) > 5):
        payload['payloadHomol']['saidas']['isDelayPP'] = 1 
        passouFlagHardFilter = 10

    # Atraso Original
    payload['payloadHomol']['saidas']['isDelayOr'] = 0
    if (max(qtdDiasAtrasoCartao,qtdDiasAtrasoParcelados) > 5):
        payload['payloadHomol']['saidas']['isDelayOr'] = 1
        passouFlagHardFilter = 11

    # Reneg ou FNV Ativo
    payload['payloadHomol']['saidas']['isRenegFNV'] = 0
    if (flagFNV == 1 or flagReneg == 1 or flagBloqueados == 1 ):
        payload['payloadHomol']['saidas']['isRenegFNV'] = 1
        passouFlagHardFilter = 12

    # Valor de Restritivo BVS >= PCT da Renda
    payload['payloadHomol']['saidas']['hasRestrictionBVS'] = 0
    if ((max(vlrMaxRestritivosBVSM1,vlrMaxRestritivosBVSM2,vlrMaxRestritivosBVSM3) >= (percentualRenda/100)*rendaLiquidaPicpay)):
        payload['payloadHomol']['saidas']['hasRestrictionBVS'] = 1
        passouFlagHardFilter = 13

    # Valor de Restritivo Serasa >= PCT da Renda
    payload['payloadHomol']['saidas']['hasRestrictionSerasa'] = 0
    if ((vlrMaxRestritivosSerasa90Dias >= (percentualRenda/100)*rendaLiquidaPicpay)):
        payload['payloadHomol']['saidas']['hasRestrictionSerasa'] = 1
        passouFlagHardFilter = 14

    # Desenrola
    payload['payloadHomol']['saidas']['isUserDesenrola'] = 0
    if (flagDesenrola == 1):
        payload['payloadHomol']['saidas']['isUserDesenrola'] = 1
        passouFlagHardFilter = 15
        
    # No Hit Bureaus
    payload['payloadHomol']['saidas']['isNoHitBureau'] = 0
    if  ((payload['payloadHomol']['saidas']['hasInvalidRegistration'] +
            payload['payloadHomol']['saidas']['hasDeniedOngoingRegistration'] +
            payload['payloadHomol']['saidas']['hasDeactivatedOrTestAccount'] +
            payload['payloadHomol']['saidas']['hasOutdatedOs'] +
            payload['payloadHomol']['saidas']['isInvalidCpf'] +
            payload['payloadHomol']['saidas']['isDeadUser'] +
            payload['payloadHomol']['saidas']['isFraudPP'] +
            payload['payloadHomol']['saidas']['isChargebackPP'] +
            payload['payloadHomol']['saidas']['isOutAgeRange'] +
            payload['payloadHomol']['saidas']['isDelayPP'] +
            payload['payloadHomol']['saidas']['isDelayOr'] +
            payload['payloadHomol']['saidas']['isRenegFNV'] +
            payload['payloadHomol']['saidas']['hasRestrictionBVS'] +
            payload['payloadHomol']['saidas']['hasRestrictionSerasa'] +
            payload['payloadHomol']['saidas']['isUserDesenrola']) == 0 and rendaLiquidaPicpay == -99999):
        payload['payloadHomol']['saidas']['isNoHitBureau'] = 1
        passouFlagHardFilter = 16

    # MENSAGEM DOS HARD FILTERS - POR PRIORIZAÇÃO DAS FLAGS.

    #if (payload['payloadHomol']['saidas']['hasInvalidRegistration'] == 1):
    #    hardFilter = "02. Cadastro Irregular"
    #    statusDecisao = "NEGADO"
    #    regrasNegativas.append({'nomeRegra': 'hardFilter','descricao': hardFilter})
    #    passouMsgHardFilter = 1

    #elif (payload['payloadHomol']['saidas']['hasDeniedOngoingRegistration'] == 1):
    #    hardFilter = "03. Cadastro Negado ou Em Andamento"
    #    statusDecisao = "NEGADO"
    #    regrasNegativas.append({'nomeRegra': 'hardFilter','descricao': hardFilter})
    #    passouMsgHardFilter = 2

    #elif (payload['payloadHomol']['saidas']['hasDeactivatedOrTestAccount'] == 1):
    #    hardFilter = "04. Conta Inativa ou Conta Teste PicPay"
    #    statusDecisao = "NEGADO"
    #    regrasNegativas.append({'nomeRegra': 'hardFilter','descricao': hardFilter})
    #    passouMsgHardFilter = 3

    #elif (payload['payloadHomol']['saidas']['hasOutdatedOs'] == 1):
    #    hardFilter = "05. Sistema Operacional Defasado"
    #    statusDecisao = "NEGADO"
    #    regrasNegativas.append({'nomeRegra': 'hardFilter','descricao': hardFilter})
    #    passouMsgHardFilter = 4

    if (payload['payloadHomol']['saidas']['isInvalidCpf']  == 1):
        hardFilter = "06. CPF Invalido"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '129. CPF invalido'})
        passouMsgHardFilter = 5

    elif (payload['payloadHomol']['saidas']['isDeadUser'] == 1):
        hardFilter = "07. Obito"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '40. Obito = SIM'})
        passouMsgHardFilter = 6

    elif (payload['payloadHomol']['saidas']['isFraudPP']  == 1):
        hardFilter = "09. Fraude PicPay"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '21. Status fraude'})
        passouMsgHardFilter = 7

    elif (payload['payloadHomol']['saidas']['isChargebackPP'] == 1):
        hardFilter = "10. Chargeback PicPay"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '130. Chargeback PicPay'})
        passouMsgHardFilter = 8

    elif (payload['payloadHomol']['saidas']['isOutAgeRange']  == 1):
        hardFilter = "11. Idade < 18 ou >= 80"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '131. Idade < 18 ou >= 80'})
        passouMsgHardFilter = 9

    elif (payload['payloadHomol']['saidas']['isDelayPP'] == 1):
        hardFilter = "12. Atraso PicPay"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '132. Atraso PicPay'})  
        passouMsgHardFilter = 10

    elif (payload['payloadHomol']['saidas']['isDelayOr'] == 1):
        hardFilter = "13. Atraso Original"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '133. Atraso Original'})  
        passouMsgHardFilter = 11

    elif (payload['payloadHomol']['saidas']['isRenegFNV'] == 1):
        hardFilter = "14. Reneg ou FNV Ativo"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '134. Reneg ou FNV ativo'})
        passouMsgHardFilter = 12

    elif (payload['payloadHomol']['saidas']['hasRestrictionBVS'] == 1):
        hardFilter = "15. Valor de Restritivo BVS >= " + pct + "% da Renda"
        statusDecisao = "NEGADO"
        if etapa == "":
            regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '135. Valor de Restritivo BVS >= 50% da Renda'})
        else:
            regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '139. Valor de Restritivo BVS >= 20% da Renda'})
        passouMsgHardFilter = 13

    elif (payload['payloadHomol']['saidas']['hasRestrictionSerasa'] == 1):
        hardFilter = "16. Valor de Restritivo Serasa >= " + pct + "% da Renda"
        statusDecisao = "NEGADO"
        if etapa == "":
            regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '136. Valor de Restritivo Serasa >= 50% da Renda'})
        else:
            regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '140. Valor de Restritivo Serasa >= 20% da Renda'})
        passouMsgHardFilter = 14

    elif (payload['payloadHomol']['saidas']['isUserDesenrola'] == 1):
        hardFilter = "24. Desenrola"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '137. Desenrola'})
        passouMsgHardFilter = 15

    elif  (payload['payloadHomol']['saidas']['isNoHitBureau'] == 1):
        hardFilter = "25. No hit Bureaus"
        statusDecisao = "NEGADO"
        regrasNegativas.append({'nomeRegra': 'hardFilters','descricao': '138.  No hit Bureaus'})
        passouMsgHardFilter = 16

    else:
        hardFilter = "99. Accepted"
        passouMsgHardFilter = 17

    if etapa == "":
        payload['payloadHomol']['saidas']['mensagemFiltro'] = hardFilter
    else:
        payload['payloadHomol']['saidas']['mensagemFinal'] = hardFilter

    # CRAVA PRÓXIMA ETAPA
    if (etapa == ""):
        etapa = "FILTROS POLITICA"

    if (etapa == "BACEN M-2, M-3"):
        etapa = "POLITICA CONCESSAO"
        
    payload['payloadHomol']['saidas']['statusDecisao'] = statusDecisao
    payload['payloadHomol']['saidas']['etapa'] = etapa

    payload['payloadHomol']['intermediarias']['passouMsgHardFilter'] = passouMsgHardFilter # para facilitar debugs e validação de massas
    payload['payloadHomol']['intermediarias']['passouMsgHardFilter'] = passouMsgHardFilter # para facilitar debugs e validação de massas
    payload['payloadHomol']['intermediarias']['passouFlagHardFilter'] = passouFlagHardFilter # para facilitar debugs e validação de massas

    return payload

##### Criação de variáveis Intermediarias
### 2 Criação de flags ranges filtros
#Digito CPF Concomitante CP
# def DigitoCPFConcomitanteCP(payload):
#     numeroDocumento = payload['solicitante']['numeroDocumento']
#     ######################################################################################################################

#     digitoCPFConcomitanteCP = numeroDocumento[5:7] # 6 e 7 digito

#     payload['payloadHomol']['intermediarias']['digitoCPFConcomitanteCP'] = digitoCPFConcomitanteCP

#     return payload

#RDG Grupo Small Limits
def RDGSmallLimits(payload):
    numeroDocumento = payload['solicitante']['numeroDocumento']
    ######################################################################################################################

    cpf6e7Digito = numeroDocumento[5:7] # 6 e 7 digito

    payload['payloadHomol']['intermediarias']['cpf6e7Digito'] = cpf6e7Digito
    
    return payload

#Data de admissão
def DataAdmissao(payload):
    dataAdmissaoFopag = payload['solicitante']['dataAdmissaoFopag']
    dataAdmissaoOriginal = payload['solicitante']['dataRegistroPrimeiroContrato']
    dataCriacaoConta = payload['solicitante']['dataRegistro']
    ######################################################################################################################

    if verificaNulo(dataAdmissaoFopag) == False:
        dataAdmissao = dataAdmissaoFopag
    elif verificaNulo(dataAdmissaoOriginal) == False:
        dataAdmissao = dataAdmissaoOriginal
    elif verificaNulo(dataCriacaoConta) == False:
        dataAdmissao = dataCriacaoConta
    else:
        dataAdmissao = ""

    payload['payloadHomol']['intermediarias']['dataAdmissao'] = dataAdmissao

    return payload

#Data de admissão em meses
def DataAdmissaoMeses(payload):
    dataAdmissao = payload['payloadHomol']['intermediarias']['dataAdmissao']
    hojeDate = pd.Timestamp(date.today())  # Avaliar como pegar o dia da execução, pois teremos problemas no Pós acomp pois talvez a data da execução não será a mesma na qual avaliaremos a log
    ######################################################################################################################

    if verificaNulo(dataAdmissao): # entender o que vai acontecer com esse cálculo caso dataAdmissao vazio.
        admissaoMeses = 0
    else:
        admissaoDate = datetime.datetime.strptime(dataAdmissao, "%Y-%m-%dT%H:%M:%S.%fZ")
        admissaoMeses = (hojeDate.year - admissaoDate.year) * 12 + (hojeDate.month - admissaoDate.month)
                          
    payload['payloadHomol']['intermediarias']['admissaoMeses'] = admissaoMeses

    return payload

#Cliente existente
def ClienteExistente(payload):
    hojeDate = date.today()  # Avaliar como pegar o dia da execução, pois teremos problemas no Pós acomp pois talvez a data da execução não será a mesma na qual avaliaremos a log
    dataCriacaoConta = payload['solicitante']['dataRegistro']
    dataCriacaoContaDate = datetime.datetime.strptime(dataCriacaoConta[:10], "%Y-%m-%d").date() # FORMATA A DATA EM YYYY-MM-DD
    ######################################################################################################################
    if verificaNulo(dataCriacaoConta):
        flagClienteExistente = 0
        regraClienteExistente = 1
    else:
        delta = hojeDate - dataCriacaoContaDate
        if delta.days > 60:
            flagClienteExistente = 1
            regraClienteExistente = 2
        else:
            flagClienteExistente = 0
            regraClienteExistente = 3

    payload['payloadHomol']['intermediarias']['flagClienteExistente'] = flagClienteExistente
    payload['payloadHomol']['intermediarias']['regraClienteExistente'] = regraClienteExistente # para questões de debug

    return payload

#Cliente MAT
def ClienteMAT(payload):
    # flagMAT = payload['solicitante']['flagMAT30d']

    flagMAT = payload['solicitante']['flagMAT35d']  
    ######################################################################################################################

    if flagMAT == 1:
        clienteMAT = 1
    else:
        clienteMAT = 0

    payload['payloadHomol']['intermediarias']['clienteMAT'] = clienteMAT

    return payload

#Cliente MAT com recorrência
def ClienteMATRecorrencia(payload):
    flagMAT30d = payload['solicitante']['flagMAT30d']
    flagMAT60d = payload['solicitante']['flagMAT60d']
    flagMAT90d = payload['solicitante']['flagMAT90d']
    ######################################################################################################################

    if (flagMAT30d == 1 and flagMAT60d == 1 and flagMAT90d == 1):
        MATRecorrente = 1
    else:
        MATRecorrente = 0

    payload['payloadHomol']['intermediarias']['MATRecorrente'] = MATRecorrente

    return payload

#Cliente FOPAG
def ClienteFOPAG(payload):
    fonteRenda = payload['solicitante']['fonteRenda']
    flagPortabilidade = payload['solicitante']['flagPortabilidade']
    ######################################################################################################################

    if (fonteRenda == '01. Fopag' and flagPortabilidade == 0):
        FoPag = 1
    else:
        FoPag = 0

    payload['payloadHomol']['intermediarias']['clienteFoPag'] = FoPag

    return payload

#Renda inexistente
def RendaInexistente(payload):
    rendaPicpay = payload['solicitante']['rendaLiquidaPicpay']
    if verificaNulo(rendaPicpay) or rendaPicpay < 0:
        rendaInexistente = 1
    else:
        rendaInexistente = 0

    payload['payloadHomol']['intermediarias']['rendaInexistente'] = rendaInexistente

    return payload

#Cliente novo
# def ClienteNovo(payload):
#     hojeDate = date.today()  # Avaliar como pegar o dia da execução, pois teremos problemas no Pós acomp pois talvez a data da execução não será a mesma na qual avaliaremos a log
#     dataCriacaoConta = payload['solicitante']['dataRegistro']
#     dataCriacaoContaDate = datetime.datetime.strptime(dataCriacaoConta[:10], "%Y-%m-%d").date() # FORMATA A DATA EM YYYY-MM-DD
#     ######################################################################################################################
#     if verificaNulo(dataCriacaoConta):
#         flagClienteNovo = 0
#         regraClienteNovo = 1
#     else:
#         delta = hojeDate - dataCriacaoContaDate
#         if delta.days <= 60:
#             flagClienteNovo = 1
#             regraClienteNovo = 2
#         else:
#             flagClienteNovo = 0
#             regraClienteNovo = 3

#     payload['payloadHomol']['intermediarias']['flagClienteNovo'] = flagClienteNovo
#     payload['payloadHomol']['intermediarias']['regraflagClienteNovo'] = regraClienteNovo # questões de debug

#     return payload

#Não Cliente (Mar Aberto)
# def NaoCliente(payload):
#     dataCriacaoConta = payload['solicitante']['dataRegistro']
#     consumerID = payload['solicitante']['consumerID']
#     ######################################################################################################################
#     if verificaNulo(dataCriacaoConta) and  verificaNulo(consumerID):
#         flagNaoCliente = 1
#     else:
#         flagNaoCliente = 0

#     payload['payloadHomol']['intermediarias']['flagNaoCliente'] = flagNaoCliente

#     return payload

def criaPriorizaçãoeFaixas(payload):
    # ClienteNovo(payload)
    # NaoCliente(payload) #
    CriacaoModelosPriorizacao(payload)  # Modulo de priorização das variaveis gh e score
    SemInfoScore(payload)
    FaixaScoreInternoCurto(payload)  #alterado
    FaixaScoreExterno(payload)
    
    return payload
    
    
    
#Sem Informação de Score
def SemInfoScore(payload):

 
    scoreFinal = payload['payloadHomol']['saidas']['scoreFinal']
    ######################################################################################################################

    
    if (verificaNulo(scoreFinal) or scoreFinal <= -1):
        semInfoScore = 1
    else:
        semInfoScore = 0

    payload['payloadHomol']['intermediarias']['semInfoScore'] = semInfoScore
    
    return payload

#Valor Total Investido
def ValorTotalInvestido(payload):
    investimentoTotal = payload['solicitante']['investimentoTotal']
    #saldoInvestimento = payload['solicitante']['saldoInvestimento']
    saldoInvestimento = 0 # Sempre será 0, não temos essa variável de entrada.
    ######################################################################################################################

    if verificaNulo(investimentoTotal):
        investimentoTotal = 0
    if verificaNulo(saldoInvestimento):
        saldoInvestimento = 0

    valorTotalInvestido =  investimentoTotal + saldoInvestimento

    payload['payloadHomol']['intermediarias']['valorTotalInvestido'] = valorTotalInvestido

    return payload

#Percentual da Renda em investimento
def PercentRendaInvestimento(payload):
    valorTotalInvestido =  payload['payloadHomol']['intermediarias']['valorTotalInvestido']
    rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpay']
    ######################################################################################################################

    if rendaLiquidaPicpay > 0: # VERIFICAR O QUE DEVEMOS FAZER SE A RENDA VIER ZERADA OU VAZIA
        percentRendaInvestimento = valorTotalInvestido/rendaLiquidaPicpay
    else:
        percentRendaInvestimento = 0

    payload['payloadHomol']['intermediarias']['percentRendaInvestimento'] = percentRendaInvestimento

    return payload

#Investidor Original
def InvestidorOriginal(payload):
    valorTotalInvestido = payload['payloadHomol']['intermediarias']['valorTotalInvestido']
    rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpay']
    ######################################################################################################################
    
    if (valorTotalInvestido > 3 * rendaLiquidaPicpay):
        investidorOriginal = 1
    else:
        investidorOriginal = 0
        
    payload['payloadHomol']['intermediarias']['investidorOriginal'] = investidorOriginal

    return payload

#Principaliadde de investimento
def PrincipalidadeDeInvestimento(payload):
    investidorOriginal = payload['payloadHomol']['intermediarias']['investidorOriginal']
    flagInvestimentoPrincipalidade = payload['solicitante']['flagInvestimentoPrincipalidade']
    ######################################################################################################################

    if (investidorOriginal or flagInvestimentoPrincipalidade == 1):
        principalidadeDeInvestimento = 1
    else:
        principalidadeDeInvestimento = 0

    payload['payloadHomol']['intermediarias']['principalidadeDeInvestimento'] = principalidadeDeInvestimento

    return payload

#Principaliadde em todos os critérios
def PrincipalidadeCriterios(payload):
    flagPrincipalidade = payload['solicitante']['flagPrincipalidade']
    mesFlagPrincipalidade = payload['solicitante']['mesFlagPrincipalidade']
    ######################################################################################################################

    if (flagPrincipalidade == 1 and mesFlagPrincipalidade >= 1):
        principalidadeAll = 1
    else:
        principalidadeAll = 0

    payload['payloadHomol']['intermediarias']['principalidadeAll'] = principalidadeAll

    return payload

#Principalidade em todos os Critérios com Recorrência
def RecorrenciaPrincipalidade(payload):
    flagPrincipalidade = payload['solicitante']['flagPrincipalidade']
    mesFlagPrincipalidade = payload['solicitante']['mesFlagPrincipalidade']
    ######################################################################################################################
    if flagPrincipalidade == 1 and mesFlagPrincipalidade >= 3:
        recorrenciaPrincipalidade = 1
    else:
        recorrenciaPrincipalidade = 0

    payload['payloadHomol']['intermediarias']['recorrenciaPrincipalidade'] = recorrenciaPrincipalidade

    return payload

#Cliente Blindado
def ClienteBlindado(payload):
    flagAllowlist = payload['solicitante']['flagAllowlist']
    ######################################################################################################################
    clienteBlindado = 0

    if ("CARTAO PICPAY CONCESSAO" in flagAllowlist.upper()):
        clienteBlindado = 1
    if ("OVERALL" in flagAllowlist.upper()):
        clienteBlindado = 1
        
    payload['payloadHomol']['intermediarias']['clienteBlindado'] = clienteBlindado

    return payload

#Cliente Bloqueado
def ClienteBloqueado(payload):
    flagBlocklist = payload['solicitante']['flagBlocklist']
    ######################################################################################################################
    clienteBloqueado = 0

    if ("CARTAO PICPAY CONCESSAO" in flagBlocklist.upper()):
        clienteBloqueado = 1
    if ("OVERALL" in flagBlocklist.upper()):
        clienteBloqueado = 1

    payload['payloadHomol']['intermediarias']['clienteBloqueado'] = clienteBloqueado

    return payload


#Adicao nova -v2
def CriacaoModelosPriorizacao(payload):
    #Criação Modelos de Priorização


## ------------------ONLINE RAVEN ----------------------------------------- ##
    #credit_appcards_blend_intern
    ghAppcardsBlendInternoOnline = payload['solicitante']['ghAppcardsBlendInternoOnline']
    scoreAppcardsBlendInternoOnline = payload['solicitante']['scoreAppcardsBlendInternoOnline']
    
    #credit_appcards_blend_extern
    ghAppcardsBlendExternoOnline = payload['solicitante']['ghAppcardsBlendExternoOnline']
    scoreAppcardsBlendExternoOnline = payload['solicitante']['scoreAppcardsBlendExternoOnline']

## ------------------------------------------------------------------------ ##

    #Origem Aurora
    ghAppcardsBlendIntExt = payload['solicitante']['ghBlendIntExt']
    scoreAppcardsBlendIntExt = payload['solicitante']['scoreBlendIntExt']
    
    #Origem Serasa
    ghAppcardsSerasa = payload['solicitante']['ghSerasa']
    scoreAppcardsSerasa = payload['solicitante']['scoreSerasaAl']
    
    ghFinal = "missing"
    scoreFinal = -1
    origemModeloApplication = 'missing'
    
# -------------------------------- BATCH -----------------------------------#

    scoreBlendFinal = payload['solicitante']['blendFinalAl']
    ghFinalSCR = payload['solicitante']['ghFinalScr']
    origemBlend = payload['solicitante']['origemBlend']
    
    
    regraGhFinal = -1
    regraScoreFinal = -1
    # Conferir variaveis online, se elas tiverem valor -99999, usar as variaveis do batch (aurora).
    if ghAppcardsBlendInternoOnline != "-99999" and scoreAppcardsBlendInternoOnline != -99999 and ghAppcardsBlendExternoOnline != "-99999" and scoreAppcardsBlendExternoOnline != -99999:
        
        if ghAppcardsBlendInternoOnline != "missing" and ghAppcardsBlendInternoOnline != "0":
            ghFinal = ghAppcardsBlendInternoOnline
            regraGhFinal = 1
        elif (ghAppcardsBlendInternoOnline == "missing" or ghAppcardsBlendInternoOnline == "0") and (ghAppcardsBlendIntExt != "missing" and ghAppcardsBlendIntExt != "0" and ghAppcardsBlendIntExt != "-99999"):
            ghFinal = ghAppcardsBlendIntExt
            regraGhFinal = 2
        elif (ghAppcardsBlendInternoOnline == "missing" or ghAppcardsBlendInternoOnline == "0") and (ghAppcardsBlendIntExt == "missing" or ghAppcardsBlendIntExt == "0" or ghAppcardsBlendIntExt == "-99999") and (ghAppcardsBlendExternoOnline != "missing" and ghAppcardsBlendExternoOnline != "0"):
            ghFinal = ghAppcardsBlendExternoOnline
            regraGhFinal = 3
        elif (ghAppcardsBlendInternoOnline == "missing" or ghAppcardsBlendInternoOnline == "0") and (ghAppcardsBlendIntExt == "missing" or ghAppcardsBlendIntExt == "0" or ghAppcardsBlendIntExt == "-99999") and (ghAppcardsBlendExternoOnline == "missing" or ghAppcardsBlendExternoOnline == "0") and (ghAppcardsSerasa != "missing" and ghAppcardsSerasa != "0" and ghAppcardsSerasa != "-99999"):
            ghFinal = ghAppcardsSerasa
            regraGhFinal = 4
        else: regraGhFinal = 5
            
        if scoreAppcardsBlendInternoOnline != -1 :
            scoreFinal = scoreAppcardsBlendInternoOnline
            regraScoreFinal = 1
            origemModeloApplication = 'Interno + Serasa + SCR'
        elif scoreAppcardsBlendInternoOnline == -1 and (scoreAppcardsBlendIntExt != -1 and scoreAppcardsBlendIntExt != -99999):
            scoreFinal = scoreAppcardsBlendIntExt
            origemModeloApplication = 'Interno + Serasa'
            regraScoreFinal = 2
        elif scoreAppcardsBlendInternoOnline == -1 and (scoreAppcardsBlendIntExt == -1 or scoreAppcardsBlendIntExt == -99999) and scoreAppcardsBlendExternoOnline != -1:
            scoreFinal = scoreAppcardsBlendExternoOnline
            origemModeloApplication = ' Serasa + SCR'
            regraScoreFinal = 3
        elif scoreAppcardsBlendInternoOnline == -1 and (scoreAppcardsBlendIntExt == -1 or scoreAppcardsBlendIntExt == -99999) and (scoreAppcardsBlendExternoOnline == -1) and scoreAppcardsSerasa != -1 and scoreAppcardsSerasa != -99999:
            scoreFinal = scoreAppcardsSerasa
            origemModeloApplication = 'Serasa'
            regraScoreFinal = 4
        else: regraScoreFinal = 5
            
    else: 
        ghFinal = ghFinalSCR
        scoreFinal = scoreBlendFinal
        origemModeloApplication = origemBlend
        
        
    payload['payloadHomol']['saidas']['ghFinal'] = ghFinal
    payload['payloadHomol']['saidas']['scoreFinal'] = scoreFinal
    payload['payloadHomol']['saidas']['origemModeloApplication'] = origemModeloApplication
    
    
    #Variaveis intermediarias para debug
    
    payload['payloadHomol']['intermediarias']['regraGhFinal'] = regraGhFinal
    payload['payloadHomol']['intermediarias']['regraScoreFinal'] = regraScoreFinal
    # payload['payloadHomol']['intermediarias']['regraorigemModeloApplication'] = regraorigemModeloApplication

    
    return payload

#Faixa de Renda Liquida
def FaixaRendaLiquida(payload):
    rendaPicpay = payload['solicitante']['rendaLiquidaPicpay']
    ######################################################################################################################

    if verificaNulo(rendaPicpay) or rendaPicpay <= 0:
        faixaRendaLiquida = 'a. Sem renda'
    elif rendaPicpay < 4000:
        faixaRendaLiquida = 'b. < 4k'
    elif rendaPicpay >= 4000 and rendaPicpay < 10000:
        faixaRendaLiquida = 'c. 4k - 10k'
    elif rendaPicpay >= 10000:
        faixaRendaLiquida = 'd. >= 10k'
    else:
        faixaRendaLiquida = 'z. Verificar'

    payload['payloadHomol']['intermediarias']['faixaRendaLiquida'] = faixaRendaLiquida

    return payload

#Faixa de Renda Bruta
def FaixaRendaBruta(payload):
    rendaPicpay = payload['solicitante']['rendaLiquidaPicpay']
    ######################################################################################################################

    if verificaNulo(rendaPicpay) or rendaPicpay <= 0:
        faixaRendaBruta = 'a. Sem renda'
    elif rendaPicpay < 3442.37:
        faixaRendaBruta = 'a. < 4k'
    elif rendaPicpay >= 3442.37 and rendaPicpay < 7499.16:
        faixaRendaBruta = 'b. 4k - 10k'
    elif rendaPicpay >= 7499.16 and rendaPicpay < 11124.16:
        faixaRendaBruta = 'c. 10k - 15k'
    elif rendaPicpay >= 11124.16:
        faixaRendaBruta = 'd. >= 15k'
    else:
        faixaRendaBruta = 'z. Verificar'

    payload['payloadHomol']['intermediarias']['faixaRendaBruta'] = faixaRendaBruta

    return payload


def FaixaScoreInternoCurto(payload):
    
    ghFinal = payload['payloadHomol']['saidas']['ghFinal']
    ######################################################################################################################

    if verificaNulo(ghFinal) or ghFinal == 'missing' or ghFinal == "0":  # or gh < 0?
        faixaScoreAppcardBlend = 'Sem info'
    elif ghFinal == 'K':
        faixaScoreAppcardBlend = 'R12'
    elif ghFinal == 'J':
        faixaScoreAppcardBlend = 'R11'
    elif ghFinal == 'I':
        faixaScoreAppcardBlend = 'R10'
    elif ghFinal == 'H':
        faixaScoreAppcardBlend = 'R9'
    elif ghFinal == 'G':
        faixaScoreAppcardBlend = 'R8'
    elif ghFinal == 'F':
        faixaScoreAppcardBlend = 'R7'
    elif ghFinal == 'E':
        faixaScoreAppcardBlend = 'R6'
    elif ghFinal == 'D':
        faixaScoreAppcardBlend = 'R5'
    elif ghFinal == 'C':
        faixaScoreAppcardBlend = 'R4'
    elif ghFinal == 'B':
        faixaScoreAppcardBlend = 'R3'
    elif ghFinal == 'A':
        faixaScoreAppcardBlend = 'R2'
    elif ghFinal == 'AA':
        faixaScoreAppcardBlend = 'R1'
    else:
        faixaScoreAppcardBlend = 'verificar'

    payload['payloadHomol']['intermediarias']['faixaScoreAppcardBlend'] = faixaScoreAppcardBlend

    return payload

def FaixaScoreExterno(payload):
    ghFinal = payload['payloadHomol']['saidas']['ghFinal']
    ######################################################################################################################

    if verificaNulo(ghFinal) or ghFinal == 'missing' or ghFinal == "0":    #or ghFinal < 0:
        faixaScoreExterno = 'Sem info'
    elif ghFinal in ('I','J','K'):
        faixaScoreExterno = 'R10'
    elif ghFinal == 'H':
        faixaScoreExterno = 'R9'
    elif ghFinal == 'G':
        faixaScoreExterno = 'R8'
    elif ghFinal == 'F':
        faixaScoreExterno = 'R7'
    elif ghFinal == 'E':
        faixaScoreExterno = 'R6'
    elif ghFinal == 'D':
        faixaScoreExterno = 'R5'
    elif ghFinal == 'C':
        faixaScoreExterno = 'R4'
    elif ghFinal == 'B':
        faixaScoreExterno = 'R3'
    elif ghFinal == 'A':
        faixaScoreExterno = 'R2'
    elif ghFinal == 'AA':
        faixaScoreExterno = 'R1'
    else:
        faixaScoreExterno = 'verificar'    

    payload['payloadHomol']['intermediarias']['faixaScoreExterno'] = faixaScoreExterno

    return payload

    
def CriacaoFlagsRangesFiltros(payload): # COMO IREMOS TESTAR O TOPICO 2.1 COMO UM CONJUNTO, UMA REGRA QUE CHAMA TODAS AS FUNCOES.

    # DigitoCPFConcomitanteCP(payload)  #descontinuado
    RDGSmallLimits(payload)
    DataAdmissao(payload)
    DataAdmissaoMeses(payload)
    ClienteExistente(payload)
    ClienteMAT(payload)
    ClienteMATRecorrencia(payload)
    ClienteFOPAG(payload)
    RendaInexistente(payload)
    # ClienteNovo(payload)
    # NaoCliente(payload)
    # ScoreSerasaInvalido(payload)
    #criacao dos modelos de priorizacao (ghFinal e scoreFinal). Jogado para antes do modulo sem info score.
    # CriacaoModelosPriorizacao(payload) ### adicionado na nova versão v2 
    # SemInfoScore(payload)   # Alterado para v2
    ValorTotalInvestido(payload)
    PercentRendaInvestimento(payload)
    InvestidorOriginal(payload)
    PrincipalidadeDeInvestimento(payload)
    PrincipalidadeCriterios(payload)
    RecorrenciaPrincipalidade(payload)
    ClienteBlindado(payload)
    ClienteBloqueado(payload)
    FaixaRendaLiquida(payload)
    FaixaRendaBruta(payload)
    # FaixaScoreInternoCurto(payload)  #alterado
    # FaixaScoreExterno(payload)   #alterado
    criaPriorizaçãoeFaixas(payload) # chama clienteNovo, NaoCliente, criacaoModelosPriorizacao, SemInfoScore,FaixaScoreInternoCurto e FaixaScoreExterno

    return payload

### 2 Filtros Visão Política ( Regras sem renda ) - Antigo cascata
def FiltroVisaoPolitica(payload):
    flagCartaoCreditoContratado = payload['solicitante']['flagCartaoCreditoContratado']
    flagLimiteGarantidoContratado = payload['solicitante']['flagLimiteGarantidoContratado']
    flagBlindados = payload['payloadHomol']['intermediarias']['clienteBlindado']
    flagBlocklist = payload['payloadHomol']['intermediarias']['clienteBloqueado']
    # serasaInvalido = payload['payloadHomol']['intermediarias']['serasaInvalido']
    hardFilter = payload['payloadHomol']['saidas']['mensagemFiltro'] # Variável de Hard Filter - Essa variável é de um RMA separado de concessão
    semInfoScore = payload['payloadHomol']['intermediarias']['semInfoScore']
    #regrasNegativas = payload['payloadHomol']['saidas']['regrasNegativas']
    rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpayBatch']
    ######################################################################################################################  

    statusDecisao = "APROVADO"

    #Verifica hard filter para montar regras
    if (hardFilter in ["99. Accepted"]):
        flagHardFilter = 0
    else:
        flagHardFilter = 1

    #verifica se existe info de renda
    if verificaNulo(rendaLiquidaPicpay):
        semInfoRenda = 1
    else:
        semInfoRenda = 0

    if (flagCartaoCreditoContratado == 1):
        mensagemFiltroPolitica = '97. PPCard Credito Contratado'
        statusDecisao = "NEGADO"
        #regrasNegativas.append({'nomeRegra': 'FiltroVisaoPolitica','descricao': mensagemFiltroPolitica})
        passouRegraFiltros = 1
    elif (flagLimiteGarantidoContratado == 1):
        mensagemFiltroPolitica = '98. LG Contratado'
        statusDecisao = "NEGADO"
        #regrasNegativas.append({'nomeRegra': 'FiltroVisaoPolitica','descricao': mensagemFiltroPolitica})
        passouRegraFiltros = 2
    # elif (flagHardFilter == 0 and serasaInvalido == 1 and flagBlindados == 0):
    #     mensagemFiltroPolitica = '26. Score <= 454'
    #     statusDecisao = "NEGADO"
        #regrasNegativas.append({'nomeRegra': 'FiltroVisaoPolitica','descricao': mensagemFiltroPolitica})
        passouRegraFiltros = 3
    elif (flagHardFilter == 0 and semInfoScore == 1 and flagBlindados == 0):
        mensagemFiltroPolitica = '27. Score Invalido Concessao'
        statusDecisao = "NEGADO"
        #regrasNegativas.append({'nomeRegra': 'FiltroVisaoPolitica','descricao': mensagemFiltroPolitica})
        passouRegraFiltros = 4
    elif (flagHardFilter == 0 and flagBlocklist == 1 and flagBlindados == 0):
        mensagemFiltroPolitica = '29. BlockList Concessao'
        statusDecisao = "NEGADO"
        #regrasNegativas.append({'nomeRegra': 'FiltroVisaoPolitica','descricao': mensagemFiltroPolitica})
        passouRegraFiltros = 5
    elif (flagBlindados ==  1 and flagCartaoCreditoContratado == 0 and flagLimiteGarantidoContratado == 1):
        mensagemFiltroPolitica = '99. Accepted'
    elif (flagHardFilter == 0 and flagCartaoCreditoContratado == 0 and 
          flagLimiteGarantidoContratado == 0  and    # and serasaInvalido == 0 comentado
          semInfoScore == 0 and semInfoRenda == 0 and flagBlocklist == 0):
        mensagemFiltroPolitica = "99. Accepted"
        passouRegraFiltros = 6
    else:
        mensagemFiltroPolitica = hardFilter
        ## NÃO DEVERIA CHEGAR NESSA ETAPA NINGUÉM DIFERENTE DE 99. Accepted pois desviamos os NEGADOS de hardfilter no fluxo (dmps)
        if (mensagemFiltroPolitica in ["99. Accepted"]): 
            statusDecisao = "APROVADO"
            passouRegraFiltros = 7
        else:
            statusDecisao = "NEGADO"
            passouRegraFiltros = 8
        

    payload['payloadHomol']['saidas']['mensagemFiltroPolitica'] = mensagemFiltroPolitica
    payload['payloadHomol']['intermediarias']['passouRegraFiltroPolitica'] = passouRegraFiltros # para facilitar debugs se necessário
    payload['payloadHomol']['saidas']['statusDecisao'] = statusDecisao
    #payload['payloadHomol']['saidas']['regrasNegativas'] = regrasNegativas

    return payload

### 3.3 Segmentação de Negócio
def SegmentacaoNegocio(payload):
    saldoMedio = payload['solicitante']['saldoMedio']
    rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpay']
    flagPrincipalidadeAll = payload['solicitante']['flagPrincipalidadeAll']
    idade = payload['solicitante']['idade']  #idade de solicitacao
    ######################################################################################################################  
    
    if (saldoMedio >= 100000):
        segmentacaoNegocio = "Alta renda investidor"
    elif (idade in (18,19)):
        segmentacaoNegocio = 'Jovem Cliente'  #add nova versao v2 
    elif (saldoMedio >= 40000 or rendaLiquidaPicpay >= 11124.16 or (rendaLiquidaPicpay >= 7499.16 and flagPrincipalidadeAll == 1)):
        segmentacaoNegocio = "Alta renda"
    elif (rendaLiquidaPicpay >= 3442.37 or flagPrincipalidadeAll == 1):
        segmentacaoNegocio = "Varejo+"
    else:
        segmentacaoNegocio = "Varejo"

    payload['payloadHomol']['intermediarias']['segmentacaoNegocio'] = segmentacaoNegocio

    return payload


### 4.2 Segmentação Politica
def SegmentacaoPoliticaAntigos(payload):
    segmentacaoNegocio = payload['payloadHomol']['intermediarias']['segmentacaoNegocio']
    flagFuncionario = payload['solicitante']['flagFuncionario']
    FoPag = payload['payloadHomol']['intermediarias']['clienteFoPag']
    principalidadeAll =  payload['payloadHomol']['intermediarias']['principalidadeAll']
    flagMAT = payload['payloadHomol']['intermediarias']['clienteMAT']    #flagMat35d
    rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpay']    #Renda recalculada
    ######################################################################################################################  

    if verificaNulo(segmentacaoNegocio):
        segmentacaoNegocio = "Varejo"

    if (flagFuncionario == 1):
        segmentacaoPolitica = "P1 - Funcionarios"
    elif (FoPag == 1):
        segmentacaoPolitica = "P2 - FOPAG"
    elif (segmentacaoNegocio == "Alta renda investidor"):
        segmentacaoPolitica = "P3 - Investidor Alta Renda"
    elif (segmentacaoNegocio == "Jovem Cliente" and principalidadeAll == 1):
        segmentacaoPolitica = "P7.1 – Jovem Cliente Principalidade"
    elif (segmentacaoNegocio == "Jovem Cliente" and flagMAT == 1):
        segmentacaoPolitica = "P7.2 – Jovem Cliente MAT"  
    elif (segmentacaoNegocio == "Jovem Cliente"):
        segmentacaoPolitica = "P7.3 – Jovem Cliente Inativos"
    elif (segmentacaoNegocio == "Alta renda" and principalidadeAll == 1):
        segmentacaoPolitica = "P4.1 - Alta Renda Principalidade"
    elif (segmentacaoNegocio == "Alta renda" and flagMAT == 1):
        segmentacaoPolitica = "P4.2 - Alta Renda MAT"
    elif (segmentacaoNegocio == "Alta renda"):
        segmentacaoPolitica = "P4.3 - Alta Renda Inativo"
    elif (segmentacaoNegocio == "Varejo+" and rendaLiquidaPicpay < 3442.37):
        segmentacaoPolitica = "P5.4 - Varejo+ Principalidade Renda < 4k"
    elif (segmentacaoNegocio == "Varejo+" and rendaLiquidaPicpay >= 3442.37 and principalidadeAll == 1):
        segmentacaoPolitica = "P5.1 - Varejo+ Principalidade Renda >= 4k"
    elif (segmentacaoNegocio == "Varejo+" and flagMAT == 1):
        segmentacaoPolitica = "P5.2 - Varejo+ MAT"
    elif (segmentacaoNegocio == "Varejo+"):
        segmentacaoPolitica = "P5.3 - Varejo+ Inativos"
    elif ((segmentacaoNegocio == "Varejo" and principalidadeAll == 1) or (segmentacaoNegocio == "Varejo" and flagMAT ==1)):
        segmentacaoPolitica = "P6.1 - Varejo MAT"
    elif (segmentacaoNegocio == "Varejo"):
        segmentacaoPolitica = "P6.2 - Varejo Inativos"
    else:
        segmentacaoPolitica = "P6.2 - Varejo Inativos"

    payload['payloadHomol']['intermediarias']['PassousegmentacaoPolitica'] = "ANTIGO" #  marcando para auxilair em possiveis analises
    payload['payloadHomol']['intermediarias']['segmentacaoPolitica'] = segmentacaoPolitica

    return payload


### 4.3 Faixa de Risco Interno Agrupada
def FaixaRiscoInternoAgrupada(payload):
    
    segmentacaoPolitica = payload['payloadHomol']['intermediarias']['segmentacaoPolitica']  ## Variavel nova a ser usada na versão v2.   
    faixaScoreAppcardBlend = payload['payloadHomol']['intermediarias']['faixaScoreAppcardBlend']   #range_appcard_blend
    ###################################################################################################################### 
    
    #"P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos"
    if segmentacaoPolitica in ("P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos") and faixaScoreAppcardBlend == "R1":
        faixaRiscoInternoAgrupada = 'R2'
        regraFaixaRiscoInternoAgrupada = 1
    elif segmentacaoPolitica in ("P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos") and faixaScoreAppcardBlend == "R2":
        faixaRiscoInternoAgrupada = 'R3'
        regraFaixaRiscoInternoAgrupada = 2
    elif segmentacaoPolitica in ("P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos") and faixaScoreAppcardBlend == "R3":
        faixaRiscoInternoAgrupada = 'R4'
        regraFaixaRiscoInternoAgrupada = 3
    elif segmentacaoPolitica in ("P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos") and faixaScoreAppcardBlend == "R4":
        faixaRiscoInternoAgrupada = 'R5'
        regraFaixaRiscoInternoAgrupada = 4
    elif segmentacaoPolitica in ("P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos") and faixaScoreAppcardBlend == "R5":
        faixaRiscoInternoAgrupada = 'R6'
        regraFaixaRiscoInternoAgrupada = 5
    elif segmentacaoPolitica in ("P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos") and faixaScoreAppcardBlend == "R6":
        faixaRiscoInternoAgrupada = 'R7'
        regraFaixaRiscoInternoAgrupada = 6
    elif segmentacaoPolitica in ("P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos") and faixaScoreAppcardBlend == "R7":
        faixaRiscoInternoAgrupada = 'R8'
        regraFaixaRiscoInternoAgrupada = 7
    elif segmentacaoPolitica in ("P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos") and faixaScoreAppcardBlend == "R8":
        faixaRiscoInternoAgrupada = 'R9'
        regraFaixaRiscoInternoAgrupada = 8
    elif segmentacaoPolitica in ("P4.3 - Alta Renda Inativo","P5.3 - Varejo+ Inativos") and faixaScoreAppcardBlend in ("R9","R10","R11","R12"):
        faixaRiscoInternoAgrupada = 'R10'
        regraFaixaRiscoInternoAgrupada = 9
        
    #"P6.2 - Varejo Inativos"
    elif segmentacaoPolitica in ("P6.2 - Varejo Inativos") and faixaScoreAppcardBlend in ("R1","R2","R3"):
        faixaRiscoInternoAgrupada = 'R3'
        regraFaixaRiscoInternoAgrupada = 10
        
    #"P5.1 - Varejo+ Principalidade Renda >= 4k"
    
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend in ("R1","R2"):
        faixaRiscoInternoAgrupada = 'R1'
        regraFaixaRiscoInternoAgrupada = 11
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend == "R3":
        faixaRiscoInternoAgrupada = 'R2'
        regraFaixaRiscoInternoAgrupada = 12
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend == "R4":
        faixaRiscoInternoAgrupada = 'R3'
        regraFaixaRiscoInternoAgrupada = 13
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend in ("R5","R6"):
        faixaRiscoInternoAgrupada = 'R4'
        regraFaixaRiscoInternoAgrupada = 14
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend == "R7":
        faixaRiscoInternoAgrupada = 'R5'
        regraFaixaRiscoInternoAgrupada = 15
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend == "R8":
        faixaRiscoInternoAgrupada = 'R6'
        regraFaixaRiscoInternoAgrupada = 16
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend == "R9":
        faixaRiscoInternoAgrupada = 'R7'
        regraFaixaRiscoInternoAgrupada = 17
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend == "R10":
        faixaRiscoInternoAgrupada = 'R8'
        regraFaixaRiscoInternoAgrupada = 18
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend == "R11":
        faixaRiscoInternoAgrupada = 'R9'
        regraFaixaRiscoInternoAgrupada = 19
    elif segmentacaoPolitica in ("P5.1 - Varejo+ Principalidade Renda >= 4k") and faixaScoreAppcardBlend == "R12":
        faixaRiscoInternoAgrupada = 'R10'
        regraFaixaRiscoInternoAgrupada = 20
    elif faixaScoreAppcardBlend in ("R11","R12"):
        faixaRiscoInternoAgrupada = "R10"
        regraFaixaRiscoInternoAgrupada = 21
    else:
        faixaRiscoInternoAgrupada = faixaScoreAppcardBlend
        regraFaixaRiscoInternoAgrupada = 22                                                 
    
    
        

    payload['payloadHomol']['intermediarias']['faixaRiscoInternoAgrupada'] = faixaRiscoInternoAgrupada
    payload['payloadHomol']['intermediarias']['regraFaixaRiscoInternoAgrupada'] = regraFaixaRiscoInternoAgrupada # para auxiliar em possiveis debugs

    return payload



### 4.5 Sub Grupos (Caselas de Já clientes)
def subGruposAntigo(payload):
    faixaRiscoInternoAgrupada = payload['payloadHomol']['intermediarias']['faixaRiscoInternoAgrupada'] #rangeScoreInterno
    segmentacaoPolitica = payload['payloadHomol']['intermediarias']['segmentacaoPolitica']
    faixaRendaBruta = payload['payloadHomol']['intermediarias']['faixaRendaBruta']
    flagFuncionario = payload['solicitante']['flagFuncionario']
    # cpf6e7DigitoInt = int(payload['payloadHomol']['intermediarias']['cpf6e7Digito']) 
    cpf6e7Digito = int(payload['payloadHomol']['intermediarias']['cpf6e7Digito'])  #transforma pra string devido ao 0 a esquerda nas faixas
    flagCCRot = payload['solicitante']['flagCCRot']
    mediaSaldoTotal = payload['solicitante']['mediaSaldoTotal']
    ######################################################################################################################  

    tabelaFiltros = []

    # Tratamento caso o valor dos digitos do cpf venha como "0"
    if str(cpf6e7Digito) == "0":
        faixaCPF = "00"
        
        
        
    if ("00" <= str(cpf6e7Digito) <= "09"):
        faixaCPF = "00 <= .. <= 09"
    elif ("10" <= str(cpf6e7Digito) <= "19"):
        faixaCPF = "10 <= .. <= 19"
    elif ("20" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "20 <= .. <= 99"
    elif ("10" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "10 <= .. <= 99"
    elif ("00" <= str(cpf6e7Digito) <= "19"):
        faixaCPF = "00 <= .. <= 19"
    elif ("20" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "20 <= .. <= 99"
    elif ("00" <= str(cpf6e7Digito) <= "19"):
        faixaCPF = "00 <= .. <= 19"
    elif ("10" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "10 <= .. <= 99"
    elif ("50" <= str(cpf6e7Digito) <= "59"):
        faixaCPF = "50 <= .. <= 59"
    elif ("60" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "60 <= .. <= 99"
    elif ("00" <= str(cpf6e7Digito) <= "09"):
        faixaCPF = "00 <= .. <= 09"
    elif ("10" <= str(cpf6e7Digito) <= "19"):
        faixaCPF = "10 <= .. <= 19"
    elif ("20" <= str(cpf6e7Digito) <= "49"):
        faixaCPF = "20 <= .. <= 49"
    elif ("50" <= str(cpf6e7Digito) <= "59"):
        faixaCPF = "50 <= .. <= 59"
    elif ("60" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "60 <= .. <= 99"
    elif ("00" <= str(cpf6e7Digito) <= "14"):
        faixaCPF = "00 <= .. <= 14"
    elif ("15" <= str(cpf6e7Digito) <= "94"):
        faixaCPF = "15 <= .. <= 94"
    elif ("95" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "95 <= .. <= 99"
    elif ("00" <= str(cpf6e7Digito) <= "19"):
        faixaCPF = "00 <= .. <= 19"
    elif ("20" <= str(cpf6e7Digito) <= "89"):
        faixaCPF = "20 <= .. <= 89"
    elif ("90" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "90 <= .. <= 99"
    elif ("15" <= str(cpf6e7Digito) <= "94"):
        faixaCPF = "15 <= .. <= 94"
    elif ("10" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "10 <= .. <= 99"
    elif ("10" <= str(cpf6e7Digito) <= "49"):
        faixaCPF = "10 <= .. <= 49"
    elif ("10" <= str(cpf6e7Digito) <= "79"):
        faixaCPF = "10 <= .. <= 79"
    elif ("80" <= str(cpf6e7Digito) <= "99"):
        faixaCPF = "80 <= .. <= 99"
    elif ("00" <= str(cpf6e7Digito) <= "69"):
        faixaCPF = "00 <= .. <= 69"
    elif ("70" <= str(cpf6e7Digito) <= "79"):
        faixaCPF = "70 <= .. <= 79"


    if (faixaRiscoInternoAgrupada == "R1"):
        if ("00" <= str(cpf6e7Digito) <= "09"):
            faixaCPF = "00 <= ..<= 09"
        if (10 <= cpf6e7Digito <= 19):
            faixaCPF = "10 <= ..<= 19"
        if (20 <= cpf6e7Digito <= 99):
            faixaCPF = "20 <= ..<= 99"
        if (10 <= cpf6e7Digito <= 99):
            faixaCPF = "10 <= ..<= 99"
        if (10 <= cpf6e7Digito <= 79):
            faixaCPF = "10 <= ..<= 79"
        if (80 <= cpf6e7Digito <= 99):
            faixaCPF = "80 <= ..<= 99"
        if (00 <= cpf6e7Digito <= 69):
            faixaCPF = "00 <= ..<= 69"
        if (70 <= cpf6e7Digito <= 79):
            faixaCPF = "70 <= ..<= 79"
            
    #  if (faixaRiscoInternoAgrupada == "R2"):
         
            
            
            
    # faixas de mediaSaldoTotal
    if mediaSaldoTotal >= 40000:
        mediaSaldoTotalfaixa = ">= 40000"
    else:
        mediaSaldoTotalfaixa = "< 40000"


    # Filtro - Nível rangeScoreInterno
    filtroNivelRangeScoreInterno = dftabClienteAntigo.loc[(dftabClienteAntigo['rangeScoreInterno'] == faixaRiscoInternoAgrupada)]
    if filtroNivelRangeScoreInterno.empty:
        filtroNivelRangeScoreInterno = dftabClienteAntigo.loc[(dftabClienteAntigo['rangeScoreInterno'] == "QUALQUER")]
        tabelaFiltros.append(1)
        
    # Filtro - Nível segmentacaoPolitica
    filtroNivelSegmentacaoPolitica = filtroNivelRangeScoreInterno.loc[(filtroNivelRangeScoreInterno['segmentacaoPolitica'] == segmentacaoPolitica)]
    if filtroNivelSegmentacaoPolitica.empty:
        filtroNivelSegmentacaoPolitica = filtroNivelRangeScoreInterno.loc[(filtroNivelRangeScoreInterno['segmentacaoPolitica'] == "QUALQUER")]
        tabelaFiltros.append(2)
        
    # Filtro - Nível faixaRendaBruta
    filtroNivelFaixaRendaBruta = filtroNivelSegmentacaoPolitica.loc[(filtroNivelSegmentacaoPolitica['faixaRendaBruta'] == faixaRendaBruta)]
    if filtroNivelFaixaRendaBruta.empty:
        filtroNivelFaixaRendaBruta = filtroNivelSegmentacaoPolitica.loc[(filtroNivelSegmentacaoPolitica['faixaRendaBruta'] == "QUALQUER")]
        tabelaFiltros.append(3)
        
    # Filtro - Nível flagFuncionario
    filtroNivelflagFuncionario = filtroNivelFaixaRendaBruta.loc[(filtroNivelFaixaRendaBruta['flagFuncionario'] == flagFuncionario)]
    if filtroNivelflagFuncionario.empty:
        filtroNivelflagFuncionario = filtroNivelFaixaRendaBruta.loc[(filtroNivelFaixaRendaBruta['flagFuncionario'] == "QUALQUER")]
        tabelaFiltros.append(4)
        
    # Filtro - Nível 6e7 Digito CPF
    filtroNivelCPF = filtroNivelflagFuncionario.loc[(filtroNivelflagFuncionario['cpf6e7Digito'] == faixaCPF)]
    if filtroNivelCPF.empty:
        filtroNivelCPF = filtroNivelflagFuncionario.loc[(filtroNivelflagFuncionario['cpf6e7Digito'] == "QUALQUER")]
        tabelaFiltros.append(5)
        
    # FILTROS PARA DIFERENCIAR OS PERFIS DE INVESTIDOR
    # Filtro - rotativoCCBacen
    filtroNivelrotativoCCBacen = filtroNivelCPF.loc[(filtroNivelCPF['FLAG_CC_ROT'] == str(flagCCRot))]
    if filtroNivelrotativoCCBacen.empty:
        filtroNivelrotativoCCBacen = filtroNivelCPF.loc[(filtroNivelCPF['FLAG_CC_ROT'] == "QUALQUER")]
        tabelaFiltros.append(6)
        
    # Filtro - mean_total_balance_d0
    filtroSaldoTotal = filtroNivelrotativoCCBacen.loc[(filtroNivelrotativoCCBacen['mean_total_balance_d0'] == mediaSaldoTotalfaixa)]
    if filtroSaldoTotal.empty:
        filtroSaldoTotal = filtroNivelrotativoCCBacen.loc[(filtroNivelrotativoCCBacen['mean_total_balance_d0'] == "QUALQUER")]
        tabelaFiltros.append(7)

    linha_tab = filtroSaldoTotal
    # Teste para ver se após filtrar na tabela o cliente foi encontrado
    if linha_tab.empty:
        segmentacao = "0"
        alavancagem = 0
        limiteTeto = 0
        limitePiso = 0
        limiteFixo = 0
        aprovadoBAUAux = 0
        aprovadoSLAux = 0
        cenario = 999
    else:
        segmentacao         = linha_tab.iloc[0]['SEGMENTAÇÃO FINAL']
        alavancagem         = linha_tab.iloc[0]['alavancagem']
        limiteTeto          = linha_tab.iloc[0]['limiteTeto']
        limitePiso          = linha_tab.iloc[0]['limitePiso']
        limiteFixo          = linha_tab.iloc[0]['limiteFixo']
        aprovadoBAUAux      = linha_tab.iloc[0]['isApprovedBauAux']
        aprovadoSLAux       = linha_tab.iloc[0]['isApprovedSmallLimits']
        cenario             = linha_tab.iloc[0]['cenario']

        
    payload['payloadHomol']["intermediarias"]["segmentacao"]    = str(segmentacao)           
    payload['payloadHomol']["intermediarias"]["alavancagem"]    = float(alavancagem)            
    payload['payloadHomol']["intermediarias"]["limiteTeto"]     = float(limiteTeto)        
    payload['payloadHomol']["intermediarias"]["limitePiso"]     = float(limitePiso)
    payload['payloadHomol']["intermediarias"]["limiteFixo"]     = int((str(limiteFixo).replace("QUALQUER","0")))          
    payload['payloadHomol']["intermediarias"]["aprovadoBAUAux"] = int(aprovadoBAUAux)         
    payload['payloadHomol']["intermediarias"]["aprovadoSLAux"]  = int(aprovadoSLAux)         
    payload['payloadHomol']["intermediarias"]["cenario"]        = int(cenario)    
    payload['payloadHomol']['intermediarias']['tabelaFiltros']  = tabelaFiltros         

    return payload

# 4.6/5.4
# Flag Aprovado BAU - Regra igual para já cliente e mar aberto
def FlagAprovadoBAU(payload):
    mensagemFinal = payload['payloadHomol']['saidas']['mensagemFinal'] # vem do processo de hard filters
    aprovadoBAUAux = payload['payloadHomol']["intermediarias"]["aprovadoBAUAux"] # vem da tabela
    ######################################################################################################################  

    if (mensagemFinal == "99. Accepted" and aprovadoBAUAux == 1):
        flagAprovadoBAU = 1
    else:
        flagAprovadoBAU = 0

    payload['payloadHomol']['intermediarias']['flagAprovadoBAU'] = flagAprovadoBAU

    return payload

# Flag Concomitante SL CP - Regra igual para já cliente e mar aberto
# def FlagConcomitanteSLCP(payload):
#     aprovadoSLAuxiliar = payload['payloadHomol']["intermediarias"]["aprovadoSLAux"] # vem da tabela
#     mensagemFinal = payload['payloadHomol']['saidas']['mensagemFinal'] # vem do processo de hard filters
#     DigitoCPFConcomitanteCP = payload['payloadHomol']['intermediarias']['digitoCPFConcomitanteCP']
#     flagSmallLimits = payload['solicitante']['flagSmallLimits']
#     ######################################################################################################################  

#     if (aprovadoSLAuxiliar == 1 and  mensagemFinal == "99. Accepted" and flagSmallLimits == 1 and (50 <= DigitoCPFConcomitanteCP <= 74)):
#         flagConcomitanteSLCP = 1
#     else:
#         flagConcomitanteSLCP = 0

#     payload['payloadHomol']['intermediarias']['flagConcomitanteSLCP'] = flagConcomitanteSLCP

#     return payload

# Flag Aprovado SL - Regra igual para já cliente e mar aberto
def FlagAprovadoSL(payload):
    aprovadoSLAuxiliar = payload['payloadHomol']["intermediarias"]["aprovadoSLAux"] # vem da tabela
    # flagConcomitanteSLCP = payload['payloadHomol']['intermediarias']['flagConcomitanteSLCP']
    mensagemFinal = payload['payloadHomol']['saidas']['mensagemFinal'] # vem do processo de hard filters
    ######################################################################################################################  

    if (mensagemFinal == "99. Accepted" and aprovadoSLAuxiliar == 1): #and flagConcomitanteSLCP == 0
        flagAprovadoSL = 1
    else:
        flagAprovadoSL = 0

    payload['payloadHomol']['intermediarias']['flagAprovadoSL'] = flagAprovadoSL

    return payload

# Marcações de aprovados - chamando os modulos dos respectivos cálculos
def MarcacoesAprovados(payload):
    FlagAprovadoBAU(payload)
    # FlagConcomitanteSLCP(payload)
    FlagAprovadoSL(payload)

    return payload

### 4.6.1 Limite Final e 5.4.1 Limite final - No momento é a mesma regra
def LimiteFinal(payload):
    rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpay']
    alavancagem = payload['payloadHomol']["intermediarias"]["alavancagem"]
    limitePiso = payload['payloadHomol']["intermediarias"]["limitePiso"]
    limiteTeto = payload['payloadHomol']["intermediarias"]["limiteTeto"]
    limiteFixo = payload['payloadHomol']["intermediarias"]["limiteFixo"]
    ######################################################################################################################  
    if (limiteFixo > 0):
        limiteFinal = limiteFixo
    else:
        limiteFinal = min(max(rendaLiquidaPicpay*alavancagem,limitePiso),limiteTeto)
        limiteFinal = (round(limiteFinal/50))*50
        
        
        ## adicionado na v2    
        if 3600 <= limiteFinal <= 4000:
            limiteFinal = 4000  #saida ou intermediaria?
        elif 9500 <= limiteFinal <= 10000:
            limiteFinal = 10000
    


        
        
    payload['payloadHomol']['saidas']['limiteFinal'] = limiteFinal

    return payload

##### 5. Politica Publico Clientes mar aberto
# obs: É considerado “Público Clientes Antigos” quem possuir a flag cliente existente = 1.
### 5.1 Segmentação Política - Novos Clientes e Mar Aberto
def SegmentacaoPoliticaMarAberto(payload):
    FoPag = payload['payloadHomol']['intermediarias']['clienteFoPag']  
    rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpay']
    flagClienteExistente = payload['payloadHomol']['intermediarias']['flagClienteExistente']
    idade = payload['idade']  #idade de solicitacao
    flagFuncionario = payload['solicitante']['flagFuncionario'] # is_employee
    ######################################################################################################################  
    
    if flagClienteExistente == 0:
        if (flagFuncionario == 1):
            segmentacaoPolitica = "N1.1 - FUNCIONARIO"
        elif (FoPag == 1):
            segmentacaoPolitica = "N1.2 – FOPAG"
        elif (idade in (18,19)):
            segmentacaoPolitica = "N3 – Jovem Cliente"
        elif (rendaLiquidaPicpay < 3442.37):
            segmentacaoPolitica = "N2.1 - Varejo"
        elif (rendaLiquidaPicpay < 11124.16):
            segmentacaoPolitica = "N2.2 - Varejo+"
        elif (rendaLiquidaPicpay >= 11124.16):
            segmentacaoPolitica = "N2.3 - Alta Renda"
        else:
            segmentacaoPolitica = "N2.1 - Varejo"
    else:
        segmentacaoPolitica = "N2.1 - Varejo"

    payload['payloadHomol']['intermediarias']['PassousegmentacaoPolitica'] = "MAR ABERTO" #  marcando para auxilair em possiveis analises
    payload['payloadHomol']['intermediarias']['segmentacaoPolitica'] = segmentacaoPolitica

    return payload


### 5.3 Sub Gruposs ( Caselas de Clientes Novos e Mar aberto )
def SubGruposMarAberto(payload):
    faixaScoreExterno = payload['payloadHomol']['intermediarias']['faixaScoreExterno'] #rangeScoreInterno
    segmentacaoPolitica = payload['payloadHomol']['intermediarias']['segmentacaoPolitica']
    faixaRendaLiquida = payload['payloadHomol']['intermediarias']['faixaRendaLiquida']
    dataAdmissaoMeses = payload['payloadHomol']['intermediarias']['admissaoMeses']
    cpf6e7Digito = int(payload['payloadHomol']['intermediarias']['cpf6e7Digito'])
    ######################################################################################################################  

    tabelaFiltros = []

    # Faixas de CPF - No Público Mar Aberto temos faixas especificas para alguns scores.
    if (faixaScoreExterno == "R5"):
        if (0 <= cpf6e7Digito <= 44):
            faixaCPF = "0 <= ..<= 44"
        if (45 <= cpf6e7Digito <= 89):
            faixaCPF = "45 <= ..<= 89"
        if (90 <= cpf6e7Digito <= 99):
            faixaCPF = "90 <= ..<= 99"
    elif (faixaScoreExterno == "R6"):
        if (0 <= cpf6e7Digito <= 40):
            faixaCPF = "0 <= ..<= 40"
        if (41 <= cpf6e7Digito <= 81):
            faixaCPF = "41 <= ..<= 81"
        if (82 <= cpf6e7Digito <= 89):
            faixaCPF = "82 <= ..<= 89"
        if (90 <= cpf6e7Digito <= 99):
            faixaCPF = "90 <= ..<= 99"
    elif (faixaScoreExterno == "R7"):
        if (0 <= cpf6e7Digito <= 38):
            faixaCPF = "0 <= ..<= 38"
        if (39 <= cpf6e7Digito <= 79):
            faixaCPF = "39 <= ..<= 79"
        if (80 <= cpf6e7Digito <= 89):
            faixaCPF = "80 <= ..<= 89"
        if (90 <= cpf6e7Digito <= 99):
            faixaCPF = "90 <= ..<= 99"
    elif (faixaScoreExterno == "R8"):
        if (0 <= cpf6e7Digito <= 39):
            faixaCPF = "0 <= ..<= 39"
        if (40 <= cpf6e7Digito <= 81):
            faixaCPF = "40 <= ..<= 81"
        if (82 <= cpf6e7Digito <= 89):
            faixaCPF = "82 <= ..<= 89"
        if (90 <= cpf6e7Digito <= 99):
            faixaCPF = "90 <= ..<= 99"
    elif (faixaScoreExterno == "R9"):
        if (0 <= cpf6e7Digito <= 40):
            faixaCPF = "0 <= ..<= 40"
        if (41 <= cpf6e7Digito <= 82):
            faixaCPF = "41 <= ..<= 82"
        if (83 <= cpf6e7Digito <= 89):
            faixaCPF = "83 <= ..<= 89"
        if (90 <= cpf6e7Digito <= 99):
            faixaCPF = "90 <= ..<= 99"
    elif (faixaScoreExterno == "R10"):
        if (0 <= cpf6e7Digito <= 26):
            faixaCPF = "0 <= ..<= 26"
        if (27 <= cpf6e7Digito <= 53):
            faixaCPF = "27 <= ..<= 53"
        if (54 <= cpf6e7Digito <= 59):
            faixaCPF = "54 <= ..<= 59"
        if (60 <= cpf6e7Digito <= 99):
            faixaCPF = "60 <= ..<= 99"
    else:
         faixaCPF = "QUALQUER"

    # Primeiro Filtro - Nível faixaScoreExterno
    filtroNivelRangeScoreExterno = dftabClienteMarAberto.loc[(dftabClienteMarAberto['rangeScoreExterno'] == faixaScoreExterno)]
    if filtroNivelRangeScoreExterno.empty:
        filtroNivelRangeScoreExterno = dftabClienteMarAberto.loc[(dftabClienteMarAberto['rangeScoreExterno'] == "QUALQUER")]
        tabelaFiltros.append(1)
    # Segundo Filtro - Nível segmentacaoPolitica
    filtroNivelSegmentacaoPolitica = filtroNivelRangeScoreExterno.loc[(filtroNivelRangeScoreExterno['segmentacaoPolitica'] == segmentacaoPolitica)]
    if filtroNivelSegmentacaoPolitica.empty:
        filtroNivelSegmentacaoPolitica = filtroNivelRangeScoreExterno.loc[(filtroNivelRangeScoreExterno['segmentacaoPolitica'] == "QUALQUER")]
        tabelaFiltros.append(2)
    # Terceiro Filtro - Nível faixaRenda
    filtroNivelFaixaRenda = filtroNivelSegmentacaoPolitica.loc[(filtroNivelSegmentacaoPolitica['faixaRendaLiquida'] == faixaRendaLiquida)]
    if filtroNivelFaixaRenda.empty:
        filtroNivelFaixaRenda = filtroNivelSegmentacaoPolitica.loc[(filtroNivelSegmentacaoPolitica['faixaRendaLiquida'] == "QUALQUER")]
        tabelaFiltros.append(3)
    # Quarto Filtro - Nível dataAdmissaoMeses
    filtroNiveldataAdmissaoMeses = filtroNivelFaixaRenda.loc[(filtroNivelFaixaRenda['dataAdmissaoMeses'].str.strip() == str(dataAdmissaoMeses))]
    if filtroNiveldataAdmissaoMeses.empty:
        filtroNiveldataAdmissaoMeses = filtroNivelFaixaRenda.loc[(filtroNivelFaixaRenda['dataAdmissaoMeses'] == "QUALQUER")]
        tabelaFiltros.append(4)
    # Quinto Filtro - Nível 5e6 Digito CPF
    filtroNivelCPF = filtroNiveldataAdmissaoMeses.loc[(filtroNiveldataAdmissaoMeses['cpf6e7Digito'] == faixaCPF)]
    if filtroNivelCPF.empty:
        filtroNivelCPF = filtroNiveldataAdmissaoMeses.loc[(filtroNiveldataAdmissaoMeses['cpf6e7Digito'] == "QUALQUER")]
        tabelaFiltros.append(5)

    linha_tab = filtroNivelCPF
    # Teste para ver se após filtrar na tabela o cliente foi encontrado
    if linha_tab.empty:
        segmentacao = "0"
        alavancagem = 0
        limiteTeto = 0
        limitePiso = 0
        limiteFixo = 0
        aprovadoBAUAux = 0
        aprovadoSLAux = 0
        cenario = 999
    else:
        segmentacao         = linha_tab.iloc[0]['SEGMENTAÇÃO FINAL']
        alavancagem         = linha_tab.iloc[0]['alavancagem']
        limiteTeto          = linha_tab.iloc[0]['limiteTeto']
        limitePiso          = linha_tab.iloc[0]['limitePiso']
        limiteFixo          = linha_tab.iloc[0]['limiteFixo']
        aprovadoBAUAux      = linha_tab.iloc[0]['isApprovedBau']
        aprovadoSLAux       = linha_tab.iloc[0]['isApprovedSmallLimits']
        cenario             = linha_tab.iloc[0]['cenario']

    payload['payloadHomol']["intermediarias"]["segmentacao"]    = str(segmentacao)           
    payload['payloadHomol']["intermediarias"]["alavancagem"]    = float(alavancagem)            
    payload['payloadHomol']["intermediarias"]["limiteTeto"]     = float(limiteTeto)        
    payload['payloadHomol']["intermediarias"]["limitePiso"]     = float(limitePiso) 
    payload['payloadHomol']["intermediarias"]["limiteFixo"]     = int((str(limiteFixo).replace("QUALQUER","0")))        
    payload['payloadHomol']["intermediarias"]["aprovadoBAUAux"] = int(aprovadoBAUAux)         
    payload['payloadHomol']["intermediarias"]["aprovadoSLAux"]  = int(aprovadoSLAux)         
    payload['payloadHomol']["intermediarias"]["cenario"]        = int(cenario)    
    payload['payloadHomol']['intermediarias']['tabelaFiltros']  = tabelaFiltros    
    
    

    return payload

##### Regras do DCO ( Era um RMA separado, estaremos juntando com o de concessão cartões )
## Etapa Validações Internas DCO
def validacoesInternas (payload):
    limiteDisponivel = payload['solicitante']['limites']['limiteDisponivel']
    valorContratadoCartao = payload['solicitante']['limites']['valorContratadoCartao']
    marcacaoMesa = payload['marcacaoMesa']
    maxDiasAtraso = payload['solicitante']['maxDiasAtraso']
    indFNV = payload['solicitante']['indFNV']
    indReneg = payload['solicitante']['indReneg']
    flagErroAurora = payload['flagErroAurora']
    flagErroVisaoCliente = payload['flagErroVisaoCliente']

    regrasNegativasTemp = payload['payloadHomol']['saidas']['regrasNegativas']
    passouValidacaoInterna = 0
    
    if marcacaoMesa != 'MESA':
        if flagErroAurora == False and flagErroVisaoCliente == False:
            #Execução da função que retorna verdadeiro ou falso caso a variável seja de um formato inválido
            if verificaNulo(payload['solicitante']['limites']['limiteDisponivel']) == True: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '54. Não possui a informação de limite disponível'})
                passouValidacaoInterna = 1
            if verificaNulo(payload['solicitante']['limites']['valorContratadoCartao']) == True: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '55. Não possui a informação de valor contratado cartão'})
                passouValidacaoInterna = 2
            if verificaNulo(payload['marcacaoMesa']) == True: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '56. Não possui a informação de marcação mesa'})
                passouValidacaoInterna = 3
            if verificaNulo(payload['solicitante']['maxDiasAtraso']) == True: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '57. Não possui a informação de maximo dias atraso'})
                passouValidacaoInterna = 4
            if verificaNulo(payload['solicitante']['indFNV']) == True: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '58. Não possui a informação do indicador fique no verde'})
                passouValidacaoInterna = 5
            if verificaNulo(payload['solicitante']['indReneg']) == True: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '59. Não possui a informação do inidicador renegociação'})
                passouValidacaoInterna = 6

            if payload['payloadHomol']['saidas']['regrasNegativas'] != []: indVarEntradaNula = True
            else: indVarEntradaNula = False

            if indVarEntradaNula == False:
                            if limiteDisponivel <= 0:
                                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'      
                                regrasNegativasTemp.append({'nomeRegra': 'rl_limiteDisponivel','descricao': '34. limite disponível < = 0'})
                                passouValidacaoInterna = 7
                            if valorContratadoCartao > 0: 
                                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                                regrasNegativasTemp.append({'nomeRegra': 'rl_limiteCartao','descricao': '35. O produto cartão possui limite > 0 contratado'})
                                passouValidacaoInterna = 8
                            if marcacaoMesa == 'MESA': 
                                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                                regrasNegativasTemp.append({'nomeRegra': 'rl_marcacaoMesa','descricao': '36. O cliente tem marcação na variável userRole = MESA'})
                                passouValidacaoInterna = 9
                            if float(maxDiasAtraso) > 5: 
                                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                                regrasNegativasTemp.append({'nomeRegra': 'rl_atrasoInterno','descricao': '37. Possui atraso > 5 dias em qualquer produto'})
                                passouValidacaoInterna = 10
                            if indFNV == True: 
                                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                                regrasNegativasTemp.append({'nomeRegra': 'rl_validaFNV','descricao': '38. O cliente possui fique no verde ativo'})
                                passouValidacaoInterna = 11
                            if indReneg == True: 
                                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                                regrasNegativasTemp.append({'nomeRegra': 'rl_validaReneg','descricao': '39. O cliente possui renegociação ativa'})
                                passouValidacaoInterna = 12
        else:
            payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
            regrasNegativasTemp.append({'nomeRegra': 'erroChamadaBureau','descricao': '76. Erro Chamada Bureaus - Validações Internas'})
            passouValidacaoInterna = 13
 
                
    payload['payloadHomol']['saidas']['regrasNegativas'] = regrasNegativasTemp
    payload['payloadHomol']['intermediarias']['passouValidacaoInterna'] = passouValidacaoInterna
            
    return payload

## Etapa Bureau Cadastral DCO
def validaDadosCadastrais (payload):
    situacaoDocumento = payload['solicitante']['credilink']['situacaoDocumento']
    obito = payload['solicitante']['credilink']['obito']
    flagErroCredilink = payload['flagErroCredilink']
    flagErroReceitaFederal = payload['flagErroReceitaFederal']
    marcacaoMesa = payload['marcacaoMesa']

    regrasNegativasTemp = payload['payloadHomol']['saidas']['regrasNegativas']
    passouDadosCadastrais = 0

    if marcacaoMesa != 'MESA':
        if flagErroCredilink == False and flagErroReceitaFederal == False:
            #Execução da função que retorna verdadeiro ou falso caso a variável seja de um formato inválido
            if verificaNulo(payload['solicitante']['credilink']['situacaoDocumento']) == True: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosDadosCadastrais','descricao': '8. Não possui a informação da situação do documento'})
                passouDadosCadastrais = 1
            if verificaNulo(payload['solicitante']['credilink']['obito']) == True: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosDadosCadastrais','descricao': '65. Não possui a informação de obito'})
                passouDadosCadastrais = 2
            if payload['payloadHomol']['saidas']['regrasNegativas'] != []: indVarEntradaNula = True
            else: indVarEntradaNula = False

            if indVarEntradaNula == False:
                if situacaoDocumento != 'regular': 
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_statusCPF','descricao': '20. CPF diferente de REGULAR na Receita Federal'})
                    passouDadosCadastrais = 3
                if obito == "SIM": 
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_Obito','descricao': '40. Obito = SIM'})
                    passouDadosCadastrais = 4
        else:
            payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
            regrasNegativasTemp.append({'nomeRegra': 'erroChamadaBureau','descricao': '77. Erro Chamada Bureaus - Validações Cadastrais'})
            passouDadosCadastrais = 5
        
    payload['payloadHomol']['saidas']['regrasNegativas'] = regrasNegativasTemp
    payload['payloadHomol']['intermediarias']['passouDadosCadastrais'] = passouDadosCadastrais
    
    return payload

## Serasa DCO e Etapa BACEN DCO
#serasa
def validaSerasa0IF (payload):
    rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpay']
    flagErroSerasa = payload['flagErroSerasa']
    marcacaoMesa = payload['marcacaoMesa']
    numeroDocumento = payload['solicitante']['numeroDocumento']
    #elegivelSmallLimit = payload['solicitante']['elegivelSmallLimit']
    qtdIF = payload['solicitante']['qtdIF']
    limitePoliticaConcessao = payload['limitePoliticaConcessao']
    limiteDisponivel = payload['solicitante']['limites']['limiteDisponivel']
    
    segmentacaoSubGrupo = payload['payloadHomol']['intermediarias']['segmentacao']
    segmentacaoPolitica = payload['payloadHomol']['intermediarias']['segmentacaoPolitica']
    
    passou0IF = 0
    passouRegrasSerasa = 0
    regrasNegativasTemp = payload['payloadHomol']['saidas']['regrasNegativas']

    payload['payloadHomol']['saidas']['flagSmallLimit'] = False # inicializando como false.

    # a principio recebe o valor da etapa anterior.
    payload['payloadHomol']['saidas']['vlrLimiteAprovado'] = limiteDisponivel
    payload['payloadHomol']['saidas']['limiteFinal'] = limitePoliticaConcessao

    if marcacaoMesa != 'MESA':
        if flagErroSerasa == False:
            #Execução da função que retorna verdadeiro ou falso caso a variável seja de um formato inválido
            if verificaNulo(payload['solicitante']['rendaLiquidaPicpay']): 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '60. Não possui a informação da renda picpay'})
                passouRegrasSerasa = 1
            if verificaNulo(payload['solicitante']['qtdIF']): 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '109. Não possui a informação de quantidade de instituições financeiras'})
                passouRegrasSerasa = 2

            if payload['solicitante']['listaRestritivosSerasa'] != [] :
                #Verifica se alguma das informações da lista da serasa não está vindo preenchida
                contNuloTipo = 0
                contNuloMontante = 0
                contNuloValor = 0
                for RestritivosSerasa in payload['solicitante']['listaRestritivosSerasa']:
                    indNuloTipo = verificaNulo(RestritivosSerasa['tipo'])
                    indNuloMontanteTotal = verificaNulo(RestritivosSerasa['montanteTotal'])
                    indNuloValorTotal = verificaNulo(RestritivosSerasa['valorTotal'])

                    if indNuloTipo == True:
                        contNuloTipo = contNuloTipo + 1 
                    if indNuloMontanteTotal == True: 
                        contNuloMontante = contNuloMontante + 1
                    if indNuloValorTotal == True: 
                        contNuloValor = contNuloValor + 1
            else:
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '64. Não possui a informação da lista restrição serasa'})
                passouRegrasSerasa = 4

            #Crava a recusa individualizada das variáveis da lista
            if contNuloTipo > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '61. Não possui a informação do tipo restrição serasa'})
                passouRegrasSerasa = 5

            if contNuloMontante > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '62. Não possui a informação do montante total restrição serasa'})
                passouRegrasSerasa = 6

            if contNuloValor > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosValidacoesInternas','descricao': '63. Não possui a informação do valor total restrições serasa'})
                passouRegrasSerasa = 7

            if payload['payloadHomol']['saidas']['regrasNegativas'] != []: indVarEntradaNula = True
            else: indVarEntradaNula = False

            if indVarEntradaNula == False:
                    somaRestritivos = 0

                    # Variáveis Small Limits
                    cpf6e67igito = int(numeroDocumento[5:7])    #int(numeroDocumento[6:8])

                    if cpf6e67igito >= 0 and cpf6e67igito <= 79: grupoSmall = "GRUPO 100"
                    elif cpf6e67igito >= 80 and cpf6e67igito <= 94: grupoSmall = "GRUPO 200"
                    else: grupoSmall = "SEM GRUPO"

                    if grupoSmall == "GRUPO 100": limiteSmall = 100
                    elif grupoSmall == "GRUPO 200": limiteSmall = 200
                    else: limiteSmall = round(max(min(0.3*rendaLiquidaPicpay, 1200), 300)/50)*50

                    if qtdIF == 0: flagSemIF = True
                    else: flagSemIF = False
                    
                    
                    
                    ## subgrupo small limits
                    if segmentacaoSubGrupo in ('VR_M_10', 'VR_I_10', 'VR_I_9', 'VR_09'):
                        limiteSmall = payload['solicitante']['limites']['limiteDisponivel']
                    elif SegmentacaoNegocio == "Jovem Cliente" or segmentacaoPolitica == "N3 – Jovem Cliente":
                        limiteSmall = payload['solicitante']['limites']['limiteDisponivel']

                    flag_motivo_41 = 0
                    flag_motivo_42 = 0
                    for listaRestritivosSerasa in payload['solicitante']['listaRestritivosSerasa']:
                        # A informação virá sumarizada na posição 0.
                        tipo = listaRestritivosSerasa['tipo'] 
                        montanteTotal = listaRestritivosSerasa['montanteTotal']
                        valorTotal = listaRestritivosSerasa['valorTotal']

                        if tipo == 'REFIN' or tipo == 'PEFIN' or tipo == 'PROTESTO' or tipo == 'DIVIDA_VENCIDA': 
                            somaRestritivos = somaRestritivos + valorTotal

                        if tipo == 'CHEQUE_SEM_FUNDO' and montanteTotal > 0: 
                            flag_motivo_41 = 1
                            passouRegrasSerasa = 8

                        if tipo == 'ACAO' and montanteTotal > 0: 
                            flag_motivo_42 = 1
                            passouRegrasSerasa = 9

                    if flag_motivo_41 == 1:
                        payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                        regrasNegativasTemp.append({'nomeRegra': 'rl_validaCCF','descricao': '41. Cliente possui restritivos CCF'})
                    if flag_motivo_42 == 1:
                        payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                        regrasNegativasTemp.append({'nomeRegra': 'rl_validaAcaoJudicial','descricao': '42. Cliente possui restritivos Ação Judicial'})

                    if rendaLiquidaPicpay >= 600 and somaRestritivos >= (rendaLiquidaPicpay)*0.2: # antes era 0.1
                        payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                        regrasNegativasTemp.append({'nomeRegra': 'rl_percent10Renda','descricao': '127. Cliente possui restritivos (Refin+Pefin+Protesto+DividaVencida) > = 20% Renda'})
                        passouRegrasSerasa = 10

                    if payload['solicitante']['rendaLiquidaPicpay'] < 600 :    
                        if somaRestritivos >= (600*0.2): # antes era 0.1
                            payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                            regrasNegativasTemp.append({'nomeRegra': 'rl_percent10SalarioMin','descricao': '128. Cliente possui restritivos (Refin+Pefin+Protesto+DividaVencida) > = 20% Salario Minimo'})
                            passouRegrasSerasa = 11


                    if flagSemIF == True:
                        if rendaLiquidaPicpay > 3442.37:
                            payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                            regrasNegativasTemp.append({'nomeRegra': 'rl_rendaRangeScore','descricao': '104. Renda > 3442.37'})
                            passou0IF = 1
                        if rendaLiquidaPicpay <= 3442.37:
                            payload['payloadHomol']['saidas']['limiteFinal'] = limiteSmall
                            payload['payloadHomol']['saidas']['vlrLimiteAprovado'] = limiteSmall
                            payload['payloadHomol']['saidas']['flagSmallLimit'] = True 
                            passou0IF = 2
                            # Não devemos aprovar casos de SL se o cliente tiver negativas.
                            if  regrasNegativasTemp != []:
                                payload['payloadHomol']['saidas']['limiteFinal'] = 0
                                payload['payloadHomol']['saidas']['vlrLimiteAprovado'] = 0
                                payload['payloadHomol']['saidas']['flagSmallLimit'] = False    
                                passou0IF = 3

        else:
            payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
            regrasNegativasTemp.append({'nomeRegra': 'erroChamadaBureau','descricao': '78. Erro Chamada Bureaus - Serasa'})
            passouRegrasSerasa = 12
    
    if payload['payloadHomol']['saidas']['statusDecisao'] == 'NEGADO':
        payload['payloadHomol']['saidas']['vlrLimiteAprovado'] = 0
        payload['payloadHomol']['saidas']['limiteFinal'] = 0
        
    payload['payloadHomol']['saidas']['regrasNegativas'] = regrasNegativasTemp
    payload['payloadHomol']['intermediarias']['passou0IF'] = passou0IF
    payload['payloadHomol']['intermediarias']['passouRegrasSerasa'] = passouRegrasSerasa

    return payload

#bacen       
# # BACEN SEM 0IF E SMALL LIMITS
# # BACEN SEM 0IF E SMALL LIMITS
def validaBacen(payload):

    flagErroBacen = payload['flagErroBacen']
    marcacaoMesa  = payload['marcacaoMesa']
    passouRegraBacen = 0

    # FLEXIBILIZAÇÃO NA REGRA DE RENDA DEPENDENDO DA ETAPA A SER CHAMADA
    if (payload['etapa'] == 'BACEN M-1'):
        percentualRendaGeral = 50
        percentualRendaEspecifico = 50
        rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpayBatch'] # Renda Batch para aplicação do bacen flexibilizado
    else:
        percentualRendaGeral = 20
        percentualRendaEspecifico = 10
        rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpay'] # Renda Picpay liquida online para aplicação do bacen padrão

    regrasNegativasTemp = payload['payloadHomol']['saidas']['regrasNegativas'] 

    if marcacaoMesa != 'MESA':
        if flagErroBacen == False:
            #Execução da função que retorna verdadeiro ou falso caso a variável seja de um formato inválido
            if verificaNulo(rendaLiquidaPicpay): 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '60. Não possui a informação da renda picpay'})
                passouRegraBacen = 1

            if payload['solicitante']['listaRestritivosBacen'] != []:
                contNuloSaldo = 0
                contNuloPrej = 0
                contNuloVlrCredAdpos = 0
                contNuloCredFinanc = 0
                contNuloCredVencido = 0
                contNuloCredAVencer = 0
                contNuloLimiteDisp = 0
                contNuloLimiteCheque = 0
                contNuloSaldoBacen = 0
                contNuloReneg = 0

                #Verifica se alguma das informações da lista da serasa não está vindo preenchida
                #for RestritivosBacen in payload['solicitante']['listaRestritivosBacen']:
                # consideramos apenas BACEN M-1
                indNuloSaldoVencido         = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['saldoVencido'])
                indNuloPrejuizo             = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['prejuizo'])
                indNuloVlrCredAVencerAdpos  = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['creditoAVencerAdpos'])
                indNuloCredFinancCartao     = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['creditoFinanciadoCartoes'])
                indNuloCredVencidoCartao    = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['creditoVencidoCartao'])
                indNuloCredAVencerCartao    = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['creditoAVencerCartao'])
                indNuloLimiteCartaoDisp     = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['limiteCartaoDisponivel'])
                indNuloLimiteChequeDisp     = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['limiteChequeDisponivel'])
                indNuloSaldoBacenCheque     = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['saldoBacenCheque'])
                indNuloRenegociacaoBacen    = verificaNulo(payload['solicitante']['listaRestritivosBacen'][0]['renegociacaoBacen'])

                if indNuloSaldoVencido == True:
                    contNuloSaldo = contNuloSaldo + 1
                if indNuloPrejuizo == True:
                    contNuloPrej = contNuloPrej + 1
                if indNuloVlrCredAVencerAdpos == True:
                    contNuloVlrCredAdpos = contNuloVlrCredAdpos + 1
                if indNuloCredFinancCartao == True:
                    contNuloCredFinanc = contNuloCredFinanc + 1
                if indNuloCredVencidoCartao == True:
                    contNuloCredVencido =  contNuloCredVencido + 1
                if indNuloCredAVencerCartao == True:
                    contNuloCredAVencer = contNuloCredAVencer + 1
                if indNuloLimiteCartaoDisp == True:
                    contNuloLimiteDisp = contNuloLimiteDisp + 1
                if indNuloLimiteChequeDisp == True:
                    contNuloLimiteCheque = contNuloLimiteCheque + 1
                if indNuloSaldoBacenCheque == True:
                    contNuloSaldoBacen = contNuloSaldoBacen + 1
                if indNuloRenegociacaoBacen == True:
                    contNuloReneg = contNuloReneg + 1

            #Crava a recusa individualizada das variáveis da lista
            if contNuloSaldo > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '66. Não possui a informação do saldo vencido'})
                passouRegraBacen = 2
            if contNuloPrej> 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '67. Não possui a informação do prejuizo'})
                passouRegraBacen = 3
            if contNuloVlrCredAdpos > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '68. Não possui a informação do credito a vencer adpos'})
                passouRegraBacen = 4
            if contNuloCredFinanc > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '69. Não possui a informação do credito financiado cartoes'})
                passouRegraBacen = 5
            if contNuloCredVencido > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '70. Não possui a informação do credito vencido cartao'})
                passouRegraBacen = 6
            if contNuloCredAVencer > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '71. Não possui a informação do credito a vencer cartao'})
                passouRegraBacen = 7
            if contNuloLimiteDisp > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '72. Não possui a informação do limite cartao disponivel'})
                passouRegraBacen = 8
            if contNuloLimiteCheque > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '73. Não possui a informação do limite cheque disponivel'})
                passouRegraBacen = 9
            if contNuloSaldoBacen > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '74. Não possui a informação do saldo bacen cheque'})
                passouRegraBacen = 10
            if contNuloReneg > 0: 
                payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                regrasNegativasTemp.append({'nomeRegra': 'fn_nulosBacen0IF','descricao': '75. Não possui a informação da renegociacao bacen'})
                passouRegraBacen = 11

            if len(payload['payloadHomol']['saidas']['regrasNegativas']) > 0: indVarEntradaNula = True
            else: indVarEntradaNula = False


            flag_motivo_45 = 0
            flag_motivo_46 = 0
            flag_motivo_47 = 0 
            flag_motivo_48 = 0
            flag_motivo_49 = 0
            flag_motivo_50 = 0

            if indVarEntradaNula == False:  
                if len(payload['solicitante']['listaRestritivosBacen']) > 0:
                    #for listaRestritivosBacen in payload['solicitante']['listaRestritivosBacen']:
                    # consideramos apenas BACEN M-1
                    creditoFinanciadoCartoes = payload['solicitante']['listaRestritivosBacen'][0]['creditoFinanciadoCartoes']
                    creditoVencidoCartao     = payload['solicitante']['listaRestritivosBacen'][0]['creditoVencidoCartao'] 
                    creditoAVencerCartao     = payload['solicitante']['listaRestritivosBacen'][0]['creditoAVencerCartao']
                    limiteCartaoDisponivel   = payload['solicitante']['listaRestritivosBacen'][0]['limiteCartaoDisponivel']
                    limiteChequeDisponivel   = payload['solicitante']['listaRestritivosBacen'][0]['limiteChequeDisponivel']
                    saldoBacenCheque         = payload['solicitante']['listaRestritivosBacen'][0]['saldoBacenCheque']
                    saldoVencido             = payload['solicitante']['listaRestritivosBacen'][0]['saldoVencido'] 
                    prejuizo                 = payload['solicitante']['listaRestritivosBacen'][0]['prejuizo']
                    creditoAVencerAdpos      = payload['solicitante']['listaRestritivosBacen'][0]['creditoAVencerAdpos']     
                    renegociacaoBacen        = payload['solicitante']['listaRestritivosBacen'][0]['renegociacaoBacen']

                    if limiteCartaoDisponivel != 0 or creditoAVencerCartao != 0 or creditoVencidoCartao != 0:
                        saldoFinanciadoCartoes = creditoFinanciadoCartoes/(limiteCartaoDisponivel+creditoAVencerCartao+creditoVencidoCartao)
                    else: 
                        saldoFinanciadoCartoes = 0           
                    if limiteChequeDisponivel != 0 or saldoBacenCheque != 0: 
                        usoChequeEspecial = saldoBacenCheque/(limiteChequeDisponivel+saldoBacenCheque)
                    else:
                        usoChequeEspecial = 0
                    if saldoVencido >= (rendaLiquidaPicpay*(percentualRendaGeral/100)): 
                        flag_motivo_45 = 1
                        passouRegraBacen = 12                              
                    if prejuizo >= (rendaLiquidaPicpay*(percentualRendaGeral/100)): 
                        flag_motivo_46 = 1
                        passouRegraBacen = 13
                                        
                    if creditoAVencerAdpos >= (rendaLiquidaPicpay*(percentualRendaEspecifico/100)): 
                        flag_motivo_47 = 1
                        passouRegraBacen = 14
                    if saldoFinanciadoCartoes > 0.7 and (creditoAVencerCartao+creditoVencidoCartao) > 100: 
                        flag_motivo_48 = 1
                        passouRegraBacen = 15
                    if usoChequeEspecial > 0.9 and saldoBacenCheque > 100: 
                        flag_motivo_49 = 1
                        passouRegraBacen = 16
                    if renegociacaoBacen >= (rendaLiquidaPicpay*(percentualRendaEspecifico/100)): 
                        flag_motivo_50 = 1
                        passouRegraBacen = 17

                ## flag_motivo_45 e 46 foram flexibilizadas em 20%
                if flag_motivo_45 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'             	
                    if (payload['etapa'] == 'BACEN M-1'):
                        regrasNegativasTemp.append({'nomeRegra': 'rl_saldosVencidos','descricao': '45. Cliente com somatório de saldos vencidos (atraso > 15 dias) > = ' + str(percentualRendaGeral) + ' porcento da Renda Picpay'})
                    else:
                        regrasNegativasTemp.append({'nomeRegra': 'rl_saldosVencidos','descricao': '125. Cliente com somatorio de saldos vencidos (atraso > 15 dias) > = ' + str(percentualRendaGeral) + ' porcento da Renda Picpay'})
                if flag_motivo_46 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    if (payload['etapa'] == 'BACEN M-1'):
                        regrasNegativasTemp.append({'nomeRegra': 'rl_prejuizo','descricao': '46. Cliente com saldo em prejuízo >= ' + str(percentualRendaGeral) + ' porcento da Renda Picpay'})                   
                    else:
                        regrasNegativasTemp.append({'nomeRegra': 'rl_prejuizo','descricao': '126. Cliente com saldo em prejuizo >= ' + str(percentualRendaGeral) + ' porcento da Renda Picpay'})                   
                if flag_motivo_47 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_somatoriaADPOS','descricao': '47. Cliente com somatórios dos valores ADPOS ativo >= ' + str(percentualRendaEspecifico) + ' porcento da Renda Picpay'})
                if flag_motivo_48 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_saldoFinanciadoCartoes','descricao': '48. Cliente com saldo financiado de cartões maior que 70%'})     
                if flag_motivo_49 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_chequeEspecial','descricao': '49. Cliente com uso do cheque especial maior que 90%'})
                if flag_motivo_50 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_renegociacao','descricao': '50. Cliente com com renegociações >= ' + str(percentualRendaEspecifico) + ' porcento da Renda Picpay'})

        else:
            payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
            regrasNegativasTemp.append({'nomeRegra': 'erroChamadaBureau','descricao': '79. Erro Chamada Bureaus - Bacen'})
            passouRegraBacen = 18

    payload['payloadHomol']['saidas']['regrasNegativas'] = regrasNegativasTemp
    payload['payloadHomol']['intermediarias']['passouRegraBacen'] = passouRegraBacen    

    return payload

##############################################################################
#################### ETAPAS DE EXECUÇÃO ######################################
##############################################################################
def etapaHardFilter(payload):
    if payload['etapa'] == "":

        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            HardFilters(payload)

        #DETERMINA PRÓXIMA ETAPA
        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            payload['payloadHomol']['saidas']['etapa'] = "FILTROS POLITICA"
        else:
            payload['payloadHomol']['saidas']['etapa'] = "FIM"

    if payload['etapa'] == "BACEN M-2, M-3":

        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            HardFilters(payload)

        #DETERMINA PRÓXIMA ETAPA
        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            payload['payloadHomol']['saidas']['etapa'] = "POLITICA CONCESSAO"
        else:
            payload['payloadHomol']['saidas']['etapa'] = "FIM"

    addListaLogs(payload) 
    return payload

def etapaFiltrosPolitica(payload):
    if payload['etapa'] == "FILTROS POLITICA":

        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            if payload['flagErroAurora'] == False and payload['flagErroVisaoCliente'] == False:
                validacoesInternas(payload)
            else:
                payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
                payload['payloadHomol']['saidas']['regrasNegativas'].append({'nomeRegra': 'erroChamadaBureau','descricao': '76. Erro Chamada Bureaus - Validações Internas'})                
        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            CriacaoFlagsRangesFiltros(payload)  #calculos intermediarios (etapa filtro politica) no RMA
            FiltroVisaoPolitica(payload)

        DefineFlagRenda(payload)

        #DETERMINA PRÓXIMA ETAPA
        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            payload['payloadHomol']['saidas']['etapa'] = "BACEN M-1"
        else:
            payload['payloadHomol']['saidas']['etapa'] = "FIM"

    addListaLogs(payload) 
    return payload

def etapaBacenFlex(payload):
    if payload['etapa'] == "BACEN M-1":
        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            if payload['flagErroBacen'] == False:
                validaBacen(payload)
            else:
                payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
                payload['payloadHomol']['saidas']['regrasNegativas'].append({'nomeRegra': 'erroChamadaBureau','descricao': '77. Erro Chamada Bureaus - Bacen'})
        #DETERMINA PRÓXIMA ETAPA
        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            payload['payloadHomol']['saidas']['etapa'] = "BACEN M-2, M-3"
        else:
            payload['payloadHomol']['saidas']['etapa'] = "FIM"

    addListaLogs(payload) 
    return payload

def etapapoliticaConcessao(payload):
    if payload['etapa'] ==  "POLITICA CONCESSAO":
        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            CriacaoFlagsRangesFiltros(payload)
            SegmentacaoNegocio(payload)
            # SegmentacaoRisco(payload)       // Nao utilizado na nova versão  
            FaixaRiscoInternoAgrupada(payload)

            if (payload['payloadHomol']['intermediarias']['flagClienteExistente'] == 1):
                SegmentacaoPoliticaAntigos(payload)
                # DefinicaoGruposAntigo(payload)   // Nao utilizado na nova versão  
                subGruposAntigo(payload)
            else:
                SegmentacaoPoliticaMarAberto(payload)
                # DefinicaoGruposMarAberto(payload)
                SubGruposMarAberto(payload)

            MarcacoesAprovados(payload)
            LimiteFinal(payload)

        # RECUSA CASO O CLIENTE NÃO ENQUADROU EM NENHUMA CASELA DA TABELA OU FICOU RECUSADO PELAS FLAGS.
        if (payload['payloadHomol']['intermediarias']['flagAprovadoBAU'] == 0 and payload['payloadHomol']['intermediarias']['flagAprovadoSL'] == 0):
            payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
            payload['payloadHomol']['saidas']['limiteFinal'] = 0
            payload['payloadHomol']['saidas']['regrasNegativas'].append({'nomeRegra': 'fn_verificaDecisaoPoliticaConcessao','descricao': '120. Não há aprovação para grupo BAU ou Small Limits'})

        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            if payload['flagErroBacen'] == False:
                validaBacen(payload)
            else:
                payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
                payload['payloadHomol']['saidas']['regrasNegativas'].append({'nomeRegra': 'erroChamadaBureau','descricao': '77. Erro Chamada Bureaus - Bacen'})
        # Caso for recusado na etapa de validação do Bacen.
        if (payload['payloadHomol']['saidas']['regrasNegativas'] != [] or payload['payloadHomol']['saidas']['statusDecisao'] == 'NEGADO'):
            payload['payloadHomol']['saidas']['limiteFinal'] = 0

        #DETERMINA PRÓXIMA ETAPA
        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            payload['payloadHomol']['saidas']['etapa'] = "BUREAU CADASTRAL"
        else:
            payload['payloadHomol']['saidas']['etapa'] = "FIM"

    addListaLogs(payload) 
    return payload

def etapaBureauCadastral(payload):
    if payload['etapa'] ==  "BUREAU CADASTRAL":

        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            if payload['flagErroCredilink'] == False and payload['flagErroReceitaFederal'] == False:
                validaDadosCadastrais(payload)
            else:
                payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
                payload['payloadHomol']['saidas']['regrasNegativas'].append({'nomeRegra': 'erroChamadaBureau','descricao': '77. Erro Chamada Bureaus - Validações Cadastrais'})               
        #DETERMINA PRÓXIMA ETAPA
        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            payload['payloadHomol']['saidas']['etapa'] = "SERASA"
        else:
            payload['payloadHomol']['saidas']['etapa'] = "FIM"

    addListaLogs(payload) 
    return payload

def etapaSerasa(payload):
    if payload['etapa'] ==  "SERASA":

        if (payload['payloadHomol']['saidas']['statusDecisao'] == 'APROVADO'):
            if payload['flagErroSerasa'] == False:
                validaSerasa0IF(payload)
            else:
                payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
                payload['payloadHomol']['saidas']['flagSmallLimit'] = False
                payload['payloadHomol']['saidas']['regrasNegativas'].append({'nomeRegra': 'erroChamadaBureau','descricao': '78. Erro Chamada Bureaus - Serasa'})

        payload['payloadHomol']['saidas']['etapa'] = "FIM"

    addListaLogs(payload) 
    return payload



def addListaLogs(payload):
    intermediarias = payload['payloadHomol']['intermediarias']
    listaLogs = []

    DictIntermediarias = {
        "aprovadoBAU": intermediarias.get("aprovadoBAUAux", None),
        "aprovadoSmallLimits": intermediarias.get("aprovadoSLAux", None),
        "clienteExistente": intermediarias.get("flagClienteExistente", None),
        "dataAdmissao": intermediarias.get("dataAdmissao", None),
        "dataAdmissaoMeses": intermediarias.get("admissaoMeses", None),
        "clienteMAT": intermediarias.get("clienteMAT", None),
        "fonteRenda": intermediarias.get("flagTipoRenda", None),
        "rendaInexistente": intermediarias.get("rendaInexistente", None),
        "clienteNovo": intermediarias.get("flagClienteNovo", None),
        "naoCliente": intermediarias.get("flagNaoCliente", None),
        "scoreInexistente": intermediarias.get("semInfoScore", None),
        "principalidadeInvestimento": intermediarias.get("flagPrincipalidade", None),
        "principalidadeAll": intermediarias.get("principalidadeAll", None),
        "principalidadeAllRecorrencia": intermediarias.get("recorrenciaPrincipalidade", None),
        "clienteBlindado": intermediarias.get("clienteBlindado", None),
        "clienteBloqueado": intermediarias.get("clienteBloqueado", None),
        "faixaRendaLiquida": intermediarias.get("faixaRendaLiquida", None),
        "faixaRendaBruta": intermediarias.get("faixaRendaBruta", None),
        "faixaScoreAppcardBlend": intermediarias.get("faixaScoreAppcardBlend", None),
        "faixaScoreExterno": intermediarias.get("faixaScoreExterno", None),
        "segmentacaoNegocio": intermediarias.get("segmentacaoNegocio", None),
        "segmentacaoPolitica": intermediarias.get("segmentacaoPolitica", None),
        "rangeScoreInterno": intermediarias.get("faixaRiscoInternoAgrupada", None),
        "segmentacaoSubgrupo": intermediarias.get("segmentacao", None),
        "aprovadoBAUAux": intermediarias.get("aprovadoBAUAux", None),
        "aprovadoSmallLimitsAux": intermediarias.get("aprovadoSLAux", None),
        "limiteFixo": intermediarias.get("limiteFixo", None),
        "alavancagem": intermediarias.get("alavancagem", None),
        "limiteTeto": intermediarias.get("limiteTeto", None),
        "limitePiso": intermediarias.get("limitePiso", None)
    }

    # for key, value in DictIntermediarias.items():
    #     if value is not None:
    #         ListaLogs.append({"nome": key, "valor": value})
    
            
    varTemp = [listaLogs.append({"nome": key, "valor": value}) for key, value in DictIntermediarias.items() if value is not None]

    payload['payloadHomol']['saidas']['listaLogs'] = listaLogs

    return payload



##############################################################################
######################## DECISION FLOW #######################################
##############################################################################
def decisionFlow(payload):
    etapa = payload['etapa']

    #iniciamos o payloadHomol e suas subclasses.
    payload['payloadHomol'] = {
        "intermediarias":{},
        "saidas":{"regrasNegativas":[], "listaLogs":[]}
    } 

    # VALORES INICIAIS
    payload['payloadHomol']['saidas']['statusDecisao'] = 'APROVADO'
    payload['payloadHomol']['saidas']['vlrLimiteAprovado'] = payload['solicitante']['limites']['limiteDisponivel']
    payload['payloadHomol']['saidas']['flagSmallLimit'] = False

    # HARD FILTER RENDA FLEXIBILIZADA
    etapaHardFilter(payload)

    payload['etapa'] = payload['payloadHomol']['saidas']['etapa'] 

    # FILTROS POLITICA
    etapaFiltrosPolitica(payload)

    payload['etapa'] = payload['payloadHomol']['saidas']['etapa'] 

    # BACEN DCO - FLEXIBILIZADO EM 50%
    etapaBacenFlex(payload)

    payload['etapa'] = payload['payloadHomol']['saidas']['etapa'] 

    # HARD FILTER ( SEM FLEXIBILIZAÇÃO DA RENDA )
    etapaHardFilter(payload)

    payload['etapa'] = payload['payloadHomol']['saidas']['etapa'] 

    # BACEN DCO - SEM FLEXIBILIZAÇÃO E SEM 0IF
    etapapoliticaConcessao(payload)

    payload['etapa'] = payload['payloadHomol']['saidas']['etapa'] 

    # ETAPA BUREAU CADASTRAL DCO
    etapaBureauCadastral(payload)



    # Chama a funcao que adiciona as variaveis intermediarias na listaLogs
    addListaLogs(payload)  #avaliar se vamos manter a listaLogs
    
    if etapa not in ['FIM','FILTROS POLITICA','BACEN M-1','SERASA','POLITICA CONCESSAO','BUREAU CADASTRAL']:
        payload['payloadHomol']['saidas']['etapa'] = 'FIM'
        payload['payloadHomol']['saidas']['regrasNegativas'].append({'nomeRegra': 'etapaInvalida','descricao': '52. Etapa Enviada Inválida'})
        payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
        payload['payloadHomol']['saidas']['vlrLimiteAprovado'] = 0
        payload['payloadHomol']['saidas']['limiteFinal'] = 0 




    return payload
