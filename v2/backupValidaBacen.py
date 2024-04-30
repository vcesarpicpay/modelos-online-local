def validaBacen(payload):

    flagErroBacen = payload['flagErroBacen']
    marcacaoMesa  = payload['marcacaoMesa']
    passouRegraBacen = 0

    # FLEXIBILIZAÇÃO NA REGRA DE RENDA DEPENDENDO DA ETAPA A SER CHAMADA
    if (payload['etapa'] == 'BACEN M-1'):
        percentualRenda = 50
        rendaLiquidaPicpay = payload['solicitante']['rendaLiquidaPicpayBatch'] # Renda Batch para aplicação do bacen flexibilizado
    else:
        percentualRenda = 20
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
                    if saldoVencido >= (rendaLiquidaPicpay*(percentualRenda/100)): 
                        flag_motivo_45 = 1
                        passouRegraBacen = 12                              
                    if prejuizo >= (rendaLiquidaPicpay*(percentualRenda/100)): 
                        flag_motivo_46 = 1
                        passouRegraBacen = 13
                        
                    ### 50% na etapa "BACEN M-1" e continua usando 10% nas etapas posteriores (utilizando renda recalculada raven).
                    # REPETE if do começo da func para mudar percentualRenda de 20 para 10 caso etapa diferente de BACEN M-1
                    if (payload['etapa'] == 'BACEN M-1'):
                        percentualRenda = 50
                    else:
                        percentualRenda = 10
                                   
                    if creditoAVencerAdpos >= (rendaLiquidaPicpay*(percentualRenda/100)): 
                        flag_motivo_47 = 1
                        passouRegraBacen = 14
                    if saldoFinanciadoCartoes > 0.7 and (creditoAVencerCartao+creditoVencidoCartao) > 100: 
                        flag_motivo_48 = 1
                        passouRegraBacen = 15
                    if usoChequeEspecial > 0.9 and saldoBacenCheque > 100: 
                        flag_motivo_49 = 1
                        passouRegraBacen = 16
                    if renegociacaoBacen >= (rendaLiquidaPicpay*(percentualRenda/100)): 
                        flag_motivo_50 = 1
                        passouRegraBacen = 17


                #testar
                ## flag_motivo_45 e 46 foram flexibilizadas em 20%
                if flag_motivo_45 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_saldosVencidos','descricao': '125. Cliente com somatório de saldos vencidos (atraso > 15 dias) > = ' +     str(percentualRenda) + ' porcento da Renda Picpay'})
                if flag_motivo_46 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_prejuizo','descricao': '126. Cliente com saldo em prejuízo >= ' + str(percentualRenda) + ' porcento da Renda Picpay'})                   
                if flag_motivo_47 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_somatoriaADPOS','descricao': '47. Cliente com somatórios dos valores ADPOS ativo >= ' + str    (percentualRenda) + ' porcento da Renda Picpay'})
                if flag_motivo_48 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_saldoFinanciadoCartoes','descricao': '48. Cliente com saldo financiado de cartões maior que 70%'})     
                if flag_motivo_49 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_chequeEspecial','descricao': '49. Cliente com uso do cheque especial maior que 90%'})
                if flag_motivo_50 == 1:
                    payload['payloadHomol']['saidas']['statusDecisao'] = 'NEGADO'
                    regrasNegativasTemp.append({'nomeRegra': 'rl_renegociacao','descricao': '50. Cliente com com renegociações >= ' + str(percentualRenda) + ' porcento da Renda Picpay'})

        else:
            payload['payloadHomol']['saidas']['statusDecisao'] = 'PENDENTE'
            regrasNegativasTemp.append({'nomeRegra': 'erroChamadaBureau','descricao': '79. Erro Chamada Bureaus - Bacen'})
            passouRegraBacen = 18

    payload['payloadHomol']['saidas']['regrasNegativas'] = regrasNegativasTemp
    payload['payloadHomol']['intermediarias']['passouRegraBacen'] = passouRegraBacen    

    return payload