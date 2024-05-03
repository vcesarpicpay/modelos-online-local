//Variáveis Entrada
numeroDocumento is a string initially variaveis.termSetInput.solicitante.numeroDocumento;
dataAdmissaoFopag is a string initially variaveis.termSetInput.solicitante.dataAdmissaoFopag;
dataRegistroPrimeiroContrato is a string initially variaveis.termSetInput.solicitante.dataRegistroPrimeiroContrato;
dataRegistro is a string initially variaveis.termSetInput.solicitante.dataRegistro;
flagMAT30d is an integer initially variaveis.termSetInput.solicitante.flagMAT30d;
flagMAT60d is an integer initially variaveis.termSetInput.solicitante.flagMAT60d;
flagMAT90d is an integer initially variaveis.termSetInput.solicitante.flagMAT90d;
fonteRenda is a string initially variaveis.termSetInput.solicitante.fonteRenda;
flagPortabilidade is an integer initially variaveis.termSetInput.solicitante.flagPortabilidade;
rendaLiquidaPicpay is a real initially variaveis.termSetInput.solicitante.rendaLiquidaPicpay;
rendaBrutaPicpay is a real initially variaveis.termSetInput.solicitante.rendaBrutaPicpay;
consumerID is a integer initially variaveis.termSetInput.solicitante.consumerID;
//scoreAppSerasa is an integer initially variaveis.termSetInput.solicitante.scoreAppSerasa;
//scoreInternoLongo is an integer initially variaveis.termSetInput.solicitante.scoreInternoLongo;
//scoreInterno is an integer initially variaveis.termSetInput.solicitante.scoreInterno;
//scoreExterno is an integer initially variaveis.termSetInput.solicitante.scoreExterno;
investimentoTotal is a real initially variaveis.termSetInput.solicitante.investimentoTotal;
flagPrincipalidade is an integer initially variaveis.termSetInput.solicitante.flagPrincipalidade;
flagInvestimentoPrincipalidade is an integer initially variaveis.termSetInput.solicitante.flagInvestimentoPrincipalidade;
mesFlagPrincipalidade is an integer initially variaveis.termSetInput.solicitante.mesFlagPrincipalidade; 
flagAllowlist is an string initially variaveis.termSetInput.solicitante.flagAllowlist;
flagBlocklist is an string initially variaveis.termSetInput.solicitante.flagBlocklist;
scoreAppcardsBlendInternoOnline is an integer initially variaveis.termSetInput.solicitante.scoreAppcardsBlendInternoOnline;
scoreAppcardsBlendExternoOnline  is an integer initially variaveis.termSetInput.solicitante.scoreAppcardsBlendExternoOnline;
scoreBlendIntExt  is an integer initially variaveis.termSetInput.solicitante.scoreBlendIntExt;
scoreSerasaAl  is an integer initially variaveis.termSetInput.solicitante.scoreSerasaAl;
ghAppcardsBlendInternoOnline is an string initially variaveis.termSetInput.solicitante.ghAppcardsBlendInternoOnline;
ghAppcardsBlendExternoOnline is an string initially variaveis.termSetInput.solicitante.ghAppcardsBlendExternoOnline;
ghBlendIntExt is an string initially variaveis.termSetInput.solicitante.ghBlendIntExt;
ghSerasa  is an string initially variaveis.termSetInput.solicitante.ghSerasa;
flagMAT35d is an integer initially variaveis.termSetInput.solicitante.flagMAT35d;
ghFinal is a string initially variaveis.termSetInput.saida.ghFinal;
scoreFinal is an integer initially variaveis.termSetInput.saida.scoreFinal;
origem is a string initially variaveis.termSetInput.saida.origemModeloApplication;
blendFinalAl is a integer initially variaveis.termSetInput.solicitante.blendFinalAl;
ghAurora is a string initially variaveis.termSetInput.solicitante.ghFinalScr;
origemBlend is a string initially variaveis.termSetInput.solicitante.origemBlend;




// --------------------------------------- Criacao de GH e Score Final --------------------------------------

//Priorização de GH
//Se retornos = -99999 ou "-99999" significa que não estamos consultando a Raven para esses casos, então não entrará na regra de priorização 
if (scoreAppcardsBlendInternoOnline = -99999 or scoreAppcardsBlendExternoOnline = -99999 or ghAppcardsBlendInternoOnline = "-99999" or ghAppcardsBlendExternoOnline = "-99999") then {
   scoreFinal = blendFinalAl;
   ghFinal = ghAurora;
   origem = origemBlend;
}

// mudança - troquei os or para and nos ifs 
else if (ghAppcardsBlendInternoOnline <> "missing" and ghAppcardsBlendInternoOnline <> "0") then ghFinal = ghAppcardsBlendInternoOnline;
else if (ghBlendIntExt <> "missing" and ghBlendIntExt <> "0" and ghBlendIntExt <> "-99999")  then ghFinal = ghBlendIntExt;
else if (ghAppcardsBlendExternoOnline <> "missing" and ghAppcardsBlendExternoOnline <> "0") then ghFinal = ghAppcardsBlendExternoOnline;
else if (ghSerasa <> "missing" and ghSerasa <> "0" and ghSerasa <> "-99999") then ghFinal = ghSerasa;
else ghFinal = "missing";

variaveis.ghFinal = ghFinal;
variaveis.scoreFinal = scoreFinal;
variaveis.origemFinal = origem;

//Priorização de score - 
//MUDANCA DE OR PARA AND  no if da linha 63 e mudanca de ifs para else ifs da linha 67 pra baixo.
// Mudanca do or da linha 69 para and e da linha 75 para and tambem
if (scoreAppcardsBlendInternoOnline <> -99999 and scoreAppcardsBlendExternoOnline <> -99999 and ghAppcardsBlendInternoOnline <> "-99999" and ghAppcardsBlendExternoOnline <> "-99999") then {
   if scoreAppcardsBlendInternoOnline <> -1 then {
      scoreFinal = scoreAppcardsBlendInternoOnline;
      origem = "Interno + Serasa + SCR"};  
   else if (scoreBlendIntExt <> -1 and scoreBlendIntExt <> -99999) then {
      scoreFinal = scoreBlendIntExt;
      origem = "Interno + Serasa"};
   else if scoreAppcardsBlendExternoOnline <> -1 then {
      scoreFinal = scoreAppcardsBlendExternoOnline;
      origem = "Serasa + SCR"};
   else if (scoreSerasaAl <> -1 and scoreSerasaAl <> -99999) then {
      scoreFinal = scoreSerasaAl ;
      origem = "Serasa"};
   else {
			scoreFinal = -1;
			origem = "missing"; 
   }
}

variaveis.scoreFinal = scoreFinal;
variaveis.origemFinal = origem;

//Declarando as variáveis finais escolhidas na priorização nas saídas
variaveis.termSetInput.saida.ghFinal = variaveis.ghFinal;
variaveis.termSetInput.saida.scoreFinal = variaveis.scoreFinal;
variaveis.termSetInput.saida.origemModeloApplication = variaveis.origemFinal;

//Faixa de Score Appcard Blend
if (ghFinal = "missing" or ghFinal = "0") then variaveis.faixaScoreAppcardBlend= "Sem info";
else if ghFinal = "K"  then variaveis.faixaScoreAppcardBlend = "R12";
else if ghFinal = "J"  then variaveis.faixaScoreAppcardBlend = "R11";
else if ghFinal = "I"  then variaveis.faixaScoreAppcardBlend = "R10";
else if ghFinal = "H"  then variaveis.faixaScoreAppcardBlend = "R9";
else if ghFinal = "G"  then variaveis.faixaScoreAppcardBlend = "R8";
else if ghFinal = "F"  then variaveis.faixaScoreAppcardBlend = "R7";
else if ghFinal = "E"  then variaveis.faixaScoreAppcardBlend = "R6";
else if ghFinal = "D"  then variaveis.faixaScoreAppcardBlend = "R5";
else if ghFinal = "C"  then variaveis.faixaScoreAppcardBlend = "R4";
else if ghFinal = "B"  then variaveis.faixaScoreAppcardBlend = "R3";
else if ghFinal = "A"  then variaveis.faixaScoreAppcardBlend = "R2";
else if ghFinal = "AA" then variaveis.faixaScoreAppcardBlend =  "R1";
else variaveis.faixaScoreAppcardBlend = "verificar";

//Faixa Score Externo
if (ghFinal = "missing" or ghFinal = "0") then variaveis.faixaScoreExterno = "Sem info";
else if ghFinal = ("I" or "J" or "K") then variaveis.faixaScoreExterno = "R10";
else if ghFinal = "H" then variaveis.faixaScoreExterno = "R9";
else if ghFinal = "G" then variaveis.faixaScoreExterno = "R8";
else if ghFinal = "F" then variaveis.faixaScoreExterno = "R7";
else if ghFinal = "E" then variaveis.faixaScoreExterno = "R6";
else if ghFinal = "D" then variaveis.faixaScoreExterno = "R5";
else if ghFinal = "C" then variaveis.faixaScoreExterno = "R4";
else if ghFinal = "B" then variaveis.faixaScoreExterno = "R3";
else if ghFinal = "A" then variaveis.faixaScoreExterno = "R2";
else if ghFinal = "AA" then variaveis.faixaScoreExterno = "R1";
else variaveis.faixaScoreExterno = "verificar";




//saldoInvestimento sempre será 0, não há previsão de preenchimento por enquanto 
saldoInvestimento is a real initially 0;

//Digito CPF Concomitante CP
//variaveis.cpf6e7Digito = portable().toInteger(portable().subString(numeroDocumento,6,7));

//RDG Grupo Small Limit
variaveis.cpf6e7Digito = portable().toInteger(portable().subString(numeroDocumento,6,7));

//Data de admissão
if dataAdmissaoFopag <> "1001-01-01" then variaveis.dataAdmissao = dataAdmissaoFopag
else if dataRegistroPrimeiroContrato <> "1001-01-01" then variaveis.dataAdmissao = dataRegistroPrimeiroContrato
else if dataRegistro <> "1001-01-01" then variaveis.dataAdmissao = dataRegistro
else variaveis.dataAdmissao = "1001-01-01";

//Data de admissão em meses
if variaveis.dataAdmissao <> "1001-01-01" then {
   variaveis.dataAdmissaoMeses = fn_diferencaMesesEntreDatas(portable().dateToInt(portable().date(),portable().yyyymmdd()), fn_DataStrToInt(portable().subString(variaveis.dataAdmissao,1,10)));
} else {
   variaveis.dataAdmissaoMeses = 0;
}

//Cliente Existente 
if (dataRegistro <> "1001-01-01" and (fn_TempoEntreDatasDias(fn_DataStrToInt(portable().subString(dataRegistro,1,10)), portable().dateToInt(portable().date(),portable().yyyymmdd())) ) > 60) then variaveis.clienteExistente = 1;
else variaveis.clienteExistente = 0;

//Cliente MAT
//if flagMAT30d = 1 then variaveis.clienteMAT = 1;
//else variaveis.clienteMAT = 0;

//Cliente MAT
if flagMAT35d = 1 then variaveis.clienteMAT = 1;
else variaveis.clienteMAT = 0;

//Cliente MAT com recorrência
if (flagMAT30d = 1 and flagMAT60d = 1 and flagMAT90d = 1) then variaveis.clienteMATRecorrencia = 1;
else variaveis.clienteMATRecorrencia = 0;

//Fonte de Renda --------------> Confirmat se o arnaldo já fez o ajuste para não termos mais essa variável, apenas a "clienteFopag"

//Cliente FOPAG
if (fonteRenda = "01. Fopag" and flagPortabilidade = 0) then variaveis.clienteFopag = 1; 
else variaveis.clienteFopag = 0;

//Renda inexistente
if rendaLiquidaPicpay < 0 then variaveis.rendaInexistente = 1;
else variaveis.rendaInexistente = 0; 

// Cliente Novo
if (dataRegistro <> "1001-01-01" and (fn_TempoEntreDatasDias(fn_DataStrToInt(portable().subString(dataRegistro,1,10)), portable().dateToInt(portable().date(),portable().yyyymmdd())) ) <= 60)  then variaveis.clienteNovo = 1;
else variaveis.clienteNovo = 0;

//Não Cliente (Mar Aberto)
if (dataRegistro = "1001-01-01" and consumerID = -99999) then variaveis.naoCliente = 1;
else variaveis.naoCliente = 0;

//Score Serasa Inválido
//if scoreAppSerasa <= 454 then variaveis.scoreSerasaInvalido = 1;
//else variaveis.scoreSerasaInvalido = 0;

//Sem Informação de Score
if (variaveis.clienteExistente = 1 and variaveis.scoreFinal <= -1) then variaveis.scoreInexistente = 1;
else if ((variaveis.naoCliente = 1 or variaveis.clienteNovo = 1) and variaveis.scoreFinal <= -1) then variaveis.scoreInexistente = 1;
else variaveis.scoreInexistente = 0;

//Valor Total Investido
if (investimentoTotal <> -99999 and saldoInvestimento <> -99999) then variaveis.vlrTotalInvestido = investimentoTotal + saldoInvestimento;
else if (investimentoTotal <> -99999 and saldoInvestimento = -99999) then variaveis.vlrTotalInvestido = investimentoTotal;
else if (investimentoTotal = -99999 and saldoInvestimento <> -99999) then variaveis.vlrTotalInvestido = saldoInvestimento;
else variaveis.vlrTotalInvestido = 0;

//Percentual da Renda em Investimento
if (rendaLiquidaPicpay = 0 or rendaLiquidaPicpay = -99999) then variaveis.percentRendaInvestimento = 0.0;
else variaveis.percentRendaInvestimento = variaveis.vlrTotalInvestido/rendaLiquidaPicpay;

//Investidor Original
if (variaveis.vlrTotalInvestido > (rendaLiquidaPicpay*3)) then variaveis.investidorOriginal = 1;
else variaveis.investidorOriginal = 0;

//Principalidade de Investimento
if (variaveis.investidorOriginal = 1 or flagInvestimentoPrincipalidade >= 1) then variaveis.principalidadeInvestimento = 1
else variaveis.principalidadeInvestimento = 0;

//Principalidade em todos os Critérios
if (flagPrincipalidade = 1 and mesFlagPrincipalidade >= 1) then variaveis.principalidadeAll = 1;
else variaveis.principalidadeAll = 0;

//Principalidade em todos os Critérios com Recorrência
if (flagPrincipalidade = 1 and mesFlagPrincipalidade >= 3) then variaveis.principalidadeAllRecorrencia = 1;
else variaveis.principalidadeAllRecorrencia = 0;

//Cliente blindado
if (portable().findString(portable().toUpperCase(flagAllowlist), "CARTAO PICPAY CONCESSAO") > 0 or portable().findString(portable().toUpperCase(flagAllowlist), "OVERALL") > 0) then variaveis.clienteBlindado = 1;
else variaveis.clienteBlindado = 0;

//Cliente Bloqueado
if (portable().findString(portable().toUpperCase(flagBlocklist), "CARTAO PICPAY CONCESSAO") > 0 or portable().findString(portable().toUpperCase(flagBlocklist), "OVERALL") > 0) then variaveis.clienteBloqueado = 1;
else variaveis.clienteBloqueado = 0;

//Faixa de Renda Líquida
if rendaLiquidaPicpay <= 0 then variaveis.faixaRendaLiquida = "a. Sem renda";
else if rendaLiquidaPicpay < 4000 then variaveis.faixaRendaLiquida = "b. < 4k";
else if rendaLiquidaPicpay < 10000 then variaveis.faixaRendaLiquida = "c. 4k - 10k";
else if rendaLiquidaPicpay >= 10000 then variaveis.faixaRendaLiquida = "d. >= 10k";
else variaveis.faixaRendaLiquida = "z. Verificar";

//Faixa de Renda Bruta
if rendaLiquidaPicpay <= 0 then variaveis.faixaRendaBruta = "a. Sem renda";
else if rendaLiquidaPicpay < 3442.37 then variaveis.faixaRendaBruta = "a. < 4k";
else if rendaLiquidaPicpay < 7499.16 then variaveis.faixaRendaBruta = "b. 4k - 10k";
else if rendaLiquidaPicpay < 11124.16 then variaveis.faixaRendaBruta = "c. 10k - 15k";
else if rendaLiquidaPicpay >= 11124.16 then variaveis.faixaRendaBruta = "d. >= 15k";
else variaveis.faixaRendaBruta = "z. Verificar " rendaLiquidaPicpay;

