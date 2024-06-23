########################################
# SCRIPT JESSICA ALMEIDA - TESE DE DOUTORADO 23/06/2024
# CRIANDO SCRIPT PARA LIMPEZA GERAL DE BASE DE DADOS 
####################################
# O SCRIPT FOI ELABORADO COM APOIO DE ALGUMAS FUNCOES
# DADAS NO Minicurso - Tidyverse DE Gustavo Almeida Silva
# E COM OS CODIGOS QUE FORAM ELABORADOS PELA AUTORA DE ACORDO
# COM A NECESSIDADE DA BASE DE DADOS UTILIZADA 
########################
# BASE DE DADOS DO SIM DE 2020 A 2022 - RESIDENTES DO MRJ
# O OBJETIVO SERA TRATAR A BASE DE DADOS PARA O PROCESSO DE LINK

#INSTALANDO
install.packages("pacman")

# CHAMANDO PACOTES NA BIBLIOTECA

pacman::p_load(
  rio,        # importação de dados  
  here,       # caminhos de arquicos relacionados
  janitor,    # limpeza de dados e tabelas
  lubridate,  # trabalhando com datas
  matchmaker, # limpeza baseada no dicionário
  epikit,     # funções de age_categories() 
  tidyverse,  # manejo e visualização de dados
  skimr,      # para pesquisas de strings, pode ser usado em valores "rolling-up"
  stringr,
  summarytools# apresenta uma visao geral da base de dados
)


# CHAMAR O DIRETORIO


# CHAMANDO A BASE - USANDO PACOTE "rio"

SIM2020 <- import("do_res_riodejaneiro_2020.dbf")


#OLHANDO OS NOMES DAS VARIAVEIS

names(SIM2020)

# OLHANDO A BASE - FORMA GERAL

skimr::skim(SIM2020)

print(summarytools::dfSummary(SIM2020), method = 'viewer')


#ARRUMANDO O NOME DAS COLUNAS
# pipe do conjunto de dados brutos através da 
#                  função clean_names(), atribuindo o resultado como "dados"


SIM2020 <- SIM2020 |>
  janitor::clean_names()|>
  janitor::remove_empty(which = 'rows')


# Veja os novos nomes das colunas
names(SIM2020)


# SELECIONANDO AS VARIAVEIS QUE QUERO TRABALHAR

SIM2020_select <- SIM2020|>dplyr::select("numerodo","tipobito", "dtobito","nome",
                                   "nomepai","nomemae","dtnasc","idade","sexo","racacor",
                              "estciv","esc","esc2010","ocup","codmunres" ,"baires","endres","numres" ,
                                "cepres","lococor","codmunocor","baiocor","endocor","linhaa" ,"linhab",
                                "linhac","linhad", "linhaii","causabas")


print(summarytools::dfSummary(SIM2020_select), method = 'viewer')


# OLHANDO OS DUPLICADOS E CRIANDO UM OBJETO SO COM ELES

duplic <- SIM2020_select |>
          janitor::get_dupes() 


### REALIZANDO ALGUNS FILTROS.

names(SIM2020_select)

## VERIFICANDO A MELHOR VARIAVEL

table(SIM2020_select$causabas)

table(SIM2020_select$codmunres)

table(SIM2020_select$codmunocor)

###########################################
# FILTRANDO POR IDADE PARA 18 ANOS OU MAIS
# FORAM EXCLUIDOS AS PESSOAS COM IDADE IGNORADA (999)
##########################################################

table(SIM2020_select$idade)

SIM2020_FILTRO <-SIM2020_select%>%dplyr::filter(SIM2020_select$idade > 417, SIM2020_select$idade != 999)


table(SIM2020_FILTRO$idade) 


# CRIANDO UMA NOVA VARIAVEL IDADE 

table(SIM2020_FILTRO$idade)

# EXTRAINDO DA VARIAVEL IDADE ORIGINAL OS 2 ULTIMOS CARACTER
SIM2020_FILTRO$idade2 <- substr(SIM2020_FILTRO$idade,2,3)

table(SIM2020_FILTRO$idade2)


# AJUSTANDO IDADE COM 100 ANOS OU MAIS 
table(SIM2020_FILTRO$idade2)

SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "00"] <- "100"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "01"] <- "101"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "02"] <- "102"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "03"] <- "103"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "04"] <- "104"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "05"] <- "105"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "06"] <- "106"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "07"] <- "107"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "08"] <- "108"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "09"] <- "109"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "10"] <- "110"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "12"] <- "112"
SIM2020_FILTRO$idade2[SIM2020_FILTRO$idade2 == "14"] <- "114"

#verificando
table(SIM2020_FILTRO$idade2)

# EXCLUINDO ALGUMAS BASES
rm(duplic,SIM2020_select,SIM2020)

########################
# PADRONIZANDO ALGUMAS VARIAVEIS
##############################
# TRANSFORMANDO TUDO P/ MAIUSCULO FUNCAO "toupper" DO PACOTE stringi
#            E RETIRANDO OS ESPACOS FUNCAO "str_squish" DO PACOTE stringr
#str_trim == retira apenas os espacos a esquerda e a direita
#############################

names(SIM2020_FILTRO)

#NOME 
SIM2020_FILTRO$nome <-str_squish(toupper(SIM2020_FILTRO$nome)) 

#NOME_MAE
SIM2020_FILTRO$nomemae <-str_squish(toupper(SIM2020_FILTRO$nomemae))


# OLHANDO A BASE
View(SIM2020_FILTRO)


####### RETIRANDO OS ACENTOS

#NOME
SIM2020_FILTRO$nome<- iconv(SIM2020_FILTRO$nome, to="ASCII//TRANSLIT")

#NOME_MAE (from="UTF-8")
SIM2020_FILTRO$nomemae <- iconv(SIM2020_FILTRO$nomemae, to="ASCII//TRANSLIT")


### RETIRANDO AS PREPOSICOES

#NOME

#retirando DE, DA, DO, DAS, DOS
SIM2020_FILTRO$nome <- gsub(" DE ", " ", SIM2020_FILTRO$nome)
SIM2020_FILTRO$nome <- gsub(" DA ", " ", SIM2020_FILTRO$nome)
SIM2020_FILTRO$nome <- gsub(" DO ", " ", SIM2020_FILTRO$nome)
SIM2020_FILTRO$nome <- gsub(" DAS ", " ", SIM2020_FILTRO$nome)
SIM2020_FILTRO$nome <- gsub(" DOS ", " ",SIM2020_FILTRO$nome)


table(SIM2020_FILTRO$nome)

# RETIRANDO CARACTERES INDESEJAVEIS 
###################
# FUNCAO str_replace_all DO PACOTE  - stringr
# SIM2020_FILTRO$nome<-gsub("]","",SIM2020_FILTRO$nome)


SIM2020_FILTRO$nome<-str_replace_all(SIM2020_FILTRO$nome,
                                     "[@,!,},&,{,.,#,!,?,*,|,/,),(,$,-.%,1,2,3,4,5,6,7,8,9,0]", "")

# NOME MAE

#retirando DE, DA, DO, DAS, DOS
SIM2020_FILTRO$nomemae <- gsub(" DE ", " ",SIM2020_FILTRO$nomemae)
SIM2020_FILTRO$nomemae <- gsub(" DA ", " ",SIM2020_FILTRO$nomemae)
SIM2020_FILTRO$nomemae <- gsub(" DO ", " ", SIM2020_FILTRO$nomemae)
SIM2020_FILTRO$nomemae <- gsub(" DAS ", " ", SIM2020_FILTRO$nomemae)
SIM2020_FILTRO$nomemae <- gsub(" DOS ", " ", SIM2020_FILTRO$nomemae)

# RETIRANDO CARACTERES INDESEJAVEIS 
###################

SIM2020_FILTRO$nomemae<-str_replace_all(SIM2020_FILTRO$nomemae,
                                     "[@,!,},&,{,.,#,!,?,*,|,/,),(,$,-.%,1,2,3,4,5,6,7,8,9,0]", "")


###### FOMATANDO E DECOMPONDO A DATA DE NASCIMENTO

#OLHANDO A CLASSIFICACAO DA VARIAVEL


class(SIM2020_FILTRO$dtnasc)

#OLHANDO AS 10 PRIMEIRAS LINHAS

SIM2020_FILTRO$dtnasc[1:10] 

#date
SIM2020_FILTRO<-SIM2020_FILTRO %>%
  mutate( dtnasc = as.Date(dtnasc, format = "%d%m%Y"))         


SIM2020_FILTRO$dtnasc[1:10] 


#VERFICANDO A BASE
print(summarytools::dfSummary(SIM2020_FILTRO), method = 'viewer')


# APOS REALIZAR A LIMPEZA AS BASES DE DADOS  LEMBRANDO DE REPETIR O MESMO PROCESSO PARA 
# AS BASES DE TODOS OS ANOS QUE IRA TRABALHAR AQUI TRABALHEI COM A BASE DO SIM DE 2020 A 2022.
# IREMOS UNI-LAS E TRANSFORMAR EM UMA UNICA BASE DE DADOS COM OS TRES ANOS.

##########################################################################
# UNIDO OS BANCOS 2020,2021,2022
#
######################################################################

#OLHANDO A DIMENSAO DOS BANCOS

dim(SIM2020_FILTRO)
names(SIM2020_FILTRO)

dim(SIM2021_FILTRO)
names(SIM2021_FILTRO)

dim(SIM2022_FILTRO)
names(SIM2022_FILTRO)

# juntando base 
SIM_TOTAL<- rbind(SIM2020_FILTRO,SIM2021_FILTRO,SIM2022_FILTRO) 

nrow(SIM_TOTAL)

# ver estrutura
glimpse(SIM_TOTAL)


skimr::skim(SIM_TOTAL)

print(summarytools::dfSummary(SIM_TOTAL), method = 'viewer')


# SALVANDO Rdata

#CSV
write.csv(SIM_TOTAL,"SIM_TOTAL.csv")

#dbf
library(rio)

export(SIM_TOTAL,"SIM_TOTAL.dbf")
