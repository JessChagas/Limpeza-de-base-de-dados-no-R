########################################
# CRIANDO SCRIPT PARA LIMPEZA GERAL DE BASE DE DADOS
# 
####################################
##CADEIA DE LIMPEZA
#Importação de dados
#Nomes de colunas limpos ou alterados
#Remoção de duplicidades
# diagnostico geral



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


# INFORMANDO AO R O MEU AMBIENTE DE TRABALHO (DIRETORIO)

here::here("C:/Users/jessi/OneDrive/Área de Trabalho/Doutorado/Proj_Qualificacao2023/Analise/Limpeza_pre-processamento")



#####################################################
# OLHANDO AS BASES DO SIM ANTES DE MANIPULAR NO R
# IREI FAZER UM RBIND E LEVAR A BASE PARA O SQL
# ME CERTIFICANDO Q N HAJA OMISSAO DE LINHAS
##############################################
 
# APROVEITEI E CHAMEI TODAS AS BASES DE UMA VEZ (SEM MANIPULAR)

# SIM 2020 = 72.175
# SIM 2021 = 75.391
# SIM 2022 = 59.927


# juntando base 
SIM_TOTAL_SEM_FILTRO<- rbind(SIM2020,SIM2021,SIM2022) 


# SALVANDO COM DBF
library(rio)

export(SIM_TOTAL_SEM_FILTRO,"SIM_TOTAL_SEM_FILTRO.dbf")

#################################################################


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
          janitor::get_dupes() # 0 DUPLICADOS



### REALIZANDO ALGUNS FILTROS.

names(SIM2020_select)

## VERIFICANDO A MELHOR VARIAVEL

table(SIM2020_select$causabas)

table(SIM2020_select$codmunres)#72.175

table(SIM2020_select$codmunocor)

###########################################
# FILTRANDO POR IDADE PARA 18 ANOS OU MAIS
# FORAM EXCLUIDOS AS PESSOAS COM IDADE IGNORADA (999)
##########################################################

table(SIM2020_select$idade)

SIM2020_FILTRO <-SIM2020_select%>%dplyr::filter(SIM2020_select$idade > 417, SIM2020_select$idade != 999)


table(SIM2020_FILTRO$idade) 


# FORAM EXCLUIDOS 2.241 OBITOS NA FXET 0 A 17 ANOS
# FORAM EXCLUIDOS 44 OBITOS COM IDADE IGNORADA 

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


##############################
## CRIANDO A VARIAVEL STATUS
# STATUS
###################################
### Abrindo planilha CID-10 4C

cid <- import("CID-10-SUBCATEGORIAS.csv", encoding = "Latin-1") %>%
  select(SUBCAT, DESCRABREV)


### Criando variÃ¡vel de causa bÃ¡sica 2 ( dados Ã© a base do SIM)

SIM2020_FILTRO$STATUS <- SIM2020_FILTRO$causabas


# join com a planilha de cid

SIM2020_FILTRO <- left_join(SIM2020_FILTRO, cid, by = c("STATUS" = "SUBCAT")) %>%
  select(-STATUS) %>%
  rename("STATUS" = "DESCRABREV")

table(SIM2020_FILTRO$STATUS)

#REMOVENDO ALGUNS BANCOS

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


##################################
# ABRINDO O BANCO DE 2021
########################
################

# CHAMANDO A BASE - USANDO PACOTE "rio"

SIM2021 <- import("do_res_riodejaneiro_2021.dbf")

#OLHANDO OS NOMES DAS VARIAVEIS

names(SIM2021)


# OLHANDO A BASE - FORMA GERAL

skimr::skim(SIM2021)

print(summarytools::dfSummary(SIM2021), method = 'viewer')


#ARRUMANDO O NOME DAS COLUNAS
# pipe do conjunto de dados brutos através da 
#                  função clean_names(), atribuindo o resultado como "dados"


SIM2021 <- SIM2021 |>
  janitor::clean_names()|>
  janitor::remove_empty(which = 'rows')


# Veja os novos nomes das colunas
names(SIM2021)


# SELECIONANDO AS VARIAVEIS QUE QUERO TRABALHAR

SIM2021_select <- SIM2021 |>dplyr:: select("numerodo","tipobito", "dtobito","nome",
                                    "nomepai","nomemae","dtnasc","idade","sexo","racacor",
                                    "estciv","esc","esc2010","ocup","codmunres" ,"baires","endres","numres" ,
                                    "cepres","lococor","codmunocor","baiocor","endocor","linhaa" ,"linhab",
                                    "linhac","linhad", "linhaii","causabas")





print(summarytools::dfSummary(SIM2021_select), method = 'viewer')


# OLHANDO OS DUPLICADOS E CRIANDO UM OBJETO SO COM ELES

duplic <- SIM2021_select |>
  janitor::get_dupes() # 0 DUPLICADOS

#############################
### REALIZANDO ALGUNS FILTROS.
##################################


names(SIM2021_select)

## VERIFICANDO A MELHOR VARIAVEL

table(SIM2021_select$causabas)

table(SIM2021_select$codmunres)# 75391 

table(SIM2021_select$codmunocor)

###########
# FILTRANDO POR IDADE PARA 18 ANOS OU MAIS
# FORAM EXCLUIDOS AS PESSOAS COM IDADE IGNORADA (999)

table(SIM2021_select$idade)


SIM2021_FILTRO <-SIM2021_select%>%dplyr::filter(SIM2021_select$idade > 417, SIM2021_select$idade != 999)


table(SIM2021_FILTRO$idade) 


# FORAM EXCLUIDOS 2.076 OBITOS NA FXET 0 A 17 ANOS
# FORAM EXCLUIDOS 63 OBITOS COM IDADE IGNORADA 


# CRIANDO UMA NOVA VARIAVEL IDADE 

table(SIM2021_FILTRO$idade)

# EXTRAINDO DA VARIAVEL IDADE ORIGINAL OS 2 ULTIMOS CARACTER
SIM2021_FILTRO$idade2 <- substr(SIM2021_FILTRO$idade,2,3)

table(SIM2021_FILTRO$idade2)


# AJUSTANDO IDADE COM 100 ANOS OU MAIS 
table(SIM2021_FILTRO$idade2)

SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "00"] <- "100"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "01"] <- "101"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "02"] <- "102"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "03"] <- "103"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "04"] <- "104"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "05"] <- "105"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "06"] <- "106"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "07"] <- "107"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "08"] <- "108"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "09"] <- "109"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "10"] <- "110"
SIM2021_FILTRO$idade2[SIM2021_FILTRO$idade2 == "11"] <- "111"


#verificando
table(SIM2021_FILTRO$idade2)


##################################
## CRIANDO A VARIAVEL STATUS
# STATUS
###############################
# AQUI IREMOS USAR A TAB, CID USARDA ANTERIORMENTE

# CRIANDO VARIAVEL CAUSA BASICA2 (  DADOS BASE SIM)

SIM2021_FILTRO$STATUS <- SIM2021_FILTRO$causabas


# join com a planilha de cid

SIM2021_FILTRO <- left_join(SIM2021_FILTRO, cid, by = c("STATUS" = "SUBCAT")) %>%
  select(-STATUS) %>%
  rename("STATUS" = "DESCRABREV")

table(SIM2021_FILTRO$STATUS)

#REMOVENDO ALGUNS BANCOS

rm(duplic,SIM2021_select,SIM2021)



########################
# PADRONIZANDO ALGUMAS VARIAVEIS
##############################
# TRANSFORMANDO TUDO P/ MAIUSCULO FUNCAO "toupper" DO PACOTE stringi
#            E RETIRANDO OS ESPACOS FUNCAO "str_squish" DO PACOTE stringr
#str_trim == retira apenas os espacos a esquerda e a direita
#############################

names(SIM2021_FILTRO)

#NOME 
SIM2021_FILTRO$nome <-str_squish(toupper(SIM2021_FILTRO$nome)) 

#NOME_MAE
SIM2021_FILTRO$nomemae <-str_squish(toupper(SIM2021_FILTRO$nomemae))



#### RETIRANDO OS ACENTOS

#NOME
SIM2021_FILTRO$nome<- iconv(SIM2021_FILTRO$nome, to="ASCII//TRANSLIT")

#NOME_MAE (from="UTF-8")
SIM2021_FILTRO$nomemae <- iconv(SIM2021_FILTRO$nomemae, to="ASCII//TRANSLIT")


##### RETIRANDO AS PREPOSICOES


#NOME

# DE, DA, DO, DAS, DOS
SIM2021_FILTRO$nome <- gsub(" DE ", " ", SIM2021_FILTRO$nome)
SIM2021_FILTRO$nome <- gsub(" DA ", " ", SIM2021_FILTRO$nome)
SIM2021_FILTRO$nome <- gsub(" DO ", " ", SIM2021_FILTRO$nome)
SIM2021_FILTRO$nome <- gsub(" DAS ", " ", SIM2021_FILTRO$nome)
SIM2021_FILTRO$nome <- gsub(" DOS ", " ",SIM2021_FILTRO$nome)


# RETIRANDO CARACTERES INDESEJAVEIS 
###################

SIM2021_FILTRO$nome<-str_replace_all(SIM2021_FILTRO$nome,
                                        "[@,!,},&,{,.,#,!,?,*,|,/,),(,$,-.%,1,2,3,4,5,6,7,8,9,0]", "")


# NOME MAE

# DE, DA, DO, DAS, DOS
SIM2021_FILTRO$nomemae <- gsub(" DE ", " ",SIM2021_FILTRO$nomemae)
SIM2021_FILTRO$nomemae <- gsub(" DA ", " ",SIM2021_FILTRO$nomemae)
SIM2021_FILTRO$nomemae <- gsub(" DO ", " ", SIM2021_FILTRO$nomemae)
SIM2021_FILTRO$nomemae <- gsub(" DAS ", " ", SIM2021_FILTRO$nomemae)
SIM2021_FILTRO$nomemae <- gsub(" DOS ", " ", SIM2021_FILTRO$nomemae)


# RETIRANDO CARACTERES INDESEJAVEIS 
###################

SIM2021_FILTRO$nomemae<-str_replace_all(SIM2021_FILTRO$nomemae,
                                        "[@,!,},&,{,.,#,!,?,*,|,/,),(,$,-.%,1,2,3,4,5,6,7,8,9,0]", "")



###### FOMATANDO E DECOMPONDO A DATA DE NASCIMENTO

#OLHANDO A CLASSIFICACAO DA VARIAVEL
class(SIM2021_FILTRO$dtnasc)

#OLHANDO AS 10 PRIMEIRAS LINHAS

SIM2021_FILTRO$dtnasc[1:10] 

#date
SIM2021_FILTRO<-SIM2021_FILTRO %>%
  mutate( dtnasc = as.Date(dtnasc, format = "%d%m%Y"))         


SIM2021_FILTRO$dtnasc[1:10] 


#VERFICANDO A BASE
print(summarytools::dfSummary(SIM2021_FILTRO), method = 'viewer')


##################################
# ABRINDO O BANCO DE 2022
########################
################

# CHAMANDO A BASE - USANDO PACOTE "rio"


SIM2022 <- import("do_res_riodejaneiro_2022.dbf")

#OLHANDO OS NOMES DAS VARIAVEIS

names(SIM2022)


# OLHANDO A BASE - FORMA GERAL

skimr::skim(SIM2022)

print(summarytools::dfSummary(SIM2022), method = 'viewer')

######################################
#ARRUMANDO O NOME DAS COLUNAS
# pipe do conjunto de dados brutos através da 
#                  função clean_names(), atribuindo o resultado como "dados"


SIM2022 <- SIM2022 |>
  janitor::clean_names()|>
  janitor::remove_empty(which = 'rows')


# Veja os novos nomes das colunas
names(SIM2022)


# SELECIONANDO AS VARIAVEIS QUE QUERO TRABALHAR

SIM2022_select <- SIM2022 |>dplyr::select("numerodo","tipobito", "dtobito","nome",
                                    "nomepai","nomemae","dtnasc","idade","sexo","racacor",
                                    "estciv","esc","esc2010","ocup","codmunres" ,"baires","endres","numres" ,
                                    "cepres","lococor","codmunocor","baiocor","endocor","linhaa" ,"linhab",
                                    "linhac","linhad", "linhaii","causabas")


print(summarytools::dfSummary(SIM2022_select), method = 'viewer')


# OLHANDO OS DUPLICADOS E CRIANDO UM OBJETO SO COM ELES

duplic <- SIM2022_select |>
  janitor::get_dupes() # 0 DUPLICADOS

##################################
### REALIZANDO ALGUNS FILTROS
########################


names(SIM2022_select)

## VERIFICANDO A MELHOR VARIAVEL

table(SIM2022_select$causabas)

table(SIM2022_select$codmunres)# 59927 

table(SIM2022_select$codmunocor)


################################################
# FILTRANDO POR IDADE PARA 18 ANOS OU MAIS
# FORAM EXCLUIDOS AS PESSOAS COM IDADE IGNORADA (999)
####################################################

table(SIM2022_select$idade)

SIM2022_FILTRO <-SIM2022_select%>%dplyr::filter(SIM2022_select$idade > 417, SIM2022_select$idade != 999)


table(SIM2022_FILTRO$idade)

# FORAM EXCLUIDOS 2.006  OBITOS NA FXET 0 A 17 ANOS
# FORAM EXCLUIDOS 26 OBITOS COM IDADE IGNORADA 


# CRIANDO UMA NOVA VARIAVEL IDADE 

table(SIM2022_FILTRO$idade)

# EXTRAINDO DA VARIAVEL IDADE ORIGINAL OS 2 ULTIMOS CARACTER
SIM2022_FILTRO$idade2 <- substr(SIM2022_FILTRO$idade,2,3)

table(SIM2022_FILTRO$idade2)


# AJUSTANDO IDADE COM 100 ANOS OU MAIS 
table(SIM2022_FILTRO$idade2)

SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "00"] <- "100"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "01"] <- "101"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "02"] <- "102"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "03"] <- "103"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "04"] <- "104"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "05"] <- "105"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "06"] <- "106"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "07"] <- "107"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "08"] <- "108"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "09"] <- "109"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "10"] <- "110"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "11"] <- "111"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "12"] <- "112"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "14"] <- "114"
SIM2022_FILTRO$idade2[SIM2022_FILTRO$idade2 == "15"] <- "115"

#verificando
table(SIM2022_FILTRO$idade2)

# TEMOS UM CASO COM 118 ANOS NO MEIO DOS CASOS DE 18 ANOS 
# NUMERO DA DO = 32480745 - VERIFICAR COMO IREI AJUSTAR ESSA IDADE



#############################
## CRIANDO A VARIAVEL STATUS
# STATUS
#####################

# AQUI IREMOS USAR A TAB, CID USARDA ANTERIORMENTE

# CRIANDO VARIAVEL CAUSA BASICA2 (  DADOS BASE SIM)

SIM2022_FILTRO$STATUS <- SIM2022_FILTRO$causabas


# join com a planilha de cid

SIM2022_FILTRO <- left_join(SIM2022_FILTRO, cid, by = c("STATUS" = "SUBCAT")) %>%
  select(-STATUS) %>%
  rename("STATUS" = "DESCRABREV")

table(SIM2022_FILTRO$STATUS)

#REMOVENDO ALGUNS BANCOS

rm(duplic,SIM2022_select,SIM2022,cid)


########################
# PADRONIZANDO ALGUMAS VARIAVEIS
##############################
# TRANSFORMANDO TUDO P/ MAIUSCULO FUNCAO "toupper" DO PACOTE stringi
#            E RETIRANDO OS ESPACOS FUNCAO "str_squish" DO PACOTE stringr
#str_trim == retira apenas os espacos a esquerda e a direita
#############################

names(SIM2022_FILTRO)

#NOME 
SIM2022_FILTRO$nome <-str_squish(toupper(SIM2022_FILTRO$nome)) 

#NOME_MAE
SIM2022_FILTRO$nomemae <-str_squish(toupper(SIM2022_FILTRO$nomemae))



#### RETIRANDO OS ACENTOS


#NOME
SIM2022_FILTRO$nome<- iconv(SIM2022_FILTRO$nome, to="ASCII//TRANSLIT")

#NOME_MAE 
SIM2022_FILTRO$nomemae <- iconv(SIM2022_FILTRO$nomemae, to="ASCII//TRANSLIT")


##### RETIRANDO AS PREPOSICOES


#NOME

#r DE, DA, DO, DAS, DOS
SIM2022_FILTRO$nome <- gsub(" DE ", " ", SIM2022_FILTRO$nome)
SIM2022_FILTRO$nome <- gsub(" DA ", " ", SIM2022_FILTRO$nome)
SIM2022_FILTRO$nome <- gsub(" DO ", " ", SIM2022_FILTRO$nome)
SIM2022_FILTRO$nome <- gsub(" DAS ", " ", SIM2022_FILTRO$nome)
SIM2022_FILTRO$nome <- gsub(" DOS ", " ",SIM2022_FILTRO$nome)


# RETIRANDO CARACTERES INDESEJAVEIS 
###################

SIM2022_FILTRO$nome<-str_replace_all(SIM2022_FILTRO$nome,
                                        "[@,!,},&,{,.,#,!,?,*,|,/,),(,$,-.%,1,2,3,4,5,6,7,8,9,0]", "")


# NOME MAE

#DE, DA, DO, DAS, DOS
SIM2022_FILTRO$nomemae <- gsub(" DE ", " ",SIM2022_FILTRO$nomemae)
SIM2022_FILTRO$nomemae <- gsub(" DA ", " ",SIM2022_FILTRO$nomemae)
SIM2022_FILTRO$nomemae <- gsub(" DO ", " ", SIM2022_FILTRO$nomemae)
SIM2022_FILTRO$nomemae <- gsub(" DAS ", " ", SIM2022_FILTRO$nomemae)
SIM2022_FILTRO$nomemae <- gsub(" DOS ", " ", SIM2022_FILTRO$nomemae)



# RETIRANDO CARACTERES INDESEJAVEIS 
###################

SIM2022_FILTRO$nomemae<-str_replace_all(SIM2022_FILTRO$nomemae,
                                        "[@,!,},&,{,.,#,!,?,*,|,/,),(,$,-.%,1,2,3,4,5,6,7,8,9,0]", "")


###### FOMATANDO E DECOMPONDO A DATA DE NASCIMENTO

#OLHANDO A CLASSIFICACAO DA VARIAVEL


class(SIM2022_FILTRO$dtnasc)

#OLHANDO AS 10 PRIMEIRAS LINHAS

SIM2022_FILTRO$dtnasc[1:10] 

#date
SIM2022_FILTRO<-SIM2022_FILTRO %>%
  mutate( dtnasc = as.Date(dtnasc, format = "%d%m%Y"))         


SIM2022_FILTRO$dtnasc[1:10] 


#VERFICANDO A BASE
print(summarytools::dfSummary(SIM2022_FILTRO), method = 'viewer')


##########################################################################
# UNIDO OS BANCOS 2020,2021,2022
#
######################################################################

#OLHANDO A DIMENSAO DOS BANCOS

dim(SIM2020_FILTRO)#69890 - 31
names(SIM2020_FILTRO)

dim(SIM2021_FILTRO)#73252 - 31
names(SIM2021_FILTRO)

dim(SIM2022_FILTRO)# 57895 - 31
names(SIM2022_FILTRO)

# juntando base 
SIM_TOTAL<- rbind(SIM2020_FILTRO,SIM2021_FILTRO,SIM2022_FILTRO) 

nrow(SIM_TOTAL)# 201037

# ver estrutura
glimpse(SIM_TOTAL)


skimr::skim(SIM_TOTAL)

print(summarytools::dfSummary(SIM_TOTAL), method = 'viewer')


# SALVANDO Rdata

#CSV
write.csv(SIM_TOTAL,"SIM_TOTAL.csv")

library(rio)

export(SIM_TOTAL,"SIM_TOTAL.dbf")





















