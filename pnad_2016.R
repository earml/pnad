################################################################################################
###      http://rcoster.blogspot.com.br/2014/02/lendo-grandes-bancos-de-dados.html           ###
################################################################################################
rm(list=ls(all=T))
setwd("D:\\Pnad") 
dir() 

library(SAScii)
library(descr)
library(RSQLite)
library(downloader)
library(readr)
library(data.table)
library(sqldf)

#####                          #####
##    Lendo o Script SAS em txt   ##
####                           #####
pnad<-parse.SAScii('*D:\\anuario_final\\Pnad\\DIC_PNADC_2016.txt',beginline = 11)
pnad<-parse.SAScii('Input_PNADC_5entr_2017.txt',beginline = 10)

## Como os dados sÃ£o um arquivo formatado com largura fixa, tenho que saber a largura de cada
## variÃ¡vel e onde ela inicia
largura<-pnad[2]
largura<-as.vector(largura[,1]) #converto para vetor

nome<-pnad[1]
nome<-as.character(nome[,1]) #converto para caracteres

## criando uma conexÃ£o com o banco de dados (que serÃ¡ criado automaticamente caso nÃ£o exista) 
## a pasta de trabalho
con <- dbConnect(RSQLite::SQLite(), ":memory:")

## convertendo o arquivo Dados_PNADC_2016.txt em um banco de dados SQLite
dbWriteTable(conn = con, name = "pnad_2017_5", value = "PNADC_2017_entr5.txt", header = FALSE, 
             row.names = FALSE, overwrite = TRUE, eol = '\r\n')

## A posiÃ§Ã£o que se inicia cada variÃ¡vel
inicio<-cumsum(largura)-largura+1
#write.table(inicio,file='inicio.RData') # salva os dados no diretótio

## criar a query SQLite que quebra o banco de uma Ãºnica variÃ¡vel no nÃºmero certo de variÃ¡veis, 
## que serÃ¡ executada no quarto comando
sintaxe <- paste("select", paste0("substr(V1, ", inicio, ",", largura, ") `", nome, "`", 
                                  collapse = ","), "from pnad_2017_5")
dados <- dbGetQuery(con, sintaxe)

write.table(dados, file = "PNADC_2017_5.csv", sep = "|",row.names = F,col.names = TRUE,na = "NA",dec = ",")

odbcCloseAll()# para fechar o banco
#Sergipe <- subset(dados, UF==28)
system.time(
  dados_metas <- fread(input='PNADC_2016_5.csv', sep='|', sep2='|', integer64='double')
)

metas <- as.data.frame(dados_metas[,.(UF, V2007, V2009, V2010, V3001, V3002, V3003A, V3004, V3005A, V3006, V3007, V3008, V3009A, 
                        V3010, V3011A, V3012, V3013, V3014, V4010, V4014, VD3001, VD3002)])


###    META 2   ###
#### GRÁFICOS 5 E 6####
#sum(table(metas$V2009)[7:15])
grafico_5 = subset(metas, metas$V2009 > "005" & metas$V2009 < "015")
grafico_5 = subset(metas, V2009 > "005" & V2009 < "015")

grafico_5_2 = subset(metas, ((metas$V3003A == "04" | metas$V3003A == "05") & (metas$V2009 > "005" & metas$V2009 < "015")))
grafico_5_2 = subset(metas,metas$V3002==1 & (metas$V3003A=="04" & metas$V3004==1 & (metas$V3006=="02" | 
                                                                    metas$V3006=="03" | metas$V3006=="04" |
                                                                    metas$V3006=="05" | metas$V3006=="06" | 
                                                                    metas$V3006=="07" | metas$V3006=="08")))
dim(grafico_5_2)[1]/dim(grafico_5)[1]*100
estado_meta5<-as.vector(names(table(metas[,'UF'])))





V2009<-metas$V2009;V3002<-metas$V3002;V3003A<-metas$V3003A; V3004<-metas$V3004;V3006<-metas$V3006
V3008<-metas$V3008;V3009A<-metas$V3009A;V3010<-metas$V3010; V3012<-metas$V3012;V3013<-metas$V3013
V3014<-metas$V3014

selecao<-"SELECT V2009, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014
            FROM metas
            WHERE (V2009 > '005' AND V2009 < '015') AND
            (
                (
                  V3002 = 1 AND
                  (
                      (V3003A = '04' AND V3004 = 1 AND (V3006 = '02' OR V3006 = '03' OR V3006 = '04' OR V3006 = '05' OR V3006 = '06' OR V3006 = '07' OR V3006 = '08'))
                      OR
                      (V3003A = '04' AND V3004 = 2 AND (V3006 = '03' OR V3006 = '04' OR V3006 = '05' OR V3006 = '06' OR V3006 = '07' OR V3006 = '08' OR V3006 = '09'))
                      OR
                      (V3003A = '05' AND (V3006 = '02' OR V3006 = '03' OR V3006 = '04' OR V3006 = '05' OR V3006 = '06' OR V3006 = '07' OR V3006 = '08'))
                  )
                )
                OR 
                (
                  V3002 = 2 AND
                  (
                
                      (V3008 = 1 AND V3009A = '05' AND V3012 = 1 AND (V3013 = '01' OR V3013 = '02' OR V3013 = '03' OR V3013 = '04' OR V3013 = '05' OR V3013 = '06'))
                      OR
                      (V3008 = 1 AND V3009A = '06' AND V3012 = 1 AND ((V3013 = '01' OR V3013 = '02' OR V3013 = '03') OR (V3013 = '04' AND V3014 = 2)))
                      OR
                      (V3008 = 1 AND V3009A = '06' AND V3012 = 2)
                      OR
                      (V3008 = 1 AND V3009A = '06' AND V3012 = 3 AND V3014 = 2)
                      OR
                      (V3008 = 1 AND V3009A = '07' AND V3010 = 1 AND V3012 = 1 AND (V3013 = '01' OR V3013 = '02' OR V3013 = '03' OR V3013 = '04' OR V3013 = '05' OR V3013 = '06' OR V3013 = '07'))
                      OR
                      (V3008 = 1 AND V3009A = '07' AND V3010 = 2 AND V3012 = 1 AND (V3013 = '02' OR V3013 = '03' OR V3013 = '04' OR V3013 = '05' OR V3013 = '06' OR V3013 = '07' OR V3013 = '08'))
                      OR
                      (V3008 = 1 AND V3009A = '08' AND V3012 = 1 AND (V3013 = '01' OR V3013 = '02' OR V3013 = '03' OR V3013 = '04' OR V3013 = '05' OR V3013 = '06' OR V3013 = '07'))
                  )
                )
)
"
saida2 <- as.data.frame(sqldf(selecao))


selecao1<-"SELECT V2009, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014
            FROM metas
            WHERE (V2009 > '005' AND V2009 < '015') 
              
"
saida1 <- as.data.frame(sqldf(selecao1))

##### META 8  ####
### Gráfico 34 e 35 ####

escolaridade_brasil <-"SELECT UF, V2009, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014, VD3002
            FROM metas
            WHERE V2009 BETWEEN '018' AND '029'"

escolaridade_br <- as.data.frame(sqldf(escolaridade_brasil))
media_anos_estudo_br<-table(escolaridade_br$VD3002)
anos<-0:15
sum(as.matrix(media_anos_estudo_br)*anos)/(sum(media_anos_estudo_br))

escolaridade_nordeste <-"SELECT UF, V2009, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014, VD3002
            FROM metas
            WHERE (V2009 BETWEEN '018' AND '029')
            AND (UF BETWEEN '21' AND '29')
            "
escolaridade_ne <- as.data.frame(sqldf(escolaridade_nordeste))
media_anos_estudo_ne<-table(escolaridade_ne$VD3002)
anos<-0:15
sum(as.matrix(media_anos_estudo_ne)*anos)/(sum(media_anos_estudo_ne))


for (i in 21:29) {
  estados_nordeste<-subset(escolaridade_ne,UF==i)
  media_anos_estudo_nordeste<-table(estados_nordeste$VD3002)
  anos<-0:15
  media_estado<-sum(as.matrix(media_anos_estudo_nordeste)*anos)/(sum(media_anos_estudo_nordeste))
  print(paste0('Estado: ',i,"  Média: ",media_estado))
  
  
}


##### META 8  ####
### Gráfico 36 - 38 ####

escolaridade_brasil <-"SELECT UF, V2009, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014, VD3002
                              FROM metas
                              WHERE (V2009 BETWEEN '018' AND '029')
                              AND (V2010 = 1 OR V2010 = 2)"

escolaridade_br <- as.data.frame(sqldf(escolaridade_brasil))
media_anos_estudo_br<-table(escolaridade_br$VD3002)
anos<-0:15
sum(as.matrix(media_anos_estudo_br)*anos)/(sum(media_anos_estudo_br))

escolaridade_nordeste <-"SELECT UF, V2009, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014, VD3002
                                FROM metas
                                WHERE (V2009 BETWEEN '018' AND '029')
                                AND (UF BETWEEN '21' AND '29')
                                AND (V2010 = 1 OR V2010 = 2)"

escolaridade_ne <- as.data.frame(sqldf(escolaridade_nordeste))
media_anos_estudo_ne<-table(escolaridade_ne$VD3002)
anos<-0:15
sum(as.matrix(media_anos_estudo_ne)*anos)/(sum(media_anos_estudo_ne))


for (i in 21:29) {
  estados_nordeste<-subset(escolaridade_ne,UF==i)
  media_anos_estudo_nordeste<-table(estados_nordeste$VD3002)
  anos<-as.numeric(labels(media_anos_estudo_nordeste)[[1]])
  media_estado<-sum(as.matrix(media_anos_estudo_nordeste)*anos)/(sum(media_anos_estudo_nordeste))
  print(paste0('Estado: ',i,"  Média: ",media_estado))
  
  
}




##### META 8  ####
### Gráfico 39 - 41 ####

escolaridade_brasil <-"SELECT UF, V2009, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014, VD3002
                              FROM metas
                              WHERE (V2009 BETWEEN '018' AND '029')
                              AND V2007 = 1"

escolaridade_br <- as.data.frame(sqldf(escolaridade_brasil))
media_anos_estudo_br<-table(escolaridade_br$VD3002)
anos<-0:15
sum(as.matrix(media_anos_estudo_br)*anos)/(sum(media_anos_estudo_br))

escolaridade_nordeste <-"SELECT UF, V2009, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014, VD3002
                                FROM metas
                                WHERE (V2009 BETWEEN '018' AND '029')
                                AND (UF BETWEEN '21' AND '29')
                                AND V2007 = 1"

escolaridade_ne <- as.data.frame(sqldf(escolaridade_nordeste))
media_anos_estudo_ne<-table(escolaridade_ne$VD3002)
anos<-0:15
sum(as.matrix(media_anos_estudo_ne)*anos)/(sum(media_anos_estudo_ne))


for (i in 21:29) {
  estados_nordeste<-subset(escolaridade_ne,UF==i)
  media_anos_estudo_nordeste<-table(estados_nordeste$VD3002)
  anos<-as.numeric(labels(media_anos_estudo_nordeste)[[1]])
  media_estado<-sum(as.matrix(media_anos_estudo_nordeste)*anos)/(sum(media_anos_estudo_nordeste))
  print(paste0('Estado: ',i,"  Média: ",media_estado))
  
  
}





##### META 9  ####
### Gráfico 40 - 43 ####

escolaridade_brasil <-"SELECT UF, V2009, V3001, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014, VD3002
                              FROM metas
                              WHERE V2009 > '014'"
                              #AND V3001 = 2"


pop_tot <- as.data.frame(metas[,c('UF','V3001')])
pop_tot <- c(pop_tot)
pop_tot[,2]

anos<-0:15
sum(as.matrix(media_anos_estudo_br)*anos)/(sum(media_anos_estudo_br))

escolaridade_nordeste <-"SELECT UF, V2009, V3001, V3002, V3003A, V3004, V3006, V3008, V3009A, V3012, V3013, V3014, VD3002
                                FROM metas
                                WHERE V2009 > '014'
                                AND (UF BETWEEN '21' AND '29')
                                AND V3001 = 2"

escolaridade_ne <- as.data.frame(sqldf(escolaridade_nordeste))
media_anos_estudo_ne<-table(escolaridade_ne$VD3002)
anos<-0:15
sum(as.matrix(media_anos_estudo_ne)*anos)/(sum(media_anos_estudo_ne))


for (i in 21:29) {
  estados_nordeste<-subset(escolaridade_ne,UF==i)
  media_anos_estudo_nordeste<-table(estados_nordeste$VD3002)
  anos<-as.numeric(labels(media_anos_estudo_nordeste)[[1]])
  media_estado<-sum(as.matrix(media_anos_estudo_nordeste)*anos)/(sum(media_anos_estudo_nordeste))
  print(paste0('Estado: ',i,"  Média: ",media_estado))
  
  
}










for (i in estado_meta5) {
  grafico_5 = subset(metas, (metas$V2009 > "005" & metas$V2009 < "015") & UF == i)
  grafico_5_2 = subset(metas,UF == i & ((metas$V3003A == "04" | metas$V3003A == "05") & (metas$V2009 > "005" & metas$V2009 < "015")))
  percentual <- dim(grafico_5_2)[1]/dim(grafico_5)[1]*100
  print(paste0(i,"   percentual: ",percentual))
  
}

grafico_5 = subset(metas, (metas$V2009 > "005" & metas$V2009 < "015") & (UF >= "21" & UF<="29"))
grafico_5_2 = subset(metas,(UF >= "21" & UF<="29") & ((metas$V3003A == "04" | metas$V3003A == "05") & (metas$V2009 > "005" & metas$V2009 < "015")))
dim(grafico_5_2)[1]/dim(grafico_5)[1]*100

###    META 2    ###
#### GRÁFICOS 7 ####
grafico_7 = subset(metas, metas$V2009 == "016")
grafico_7_2 = subset(metas, (metas$VD3001 == "3") & (metas$V2009 == "016"))
dim(grafico_7_2)[1]/dim(grafico_7)[1]*100
estado_meta5<-as.vector(names(table(metas[,'UF'])))


selecao <- (metas$V2009 == "016")
grafico_5 = subset(metas, metas$V2009 == "016")
selecao2 <- (metas$V3003A == '04' & (metas$V2009 >= "006" & metas$V2009 <= "014"))
grafico_5_2 = subset(metas, (metas$V3009A == '07' | metas$V3009A == '08')
                     & metas$V3014 == "1" & metas$V2009 == "016")

table(metas$V2009)[7:3]

mode(metas)
typeof(metas)

library(RODBC)

query<-'SELECT UF, V2007, V2009, V2010, V3001, V3002, V3003A, V4010, V4014, VD3001, VD3002
               FROM dados'
metas <- as.data.frame(sqldf(query))

#meta1 <- na.omit(as.matrix(sapply(metas[,c('UF','V2009','V3002')], as.numeric)))
meta1 <- metas[,c('UF','V2009','V3002','V3003A')]
estado_meta1<-as.vector(names(table(meta1[,'UF'])))

porc_4_5_escola<-filter(meta1, meta1$V2009 == "004")
newdf <- meta1[which(meta1$V2009 == "005"),2 ];
porc_4_5_escola <- subset(meta1$V2009,meta1$V2009 >= "006" & meta1$V2009 <= "014")

porc_4_5_escola_ok <- as.matrix(subset(meta1,(meta1$V2009 >= "006" & meta1$V2009 <= "014") & meta1$V3002A == "04"))


# Seleciona todos os dados do banco 'dados'
query<-"SELECT * FROM dados" 

# Seleciona algumas variáveis do banco
query<-"SELECT ANO, TRIMESTRE, UF, CAPITAL, V1022 FROM dados" 

# Seleciona algumas variáveis, onde UF=28
# Operadores Lógicos (=, >, <, >=, <=, <>)

query<-"SELECT ANO, UF, CAPITAL,V1022
          FROM dados
          WHERE UF=28
          OR  V1022=2"


Sergipe2 <- sqldf(query)

# Seleciona uma variável e altera o nome dela, bem como determina a quantidade de amostras requerida
query<-"SELECT V1022 AS Situacao
            FROM Sergipe2
            LIMIT 6"

sergie3<-sqldf(query)

query<-"SELECT V1022 AS Situacao, UF AS Estado, CAPITAL AS Cap
            FROM Sergipe2
            WHERE UF=28"
sergie3<-sqldf(query)

#Selecionando uma variável sem informações repetidas. Neste caso eu estou selecionando na vaiável
# UF todos os valores que não são repetidos
query<-"SELECT DISTINCT UF 
            FROM Sergipe2"
query<-"SELECT DISTINCT UF AS UnidadeFederacao 
            FROM Sergipe2"

sergipe<-sqldf(query)

# Selecionando algumas variáveis e ordenando por valor do rendimento (SD06003)
query<-"SELECT UF, CAPITAL, V1022, SD06003
        FROM dados
        WHERE UF=28
        ORDER BY SD06003"

query<-"SELECT UF AS Estado, CAPITAL, V1022 AS Situacao, SD06003
        FROM dados
        WHERE UF=28
        ORDER BY SD06003"


sergipe<-sqldf(query)

# Selecionando por ordem decrescente ou crescente
query<-"SELECT UF AS Estado, CAPITAL, V1022 AS Situacao, SD06003
        FROM dados
        WHERE UF=28
        ORDER BY SD06003 DESC"

query<-"SELECT UF AS Estado, CAPITAL, V1022 AS Situacao, SD06003
        FROM dados
        WHERE UF=28
        ORDER BY SD06003 ASC"

# Usando 'LIKE'
query<-"SELECT UF AS Estado, CAPITAL, V1022 AS Situacao, SD06003
        FROM dados
        WHERE UF LIKE 28
        ORDER BY SD06003"

query<-"SELECT UF AS Estado, CAPITAL, V1022 AS Situacao, SD06003
        FROM dados
        WHERE UF NOT LIKE 28
        ORDER BY SD06003"

sergipe<-sqldf(query)

# OBS: Posso ordenando por ordem alfabética, basta usar 'A%' ou colocar NOT LIKE '%an%'
# que seleciono valores da variável que não contém 'an'
sergipe<-sqldf(query)


