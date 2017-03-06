############################################################################################
########## Word Cloud of search phrases for Heavy, Medium and Light Usage devices ##########
############################################################################################

## Required Libraries
library(wordcloud)

## Getting working directory
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project Diagnosing Usage @Home/")

## Loading data
data1 <- read.csv(file = "SearchPhrase.csv", header = T, stringsAsFactors = F)

## filtering out relevant data
data1 <- subset(data1, select = c(edata.eks.id,Flags))
data1$edata.eks.id <- tolower(data1$edata.eks.id)

## dividing the data into heavy, medium and light devices
heavy <- subset(data1, Flags == "Heavy")
heavy1 <- gsub("[[:space:]]", "_", heavy$edata.eks.id)
# heavy1 <- strsplit(paste(heavy$edata.eks.id, collapse = " "), ' ')[[1]]
heavy2 <- as.data.frame(table(heavy1))
heavy2 <- subset(heavy2, heavy1 != "")

medium <- subset(data1, Flags == "Medium")
medium1 <- gsub("[[:space:]]", "_", medium$edata.eks.id)
# medium1 <- strsplit(paste(medium$edata.eks.id, collapse = " "), ' ')[[1]]
medium2 <- as.data.frame(table(medium1))
medium2 <- subset(medium2, medium1 != "")

light <- subset(data1, Flags == "Light")
light1 <- gsub("[[:space:]]", "_", light$edata.eks.id)
# light1 <- strsplit(paste(light$edata.eks.id, collapse = " "), ' ')[[1]]
light2 <- as.data.frame(table(light1))
light2 <- subset(light2, light1 != "")

## Creating word cloud of Search Phrases
wordcloud(words = heavy2$heavy1,freq = heavy2$Freq,min.freq = 1,max.words = 100,colors=brewer.pal(8, "Dark2"),scale=c(4,.2), random.order=FALSE)
wordcloud(words = medium2$medium1,freq = medium2$Freq,min.freq = 1,max.words = 100,colors=brewer.pal(8, "Dark2"),scale=c(2,.2), random.order=FALSE)
wordcloud(words = light2$light1,freq = light2$Freq,min.freq = 1,max.words = 100,colors=brewer.pal(8, "Dark2"),scale=c(4,.2), random.order=FALSE)

################################### End of Code ######################################