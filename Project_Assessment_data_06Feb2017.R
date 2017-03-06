#####################################################
############ Project Assessment Data ################
#####################################################

## Required libraries
library(dplyr)
library(reshape2)

## Input Data
data_in <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - OE_ASSESS/Jan2017_OE_ASSESS.csv", header = T, stringsAsFactors = F)

## creating date field
data_in$dummy <- strtrim(data_in$ts, width = 19)
data_in$date <- as.Date(data_in$dummy, format = "%Y-%m-%dT%H:%M:%S")
data_in$dummy <- NULL

# selecting relevant variables
d1 <- data_in[, c(which(colnames(data_in) %in% c("uid","did","date","edata.eks.qid","edata.eks.length","edata.eks.pass","eid","gdata.id","udata.standard")),grep("resvalues", colnames(data_in)))]

# filtering out relevant data
require(dplyr)
# d2 <- filter(d1, did == "ef37fc07aee31d87b386a408e0e4651e00486618" | did == "bba79383d17854e4d57cb8452d0a2d1ea6b4255b", gdata.id == "do_30093446" | gdata.id == "do_30103866" | gdata.id == "do_30103869" | gdata.id == "do_30103906" | gdata.id == "do_30103925")
d2 <- filter(d1, did == "ef37fc07aee31d87b386a408e0e4651e00486618" | did == "bba79383d17854e4d57cb8452d0a2d1ea6b4255b")

## Getting all the result values for each qid
require(reshape2)
d3 <- melt(d2, id.vars = c("uid","did","date","edata.eks.qid","edata.eks.length","edata.eks.pass","eid","gdata.id","udata.standard"), na.rm = T)
d3 <- subset(d3, value != "")

## writing the output
write.csv(d3, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project Assesment Data for Two Devices Jan 2017/data_output.csv",row.names = F)

############################# End of Code ################################