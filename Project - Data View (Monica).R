#####################################################
############### Project - Data View  ################
#####################################################

## Required libraries
library(dplyr)
library(reshape2)

## Input Data
data_in <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - Data View (Monica)/oa_feb(9-12)_csv.csv", header = T, stringsAsFactors = F)
data_ge_cp <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Feb2017_GE_CREATE_PROFILE(1Feb17-12Feb17).csv", header = T, stringsAsFactors = F)
cname <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - Data View (Monica)/child name.csv", header = T, stringsAsFactors = F)

# getting the child handle name and age from data_ge_cp
ge_cp <- unique(subset(data_ge_cp, select = c(uid,edata.eks.age)))
cname1 <- unique(subset(cname, select = c(uid..Descending,udata.handle..Descending)))

data1 <- merge(data_in,cname1,by.x=c("uid"),by.y=c("uid..Descending"),all.x = T)
data1 <- merge(data1,ge_cp,by=c("uid"),all.x=T)
data1 <- rename(data1, child_name = udata.handle..Descending)
data1 <- rename(data1, child_age = edata.eks.age)
rm(ge_cp,cname1)

## creating date field
data1$dummy <- strtrim(data1$ts, width = 19)
data1$date <- as.Date(data1$dummy, format = "%Y-%m-%dT%H:%M:%S")
data1$dummy <- NULL

# selecting relevant variables
d1 <- data1[, c(which(colnames(data1) %in% c("uid","child_name","child_age","did","date","edata.eks.qid","edata.eks.pass","gdata.id","edata.eks.qtitle")),grep("resvalues", colnames(data1)))]

# creating a single result column having all the options selected
d1 <- lapply(d1, function(x) {gsub("#N/A", NA, x)})
d1 <- as.data.frame(d1)
d1$results <- do.call(paste, c(d1[,grep("resvalues", colnames(d1))],sep=","))
d1$results <- gsub("NA,","",d1$results)
d1$results <- gsub(",NA","",d1$results)
d1$results <- gsub("NA","",d1$results)

# filtering the relevant tagged devices
require(reshape2)
t1 <- data_in[, c(which(colnames(data_in) %in% c("uid","did","date")),grep("tags.genie", colnames(data_in)))]
t2 <- melt(t1, id.vars = c("uid","did","date"), na.rm = T)
t3 <- subset(t2, value == "934742694d73dd3d3916dde699e7fa8177b72f3b")
did1 <- unique(t3$did)
require(dplyr)
d2 <- filter(d1, did %in% did1)

# getting filtered data out
d3 <- d2[, c(which(colnames(d2) %in% c("uid","child_name","child_age","date","edata.eks.qid","gdata.id","edata.eks.qtitle","results")))]
require(dplyr)
d3 <- arrange(d3, uid, date, gdata.id, edata.eks.qid)
d3 <- filter(d3, results != "")

# getting the final list of data
d4 <- d3[c("uid","child_name","child_age","gdata.id","edata.eks.qid","edata.eks.qtitle","results")]
d4 <- rename(d4, content_id = gdata.id, question_id = edata.eks.qid, question_title = edata.eks.qtitle, option_selected = results)

# writing the final file
write.csv(d4, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - Data View (Monica)/Data View_15Feb2017.csv", row.names = F)

################### End of Code #####################
