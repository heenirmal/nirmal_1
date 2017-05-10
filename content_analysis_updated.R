############ libraries ##########
library(reshape2)
library(doBy)
library(data.table)
library(plyr)
library(lubridate)
library(xlsx)
############ loading the input data############
setwd("D:/mafoi/ekstep/works/data_mapping/all students and content/inputs")
kibana <- fread("kibana.csv",header = T,stringsAsFactors = F)
tags <- fread("tags_tution.csv",header = T,stringsAsFactors = F)
kibana <- as.data.frame(kibana)
tags <- as.data.frame(tags)
View(tags)
oa1 <- fread("Jan2017_OE_ASSESS.csv",header = T,stringsAsFactors = F)
oa2 <- fread("Feb_1_OE_ASSESS.csv",header = T,stringsAsFactors = F)
oa3 <- fread("Feb_2OE_ASSESS.csv",header = T,stringsAsFactors = F)
oa1 <- as.data.frame(oa1)
oa2 <- as.data.frame(oa2)
oa3 <- as.data.frame(oa3)
oa <- rbind.fill(oa1,oa2,oa3)
rm(oa1,oa2,oa3)
################# Filtering oe_Assess ############
oa_fil <- oa[,which(names(oa) %in% c("ts","uid","gdata.id","did","edata.eks.qid","edata.eks.length","edata.eks.pass"))]
dummy <- strtrim(oa_fil$ts,width = 19)
oa_fil$date <- as.Date(dummy,format = "%Y-%m-%dT%H:%M:%S")
rm(oa)
################# Date filtering #################
oa_fil <- subset(oa_fil,date>="2016-12-31" & date<="2017-02-17")
oa_fil_date <- oa_fil[,c("ts","uid","gdata.id","did","edata.eks.qid","edata.eks.length","edata.eks.pass","date")]
oa_fil_attemps <- summaryBy(edata.eks.qid ~ uid + edata.eks.qid,data = oa_fil_date,FUN = length)
oa_fil_final <- merge(oa_fil_date,oa_fil_attemps,by=c("uid","edata.eks.qid"))
################ Filtering Kibana data #############
kib1 <- subset(kibana,kibana$`tags.genie: Descending` == tags$tags[1])
kib_final <- rbind(kib1)
for(i in (2:nrow(tags))){
  kib1 <- subset(kibana,kibana$`tags.genie: Descending` == tags$tags[i])
  kib_final <- rbind(kib_final,kib1)
}

View(kib_final)
kib_final <- kib_final[!(is.na(kib_final$`Sum of edata.eks.timeSpent`)),]
names(kib_final)[c(3,9)] <- c("gdata","time")
kib_final <- as.data.frame(kib_final)
kib_final1 <- kib_final[,c("gdata","tags.genie: Descending","udata.handle: Descending","udata.standard: Descending","time")]
kib_final1 <- merge(kib_final1,tags,by.x = "tags.genie: Descending",by.y = "tags" )
kib_final1$tagsid_names <- paste(kib_final1$`tags.genie: Descending`,kib_final1$tags_name,sep = "(Tag_name)")
kib_final$tagsid_names <- kib_final1$tagsid_names
kib_final$`tags.genie: Descending` = NULL
kib_final1$`tags.genie: Descending` = NULL
kib_final1$tags_name = NULL
kib_final1 <- kib_final1[!is.na(kib_final1$time),]
kib_final1$time <- as.integer(kib_final1$time)
############### Merging Kibana and OE_ASSESS data #######
data_final <- merge(kib_final,oa_fil_final,by.x = c("dimensions.did: Descending","gdata","uid: Descending"),by.y = c("did","gdata.id","uid"))
names(data_final)
data_final <- as.data.frame(data_final)
data_final <- data_final[(data_final$`udata.handle: Descending` != ""),]
data_final <- data_final[!(is.na(data_final$time)),]

oa_final <- data_final[,c("gdata","tagsid_names","udata.handle: Descending","ts","edata.eks.qid","edata.eks.length","edata.eks.pass","edata.eks.qid.length")]
names(data_final)
############### Extracting inputs #######################
data_inputs <- data_final[,c("gdata","date")]
data_inputs <- data_inputs[order(data_inputs$date),]
data_inputs <- data_inputs[!duplicated(data_inputs$gdata),]
names(data_inputs) <- c("gdata","date")
kibana$`Sum of edata.eks.timeSpent` <- as.integer(kibana$`Sum of edata.eks.timeSpent`)
names(kib_final)[c(3,9)] <- c("gdata","time")
kib_final$time <- as.integer(kib_final$time)
sum(kib_final$time,na.rm = T)
time_summary <- summaryBy(time ~ gdata,data = kib_final,FUN = sum,na.rm = T)
View(time_summary)
data_inputs <- merge(time_summary,data_inputs,by=c("gdata"),all.x = T)

sum(as.integer(kib_final1$time),na.rm = T)
################## writing output files ####################
setwd("D:/mafoi/ekstep/works/data_mapping/all students and content/inputs")
write.csv(kib_final1,"kibana_final(jan-feb).csv",row.names = F)
write.csv(oa_final,"oa_final(jan-feb).csv",row.names = F)
write.csv(data_inputs,"inputs(jan-feb).csv",row.names = F)


################# Melting for tag values ############
oa_fil_ml <- melt(oa_fil,id.vars = c("ts","uid","gdata.id","did","edata.eks.qid","edata.eks.length","edata.eks.pass"))
oa_fil_ml$variable = NULL
################ Device filtering ################
setwd("D:/mafoi/ekstep/works/data_mapping/all students and content/inputs")
devices <- read.csv("Device IDs.csv",header = T,stringsAsFactors = F)
View(devices)
oa_fil_dd <- subset(oa_fil_date,oa_fil_date$did == devices[1,1])
oa_devices <- rbind(oa_fil_dd)
oa_fil$did[1:10]
for(i in (2:nrow(devices))){
  oa_fil_dd1 <- subset(oa_fil_date,did == devices[i,1])
  oa_devices <- rbind(oa_devices,oa_fil_dd1)
  
}

############### Extracting the mapped did ##################
oa_did_all <- oa_devices[,c("value","did")]
oa_did <- oa_did_all[!duplicated(oa_did_all),]
View(oa_did)
write.csv(oa,"oa_feb.csv",row.names = F)
devices <- devices[!duplicated(devices),]
unique(devices$Did)
################ Merging device and tags ####################
oa_devices <- merge(devices,oa_did,by.x = "Did",by.y = "value",all.y = T)
write.csv(oa_devices,"mapped_devices.csv",row.names = F)
View(oa_devices)
write.csv(oa_devices,"oa_feb(6-12).csv",row.names = F)

############### summary ###################################
oa_num_att <- summaryBy(oa_devices$edata.eks.qid ~ oa_devices$uid + oa_devices$edata.eks.qid ,data = oa_devices,FUN = length)
oa_final <- merge(oa_devices,oa_num_att,by = c("uid","edata.eks.qid"),all.x = T)
View(oa_final)
names(oa_final)[9] <- "no.attempts"
write.csv(oa_final,"oa_feb(6-12).csv",row.names = F)

############# For the tution data ######################
library(xlsx)
tution_1 <- read.csv("kibana_inputs.csv",header = T,stringsAsFactors = F)
tution_2 <- read.csv("tags_tution.csv",header = T,stringsAsFactors = F)

tution_fil <- subset(tution_1,tution_1$tags.genie..Descending == tution_2$tags[1])
tution_fil_final <- rbind.fill()

