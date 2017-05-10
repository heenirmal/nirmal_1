######################## ASER Report Analysis ######################
remove(list = ls())
library(doBy)
library(dplyr)
library(reshape2)
library(zoo)
######################## Loading the inputs #########################
setwd("D:/mafoi/ekstep/works/acer_reports/inputs")
oa_oi <- read.csv("OE_LEVEL_SET_ACER.csv",header = T,stringsAsFactors = F)
oa_oi_fil <- oa_oi[,c(which(colnames(oa_oi) %in% "did"),grep("tags",colnames(oa_oi)))]
oa_oi_tags <- melt(oa_oi_fil,id.vars = c("did"))
oa_oi_tags$variable = NULL

oa_oi_fil1 <- subset(oa_oi_tags,oa_oi_tags$value == "7b4396e2882a0b235ea8557ddf4a473bf7c28e92")
oa_oi_tag_devices <- oa_oi_fil1[!duplicated(oa_oi_fil1$did),]
oa_oi_uid <- oa_oi[,c("did","uid")]
did <- unique(oa_oi_tag_devices$did)
oa_oi_uid_fil <- oa_oi_uid[oa_oi_uid$did == did[1] | oa_oi_uid$did == did[2] | oa_oi_uid$did == did[3],]
unique(oa_oi_uid_fil$uid)
write.csv(oa_oi_tag_devices,"tagged_devices.csv")


######################### ASER Analysis ############################
################ loading child info #######################
setwd("D:/mafoi/ekstep/works/acer_reports/inputs")
oa_oi <- read.csv("OE_LEVEL_SET_ACER.csv",header = T,stringsAsFactors = F)
udata_df <- read.csv("oct_pilot_per day per device per content.csv",header = T,stringsAsFactors = F)
(oa_oi$)
aser_df <- oa_oi[,c("uid","ts","eid","sid","edata.eks.qid","edata.eks.category","edata.eks.current","edata.eks.max","did","udata.age_completed_years","udata.standard","edata.eks.length")]
dummy <- strtrim(aser_df$ts,width = 19)
aser_df$timestamp<- as.POSIXct(dummy,format = "%Y-%m-%dT%H:%M:%S")
aser_df$ts = NULL
udata_info <- udata_df[,c("uid..Descending","udata.handle..Descending")]
udata_info <- udata_info[!is.na(udata_info$udata.handle..Descending),]
udata_info <- udata_info[(udata_info$udata.handle..Descending != ""),]
udata_info <- udata_info[!duplicated(udata_info$uid..Descending),]
names(udata_info) <- c("uid","student_name")


############# Merging udata info with oa #################
oa_oi_df <- merge(aser_df,udata_info,by=c("uid"))
did <- unique(oa_oi_tag_devices$did)
oa_oi_tags <- oa_oi_df[oa_oi_df$did == did[1] | oa_oi_df$did == did[2] | oa_oi_df$did == did[3] ,]
oa_oi_un_tags <- oa_oi_df[oa_oi_df$did != did[1] & oa_oi_df$did != did[2] & oa_oi_df$did != did[3] ,]

#oa_oi_tags <- oa_oi_un_tags
oa_oi_tags <- oa_oi_tags[!duplicated(oa_oi_tags),]
oa_oi_tags <- oa_oi_tags[order(oa_oi_tags$did,oa_oi_tags$timestamp),]
View(oa_oi_tags)
#oa_oi_tags <- oa_oi_tags[-(2070),]
View(oa_oi_tags)
oa_oi_tags$edata.eks.category <- na.locf(oa_oi_tags[,which(names(oa_oi_tags) %in% c("edata.eks.category"))], fromLast = TRUE)

oa_oi_tags$edata.eks.current <- na.locf(oa_oi_tags[,which(names(oa_oi_tags) %in% c("edata.eks.current"))], fromLast = TRUE)
oa_oi_tags$edata.eks.max <- na.locf(oa_oi_tags[,which(names(oa_oi_tags) %in% c("edata.eks.max"))], fromLast = TRUE)

unique(oa_oi_tags$eid)
oa_oi_tags_f1 <- subset(oa_oi_tags,oa_oi_tags$eid != "OE_LEVEL_SET")
names(oa_oi_tags_f1)
oa_oi_tags_f2 <- summaryBy(edata.eks.length ~  timestamp + student_name + udata.age_completed_years + edata.eks.category + edata.eks.current + did,data = oa_oi_tags_f1,fun=sum)
oa_oi_tags_f3 <- summaryBy(edata.eks.length ~   student_name + udata.age_completed_years + edata.eks.category + edata.eks.current + did,data = oa_oi_tags_f1,fun=sum)
oa_oi_tags_f2 <- oa_oi_tags_f2 [order(oa_oi_tags_f2$student_name,oa_oi_tags_f2$timestamp,decreasing = T),]
oa_oi_ts_math <- oa_oi_tags_f2[oa_oi_tags_f2$edata.eks.category == "MATH",]
oa_oi_ts_math <- oa_oi_ts_math[!duplicated(oa_oi_ts_math$student_name),]
oa_oi_ts_math <- oa_oi_ts_math[,c("timestamp","student_name","edata.eks.category")]
View(oa_oi_ts_math)
oa_oi_ts_red <- oa_oi_tags_f2[oa_oi_tags_f2$edata.eks.category == "READING",]
oa_oi_ts_red <- oa_oi_ts_red[!duplicated(oa_oi_ts_red$student_name),]
oa_oi_ts_red <- oa_oi_ts_red[,c("timestamp","student_name","edata.eks.category")]
View(oa_oi_ts_red)
unique(oa_oi_tags_f3$edata.eks.category)
oa_oi_tags_m1 <- oa_oi_tags_f3[oa_oi_tags_f3$edata.eks.category == "MATH",]
oa_oi_tags_m1 <- merge(oa_oi_tags_m1,oa_oi_ts_math,by=c("student_name","edata.eks.category"))
oa_oi_tags_m1$edata.eks.category = NULL
names(oa_oi_tags_m1)[3] <- "MATH LEVEL"

oa_oi_tags_m2 <- oa_oi_tags_f3[oa_oi_tags_f3$edata.eks.category == "READING",]
oa_oi_tags_m2 <- merge(oa_oi_tags_m2,oa_oi_ts_red,by=c("student_name","edata.eks.category"))
oa_oi_tags_m2$edata.eks.category = NULL
names(oa_oi_tags_m2)[3] <- "READING LEVEL"

oa_oi_final <- rbind.fill(oa_oi_tags_m1,oa_oi_tags_m2)
oa_oi_final <- oa_oi_final[order(oa_oi_final$student_name,oa_oi_final$timestamp),]
names(oa_oi_final)
oa_oi_final <- oa_oi_final[,c("timestamp","did","student_name","udata.age_completed_years","MATH LEVEL","READING LEVEL","edata.eks.length.")]
names(oa_oi_final) <- c("timestamp","did","student_name","age","MATH LEVEL","READING LEVEL","time_spent")
oa_oi_final$time_spent <- as.integer(oa_oi_final$time_spent)
oa_oi_final$`MATH LEVEL` <- ifelse(is.na(oa_oi_final$`MATH LEVEL`),"",oa_oi_final$`MATH LEVEL`)
oa_oi_final$`READING LEVEL` <- ifelse(is.na(oa_oi_final$`READING LEVEL`),"",oa_oi_final$`READING LEVEL`)
View(oa_oi_final)
write.csv(oa_oi_final,"ASER_Analysis(tags).csv",row.names = F)
View(oa_oi_final)
