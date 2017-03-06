################################################################
##################### EkStep - Project #########################
############### GENIE FUNNEL and USAGE METRICS #################
################################################################

## Required libraries
library(plyr)
library(sqldf)
library(DataCombine)
library(doBy)
library(reshape2)
library(dplyr)

## Setting the working directory for data
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT/")

#### Segregating Tagged and Untagged devices ####
sep16 <- read.csv(file = "Sep2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
oct16 <- read.csv(file = "Oct2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
nov16 <- read.csv(file = "Nov2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
dec16 <- read.csv(file = "Dec2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
jan17 <- read.csv(file = "Jan2017_GE_INTERACT.csv", header = T, stringsAsFactors = F)
feb17 <- read.csv(file = "Feb2017_GE_INTERACT(1Feb17-26Feb17).csv", header = T, stringsAsFactors = F)

## Getting all the tags information for all the data
tags_sep16 <- sep16[, c(which(colnames(sep16) == "did"),grep("genie", colnames(sep16)))]
tags_oct16 <- oct16[, c(which(colnames(oct16) == "did"),grep("genie", colnames(oct16)))]
tags_nov16 <- nov16[, c(which(colnames(nov16) == "did"),grep("genie", colnames(nov16)))]
tags_dec16 <- dec16[, c(which(colnames(dec16) == "did"),grep("genie", colnames(dec16)))]
tags_jan17 <- jan17[, c(which(colnames(jan17) == "did"),grep("genie", colnames(jan17)))]
tags_feb17 <- feb17[, c(which(colnames(feb17) == "did"),grep("genie", colnames(feb17)))]

tags_sep16 <- tags_sep16[!duplicated(tags_sep16),]
tags_oct16 <- tags_oct16[!duplicated(tags_oct16),]
tags_nov16 <- tags_nov16[!duplicated(tags_nov16),]
tags_dec16 <- tags_dec16[!duplicated(tags_dec16),]
tags_jan17 <- tags_jan17[!duplicated(tags_jan17),]
tags_feb17 <- tags_feb17[!duplicated(tags_feb17),]

## Combining all the devices
library(plyr)
tags <- rbind.fill(tags_sep16,tags_oct16,tags_nov16,tags_dec16,tags_jan17,tags_feb17)

## Getting all the tags in single row
library(reshape2)
tags1 <- melt(tags, id.vars = c("did"), na.rm = T)
tags1$variable <- NULL
tags2 <- tags1[!duplicated(tags1),]
tags2$tag_flag <- ifelse(tags2$value == "", "Untagged", "Tagged")
tags3 <- arrange(tags2, did, tag_flag)
tags3$value <- NULL
tags3 <- tags3[!duplicated(tags3$did),]

## Getting other did for tag status
me <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/ME_SESSION_SUMMARY_VISUALIZATION_1Sep16-26Feb17.csv", header = T, stringsAsFactors = F)
me$tag_flag <- ifelse(me$tags.genie..Descending == "", "Untagged", "Tagged")
me1 <- subset(me, select = c(dimensions.did..Descending,tag_flag))
colnames(me1) <- c("did","tag_flag")
me1 <- arrange(me1, did, tag_flag)
me1 <- me1[!duplicated(me1$did),]

## Appending the two datasets
tags4 <- rbind(tags3,me1)
tags4 <- arrange(tags4, did, tag_flag)
tags4 <- tags4[!duplicated(tags4$did),]

## Saving tag_flag information for all did
write.csv(tags4, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets/tag_flag.csv", row.names = F)

rm(list = ls())
##########################################

#### Segregating New and Existing devices ####

## Getting list of all active devices from ME_GENIE_LAUNCH_SUMMARY
me <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/Visualization-ME_GENIE_LAUNCH_SUMMARY_1sep16-26feb17.csv", header = T, stringsAsFactors = F)
me$date <- as.Date(me$date_time)
me <- subset(me, select = c(dimensions.did..Descending,date))
me <- me[!duplicated(me),]
colnames(me) <- c("did","date")

## Getting list of OnBoarding events for all did
ob <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/Visualization-OnBoarding_1sep16-26feb17.csv", header = T, stringsAsFactors = F)
ob$date <- as.Date(ob$date_time)
ob <- subset(ob, select = c(did..Descending,date))
ob <- ob[!duplicated(ob),]
colnames(ob) <- c("did","date")
ob <- arrange(ob,did,date)
ob <- ob[!duplicated(ob$did),]
ob$device_type <- "new"

## filtering out the data for GE_INTERACT event
sep16 <- read.csv(file = "Sep2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
oct16 <- read.csv(file = "Oct2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
nov16 <- read.csv(file = "Nov2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
dec16 <- read.csv(file = "Dec2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
jan17 <- read.csv(file = "Jan2017_GE_INTERACT.csv", header = T, stringsAsFactors = F)
feb17 <- read.csv(file = "Feb2017_GE_INTERACT(1Feb17-26Feb17).csv", header = T, stringsAsFactors = F)

require(plyr)
d2 <- rbind.fill(sep16,oct16,nov16,dec16,jan17,feb17)

# creating date field
d2$dummy <- strtrim(d2$ts, width = 19)
d2$date <- as.Date(d2$dummy, format = "%Y-%m-%dT%H:%M:%S")
d2$dummy <- NULL
d2 <- subset(d2,select = c(did,date))
d2 <- d2[!duplicated(d2),]

# flagging new and existing devices
d3 <- rbind(d2,me)
d3 <- d3[!duplicated(d3),]
d3 <- merge(d3,ob, by=c("did","date"), all.x = T)
d3$device_type <- ifelse(is.na(d3$device_type),"existing","new")

# checking for devices which have a existing tag before a new tag are flagged as existing
did <- unique(d3$did)

con <- c()
for(i in did)
{
  a <- subset(d3, did == i)
  a <- arrange(a, date)
  if(a[1,3] == "existing")
  {
    a$device_type <- "existing"
  }
  con <- rbind(con,a)
}

## Saving device_type information for all did
write.csv(con, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets/device_type.csv", row.names = F)

rm(list = ls())
##############################################

#### Getting data for GE_INTERACT
sep16 <- read.csv(file = "Sep2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
oct16 <- read.csv(file = "Oct2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
nov16 <- read.csv(file = "Nov2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
dec16 <- read.csv(file = "Dec2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
jan17 <- read.csv(file = "Jan2017_GE_INTERACT.csv", header = T, stringsAsFactors = F)
feb17 <- read.csv(file = "Feb2017_GE_INTERACT(1Feb17-26Feb17).csv", header = T, stringsAsFactors = F)

## Getting device_type for the data
device_type <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets/device_type.csv", header = T, stringsAsFactors = F)

## Getting tag_flag for the data
tag_flag <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets/tag_flag.csv", header = T, stringsAsFactors = F)
d1_sep <- merge(sep16,tag_flag,by="did",all.x = T)
d1_oct <- merge(oct16,tag_flag,by="did",all.x = T)
d1_nov <- merge(nov16,tag_flag,by="did",all.x = T)
d1_dec <- merge(dec16,tag_flag,by="did",all.x = T)
d1_jan <- merge(jan17,tag_flag,by="did",all.x = T)
d1_feb <- merge(feb17,tag_flag,by="did",all.x = T)

## creating date_time field
d1_sep$dummy <- strtrim(d1_sep$ts, width = 19)
d1_sep$date_time <- as.POSIXct(d1_sep$dummy, format = "%Y-%m-%dT%H:%M:%S")
d1_sep$dummy <- NULL

d1_oct$dummy <- strtrim(d1_oct$ts, width = 19)
d1_oct$date_time <- as.POSIXct(d1_oct$dummy, format = "%Y-%m-%dT%H:%M:%S")
d1_oct$dummy <- NULL

d1_nov$dummy <- strtrim(d1_nov$ts, width = 19)
d1_nov$date_time <- as.POSIXct(d1_nov$dummy, format = "%Y-%m-%dT%H:%M:%S")
d1_nov$dummy <- NULL

d1_dec$dummy <- strtrim(d1_dec$ts, width = 19)
d1_dec$date_time <- as.POSIXct(d1_dec$dummy, format = "%Y-%m-%dT%H:%M:%S")
d1_dec$dummy <- NULL

d1_jan$dummy <- strtrim(d1_jan$ts, width = 19)
d1_jan$date_time <- as.POSIXct(d1_jan$dummy, format = "%Y-%m-%dT%H:%M:%S")
d1_jan$dummy <- NULL

d1_feb$dummy <- strtrim(d1_feb$ts, width = 19)
d1_feb$date_time <- as.POSIXct(d1_feb$dummy, format = "%Y-%m-%dT%H:%M:%S")
d1_feb$dummy <- NULL

## getting entire language setting event data
require(plyr)
d1 <- rbind.fill(d1_sep,d1_oct,d1_nov,d1_dec,d1_jan,d1_feb)

rm(d1_feb,d1_jan,d1_dec,d1_nov,d1_oct,d1_sep)

##### Part 1 - search events #####

## filtering out the data for search events
d2 <- subset(d1,edata.eks.subtype == "SearchPhrase")
d2 <- subset(d2,select = c(did,date_time,tag_flag,edata.eks.subtype))

## setting working directory for ME_GENIE_LAUNCH_SUMMARY
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data")

#### Getting data for GE_INTERACT
ME_Genie_Launch <- read.csv(file = "Visualization-ME_GENIE_LAUNCH_SUMMARY_1sep16-26feb17.csv", header = T, stringsAsFactors = F)
ME_Genie_Launch$context.date_range.to.per.second <- NULL
ME_Genie_Launch$context.date_range.from.per.second <- NULL
colnames(ME_Genie_Launch) <- c("did","timespent","number_of_sessions","date_time")
ME_Genie_Launch$date_time <- as.POSIXct(ME_Genie_Launch$date_time)
ME_Genie_Launch$timespent <- round(as.numeric(gsub(",","",ME_Genie_Launch$timespent)),0)
ME_Genie_Launch$end_time <- ME_Genie_Launch$date_time + ME_Genie_Launch$timespent

# filtering out Launch summary session with less than 5 seconds
ME_Genie_Launch <- subset(ME_Genie_Launch,timespent >= 5)

## finding search event happening in a session or not
require(sqldf)
d3 <- sqldf("SELECT ME_Genie_Launch.did, timespent, number_of_sessions, ME_Genie_Launch.date_time, tag_flag
  FROM ME_Genie_Launch LEFT JOIN d2 
            ON d2.did = ME_Genie_Launch.did AND d2.date_time BETWEEN ME_Genie_Launch.date_time AND ME_Genie_Launch.end_time")
d4 <- d3[!duplicated(d3),]
d4$search <- ifelse(is.na(d4$tag_flag),0,1)
d4$tag_flag <- NULL

## Making d4 in usable format
d4$date <- as.Date(as.character(d4$date_time))
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(as.POSIXct(d4$date_time),"%b%Y")
d4$week <- as.numeric(format.Date(as.POSIXct(d4$date_time),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                          ifelse(d4$week == unique(d4$week)[2], "Week 1",
                                 ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                        ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag,by="did",all.x = T)
d4 <- merge(d4,device_type,by=c("did","date"),all.x = T)

### Overall Devices
overall_devices <- d4
a1 <- summaryBy(number_of_sessions + search + timespent ~ weeknum, data = overall_devices, FUN = sum)
a2 <- summaryBy(number_of_sessions + search + timespent ~ month, data = overall_devices, FUN = sum)

a1$search_per_visit <- round(a1$search.sum / a1$number_of_sessions.sum,4)
a1$minutes_per_visit <- round(a1$timespent.sum / (60*a1$number_of_sessions.sum),2)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$search_per_visit <- round(a2$search.sum / a2$number_of_sessions.sum,4)
a2$minutes_per_visit <- round(a2$timespent.sum / (60*a2$number_of_sessions.sum),2)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
a1 <- summaryBy(number_of_sessions + search + timespent ~ weeknum, data = tag_devices, FUN = sum)
a2 <- summaryBy(number_of_sessions + search + timespent ~ month, data = tag_devices, FUN = sum)

a1$search_per_visit <- round(a1$search.sum / a1$number_of_sessions.sum,4)
a1$minutes_per_visit <- round(a1$timespent.sum / (60*a1$number_of_sessions.sum),2)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$search_per_visit <- round(a2$search.sum / a2$number_of_sessions.sum,4)
a2$minutes_per_visit <- round(a2$timespent.sum / (60*a2$number_of_sessions.sum),2)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
a3 <- summaryBy(number_of_sessions + search + timespent ~ weeknum, data = untag_devices, FUN = sum)
a4 <- summaryBy(number_of_sessions + search + timespent ~ month, data = untag_devices, FUN = sum)

a3$search_per_visit <- round(a3$search.sum / a3$number_of_sessions.sum,4)
a3$minutes_per_visit <- round(a3$timespent.sum / (60*a3$number_of_sessions.sum),2)
a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
a4$search_per_visit <- round(a4$search.sum / a4$number_of_sessions.sum,4)
a4$minutes_per_visit <- round(a4$timespent.sum / (60*a4$number_of_sessions.sum),2)
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
a5 <- summaryBy(number_of_sessions + search + timespent ~ weeknum, data = new_devices, FUN = sum)
a6 <- summaryBy(number_of_sessions + search + timespent ~ month, data = new_devices, FUN = sum)

a5$search_per_visit <- round(a5$search.sum / a5$number_of_sessions.sum,4)
a5$minutes_per_visit <- round(a5$timespent.sum / (60*a5$number_of_sessions.sum),2)
a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
a6$search_per_visit <- round(a6$search.sum / a6$number_of_sessions.sum,4)
a6$minutes_per_visit <- round(a6$timespent.sum / (60*a6$number_of_sessions.sum),2)
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
a7 <- summaryBy(number_of_sessions + search + timespent ~ weeknum, data = existing_devices, FUN = sum)
a8 <- summaryBy(number_of_sessions + search + timespent ~ month, data = existing_devices, FUN = sum)

a7$search_per_visit <- round(a7$search.sum / a7$number_of_sessions.sum,4)
a7$minutes_per_visit <- round(a7$timespent.sum / (60*a7$number_of_sessions.sum),2)
a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
a8$search_per_visit <- round(a8$search.sum / a8$number_of_sessions.sum,4)
a8$minutes_per_visit <- round(a8$timespent.sum / (60*a8$number_of_sessions.sum),2)
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4a <- rbind.fill(a0,a12,a34,a56,a78)

rm(d2,d3,d4,a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 2 - language settings #####

## filtering out the data for language settings events
d2 <- subset(d1,edata.eks.subtype == "LanguageSettings-Success")
d2 <- subset(d2,select = c(did,date_time,tag_flag,edata.eks.id))

## creating date field and sorting data
d2$date <- as.Date(as.character(d2$date_time))
d2 <- arrange(d2, did, desc(date_time))
d2$date_time <- NULL
d2 <- d2[!duplicated(d2[which(names(d2) == c("did","date"))]),]
d2$lan_not_en <- ifelse(d2$edata.eks.id == "en",0,1)
d2$lan_en <- ifelse(d2$edata.eks.id == "en",1,0)
d2 <- arrange(d2, did, date)

require(DataCombine)
con <- c()

for(i in unique(d2$did))
{
  a <- subset(d2, did == i)
  if(nrow(a) == 1)
  {
    a$end_date <- as.Date("2017-02-26")
  } else {
    a <- slide(a, Var = "date", NewVar = "end_date", slideBy = 1)
    a[is.na(a)] <- as.Date("2017-02-27")
    a$end_date <- a$end_date - 1
  }
  con <- rbind(con,a)
}

rm(a,i)

# write the language setting data
write.csv(con, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets/lang_setting.csv", row.names = F)

## getting language information for each did in the session summary
d2 <- con
ME_Genie_Launch$date <- as.Date(as.character(ME_Genie_Launch$date_time))
ME_G1 <- subset(ME_Genie_Launch, select = c("did","date"))
ME_G1 <- ME_G1[!duplicated(ME_G1),]

require(sqldf)
d4 <- sqldf("SELECT ME_G1.did, ME_G1.date, lan_en
            FROM ME_G1 LEFT JOIN d2 
            ON d2.did = ME_G1.did AND ME_G1.date BETWEEN d2.date AND d2.end_date")

d4 <- subset(d4, is.na(lan_en) == F)

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag,by="did",all.x = T)
d4 <- merge(d4,device_type,by=c("did","date"),all.x = T)
d4$lan_not_en <- ifelse(d4$lan_en == 0,1,0)

### Overall Devices
overall_devices <- d4
a1 <- summaryBy(lan_en + lan_not_en ~ weeknum, data = overall_devices, FUN = sum)
a2 <- summaryBy(lan_en + lan_not_en ~ month, data = overall_devices, FUN = sum)

a1$'% of devices with lang not set to eng' <- round(a1$lan_not_en.sum / (a1$lan_en.sum + a1$lan_not_en.sum),4)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$'% of devices with lang not set to eng' <- round(a2$lan_not_en.sum / (a2$lan_en.sum + a2$lan_not_en.sum),4)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
a1 <- summaryBy(lan_en + lan_not_en ~ weeknum, data = tag_devices, FUN = sum)
a2 <- summaryBy(lan_en + lan_not_en ~ month, data = tag_devices, FUN = sum)

a1$'% of devices with lang not set to eng' <- round(a1$lan_not_en.sum / (a1$lan_en.sum + a1$lan_not_en.sum),4)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$'% of devices with lang not set to eng' <- round(a2$lan_not_en.sum / (a2$lan_en.sum + a2$lan_not_en.sum),4)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
a3 <- summaryBy(lan_en + lan_not_en ~ weeknum, data = untag_devices, FUN = sum)
a4 <- summaryBy(lan_en + lan_not_en ~ month, data = untag_devices, FUN = sum)

a3$'% of devices with lang not set to eng' <- round(a3$lan_not_en.sum / (a3$lan_en.sum + a3$lan_not_en.sum),4)
a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
a4$'% of devices with lang not set to eng' <- round(a4$lan_not_en.sum / (a4$lan_en.sum + a4$lan_not_en.sum),4)
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
a5 <- summaryBy(lan_en + lan_not_en ~ weeknum, data = new_devices, FUN = sum)
a6 <- summaryBy(lan_en + lan_not_en ~ month, data = new_devices, FUN = sum)

a5$'% of devices with lang not set to eng' <- round(a5$lan_not_en.sum / (a5$lan_en.sum + a5$lan_not_en.sum),4)
a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
a6$'% of devices with lang not set to eng' <- round(a6$lan_not_en.sum / (a6$lan_en.sum + a6$lan_not_en.sum),4)
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
a7 <- summaryBy(lan_en + lan_not_en ~ weeknum, data = existing_devices, FUN = sum)
a8 <- summaryBy(lan_en + lan_not_en ~ month, data = existing_devices, FUN = sum)

a7$'% of devices with lang not set to eng' <- round(a7$lan_not_en.sum / (a7$lan_en.sum + a7$lan_not_en.sum),4)
a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
a8$'% of devices with lang not set to eng' <- round(a8$lan_not_en.sum / (a8$lan_en.sum + a8$lan_not_en.sum),4)
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4b <- rbind.fill(a0,a12,a34,a56,a78)
d4b$lan_en.sum <- NULL
d4b$lan_not_en.sum <- NULL

rm(con,d2,d4,ME_G1,a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 3 - number of content played, time of play for each content, number of content rated, % of content rated #####

## Getting number of content played and time of play for each content
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets")
sess_len <- read.csv(file = "session_length.csv", header = T, stringsAsFactors = F)

# filtering data- removing all content which are played less than 5 seconds 
#                 capping anything above 30min to 30min
sess_len1 <- subset(sess_len, session_length >= 5)
sess_len1$session_length <- ifelse(sess_len1$session_length >= 1800,1800,sess_len1$session_length)

# creating date field for sess_len1
sess_len1$date <- as.Date(as.character(sess_len1$date_time))

## Getting GE_FEEDBACK data
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_FEEDBACK")

f_feb <- read.csv(file = "Feb2017_GE_FEEDBACK(1Feb17-26Feb17).csv", header = T, stringsAsFactors = F)
f_jan <- read.csv(file = "Jan2017_GE_FEEDBACK.csv", header = T, stringsAsFactors = F)
f_dec <- read.csv(file = "Dec2016_GE_FEEDBACK.csv", header = T, stringsAsFactors = F)
f_nov <- read.csv(file = "Nov2016_GE_FEEDBACK.csv", header = T, stringsAsFactors = F)
f_oct <- read.csv(file = "Oct2016_GE_FEEDBACK.csv", header = T, stringsAsFactors = F)
f_sep <- read.csv(file = "Sep2016_GE_FEEDBACK.csv", header = T, stringsAsFactors = F)

## filtering and getting unique content feedback for a did in a day
require(plyr)
feed <- rbind.fill(f_feb,f_jan,f_dec,f_nov,f_oct,f_sep)
feed$dummy <- strtrim(feed$ts, width = 19)
feed$date_time <- as.POSIXct(feed$dummy, format = "%Y-%m-%dT%H:%M:%S")
feed$dummy <- NULL
feed$date <- as.Date(as.character(feed$date_time))

feed <- subset(feed, select = c(did,date,edata.eks.context.id))
feed <- feed[!duplicated(feed),]
feed$con_rate <- 1

rm(f_feb,f_jan,f_dec,f_nov,f_oct,f_sep)

## Combining the data for content played and rated for tagged and untagged devices
d4 <- merge(sess_len1,feed, by.x = c("did","date","gdata.id"), by.y = c("did","date","edata.eks.context.id"), all = T)
d4$date_time <- NULL

## Getting tag flags for d4
d4 <- merge(d4,tag_flag, by = "did", all.x = T)
d4 <- subset(d4, gdata.id != "")
d4$ts <- NULL
d4 <- subset(d4, is.na(session_length) == F)
d4[is.na(d4)] <- 0

# summarizing the data at did and date level to get number of content played and length of play
require(doBy)
con_play <- summaryBy(gdata.id ~ did | date, data = d4, FUN = length)
len_play <- summaryBy(session_length ~ did | date, data = d4, FUN = sum)
con_rate <- summaryBy(con_rate ~ did | date, data = d4, FUN = sum)

d4 <- cbind(con_play,len_play$session_length.sum,con_rate$con_rate.sum)
colnames(d4) <- c("did","date","num_of_content_played","total_time_played_sec","num_of_content_rated")

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag,by="did",all.x = T)
d4 <- merge(d4,device_type,by=c("did","date"),all.x = T)

### Overall Devices
overall_devices <- d4
a1 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ weeknum, data = overall_devices, FUN = sum)
a2 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ month, data = overall_devices, FUN = sum)

a1$minutes_per_content <- round(a1$total_time_played_sec.sum / (60*a1$num_of_content_played.sum),4)
a1$'% of content played and rated' <- round(a1$num_of_content_rated.sum / a1$num_of_content_played.sum,4)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$minutes_per_content <- round(a2$total_time_played_sec.sum / (60*a2$num_of_content_played.sum),4)
a2$'% of content played and rated' <- round(a2$num_of_content_rated.sum / a2$num_of_content_played.sum,4)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
a1 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ weeknum, data = tag_devices, FUN = sum)
a2 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ month, data = tag_devices, FUN = sum)

a1$minutes_per_content <- round(a1$total_time_played_sec.sum / (60*a1$num_of_content_played.sum),4)
a1$'% of content played and rated' <- round(a1$num_of_content_rated.sum / a1$num_of_content_played.sum,4)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$minutes_per_content <- round(a2$total_time_played_sec.sum / (60*a2$num_of_content_played.sum),4)
a2$'% of content played and rated' <- round(a2$num_of_content_rated.sum / a2$num_of_content_played.sum,4)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
a3 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ weeknum, data = untag_devices, FUN = sum)
a4 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ month, data = untag_devices, FUN = sum)

a3$minutes_per_content <- round(a3$total_time_played_sec.sum / (60*a3$num_of_content_played.sum),4)
a3$'% of content played and rated' <- round(a3$num_of_content_rated.sum / a3$num_of_content_played.sum,4)
a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
a4$minutes_per_content <- round(a4$total_time_played_sec.sum / (60*a4$num_of_content_played.sum),4)
a4$'% of content played and rated' <- round(a4$num_of_content_rated.sum / a4$num_of_content_played.sum,4)
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
a5 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ weeknum, data = new_devices, FUN = sum)
a6 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ month, data = new_devices, FUN = sum)

a5$minutes_per_content <- round(a5$total_time_played_sec.sum / (60*a5$num_of_content_played.sum),4)
a5$'% of content played and rated' <- round(a5$num_of_content_rated.sum / a5$num_of_content_played.sum,4)
a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
a6$minutes_per_content <- round(a6$total_time_played_sec.sum / (60*a6$num_of_content_played.sum),4)
a6$'% of content played and rated' <- round(a6$num_of_content_rated.sum / a6$num_of_content_played.sum,4)
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
a7 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ weeknum, data = existing_devices, FUN = sum)
a8 <- summaryBy(num_of_content_played + total_time_played_sec + num_of_content_rated ~ month, data = existing_devices, FUN = sum)

a7$minutes_per_content <- round(a7$total_time_played_sec.sum / (60*a7$num_of_content_played.sum),4)
a7$'% of content played and rated' <- round(a7$num_of_content_rated.sum / a7$num_of_content_played.sum,4)
a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
a8$minutes_per_content <- round(a8$total_time_played_sec.sum / (60*a8$num_of_content_played.sum),4)
a8$'% of content played and rated' <- round(a8$num_of_content_rated.sum / a8$num_of_content_played.sum,4)
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4c <- rbind.fill(a0,a12,a34,a56,a78)

rm(len_play,sess_len,sess_len1,con_play,con_rate,feed,a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,d4,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 4 - new/existing user #####

## filtering out the data for GE_INTERACT event
d2 <- subset(d1,eid == "GE_INTERACT")
d2 <- subset(d2,select = c(did,uid,date_time,tag_flag,edata.eks.stageid))

## new users
d2_new <- subset(d2, edata.eks.stageid == "Genie-Home-OnBoardingScreen")
d2_new$date <- as.Date(as.character(d2_new$date_time))
d2_new$date_time <- NULL
d2_new <- d2_new[!duplicated(d2_new[which(names(d2_new) %in% c("uid","date"))]),]
d2_new <- subset(d2_new, uid != "")
d2_new$new_user <- 1
d2_new$did <- NULL
d2_new$tag_flag <- NULL
d2_new$edata.eks.stageid <- NULL

## existing users merged with new users
d2$date <- as.Date(as.character(d2$date_time))
d2$date_time <- NULL
d3 <- d2[!duplicated(d2[which(names(d2) %in% c("uid","date"))]),]
d3 <- subset(d3, uid != "")
d3 <- merge(d3,d2_new, by=c("uid","date"), all.x = T)
d3$new_user <- ifelse(is.na(d3$new_user),0,1)
d3$repeat_user <- ifelse(d3$new_user == 1,0,1)
d3$edata.eks.stageid <- NULL
d4 <- d3

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,device_type,by=c("did","date"),all.x = T)

### Overall Devices
overall_devices <- d4
a1 <- summaryBy(new_user + repeat_user ~ weeknum, data = overall_devices, FUN = sum)
a2 <- summaryBy(new_user + repeat_user ~ month, data = overall_devices, FUN = sum)

a1$'% of new users' <- round(a1$new_user.sum / (a1$new_user.sum + a1$repeat_user.sum),4)
a1$'% of repeat users' <- round(a1$repeat_user.sum / (a1$new_user.sum + a1$repeat_user.sum),4)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$'% of new users' <- round(a2$new_user.sum / (a2$new_user.sum + a2$repeat_user.sum),4)
a2$'% of repeat users' <- round(a2$repeat_user.sum / (a2$new_user.sum + a2$repeat_user.sum),4)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2)
### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
a1 <- summaryBy(new_user + repeat_user ~ weeknum, data = tag_devices, FUN = sum)
a2 <- summaryBy(new_user + repeat_user ~ month, data = tag_devices, FUN = sum)

a1$'% of new users' <- round(a1$new_user.sum / (a1$new_user.sum + a1$repeat_user.sum),4)
a1$'% of repeat users' <- round(a1$repeat_user.sum / (a1$new_user.sum + a1$repeat_user.sum),4)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$'% of new users' <- round(a2$new_user.sum / (a2$new_user.sum + a2$repeat_user.sum),4)
a2$'% of repeat users' <- round(a2$repeat_user.sum / (a2$new_user.sum + a2$repeat_user.sum),4)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
a3 <- summaryBy(new_user + repeat_user ~ weeknum, data = untag_devices, FUN = sum)
a4 <- summaryBy(new_user + repeat_user ~ month, data = untag_devices, FUN = sum)

a3$'% of new users' <- round(a3$new_user.sum / (a3$new_user.sum + a3$repeat_user.sum),4)
a3$'% of repeat users' <- round(a3$repeat_user.sum / (a3$new_user.sum + a3$repeat_user.sum),4)
a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
a4$'% of new users' <- round(a4$new_user.sum / (a4$new_user.sum + a4$repeat_user.sum),4)
a4$'% of repeat users' <- round(a4$repeat_user.sum / (a4$new_user.sum + a4$repeat_user.sum),4)
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
a5 <- summaryBy(new_user + repeat_user ~ weeknum, data = new_devices, FUN = sum)
a6 <- summaryBy(new_user + repeat_user ~ month, data = new_devices, FUN = sum)

a5$'% of new users' <- round(a5$new_user.sum / (a5$new_user.sum + a5$repeat_user.sum),4)
a5$'% of repeat users' <- round(a5$repeat_user.sum / (a5$new_user.sum + a5$repeat_user.sum),4)
a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
a6$'% of new users' <- round(a6$new_user.sum / (a6$new_user.sum + a6$repeat_user.sum),4)
a6$'% of repeat users' <- round(a6$repeat_user.sum / (a6$new_user.sum + a6$repeat_user.sum),4)
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
a7 <- summaryBy(new_user + repeat_user ~ weeknum, data = existing_devices, FUN = sum)
a8 <- summaryBy(new_user + repeat_user ~ month, data = existing_devices, FUN = sum)

a7$'% of new users' <- round(a7$new_user.sum / (a7$new_user.sum + a7$repeat_user.sum),4)
a7$'% of repeat users' <- round(a7$repeat_user.sum / (a7$new_user.sum + a7$repeat_user.sum),4)
a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
a8$'% of new users' <- round(a8$new_user.sum / (a8$new_user.sum + a8$repeat_user.sum),4)
a8$'% of repeat users' <- round(a8$repeat_user.sum / (a8$new_user.sum + a8$repeat_user.sum),4)
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4d <- rbind.fill(a0,a12,a34,a56,a78)
d4d$new_user.sum <- NULL
d4d$repeat_user.sum <- NULL

rm(d2,d2_new,d3,d4,a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 5 - Number of Devices and Devices with tag manually set #####

## Getting devices with tag manually set from GE_INTERACT event
d2 <- subset(d1, eid == "GE_INTERACT")
d2 <- subset(d2, select = c(did,date_time,tag_flag,edata.eks.subtype))

## getting flag for manual and auto-generated tag
d2$tag_manual <- ifelse(d2$edata.eks.subtype == "AddTag-Manual",1,0)
d2$tag_auto <- ifelse(d2$edata.eks.subtype == "AddTag-Deeplink",1,0)
d2 <- subset(d2, tag_flag == "Tagged")
d2$date <- as.Date(as.character(d2$date_time))
d2 <- arrange(d2, did, date, desc(tag_manual))
d3 <- d2[!duplicated(d2$did),]
d3 <- subset(d3, select = c(did,tag_manual,tag_auto))

## Getting number of active devices from launch summary
ME_Genie_Launch <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/Visualization-ME_GENIE_LAUNCH_SUMMARY_1sep16-26feb17.csv", header = T, stringsAsFactors = F)
ME_Genie_Launch$date <- as.Date(as.character(ME_Genie_Launch$date_time))
ME_Genie_Launch$context.date_range.to.per.second <- NULL
ME_Genie_Launch$context.date_range.from.per.second <- NULL
ME_Genie_Launch$Sum.of.edata.eks.timeSpent <- NULL
ME_Genie_Launch$date_time <- NULL
ME_Genie_Launch$Count <- NULL
d4 <- merge(ME_Genie_Launch,d3, by.x = "dimensions.did..Descending", by.y = "did",all = T)
colnames(d4) <- c("did","date","tag_manual","tag_auto")

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag,by="did",all.x = T)
d4 <- merge(d4,device_type,by=c("did","date"),all.x = T)

### Overall Devices
overall_devices <- d4
td1 <- overall_devices[!duplicated(overall_devices[which(names(overall_devices) %in% c("did","weeknum"))]),]
td1$did_count <- 1
td1$did_notags <- ifelse(is.na(td1$tag_manual),1,0)
td2 <- overall_devices[!duplicated(overall_devices[which(names(overall_devices) %in% c("did","month"))]),]
td2$did_count <- 1
td2$did_notags <- ifelse(is.na(td2$tag_manual),1,0)
a1 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ weeknum, data = td1, FUN = sum, na.rm = T)
a2 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ month, data = td2, FUN = sum, na.rm = T)

a1$'% of devices with tags manually set' <- round(a1$tag_manual.sum / (a1$did_count.sum - a1$did_notags.sum),4)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$'% of devices with tags manually set' <- round(a2$tag_manual.sum / (a2$did_count.sum - a2$did_notags.sum),4)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
td1 <- tag_devices[!duplicated(tag_devices[which(names(tag_devices) %in% c("did","weeknum"))]),]
td1$did_count <- 1
td1$did_notags <- ifelse(is.na(td1$tag_manual),1,0)
td2 <- tag_devices[!duplicated(tag_devices[which(names(tag_devices) %in% c("did","month"))]),]
td2$did_count <- 1
td2$did_notags <- ifelse(is.na(td2$tag_manual),1,0)
a1 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ weeknum, data = td1, FUN = sum, na.rm = T)
a2 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ month, data = td2, FUN = sum, na.rm = T)

a1$'% of devices with tags manually set' <- round(a1$tag_manual.sum / (a1$did_count.sum - a1$did_notags.sum),4)
a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
a2$'% of devices with tags manually set' <- round(a2$tag_manual.sum / (a2$did_count.sum - a2$did_notags.sum),4)
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
td1 <- untag_devices[!duplicated(untag_devices[which(names(untag_devices) %in% c("did","weeknum"))]),]
td1$did_count <- 1
td1$did_notags <- ifelse(is.na(td1$tag_manual),1,0)
td2 <- untag_devices[!duplicated(untag_devices[which(names(untag_devices) %in% c("did","month"))]),]
td2$did_count <- 1
td2$did_notags <- ifelse(is.na(td2$tag_manual),1,0)
a3 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ weeknum, data = td1, FUN = sum)
a4 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ month, data = td2, FUN = sum)

a3$'% of devices with tags manually set' <- round(a3$tag_manual.sum / (a3$did_count.sum - a3$did_notags.sum),4)
a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
a4$'% of devices with tags manually set' <- round(a4$tag_manual.sum / (a4$did_count.sum - a4$did_notags.sum),4)
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
td1 <- new_devices[!duplicated(new_devices[which(names(new_devices) %in% c("did","weeknum"))]),]
td1$did_count <- 1
td1$did_notags <- ifelse(is.na(td1$tag_manual),1,0)
td2 <- new_devices[!duplicated(new_devices[which(names(new_devices) %in% c("did","month"))]),]
td2$did_count <- 1
td2$did_notags <- ifelse(is.na(td2$tag_manual),1,0)
a5 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ weeknum, data = td1, FUN = sum, na.rm = T)
a6 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ month, data = td2, FUN = sum, na.rm = T)

a5$'% of devices with tags manually set' <- round(a5$tag_manual.sum / (a5$did_count.sum - a5$did_notags.sum),4)
a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
a6$'% of devices with tags manually set' <- round(a6$tag_manual.sum / (a6$did_count.sum - a6$did_notags.sum),4)
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
td1 <- existing_devices[!duplicated(existing_devices[which(names(existing_devices) %in% c("did","weeknum"))]),]
td1$did_count <- 1
td1$did_notags <- ifelse(is.na(td1$tag_manual),1,0)
td2 <- existing_devices[!duplicated(existing_devices[which(names(existing_devices) %in% c("did","month"))]),]
td2$did_count <- 1
td2$did_notags <- ifelse(is.na(td2$tag_manual),1,0)
a7 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ weeknum, data = td1, FUN = sum)
a8 <- summaryBy(did_count + did_notags + tag_manual + tag_auto ~ month, data = td2, FUN = sum)

a7$'% of devices with tags manually set' <- round(a7$tag_manual.sum / (a7$did_count.sum - a7$did_notags.sum),4)
a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
a8$'% of devices with tags manually set' <- round(a8$tag_manual.sum / (a8$did_count.sum - a8$did_notags.sum),4)
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4e <- rbind.fill(a0,a12,a34,a56,a78)
d4e$did_notags.sum <- NULL
d4e$tag_manual.sum <- NULL
d4e$tag_auto.sum <- NULL

rm(d2,d3,d4,a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,tag_devices,untag_devices,new_devices,existing_devices,overall_devices,td1,td2)
#####

##### Part 6 - Content and Genie Share for devices #####

## Getting devices with Genie Share from GE_INTERACT event
d2 <- subset(d1, eid == "GE_INTERACT")
d2 <- subset(d2, select = c(did,date_time,tag_flag,edata.eks.subtype))
d2 <- subset(d2, edata.eks.subtype == "ShareGenie-Success")

## getting number of genie shares per device per date
d2$date <- as.Date(as.character(d2$date_time))
d2$genie_share <- 1
d3a <- summaryBy(genie_share ~ did | date | tag_flag, data = d2, FUN = sum)

rm(d2)

## Getting devices with Content Share from GE_TRANSFER event
d2_feb <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_TRANSFER/Feb2017_GE_TRANSFER(1Feb17-26Feb17).csv", header = T, stringsAsFactors = F)
d2_jan <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_TRANSFER/Jan2017_GE_TRANSFER.csv", header = T, stringsAsFactors = F)
d2_dec <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_TRANSFER/Dec2016_GE_TRANSFER.csv", header = T, stringsAsFactors = F)
d2_nov <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_TRANSFER/Nov2016_GE_TRANSFER.csv", header = T, stringsAsFactors = F)
d2_oct <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_TRANSFER/Oct2016_GE_TRANSFER.csv", header = T, stringsAsFactors = F)
d2_sep <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_TRANSFER/Sep2016_GE_TRANSFER.csv", header = T, stringsAsFactors = F)

## Getting all content ids
d2_feb <- subset(d2_feb, edata.eks.datatype == "CONTENT" & edata.eks.direction == "EXPORT")
d2_feb <- d2_feb[, c(which(colnames(d2_feb) %in% c("ts","did")),grep("identifier", colnames(d2_feb)))]

d2_jan <- subset(d2_jan, edata.eks.datatype == "CONTENT" & edata.eks.direction == "EXPORT")
d2_jan <- d2_jan[, c(which(colnames(d2_jan) %in% c("ts","did")),grep("identifier", colnames(d2_jan)))]

d2_dec <- subset(d2_dec, edata.eks.datatype == "CONTENT" & edata.eks.direction == "EXPORT")
d2_dec <- d2_dec[, c(which(colnames(d2_dec) %in% c("ts","did")),grep("identifier", colnames(d2_dec)))]

d2_nov <- subset(d2_nov, edata.eks.datatype == "CONTENT" & edata.eks.direction == "EXPORT")
d2_nov <- d2_nov[, c(which(colnames(d2_nov) %in% c("ts","did")),grep("identifier", colnames(d2_nov)))]

d2_oct <- subset(d2_oct, edata.eks.datatype == "CONTENT" & edata.eks.direction == "EXPORT")
d2_oct <- d2_oct[, c(which(colnames(d2_oct) %in% c("ts","did")),grep("identifier", colnames(d2_oct)))]

d2_sep <- subset(d2_sep, edata.eks.datatype == "CONTENT" & edata.eks.direction == "EXPORT")
d2_sep <- d2_sep[, c(which(colnames(d2_sep) %in% c("ts","did")),grep("identifier", colnames(d2_sep)))]

## Combining the relevant data
require(plyr)
d2 <- rbind.fill(d2_feb,d2_jan,d2_dec,d2_nov,d2_oct,d2_sep)

rm(d2_feb,d2_jan,d2_dec,d2_nov,d2_oct,d2_sep)

## creating date field
d2$dummy <- strtrim(d2$ts, width = 19)
d2$date <- as.Date(d2$dummy, format = "%Y-%m-%dT%H:%M:%S")
d2$dummy <- NULL
d2$ts <- NULL

## Getting all the content_id in single row
require(reshape2)
d3 <- melt(d2, id.vars = c("did","date"), na.rm = T)
d3 <- subset(d3, value != "")
d3$variable <- NULL
d3$content_share <- 1
# tag_flag <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets/tag_flag.csv", header = T, stringsAsFactors = F)
d3 <- merge(d3,tag_flag,by="did",all.x = T)
d3b <- summaryBy(content_share ~ did | date | tag_flag, data = d3, FUN = sum)

## Merging all the content and genie shares
d4 <- merge(d3a,d3b,by=c("did","date","tag_flag"),all = T)
d4[is.na(d4)] <- 0

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,device_type,by=c("did","date"),all.x = T)

### Overall Devices
overall_devices <- d4
a1 <- summaryBy(genie_share.sum + content_share.sum ~ weeknum, data = overall_devices, FUN = sum)
a2 <- summaryBy(genie_share.sum + content_share.sum ~ month, data = overall_devices, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
a1 <- summaryBy(genie_share.sum + content_share.sum ~ weeknum, data = tag_devices, FUN = sum)
a2 <- summaryBy(genie_share.sum + content_share.sum ~ month, data = tag_devices, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
a3 <- summaryBy(genie_share.sum + content_share.sum ~ weeknum, data = untag_devices, FUN = sum)
a4 <- summaryBy(genie_share.sum + content_share.sum ~ month, data = untag_devices, FUN = sum)

a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
a5 <- summaryBy(genie_share.sum + content_share.sum ~ weeknum, data = new_devices, FUN = sum)
a6 <- summaryBy(genie_share.sum + content_share.sum ~ month, data = new_devices, FUN = sum)

a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
a7 <- summaryBy(genie_share.sum + content_share.sum ~ weeknum, data = existing_devices, FUN = sum)
a8 <- summaryBy(genie_share.sum + content_share.sum ~ month, data = existing_devices, FUN = sum)

a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4f <- rbind.fill(a0,a12,a34,a56,a78)

rm(d2,d3,d3a,d3b,d4,a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 7 - Content downloaded and % of devices downloaded a content piece #####

## Getting content downloaded from GE_INTERACT event
d2 <- subset(d1, eid == "GE_INTERACT")
d2 <- subset(d2, select = c(did,date_time,tag_flag,edata.eks.subtype,edata.eks.id))
d2 <- subset(d2, edata.eks.subtype == "ContentDownload-Initiate")

## filtering out content ids which are preloaded on genie
## content_id : do_30074541, do_30076072
d2 <- subset(d2, edata.eks.id != "do_30074541" & edata.eks.id != "do_30076072")

## getting number of unique content downloaded per device per date
d2$date <- as.Date(as.character(d2$date_time))
d2 <- d2[!duplicated(d2[which(names(d2) %in% c("did","date","edata.eks.id"))]),]
d2 <- subset(d2, edata.eks.id != "")
d2$download <- 1
d3 <- summaryBy(download ~ did | date | tag_flag, data = d2, FUN = sum)

# Getting all active devices in the period
d4 <- merge(ME_Genie_Launch,d3, by.x = c("dimensions.did..Descending","date"), by.y = c("did","date"), all = T)
d4$tag_flag <- NULL
d4[is.na(d4)] <- 0
colnames(d4) <- c("did","date","download.sum")
d4$atleast_one_download <- ifelse(d4$download.sum > 0, 1,0)

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag,by="did",all.x = T)
d4 <- merge(d4,device_type,by=c("did","date"),all.x = T)

### Overall Devices
overall_devices <- d4

c1 <- summaryBy(download.sum + atleast_one_download ~ did + weeknum, data = overall_devices, FUN = sum)
c2 <- summaryBy(download.sum + atleast_one_download ~ did + month, data = overall_devices, FUN = sum)

c1$atleast_one_download <- ifelse(c1$atleast_one_download.sum > 0,1,0)
c2$atleast_one_download <- ifelse(c2$atleast_one_download.sum > 0,1,0)

a1 <- summaryBy(download.sum.sum + atleast_one_download ~ weeknum, data = c1, FUN = sum)
a2 <- summaryBy(download.sum.sum + atleast_one_download ~ month, data = c2, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")

c1 <- summaryBy(download.sum + atleast_one_download ~ did + weeknum, data = tag_devices, FUN = sum)
c2 <- summaryBy(download.sum + atleast_one_download ~ did + month, data = tag_devices, FUN = sum)

c1$atleast_one_download <- ifelse(c1$atleast_one_download.sum > 0,1,0)
c2$atleast_one_download <- ifelse(c2$atleast_one_download.sum > 0,1,0)

a1 <- summaryBy(download.sum.sum + atleast_one_download ~ weeknum, data = c1, FUN = sum)
a2 <- summaryBy(download.sum.sum + atleast_one_download ~ month, data = c2, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")

c3 <- summaryBy(download.sum + atleast_one_download ~ did + weeknum, data = untag_devices, FUN = sum)
c4 <- summaryBy(download.sum + atleast_one_download ~ did + month, data = untag_devices, FUN = sum)

c3$atleast_one_download <- ifelse(c3$atleast_one_download.sum > 0,1,0)
c4$atleast_one_download <- ifelse(c4$atleast_one_download.sum > 0,1,0)

a3 <- summaryBy(download.sum.sum + atleast_one_download ~ weeknum, data = c3, FUN = sum)
a4 <- summaryBy(download.sum.sum + atleast_one_download ~ month, data = c4, FUN = sum)

a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")

c1 <- summaryBy(download.sum + atleast_one_download ~ did + weeknum, data = new_devices, FUN = sum)
c2 <- summaryBy(download.sum + atleast_one_download ~ did + month, data = new_devices, FUN = sum)

c1$atleast_one_download <- ifelse(c1$atleast_one_download.sum > 0,1,0)
c2$atleast_one_download <- ifelse(c2$atleast_one_download.sum > 0,1,0)

a5 <- summaryBy(download.sum.sum + atleast_one_download ~ weeknum, data = c1, FUN = sum)
a6 <- summaryBy(download.sum.sum + atleast_one_download ~ month, data = c2, FUN = sum)

a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")

c3 <- summaryBy(download.sum + atleast_one_download ~ did + weeknum, data = existing_devices, FUN = sum)
c4 <- summaryBy(download.sum + atleast_one_download ~ did + month, data = existing_devices, FUN = sum)

c3$atleast_one_download <- ifelse(c3$atleast_one_download.sum > 0,1,0)
c4$atleast_one_download <- ifelse(c4$atleast_one_download.sum > 0,1,0)

a7 <- summaryBy(download.sum.sum + atleast_one_download ~ weeknum, data = c3, FUN = sum)
a8 <- summaryBy(download.sum.sum + atleast_one_download ~ month, data = c4, FUN = sum)

a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4g <- rbind.fill(a0,a12,a34,a56,a78)

rm(d2,d3,d4,a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,c1,c2,c3,c4,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 8 - Children Setup per device for new and all devices #####

## Getting data for creating a child profile from GE_CREATE_PROFILE event
d2_feb <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Feb2017_GE_CREATE_PROFILE(1Feb17-26Feb17).csv",header = T,stringsAsFactors = F)
d2_jan <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Jan2017_GE_CREATE_PROFILE.csv",header = T,stringsAsFactors = F)
d2_dec <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Dec2016_GE_CREATE_PROFILE.csv",header = T,stringsAsFactors = F)
d2_nov <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Nov2016_GE_CREATE_PROFILE.csv",header = T,stringsAsFactors = F)
d2_oct <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Oct2016_GE_CREATE_PROFILE.csv",header = T,stringsAsFactors = F)
d2_sep <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Sep2016_GE_CREATE_PROFILE.csv",header = T,stringsAsFactors = F)

d2_feb <- subset(d2_feb, select = c(uid,did,ts))
d2_jan <- subset(d2_jan, select = c(uid,did,ts))
d2_dec <- subset(d2_dec, select = c(uid,did,ts))
d2_nov <- subset(d2_nov, select = c(uid,did,ts))
d2_oct <- subset(d2_oct, select = c(uid,did,ts))
d2_sep <- subset(d2_sep, select = c(uid,did,ts))

## Getting entire d2 data together
require(plyr)
d2 <- rbind.fill(d2_feb,d2_jan,d2_dec,d2_nov,d2_oct,d2_sep)

rm(d2_feb,d2_jan,d2_dec,d2_nov,d2_oct,d2_sep)

## creating date field
d2$dummy <- strtrim(d2$ts, width = 19)
d2$date <- as.Date(d2$dummy, format = "%Y-%m-%dT%H:%M:%S")
d2$dummy <- NULL
d2$ts <- NULL

## Getting the new children entered to a device
d3 <- d2[!duplicated(d2),]
d3$created <- 1
new_users <- d3

d3a <- summaryBy(created ~ did | date, data = d3, FUN = sum)

rm(d2,d3)

## Getting data for deleting a child profile from GE_DELETE_PROFILE event
d2_feb <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_DELETE_PROFILE/Feb2017_GE_DELETE_PROFILE(1Feb17-26Feb17).csv",header = T,stringsAsFactors = F)
d2_jan <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_DELETE_PROFILE/Jan2017_GE_DELETE_PROFILE.csv",header = T,stringsAsFactors = F)
d2_dec <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_DELETE_PROFILE/Dec2016_GE_DELETE_PROFILE.csv",header = T,stringsAsFactors = F)
d2_nov <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_DELETE_PROFILE/Nov2016_GE_DELETE_PROFILE.csv",header = T,stringsAsFactors = F)
d2_oct <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_DELETE_PROFILE/Oct2016_GE_DELETE_PROFILE.csv",header = T,stringsAsFactors = F)
d2_sep <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_DELETE_PROFILE/Sep2016_GE_DELETE_PROFILE.csv",header = T,stringsAsFactors = F)

d2_feb <- subset(d2_feb, select = c(uid,did,ts))
d2_jan <- subset(d2_jan, select = c(uid,did,ts))
d2_dec <- subset(d2_dec, select = c(uid,did,ts))
d2_nov <- subset(d2_nov, select = c(uid,did,ts))
d2_oct <- subset(d2_oct, select = c(uid,did,ts))
d2_sep <- subset(d2_sep, select = c(uid,did,ts))

## Getting entire d2 data together
require(plyr)
d2 <- rbind.fill(d2_feb,d2_jan,d2_dec,d2_nov,d2_oct,d2_sep)

rm(d2_feb,d2_jan,d2_dec,d2_nov,d2_oct,d2_sep)

## creating date field
d2$dummy <- strtrim(d2$ts, width = 19)
d2$date <- as.Date(d2$dummy, format = "%Y-%m-%dT%H:%M:%S")
d2$dummy <- NULL
d2$ts <- NULL

## Getting the list of deleted children from a device
d3 <- d2[!duplicated(d2),]
d3$deleted <- 1
deleted_users <- d3

d3b <- summaryBy(deleted ~ did | date, data = d3, FUN = sum)

rm(d2,d3)

## Getting information for new installs from GE_INTERACT event
d2 <- subset(d1, eid == "GE_INTERACT" & edata.eks.stageid == "Genie-Home-OnBoardingScreen")
d2 <- subset(d2, select = c(did,date_time,tag_flag))

## adding date variable and removing duplicates
d2$date <- as.Date(as.character(d2$date_time))
d2$date_time <- NULL

d3c <- d2[!duplicated(d2),]
d3c$d_type <- "n"

## getting the total intake of data for a device on a particular day
d3ab <- merge(d3a,d3b, by = c("did","date"), all = T)
d3ab[is.na(d3ab)] <- 0
d3ab$child_intake <- d3ab$created.sum - d3ab$deleted.sum

rm(d2)

## adding status of device (new/old) for d3ab
d3abc <- merge(d3ab,d3c, by = c("did","date"), all.x = T)
d3abc$tag_flag <- NULL
d3abc[is.na(d3abc)] <- "o"
d4 <- d3abc
d4$child_intake_new <- ifelse(d4$d_type == "n",d4$child_intake,0)
d4$new_device <- ifelse(d4$d_type == "n",1,0)

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag, by="did",all.x = T)
d4 <- merge(d4,device_type, by=c("did","date"), all.x = T)

### Overall Devices
overall_devices <- d4
a1 <- summaryBy(child_intake + child_intake_new + new_device ~ weeknum, data = overall_devices, FUN = sum)
a2 <- summaryBy(child_intake + child_intake_new + new_device ~ month, data = overall_devices, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
a1 <- summaryBy(child_intake + child_intake_new + new_device ~ weeknum, data = tag_devices, FUN = sum)
a2 <- summaryBy(child_intake + child_intake_new + new_device ~ month, data = tag_devices, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
a3 <- summaryBy(child_intake + child_intake_new + new_device ~ weeknum, data = untag_devices, FUN = sum)
a4 <- summaryBy(child_intake + child_intake_new + new_device ~ month, data = untag_devices, FUN = sum)

a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
a5 <- summaryBy(child_intake + child_intake_new + new_device ~ weeknum, data = new_devices, FUN = sum)
a6 <- summaryBy(child_intake + child_intake_new + new_device ~ month, data = new_devices, FUN = sum)

a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
a7 <- summaryBy(child_intake + child_intake_new + new_device ~ weeknum, data = existing_devices, FUN = sum)
a8 <- summaryBy(child_intake + child_intake_new + new_device ~ month, data = existing_devices, FUN = sum)

a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4h <- rbind.fill(a0,a12,a34,a56,a78)

rm(a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,d3a,d3b,d3c,d3ab,d3abc,d4,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 9 - Number and % of registered users per device #####

## Getting information for registered users
# getting list of all active users per device from GE_INTERACT
# getting list of all new users per device from GE_CREATE_PROFILE
# getting list of all deleted users per device from GE_DELETE_PROFILE
# add deleted users to active users & subtract new users (required list is Old Users)
# By date and did, iterate Old Users + New Users - Deleted Users <- To get the #of active users per day per did
# each did has one anonymous user, all other users are registered users
# if we get information of total users present on device at a particular day
# then we can get registered users from them by subtracting 1 anonymous user

## getting list of all active users per device from GE_INTERACT
d2 <- subset(d1, eid == "GE_INTERACT")
d2 <- subset(d2, select = c(did,uid))

d2 <- d2[!duplicated(d2),]
d2 <- subset(d2, uid != "")

## adding all the deleted users and subtracting new users from d2 to get the list of Old users
del_users1 <- subset(deleted_users, select = c(uid,did))
del_users1 <- del_users1[!duplicated(del_users1),]
new_users1 <- subset(new_users, select = c(uid,did)) 
new_users1 <- new_users1[!duplicated(new_users1),]

old_users <- union(d2,del_users1)
old_users <- setdiff(old_users,new_users1)
old_users <- old_users[!duplicated(old_users),]
old_users$users <- 1

rm(new_users1,del_users1,d2)

## iteratively, add new users to old users and subtract delete users by date and did
o_users <- summaryBy(users ~ did, data = old_users, FUN = sum)
n_users <- summaryBy(created ~ did + date, data = new_users, FUN = sum)
d_users <- summaryBy(deleted ~ did + date, data = deleted_users, FUN = sum)

## getting first active date for each did
d2 <- subset(d1, eid == "GE_INTERACT")
d2 <- subset(d2, select = c(did,date_time))

## adding date variable and removing duplicates
d2$date <- as.Date(as.character(d2$date_time))
d2$date_time <- NULL
d2 <- arrange(d2, did, date)
d2 <- d2[!duplicated(d2$did),]
d2 <- subset(d2, date < as.Date("2017-02-27"))

## Getting did information for each date
final=c()
o_dates=c()

for(i1 in unique(d2$did))
{
  a <- subset(d2, did == i1)
  min_date <- a$date
  dates <- as.data.frame(seq(from = min_date, to = as.Date("2017-02-26"), by = "day"))
  dates$did <- i1
  final <- rbind(final,dates)
  o_dates <- rbind(o_dates,cbind(i1,as.character(min_date)))
}

o_dates <- as.data.frame(o_dates)
colnames(o_dates) <- c("did","date")
colnames(final) <- c("date","did")
final <- subset(final, date >= as.Date("2016-09-01"))
o_dates$date <- as.Date(as.character(o_dates$date))
o_dates$did <- as.character(o_dates$did)

o_users <- merge(o_users,o_dates,by="did",all.x = T)
o_users$date <- ifelse(o_users$date < as.Date("2016-09-01"), as.character("2016-09-01"), as.character(o_users$date))
o_users$date <- as.Date(o_users$date)

rm(a,i1,min_date,dates,o_dates)

## Getting number of old users for each date and did
users <- merge(final,o_users,by=c("did","date"),all.x = T)

## Getting number of new users and deleted users to users
users <- merge(users,n_users,by=c("did","date"),all.x = T)
users <- merge(users,d_users,by=c("did","date"),all.x = T)

users[is.na(users)] <- 0

## getting total users
users$child_intake <- users$users.sum + users$created.sum - users$deleted.sum
users <- arrange(users, did, date)
require(dplyr)
users_final <- mutate(group_by(users,did), total_users= cumsum(child_intake))
users_final$total_users <- ifelse(users_final$total_users == 0, 1, users_final$total_users)
users_final$registered_users <- users_final$total_users - 1
d4 <- as.data.frame(users_final)

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag, by="did",all.x = T)
d4 <- merge(d4,device_type, by=c("did","date"), all.x = T)

### Overall Devices
overall_devices <- d4
overall_devices$total_users <- NULL
overall_devices$registered_users <- NULL
overall_devices$week <- ifelse(overall_devices$week < max(overall_devices$week)-3, max(overall_devices$week)-4,overall_devices$week)

b1 <- summaryBy(child_intake ~ did | week | weeknum, data = overall_devices, FUN = sum)
b2 <- summaryBy(child_intake ~ did | month, data = overall_devices, FUN = sum)

b1 <- arrange(b1,did,week)
b2 <- arrange(b2,did,desc(month))

require(dplyr)
c1 <- mutate(group_by(b1,did), total_users= cumsum(child_intake.sum))
c1$total_users <- ifelse(c1$total_users == 0, 1, c1$total_users)
c1$registered_users <- c1$total_users - 1
c1 <- as.data.frame(c1)
c2 <- mutate(group_by(b2,did), total_users= cumsum(child_intake.sum))
c2$total_users <- ifelse(c2$total_users == 0, 1, c2$total_users)
c2$registered_users <- c2$total_users - 1
c2 <- as.data.frame(c2)

a1 <- summaryBy(registered_users + total_users ~ weeknum, data = c1, FUN = sum)
a2 <- summaryBy(registered_users + total_users ~ month, data = c2, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2,b1,b2,c1,c2)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
tag_devices$total_users <- NULL
tag_devices$registered_users <- NULL
tag_devices$week <- ifelse(tag_devices$week < max(tag_devices$week)-3, max(tag_devices$week)-4,tag_devices$week)

b1 <- summaryBy(child_intake ~ did | week | weeknum, data = tag_devices, FUN = sum)
b2 <- summaryBy(child_intake ~ did | month, data = tag_devices, FUN = sum)

b1 <- arrange(b1,did,week)
b2 <- arrange(b2,did,desc(month))

require(dplyr)
c1 <- mutate(group_by(b1,did), total_users= cumsum(child_intake.sum))
c1$total_users <- ifelse(c1$total_users == 0, 1, c1$total_users)
c1$registered_users <- c1$total_users - 1
c1 <- as.data.frame(c1)
c2 <- mutate(group_by(b2,did), total_users= cumsum(child_intake.sum))
c2$total_users <- ifelse(c2$total_users == 0, 1, c2$total_users)
c2$registered_users <- c2$total_users - 1
c2 <- as.data.frame(c2)

a1 <- summaryBy(registered_users + total_users ~ weeknum, data = c1, FUN = sum)
a2 <- summaryBy(registered_users + total_users ~ month, data = c2, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
untag_devices$total_users <- NULL
untag_devices$registered_users <- NULL
untag_devices$week <- ifelse(untag_devices$week < max(untag_devices$week)-3, max(untag_devices$week)-4,untag_devices$week)

b3 <- summaryBy(child_intake ~ did | week | weeknum, data = untag_devices, FUN = sum)
b4 <- summaryBy(child_intake ~ did | month, data = untag_devices, FUN = sum)

b3 <- arrange(b3,did,week)
b4 <- arrange(b4,did,desc(month))

require(dplyr)
c3 <- mutate(group_by(b3,did), total_users= cumsum(child_intake.sum))
c3$total_users <- ifelse(c3$total_users == 0, 1, c3$total_users)
c3$registered_users <- c3$total_users - 1
c3 <- as.data.frame(c3)
c4 <- mutate(group_by(b4,did), total_users= cumsum(child_intake.sum))
c4$total_users <- ifelse(c4$total_users == 0, 1, c4$total_users)
c4$registered_users <- c4$total_users - 1
c4 <- as.data.frame(c4)

a3 <- summaryBy(registered_users + total_users ~ weeknum, data = c3, FUN = sum)
a4 <- summaryBy(registered_users + total_users ~ month, data = c4, FUN = sum)

a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
new_devices$total_users <- NULL
new_devices$registered_users <- NULL
new_devices$week <- ifelse(new_devices$week < max(new_devices$week)-3, max(new_devices$week)-4,new_devices$week)

b5 <- summaryBy(child_intake ~ did | week | weeknum, data = new_devices, FUN = sum)
b6 <- summaryBy(child_intake ~ did | month, data = new_devices, FUN = sum)

b5 <- arrange(b5,did,week)
b6 <- arrange(b6,did,desc(month))

require(dplyr)
c5 <- mutate(group_by(b5,did), total_users= cumsum(child_intake.sum))
c5$total_users <- ifelse(c5$total_users == 0, 1, c5$total_users)
c5$registered_users <- c5$total_users - 1
c5 <- as.data.frame(c5)
c6 <- mutate(group_by(b6,did), total_users= cumsum(child_intake.sum))
c6$total_users <- ifelse(c6$total_users == 0, 1, c6$total_users)
c6$registered_users <- c6$total_users - 1
c6 <- as.data.frame(c6)

a5 <- summaryBy(registered_users + total_users ~ weeknum, data = c5, FUN = sum)
a6 <- summaryBy(registered_users + total_users ~ month, data = c6, FUN = sum)

a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
existing_devices$total_users <- NULL
existing_devices$registered_users <- NULL
existing_devices$week <- ifelse(existing_devices$week < max(existing_devices$week)-3, max(existing_devices$week)-4,existing_devices$week)

b7 <- summaryBy(child_intake ~ did | week | weeknum, data = existing_devices, FUN = sum)
b8 <- summaryBy(child_intake ~ did | month, data = existing_devices, FUN = sum)

b7 <- arrange(b7,did,week)
b8 <- arrange(b8,did,desc(month))

require(dplyr)
c7 <- mutate(group_by(b7,did), total_users= cumsum(child_intake.sum))
c7$total_users <- ifelse(c7$total_users == 0, 1, c7$total_users)
c7$registered_users <- c7$total_users - 1
c7 <- as.data.frame(c7)
c8 <- mutate(group_by(b8,did), total_users= cumsum(child_intake.sum))
c8$total_users <- ifelse(c8$total_users == 0, 1, c8$total_users)
c8$registered_users <- c8$total_users - 1
c8 <- as.data.frame(c8)

a7 <- summaryBy(registered_users + total_users ~ weeknum, data = c7, FUN = sum)
a8 <- summaryBy(registered_users + total_users ~ month, data = c8, FUN = sum)

a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4i <- rbind.fill(a0,a12,a34,a56,a78)

rm(a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,b1,b2,b3,b4,b5,b6,b7,b8,c1,c2,c3,c4,c5,c6,c7,c8,d2,d4,d_users,deleted_users,final,n_users,new_users,o_users,old_users,users,users_final,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 10 - % of users who played more than 1 content in 1 Genie Visit #####

# Devices playing more than one content in one session
# Getting ME session summary data
ME_data <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/Visualization-ME_GENIE_LAUNCH_SUMMARY_1sep16-26feb17.csv", header = T, stringsAsFactors = F)
ME_data$context.date_range.to.per.second <- NULL
ME_data$start_time <- as.POSIXct(ME_data$date_time)
ME_data$timeSpent <- round(as.numeric(gsub(",","",ME_data$Sum.of.edata.eks.timeSpent)),0)
ME_data$end_time <- ME_data$start_time + ME_data$timeSpent
ME_data$Sum.of.edata.eks.timeSpent <- NULL
ME_data$Count <- NULL
ME_data$date_time <- NULL
ME_data$context.date_range.from.per.second <- NULL
colnames(ME_data) <- c("did","start_time","timespent","end_time")
# filtering out all the session with less than 5 seconds
ME_data <- subset(ME_data, timespent >= 5)
ME_data$timespent <- NULL
ME_data$sess_id <- paste(ME_data$did,ME_data$start_time,sep = "_")
ME_data$start_time <- as.character(ME_data$start_time)
ME_data$end_time <- as.character(ME_data$end_time)

## Getting number of content played
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets")
sess_len <- read.csv(file = "session_length.csv", header = T, stringsAsFactors = F)

# filtering data- removing all content which are played less than 5 seconds 
sess_len1 <- subset(sess_len, session_length >= 5)
sess_len1$session_length <- NULL
sess_len1$ts <- NULL
colnames(sess_len1) <- c("did","gdata_id","date_time")

# getting a count of unique contents played in a single session for a did
require(sqldf)
d2 <- sqldf("SELECT sess_len1.did, ME_data.start_time, sess_len1.gdata_id, ME_data.sess_id
            FROM sess_len1 LEFT JOIN ME_data 
            ON sess_len1.did = ME_data.did AND sess_len1.date_time BETWEEN ME_data.start_time AND ME_data.end_time")

d3 <- subset(d2, sess_id != "")
d3 <- d3[!duplicated(d3),]
d4 <- summaryBy(gdata_id ~ did | start_time, data = d3, FUN = length)
colnames(d4) <- c("did","start_time","number of content played")

d4 <- merge(ME_data,d4, by = c("did","start_time"), all.x = T)
d4[is.na(d4)] <- 0

# Getting count of sessions which have more than one content played in it
d4$more_than_one_content_played <- ifelse(d4$`number of content played` > 1,1,0)
d4$date <- as.Date(as.character(d4$start_time))
d4 <- summaryBy(more_than_one_content_played ~ did | date, data = d4, FUN = sum)
d4$more_than_one_content_played <- ifelse(d4$more_than_one_content_played.sum >= 1,1,0)
d4$more_than_one_content_played.sum <- NULL

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag, by="did",all.x = T)
d4 <- merge(d4,device_type, by=c("did","date"),all.x = T)

### Overall Devices
overall_devices <- d4

c1 <- summaryBy(more_than_one_content_played ~ did + weeknum, data = overall_devices, FUN = sum)
c2 <- summaryBy(more_than_one_content_played ~ did + month, data = overall_devices, FUN = sum)

c1$more_than_one_content_played <- ifelse(c1$more_than_one_content_played.sum > 0, 1,0)
c2$more_than_one_content_played <- ifelse(c2$more_than_one_content_played.sum > 0, 1,0)

a1 <- summaryBy(more_than_one_content_played ~ weeknum, data = c1, FUN = sum)
a2 <- summaryBy(more_than_one_content_played ~ month, data = c2, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2,c1,c2)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")

c1 <- summaryBy(more_than_one_content_played ~ did + weeknum, data = tag_devices, FUN = sum)
c2 <- summaryBy(more_than_one_content_played ~ did + month, data = tag_devices, FUN = sum)

c1$more_than_one_content_played <- ifelse(c1$more_than_one_content_played.sum > 0, 1,0)
c2$more_than_one_content_played <- ifelse(c2$more_than_one_content_played.sum > 0, 1,0)

a1 <- summaryBy(more_than_one_content_played ~ weeknum, data = c1, FUN = sum)
a2 <- summaryBy(more_than_one_content_played ~ month, data = c2, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")

c3 <- summaryBy(more_than_one_content_played ~ did + weeknum, data = untag_devices, FUN = sum)
c4 <- summaryBy(more_than_one_content_played ~ did + month, data = untag_devices, FUN = sum)

c3$more_than_one_content_played <- ifelse(c3$more_than_one_content_played.sum > 0, 1,0)
c4$more_than_one_content_played <- ifelse(c4$more_than_one_content_played.sum > 0, 1,0)

a3 <- summaryBy(more_than_one_content_played ~ weeknum, data = c3, FUN = sum)
a4 <- summaryBy(more_than_one_content_played ~ month, data = c4, FUN = sum)

a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")

c5 <- summaryBy(more_than_one_content_played ~ did + weeknum, data = new_devices, FUN = sum)
c6 <- summaryBy(more_than_one_content_played ~ did + month, data = new_devices, FUN = sum)

c5$more_than_one_content_played <- ifelse(c5$more_than_one_content_played.sum > 0, 1,0)
c6$more_than_one_content_played <- ifelse(c6$more_than_one_content_played.sum > 0, 1,0)

a5 <- summaryBy(more_than_one_content_played ~ weeknum, data = c5, FUN = sum)
a6 <- summaryBy(more_than_one_content_played ~ month, data = c6, FUN = sum)

a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")

c7 <- summaryBy(more_than_one_content_played ~ did + weeknum, data = existing_devices, FUN = sum)
c8 <- summaryBy(more_than_one_content_played ~ did + month, data = existing_devices, FUN = sum)

c7$more_than_one_content_played <- ifelse(c7$more_than_one_content_played.sum > 0, 1,0)
c8$more_than_one_content_played <- ifelse(c8$more_than_one_content_played.sum > 0, 1,0)

a7 <- summaryBy(more_than_one_content_played ~ weeknum, data = c7, FUN = sum)
a8 <- summaryBy(more_than_one_content_played ~ month, data = c8, FUN = sum)

a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4j <- rbind.fill(a0,a12,a34,a56,a78)

rm(a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,c1,c2,c3,c4,c5,c6,c7,c8,d2,d3,d4,ME_data,sess_len,sess_len1,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 11 - % of users finishing the content played #####

## Getting number of content played and time of play for each content
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets")
sess_len <- read.csv(file = "session_length.csv", header = T, stringsAsFactors = F)

# filtering data- removing all content which are played less than 5 seconds
sess_len1 <- subset(sess_len, session_length >= 5)
# filtering data- removing all entries which do not have content ids
sess_len1 <- subset(sess_len1, gdata.id != "")

# creating date field for sess_len1
sess_len1$date <- as.Date(as.character(sess_len1$date_time))

# creating end_date for each content play
sess_len1$date_time <- as.POSIXct(sess_len1$date_time)
sess_len1$end_date <- sess_len1$date_time + sess_len1$session_length

## Getting data for user reaching the end page of content played from OE_INTERACT event
d2_sep <- read.csv("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - OE_INTERACT/Sep2016_OE_INTERACT(stageid_ContnetApp-EndScreen).csv", header = T, stringsAsFactors = F)
d2_oct <- read.csv("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - OE_INTERACT/Oct2016_OE_INTERACT(stageid_ContnetApp-EndScreen).csv", header = T, stringsAsFactors = F)
d2_nov <- read.csv("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - OE_INTERACT/Nov2016_OE_INTERACT(stageid_ContnetApp-EndScreen).csv", header = T, stringsAsFactors = F)
d2_dec <- read.csv("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - OE_INTERACT/Dec2016_OE_INTERACT(stageid_ContnetApp-EndScreen).csv", header = T, stringsAsFactors = F)
d2_jan <- read.csv("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - OE_INTERACT/Jan2017_OE_INTERACT(stageid_ContnetApp-EndScreen).csv", header = T, stringsAsFactors = F)
d2_feb <- read.csv("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - OE_INTERACT/Feb2017_OE_INTERACT(stageid_ContnetApp-EndScreen)(1Feb17-26Feb17).csv", header = T, stringsAsFactors = F)

d2_sep <- subset(d2_sep,select = c(did,ts,gdata.id))
d2_oct <- subset(d2_oct,select = c(did,ts,gdata.id))
d2_nov <- subset(d2_nov,select = c(did,ts,gdata.id))
d2_dec <- subset(d2_dec,select = c(did,ts,gdata.id))
d2_jan <- subset(d2_jan,select = c(did,ts,gdata.id))
d2_feb <- subset(d2_feb,select = c(did,ts,gdata.id))

## getting entire OE_INTERACT event data
require(plyr)
d2 <- rbind.fill(d2_feb,d2_jan,d2_dec,d2_nov,d2_oct,d2_sep)

rm(d2_feb,d2_jan,d2_dec,d2_nov,d2_oct,d2_sep)

# getting date_time field for d2
d2$dummy <- strtrim(d2$ts, width = 19)
d2$date_time <- as.POSIXct(d2$dummy, format = "%Y-%m-%dT%H:%M:%S")
d2$dummy <- NULL
require(dplyr)
d2 <- rename(d2,gdata_id = gdata.id)
sess_len1 <- rename(sess_len1, gdata_id = gdata.id)

## getting information for content reaching end page for each content played
require(sqldf)
d4 <- sqldf("SELECT sess_len1.did, sess_len1.date, sess_len1.date_time, d2.gdata_id
            FROM sess_len1 LEFT JOIN d2 
            ON d2.did = sess_len1.did AND d2.gdata_id = sess_len1.gdata_id AND d2.date_time BETWEEN sess_len1.date_time AND sess_len1.end_date")

d4 <- unique(d4)
## flagging finishing content
d4$content_complete <- ifelse(is.na(d4$gdata_id),0,1)

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag,by="did",all.x = T)
d4 <- merge(d4,device_type,by=c("did","date"),all.x = T)

### Overall Devices
overall_devices <- d4
a1 <- summaryBy(content_complete ~ weeknum, data = overall_devices, FUN = sum)
a2 <- summaryBy(content_complete ~ month, data = overall_devices, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
a1 <- summaryBy(content_complete ~ weeknum, data = tag_devices, FUN = sum)
a2 <- summaryBy(content_complete ~ month, data = tag_devices, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
a3 <- summaryBy(content_complete ~ weeknum, data = untag_devices, FUN = sum)
a4 <- summaryBy(content_complete ~ month, data = untag_devices, FUN = sum)

a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
a5 <- summaryBy(content_complete ~ weeknum, data = new_devices, FUN = sum)
a6 <- summaryBy(content_complete ~ month, data = new_devices, FUN = sum)

a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
a7 <- summaryBy(content_complete ~ weeknum, data = existing_devices, FUN = sum)
a8 <- summaryBy(content_complete ~ month, data = existing_devices, FUN = sum)

a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4k <- rbind.fill(a0,a12,a34,a56,a78)

rm(sess_len,sess_len1,a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,d2,d4,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

##### Part 12 - Number of content detail viewed #####

## Getting information for content detail screen view from GE_INTERACT event
d2 <- subset(d1, eid == "GE_INTERACT")
d2 <- subset(d2, select = c(did,date_time,tag_flag,edata.eks.subtype,edata.eks.id))
d2 <- subset(d2, edata.eks.subtype == "Content")

## getting unique content per date per did
d2$date <- as.Date(as.character(d2$date_time))
d2$date_time <- NULL
d3 <- d2[!duplicated(d2),]
d3$cont_detail_view <- 1

## getting count of total content detail viewed per date per did
require(doBy)
d4 <- summaryBy(cont_detail_view ~ did | date, data = d3, FUN = sum)

## Making d4 in usable format
d4 <- subset(d4, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
d4$month <- format.Date(d4$date,"%b%Y")
d4$week <- as.numeric(format.Date(as.Date(d4$date),"%W"))
d4 <- arrange(d4, desc(date))
d4$weeknum <- ifelse(d4$week == unique(d4$week)[1], "This Week", 
                     ifelse(d4$week == unique(d4$week)[2], "Week 1",
                            ifelse(d4$week == unique(d4$week)[3], "Week 2",
                                   ifelse(d4$week == unique(d4$week)[4], "Week 3","others"))))
d4 <- merge(d4,tag_flag,by="did",all.x = T)
d4 <- merge(d4,device_type,by=c("did","date"),all.x = T)

### Overall Devices
overall_devices <- d4
a1 <- summaryBy(cont_detail_view.sum ~ weeknum, data = overall_devices, FUN = sum)
a2 <- summaryBy(cont_detail_view.sum ~ month, data = overall_devices, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a0 <- rbind(a1,a2)
a0$group <- "Overall"

rm(a1,a2)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
a1 <- summaryBy(cont_detail_view.sum ~ weeknum, data = tag_devices, FUN = sum)
a2 <- summaryBy(cont_detail_view.sum ~ month, data = tag_devices, FUN = sum)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'

a12 <- rbind(a1,a2)
a12$group <- "Tagged"

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
a3 <- summaryBy(cont_detail_view.sum ~ weeknum, data = untag_devices, FUN = sum)
a4 <- summaryBy(cont_detail_view.sum ~ month, data = untag_devices, FUN = sum)

a3 <- subset(a3, weeknum != "others")
names(a3)[names(a3) == 'weeknum'] <- 'time_frame'
names(a4)[names(a4) == 'month'] <- 'time_frame'

a34 <- rbind(a3,a4)
a34$group <- "Untagged"

### New Devices
new_devices <- subset(d4, device_type == "new")
a5 <- summaryBy(cont_detail_view.sum ~ weeknum, data = new_devices, FUN = sum)
a6 <- summaryBy(cont_detail_view.sum ~ month, data = new_devices, FUN = sum)

a5 <- subset(a5, weeknum != "others")
names(a5)[names(a5) == 'weeknum'] <- 'time_frame'
names(a6)[names(a6) == 'month'] <- 'time_frame'

a56 <- rbind(a5,a6)
a56$group <- "New"

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
a7 <- summaryBy(cont_detail_view.sum ~ weeknum, data = existing_devices, FUN = sum)
a8 <- summaryBy(cont_detail_view.sum ~ month, data = existing_devices, FUN = sum)

a7 <- subset(a7, weeknum != "others")
names(a7)[names(a7) == 'weeknum'] <- 'time_frame'
names(a8)[names(a8) == 'month'] <- 'time_frame'

a78 <- rbind(a7,a8)
a78$group <- "Existing"

require(plyr)
d4l <- rbind.fill(a0,a12,a34,a56,a78)

rm(a0,a1,a2,a3,a4,a5,a6,a7,a8,a12,a34,a56,a78,d2,d3,d4,tag_devices,untag_devices,new_devices,existing_devices,overall_devices)
#####

############################
##### Retention Funnel #####
############################

##### Part 13 - Active Devices at D0, D1, D3, D7, D14, D28, Total Usage in minutes and Unique Devices at D0, D1-D6, D7-D27, D28+ #####

## Getting ME_GENIE_LAUNCH_SUMMARY data for active devices
Genie_Launch <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/Visualization-ME_GENIE_LAUNCH_SUMMARY_1sep16-26feb17.csv", header = T, stringsAsFactors = F)

## creating date field for each did
Genie_Launch$date <- as.Date(as.character(Genie_Launch$date_time))
Genie_Launch$context.date_range.from.per.second <- NULL
Genie_Launch$context.date_range.to.per.second <- NULL
colnames(Genie_Launch) <- c("did","time_spent","count","date_time","date")

## Creating D0, D1, D2 and so on for all the did
did <- unique(Genie_Launch$did)
i <- did[1]
con <- c()
a <- subset(Genie_Launch, select = c(did,date))
a <- a[!duplicated(a),]

for(i in did)
{
  b <- subset(a, did == i)
  b$ref <- min(b$date)
  b$day <- paste0("D",b$date - b$ref)
  b <- b[c("date","day","did")]
  
  con <- rbind(con,b)
}

rm(a,b,i,did)

## converting time spent in numeric form and 
#             filtering out all the genie sessions of less than 5 sec
Genie_Launch$time_spent <- round(as.numeric(gsub(",","",Genie_Launch$time_spent)),0)
Genie_Launch <- subset(Genie_Launch, time_spent >= 5)

## summarizing launch summary at date and did level
require(doBy)
launch1 <- summaryBy(time_spent + count ~ did | date, data = Genie_Launch, FUN = sum)

## merge con with launch1 to get the day info for each did
launch2 <- merge(launch1, con, by = c("date","did"), all.x = T)
launch2$day_num <- as.numeric(substr(launch2$day,2,nchar(launch2$day))) 
launch2$date <- as.character(launch2$date)

## Getting number of content played and time of play for each content
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets")
sess_len <- read.csv(file = "session_length.csv", header = T, stringsAsFactors = F)
# filtering data- removing all content which are played less than 5 seconds 
#                 capping anything above 30min to 30min
sess_len1 <- subset(sess_len, session_length >= 5)
sess_len1$session_length <- ifelse(sess_len1$session_length >= 1800,1800,sess_len1$session_length)
# creating date field for sess_len1
sess_len1$date <- as.Date(as.character(sess_len1$date_time))
sess_len1$date <- as.character(sess_len1$date)
# summarizing the data at did and date level to get number of content played
require(doBy)
con_play <- summaryBy(gdata.id ~ did | date, data = sess_len1, FUN = length)
len_play <- summaryBy(session_length ~ did | date, data = sess_len1, FUN = sum)

## merge con_play and len_play with launch2 to get number of content played
launch2 <- merge(launch2, con_play, by = c("date","did"), all.x = T)
launch2$gdata.id.length <- ifelse(is.na(launch2$gdata.id.length),0,launch2$gdata.id.length)
launch2 <- merge(launch2, len_play, by = c("date","did"), all.x = T)
launch2$session_length.sum <- ifelse(is.na(launch2$session_length.sum),0,launch2$session_length.sum)

## getting weeknum and month
launch3 <- subset(launch2, date >= as.Date("2016-09-01") & date < as.Date("2017-02-27"))
launch3$month <- format.Date(launch3$date,"%b%Y")
launch3$week <- as.numeric(format.Date(launch3$date,"%W"))
launch3 <- arrange(launch3, desc(date))
launch3$weeknum <- ifelse(launch3$week == unique(launch3$week)[1], "This Week", 
                          ifelse(launch3$week == unique(launch3$week)[2], "Week 1",
                                 ifelse(launch3$week == unique(launch3$week)[3], "Week 2",
                                        ifelse(launch3$week == unique(launch3$week)[4], "Week 3","others"))))
launch3 <- merge(launch3,tag_flag,by="did",all.x = T)
launch3 <- merge(launch3,device_type,by=c("did","date"),all.x = T)

## getting active users for D0, D1, D3, D7, D14, D28
launch4 <- launch3
launch4$active_D0 <- ifelse(launch4$day == "D0",1,0)
launch4$active_D1 <- ifelse(launch4$day == "D1",1,0)
launch4$active_D3 <- ifelse(launch4$day == "D3",1,0)
launch4$active_D7 <- ifelse(launch4$day == "D7",1,0)
launch4$active_D14 <- ifelse(launch4$day == "D14",1,0)
launch4$active_D28 <- ifelse(launch4$day == "D28",1,0)

## getting daily usage in minutes on D0, D1-D6, D7-27, D28+
launch4$usage_D0 <- ifelse(launch4$day_num == 0,launch4$time_spent.sum,0)
launch4$usage_D1_D6 <- ifelse(launch4$day_num > 0 & launch4$day_num <= 6,launch4$time_spent.sum,0)
launch4$usage_D7_D27 <- ifelse(launch4$day_num > 6 & launch4$day_num <= 27,launch4$time_spent.sum,0)
launch4$usage_D28 <- ifelse(launch4$day_num > 27,launch4$time_spent.sum,0)

## getting unique users from D0, D1-D6, D7-27, D28+
launch4$unique_D0 <- ifelse(launch4$day_num == 0,launch4$did,NA)
launch4$unique_D1_D6 <- ifelse(launch4$day_num > 0 & launch4$day_num <= 6,launch4$did,NA)
launch4$unique_D7_D27 <- ifelse(launch4$day_num > 6 & launch4$day_num <= 27,launch4$did,NA)
launch4$unique_D28 <- ifelse(launch4$day_num > 27,launch4$did,NA)

## getting total genie visits from D0, D1-D6, D7-27, D28+
launch4$gvisit_D0 <- ifelse(launch4$day_num == 0,launch4$count.sum,0)
launch4$gvisit_D1_D6 <- ifelse(launch4$day_num > 0 & launch4$day_num <= 6,launch4$count.sum,0)
launch4$gvisit_D7_D27 <- ifelse(launch4$day_num > 6 & launch4$day_num <= 27,launch4$count.sum,0)
launch4$gvisit_D28 <- ifelse(launch4$day_num > 27,launch4$count.sum,0)

## getting total content played from D0, D1-D6, D7-27, D28+
launch4$cplay_D0 <- ifelse(launch4$day_num == 0,launch4$gdata.id.length,0)
launch4$cplay_D1_D6 <- ifelse(launch4$day_num > 0 & launch4$day_num <= 6,launch4$gdata.id.length,0)
launch4$cplay_D7_D27 <- ifelse(launch4$day_num > 6 & launch4$day_num <= 27,launch4$gdata.id.length,0)
launch4$cplay_D28 <- ifelse(launch4$day_num > 27,launch4$gdata.id.length,0)

## getting total content usage from D0, D1-D6, D7-27, D28+
launch4$cusage_D0 <- ifelse(launch4$day_num == 0,launch4$session_length.sum,0)
launch4$cusage_D1_D6 <- ifelse(launch4$day_num > 0 & launch4$day_num <= 6,launch4$session_length.sum,0)
launch4$cusage_D7_D27 <- ifelse(launch4$day_num > 6 & launch4$day_num <= 27,launch4$session_length.sum,0)
launch4$cusage_D28 <- ifelse(launch4$day_num > 27,launch4$session_length.sum,0)
d4 <- launch4

### Overall Devices
overall_devices <- d4
a1 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ weeknum, data = overall_devices, FUN = sum)
a2 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ month, data = overall_devices, FUN = sum)

require(plyr)
x1 <- ddply(overall_devices,~ weeknum,summarise,unique_users_D0=length(unique(unique_D0))-1)
x2 <- ddply(overall_devices,~ weeknum,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
x3 <- ddply(overall_devices,~ weeknum,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
x4 <- ddply(overall_devices,~ weeknum,summarise,unique_users_D28=length(unique(unique_D28))-1)

x <- merge(x1,x2,by="weeknum",all = T)
x <- merge(x,x3,by="weeknum",all = T)
x <- merge(x,x4,by="weeknum",all = T)

y1 <- ddply(overall_devices,~ month,summarise,unique_users_D0=length(unique(unique_D0))-1)
y2 <- ddply(overall_devices,~ month,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
y3 <- ddply(overall_devices,~ month,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
y4 <- ddply(overall_devices,~ month,summarise,unique_users_D28=length(unique(unique_D28))-1)

y <- merge(y1,y2,by="month",all = T)
y <- merge(y,y3,by="month",all = T)
y <- merge(y,y4,by="month",all = T)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
x <- subset(x, weeknum != "others")
names(x)[names(x) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'
names(y)[names(y) == 'month'] <- 'time_frame'

a1 <- merge(a1,x,by="time_frame")
a2 <- merge(a2,y,by="time_frame")

a0 <- rbind(a1,a2)
a0$group <- "Overall"
rm(a1,a2,x1,x2,x3,x4,y1,y2,y3,y4,x,y)

### Tagged Devices
tag_devices <- subset(d4, tag_flag == "Tagged")
a1 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ weeknum, data = tag_devices, FUN = sum)
a2 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ month, data = tag_devices, FUN = sum)

require(plyr)
x1 <- ddply(tag_devices,~ weeknum,summarise,unique_users_D0=length(unique(unique_D0))-1)
x2 <- ddply(tag_devices,~ weeknum,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
x3 <- ddply(tag_devices,~ weeknum,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
x4 <- ddply(tag_devices,~ weeknum,summarise,unique_users_D28=length(unique(unique_D28))-1)

x <- merge(x1,x2,by="weeknum",all = T)
x <- merge(x,x3,by="weeknum",all = T)
x <- merge(x,x4,by="weeknum",all = T)

y1 <- ddply(tag_devices,~ month,summarise,unique_users_D0=length(unique(unique_D0))-1)
y2 <- ddply(tag_devices,~ month,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
y3 <- ddply(tag_devices,~ month,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
y4 <- ddply(tag_devices,~ month,summarise,unique_users_D28=length(unique(unique_D28))-1)

y <- merge(y1,y2,by="month",all = T)
y <- merge(y,y3,by="month",all = T)
y <- merge(y,y4,by="month",all = T)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
x <- subset(x, weeknum != "others")
names(x)[names(x) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'
names(y)[names(y) == 'month'] <- 'time_frame'

a1 <- merge(a1,x,by="time_frame")
a2 <- merge(a2,y,by="time_frame")

a12 <- rbind(a1,a2)
a12$group <- "Tagged"
rm(a1,a2,x1,x2,x3,x4,y1,y2,y3,y4,x,y)

### Untagged Devices
untag_devices <- subset(d4, tag_flag == "Untagged")
a1 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ weeknum, data = untag_devices, FUN = sum)
a2 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ month, data = untag_devices, FUN = sum)

require(plyr)
x1 <- ddply(untag_devices,~ weeknum,summarise,unique_users_D0=length(unique(unique_D0))-1)
x2 <- ddply(untag_devices,~ weeknum,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
x3 <- ddply(untag_devices,~ weeknum,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
x4 <- ddply(untag_devices,~ weeknum,summarise,unique_users_D28=length(unique(unique_D28))-1)

x <- merge(x1,x2,by="weeknum",all = T)
x <- merge(x,x3,by="weeknum",all = T)
x <- merge(x,x4,by="weeknum",all = T)

y1 <- ddply(untag_devices,~ month,summarise,unique_users_D0=length(unique(unique_D0))-1)
y2 <- ddply(untag_devices,~ month,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
y3 <- ddply(untag_devices,~ month,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
y4 <- ddply(untag_devices,~ month,summarise,unique_users_D28=length(unique(unique_D28))-1)

y <- merge(y1,y2,by="month",all = T)
y <- merge(y,y3,by="month",all = T)
y <- merge(y,y4,by="month",all = T)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
x <- subset(x, weeknum != "others")
names(x)[names(x) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'
names(y)[names(y) == 'month'] <- 'time_frame'

a3 <- merge(a1,x,by="time_frame")
a4 <- merge(a2,y,by="time_frame")

a34 <- rbind(a3,a4)
a34$group <- "Untagged"
rm(a1,a2,a3,a4,x1,x2,x3,x4,y1,y2,y3,y4,x,y)

### New Devices
new_devices <- subset(d4, device_type == "new")
a1 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ weeknum, data = new_devices, FUN = sum)
a2 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ month, data = new_devices, FUN = sum)

require(plyr)
x1 <- ddply(new_devices,~ weeknum,summarise,unique_users_D0=length(unique(unique_D0))-1)
x2 <- ddply(new_devices,~ weeknum,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
x3 <- ddply(new_devices,~ weeknum,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
x4 <- ddply(new_devices,~ weeknum,summarise,unique_users_D28=length(unique(unique_D28))-1)

x <- merge(x1,x2,by="weeknum",all = T)
x <- merge(x,x3,by="weeknum",all = T)
x <- merge(x,x4,by="weeknum",all = T)

y1 <- ddply(new_devices,~ month,summarise,unique_users_D0=length(unique(unique_D0))-1)
y2 <- ddply(new_devices,~ month,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
y3 <- ddply(new_devices,~ month,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
y4 <- ddply(new_devices,~ month,summarise,unique_users_D28=length(unique(unique_D28))-1)

y <- merge(y1,y2,by="month",all = T)
y <- merge(y,y3,by="month",all = T)
y <- merge(y,y4,by="month",all = T)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
x <- subset(x, weeknum != "others")
names(x)[names(x) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'
names(y)[names(y) == 'month'] <- 'time_frame'

a5 <- merge(a1,x,by="time_frame")
a6 <- merge(a2,y,by="time_frame")

a56 <- rbind(a5,a6)
a56$group <- "New"
rm(a1,a2,a5,a6,x1,x2,x3,x4,y1,y2,y3,y4,x,y)

### Existing Devices
existing_devices <- subset(d4, device_type == "existing")
a1 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ weeknum, data = existing_devices, FUN = sum)
a2 <- summaryBy(active_D0 + active_D1 + active_D3 + active_D7 + active_D14 + active_D28 + usage_D0 + usage_D1_D6 + usage_D7_D27 + usage_D28 + gvisit_D0 + gvisit_D1_D6 + gvisit_D7_D27 + gvisit_D28 + cplay_D0 + cplay_D1_D6 + cplay_D7_D27 + cplay_D28 + cusage_D0 + cusage_D1_D6 + cusage_D7_D27 + cusage_D28 ~ month, data = existing_devices, FUN = sum)

require(plyr)
x1 <- ddply(existing_devices,~ weeknum,summarise,unique_users_D0=length(unique(unique_D0))-1)
x2 <- ddply(existing_devices,~ weeknum,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
x3 <- ddply(existing_devices,~ weeknum,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
x4 <- ddply(existing_devices,~ weeknum,summarise,unique_users_D28=length(unique(unique_D28))-1)

x <- merge(x1,x2,by="weeknum",all = T)
x <- merge(x,x3,by="weeknum",all = T)
x <- merge(x,x4,by="weeknum",all = T)

y1 <- ddply(existing_devices,~ month,summarise,unique_users_D0=length(unique(unique_D0))-1)
y2 <- ddply(existing_devices,~ month,summarise,unique_users_D1_D6=length(unique(unique_D1_D6))-1)
y3 <- ddply(existing_devices,~ month,summarise,unique_users_D7_D27=length(unique(unique_D7_D27))-1)
y4 <- ddply(existing_devices,~ month,summarise,unique_users_D28=length(unique(unique_D28))-1)

y <- merge(y1,y2,by="month",all = T)
y <- merge(y,y3,by="month",all = T)
y <- merge(y,y4,by="month",all = T)

a1 <- subset(a1, weeknum != "others")
names(a1)[names(a1) == 'weeknum'] <- 'time_frame'
x <- subset(x, weeknum != "others")
names(x)[names(x) == 'weeknum'] <- 'time_frame'
names(a2)[names(a2) == 'month'] <- 'time_frame'
names(y)[names(y) == 'month'] <- 'time_frame'

a7 <- merge(a1,x,by="time_frame")
a8 <- merge(a2,y,by="time_frame")

a78 <- rbind(a7,a8)
a78$group <- "Existing"
rm(a1,a2,a7,a8,x1,x2,x3,x4,y1,y2,y3,y4,x,y)

require(plyr)
d4m <- rbind.fill(a0,a12,a34,a56,a78)

rm(a0,a12,a34,a56,a78,con,con_play,len_play,d4,Genie_Launch,launch1,launch2,launch4,tag_devices,untag_devices,new_devices,existing_devices,overall_devices,sess_len,sess_len1)
#####

##### Part 14 - Avg Daily Usage in minutes at D0, D1-D6, D7-D27 #####

## avg daily usage = total usage in minutes / number of days
a <- launch3
a$range <- ifelse(a$day_num == 0, "D0",
                  ifelse(a$day_num >= 1 & a$day_num <= 6, "D1-D6",
                         ifelse(a$day_num >= 7 & a$day_num <= 27, "D7-D27",
                                ifelse(a$day_num >= 28, "D28+", NA))))

a$active_days <- 1

## overall devices for Weeks
o <- summaryBy(time_spent.sum + active_days ~ range | weeknum , data = a, FUN = sum)
o <- subset(o, weeknum != "others")
o$avg_usage_per_day_per_device <- o$time_spent.sum.sum / o$active_days.sum
o$time_spent.sum.sum <- NULL
o$active_days.sum <- NULL
o$group <- "Overall"
o <- rename(o, time_frame = weeknum)

## tagged and untagged devices for Weeks
b <- summaryBy(time_spent.sum + active_days ~ range | weeknum | tag_flag, data = a, FUN = sum)
b <- subset(b, is.na(b$tag_flag) == F)
b <- subset(b, weeknum != "others")
b$avg_usage_per_day_per_device <- b$time_spent.sum.sum / b$active_days.sum
b$time_spent.sum.sum <- NULL
b$active_days.sum <- NULL
b <- rename(b, group = tag_flag)
b <- rename(b, time_frame = weeknum)

## new and existing devices for Weeks
c <- summaryBy(time_spent.sum + active_days ~ range | weeknum | device_type, data = a, FUN = sum)
c <- subset(c, is.na(c$device_type) == F)
c <- subset(c, weeknum != "others")
c$avg_usage_per_day_per_device <- c$time_spent.sum.sum / c$active_days.sum
c$time_spent.sum.sum <- NULL
c$active_days.sum <- NULL
c$group <- ifelse(c$device_type == "existing","Existing",
                  ifelse(c$device_type == "new","New",NA))
c$device_type <- NULL
c <- rename(c, time_frame = weeknum)

## overall devices for months
om <- summaryBy(time_spent.sum + active_days ~ range | month, data = a, FUN = sum)
om$avg_usage_per_day_per_device <- om$time_spent.sum.sum / om$active_days.sum
om$time_spent.sum.sum <- NULL
om$active_days.sum <- NULL
om$group <- "Overall"
om <- rename(om, time_frame = month)

## tagged and untagged devices for months
d <- summaryBy(time_spent.sum + active_days ~ range | month | tag_flag, data = a, FUN = sum)
d <- subset(d, is.na(d$tag_flag) == F)
d$avg_usage_per_day_per_device <- d$time_spent.sum.sum / d$active_days.sum
d$time_spent.sum.sum <- NULL
d$active_days.sum <- NULL
d <- rename(d, group = tag_flag)
d <- rename(d, time_frame = month)

## new and existing devices for months
e <- summaryBy(time_spent.sum + active_days ~ range | month | device_type, data = a, FUN = sum)
e <- subset(e, is.na(e$device_type) == F)
e$avg_usage_per_day_per_device <- e$time_spent.sum.sum / e$active_days.sum
e$time_spent.sum.sum <- NULL
e$active_days.sum <- NULL
e$group <- ifelse(e$device_type == "existing","Existing",
                  ifelse(e$device_type == "new","New",NA))
e$device_type <- NULL
e <- rename(e, time_frame = month)

require(plyr)
comb <- rbind.fill(o,b,c,om,d,e)
comb$avg_usage_D0 <- ifelse(comb$range == "D0",comb$avg_usage_per_day_per_device,0)
comb$avg_usage_D1_D6 <- ifelse(comb$range == "D1-D6",comb$avg_usage_per_day_per_device,0)
comb$avg_usage_D7_D27 <- ifelse(comb$range == "D7-D27",comb$avg_usage_per_day_per_device,0)
comb$avg_usage_D28 <- ifelse(comb$range == "D28+",comb$avg_usage_per_day_per_device,0)
comb$range <- NULL
comb$avg_usage_per_day_per_device <- NULL

d4n <- summaryBy(avg_usage_D0 + avg_usage_D1_D6 + avg_usage_D7_D27 + avg_usage_D28 ~ time_frame | group, data = comb, FUN = sum)
d4n <- d4n[,c("time_frame","avg_usage_D0.sum","avg_usage_D1_D6.sum","avg_usage_D7_D27.sum","avg_usage_D28.sum","group")]

rm(a,b,c,d,e,o,om,comb,launch3)
#####

##### Final Part #####

## Getting final output by appending all the d4 files
d4 <- merge(d4a,d4b, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4c, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4d, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4e, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4f, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4g, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4h, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4i, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4j, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4k, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4l, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4m, by=c("time_frame","group"), all = T)
d4 <- merge(d4,d4n, by=c("time_frame","group"), all = T)

t <- subset(d4, group == "Tagged")
u <- subset(d4, group == "Untagged")
n <- subset(d4, group == "New")
e <- subset(d4, group == "Existing")

rm(d1,d4a,d4b,d4c,d4d,d4e,d4f,d4g,d4h,d4i,d4j,d4k,d4l,d4m,d4n,ME_Genie_Launch,feb17,jan17,dec16,nov16,oct16,sep16,tag_flag,device_type,i)

## For Tagged Devices

t$'# of devices' <- t$did_count.sum
t$'# of visits' <- t$number_of_sessions.sum
t$number_of_sessions.sum <- NULL
t$'# of minutes' <- round(t$timespent.sum / 60,2)
t$timespent.sum <- NULL
t$'Minutes / Visit' <- t$minutes_per_visit
t$minutes_per_visit <- NULL
t$'# of content played' <- t$num_of_content_played.sum
t$num_of_content_played.sum <- NULL
t$'Minutes / content' <- round(t$minutes_per_content,2)
t$minutes_per_content <- NULL
t$'% searches per visit' <- t$search_per_visit
t$search_per_visit <- NULL
t$'% new users' <- t$`% of new users`
t$`% of new users` <- NULL
t$'% repeat users' <- t$`% of repeat users`
t$`% of repeat users` <- NULL
t$'% devices with language not set to English' <- t$`% of devices with lang not set to eng`
t$`% of devices with lang not set to eng` <- NULL
t$'% devices with tags manually set' <- t$`% of devices with tags manually set`
t$`% of devices with tags manually set` <- NULL
t$'% content played and rated' <- t$`% of content played and rated`
t$`% of content played and rated` <- NULL
t$'# content shared via transfers' <- t$content_share.sum.sum
t$content_share.sum.sum <- NULL
t$'# children setup per device for new installs' <- round(t$child_intake_new.sum / t$new_device.sum,2)
t$child_intake_new.sum <- NULL
t$'# children setup per device for all installs' <- round(t$child_intake.sum / t$did_count.sum,2)
t$child_intake.sum <- NULL
t$'% Genie shares per device' <- round(t$genie_share.sum.sum / t$did_count.sum,4)
t$genie_share.sum.sum <- NULL
t$'% of devices who played more than one content in one Genie Visit' <- round(t$more_than_one_content_played.sum / t$did_count.sum,4) 
t$more_than_one_content_played.sum <- NULL
t$'% of devices downloaded a content piece' <- round(t$atleast_one_download.sum / t$did_count.sum,4)
t$atleast_one_download.sum <- NULL
t$'% of content played and finished' <- round(t$content_complete.sum / t$`# of content played`,4)
t$content_complete.sum <- NULL
t$'# of content detail viewed' <- t$cont_detail_view.sum.sum
t$cont_detail_view.sum.sum <- NULL
t$'# of content downloaded' <- t$download.sum.sum.sum
t$download.sum.sum.sum <- NULL
t$'# of content rated' <- t$num_of_content_rated.sum
t$num_of_content_rated.sum <- NULL
t$'active devices D0' <- t$active_D0.sum
t$active_D0.sum <- NULL
t$'active devices D1' <- t$active_D1.sum
t$active_D1.sum <- NULL
t$'active devices D3' <- t$active_D3.sum
t$active_D3.sum <- NULL
t$'active devices D7' <- t$active_D7.sum
t$active_D7.sum <- NULL
t$'active devices D14' <- t$active_D14.sum
t$active_D14.sum <- NULL
t$'active devices D28' <- t$active_D28.sum
t$active_D28.sum <- NULL
t$'total usage in minutes D0' <- round(t$usage_D0.sum/60,2)
t$usage_D0.sum <- NULL
t$'total usage in minutes D1-D6' <- round(t$usage_D1_D6.sum/60,2)
t$usage_D1_D6.sum <- NULL
t$'total usage in minutes D7-D27' <- round(t$usage_D7_D27.sum/60,2)
t$usage_D7_D27.sum <- NULL
t$'total usage in minutes D28+' <- round(t$usage_D28.sum/60,2)
t$usage_D28.sum <- NULL
t$'unique devices D0' <- t$unique_users_D0
t$unique_users_D0 <- NULL
t$'unique devices D1-D6' <- t$unique_users_D1_D6
t$unique_users_D1_D6 <- NULL
t$'unique devices D7-D27' <- t$unique_users_D7_D27
t$unique_users_D7_D27 <- NULL
t$'unique devices D28+' <- t$unique_users_D28
t$unique_users_D28 <- NULL
t$'avg daily usage in minutes D0' <- round(t$avg_usage_D0.sum/60,2)
t$avg_usage_D0.sum <- NULL
t$'avg daily usage in minutes D1-D6' <- round(t$avg_usage_D1_D6.sum/60,2)
t$avg_usage_D1_D6.sum <- NULL
t$'avg daily usage in minutes D7-D27' <- round(t$avg_usage_D7_D27.sum/60,2)
t$avg_usage_D7_D27.sum <- NULL
t$'avg daily usage in minutes D28+' <- round(t$avg_usage_D28.sum/60,2)
t$avg_usage_D28.sum <- NULL
t <- subset(t,select = -c(search.sum,did_count.sum,new_device.sum,registered_users.sum,total_users.sum,total_time_played_sec.sum,gvisit_D0.sum,gvisit_D1_D6.sum,gvisit_D7_D27.sum,gvisit_D28.sum,cplay_D0.sum,cplay_D1_D6.sum,cplay_D7_D27.sum,cplay_D28.sum,cusage_D0.sum,cusage_D1_D6.sum,cusage_D7_D27.sum,cusage_D28.sum))
t[is.na(t)] <- 0

## For Untagged Devices

u$'# of devices' <- u$did_count.sum
u$'# of visits' <- u$number_of_sessions.sum
u$number_of_sessions.sum <- NULL
u$'# of minutes' <- round(u$timespent.sum / 60,2)
u$timespent.sum <- NULL
u$'Minutes / Visit' <- u$minutes_per_visit
u$minutes_per_visit <- NULL
u$'# of content downloaded' <- u$download.sum.sum.sum
u$download.sum.sum.sum <- NULL
u$'# of content played' <- u$num_of_content_played.sum
u$num_of_content_played.sum <- NULL
u$'% searches per visit' <- u$search_per_visit
u$search_per_visit <- NULL
u$'% new users' <- u$`% of new users`
u$`% of new users` <- NULL
u$'% of registered users' <- round(u$registered_users.sum / u$total_users.sum,4)
u$'# registered users per device' <- round(u$registered_users.sum / u$did_count.sum,2)
u$'% content played and rated' <- u$`% of content played and rated`
u$`% of content played and rated` <- NULL
u$'% content shared' <- round(u$content_share.sum.sum / u$did_count.sum,4)
u$'% devices with language not set to English' <- u$`% of devices with lang not set to eng`
u$`% of devices with lang not set to eng` <- NULL
u$'# content shared via transfers' <- u$content_share.sum.sum
u$content_share.sum.sum <- NULL
u$'# children setup per device for new installs' <- round(u$child_intake_new.sum / u$new_device.sum,2)
u$child_intake_new.sum <- NULL
u$'# children setup per device for all installs' <- round(u$child_intake.sum / u$did_count.sum,2)
u$child_intake.sum <- NULL
u$'% Genie shares per device' <- round(u$genie_share.sum.sum / u$did_count.sum,4)
u$genie_share.sum.sum <- NULL
u$'% of devices who played more than one content in one Genie Visit' <- round(u$more_than_one_content_played.sum / u$did_count.sum,4) 
u$more_than_one_content_played.sum <- NULL
u$'% of devices downloaded a content piece' <- round(u$atleast_one_download.sum / u$did_count.sum,4)
u$atleast_one_download.sum <- NULL
u$'% of content played and finished' <- round(u$content_complete.sum / u$`# of content played`,4)
u$content_complete.sum <- NULL
u$'# of content detail viewed' <- u$cont_detail_view.sum.sum
u$cont_detail_view.sum.sum <- NULL
u$'# of content rated' <- u$num_of_content_rated.sum
u$num_of_content_rated.sum <- NULL
u$'active devices D0' <- u$active_D0.sum
u$active_D0.sum <- NULL
u$'active devices D1' <- u$active_D1.sum
u$active_D1.sum <- NULL
u$'active devices D3' <- u$active_D3.sum
u$active_D3.sum <- NULL
u$'active devices D7' <- u$active_D7.sum
u$active_D7.sum <- NULL
u$'active devices D14' <- u$active_D14.sum
u$active_D14.sum <- NULL
u$'active devices D28' <- u$active_D28.sum
u$active_D28.sum <- NULL
u$'total usage in minutes D0' <- round(u$usage_D0.sum/60,2)
u$usage_D0.sum <- NULL
u$'total usage in minutes D1-D6' <- round(u$usage_D1_D6.sum/60,2)
u$usage_D1_D6.sum <- NULL
u$'total usage in minutes D7-D27' <- round(u$usage_D7_D27.sum/60,2)
u$usage_D7_D27.sum <- NULL
u$'total usage in minutes D28+' <- round(u$usage_D28.sum/60,2)
u$usage_D28.sum <- NULL
u$'unique devices D0' <- u$unique_users_D0
u$unique_users_D0 <- NULL
u$'unique devices D1-D6' <- u$unique_users_D1_D6
u$unique_users_D1_D6 <- NULL
u$'unique devices D7-D27' <- u$unique_users_D7_D27
u$unique_users_D7_D27 <- NULL
u$'unique devices D28+' <- u$unique_users_D28
u$unique_users_D28 <- NULL
u$'avg daily usage in minutes D0' <- round(u$avg_usage_D0.sum/60,2)
u$avg_usage_D0.sum <- NULL
u$'avg daily usage in minutes D1-D6' <- round(u$avg_usage_D1_D6.sum/60,2)
u$avg_usage_D1_D6.sum <- NULL
u$'avg daily usage in minutes D7-D27' <- round(u$avg_usage_D7_D27.sum/60,2)
u$avg_usage_D7_D27.sum <- NULL
u$'avg daily usage in minutes D28+' <- round(u$avg_usage_D28.sum/60,2)
u$avg_usage_D28.sum <- NULL
u$`% of repeat users` <- NULL
u$`% of devices with tags manually set` <- NULL
u <- subset(u,select = -c(search.sum,minutes_per_content,did_count.sum,new_device.sum,registered_users.sum,total_users.sum,total_time_played_sec.sum,gvisit_D0.sum,gvisit_D1_D6.sum,gvisit_D7_D27.sum,gvisit_D28.sum,cplay_D0.sum,cplay_D1_D6.sum,cplay_D7_D27.sum,cplay_D28.sum,cusage_D0.sum,cusage_D1_D6.sum,cusage_D7_D27.sum,cusage_D28.sum))
u[is.na(u)] <- 0

## For New Devices

n$'# of devices' <- n$did_count.sum
n$'# of visits' <- n$number_of_sessions.sum
n$number_of_sessions.sum <- NULL
n$'# of minutes' <- round(n$timespent.sum / 60,2)
n$timespent.sum <- NULL
n$'Minutes / Visit' <- n$minutes_per_visit
n$minutes_per_visit <- NULL
n$'# of content played' <- n$num_of_content_played.sum
n$num_of_content_played.sum <- NULL
n$'Minutes / content' <- round(n$minutes_per_content,2)
n$minutes_per_content <- NULL
n$'% searches per visit' <- n$search_per_visit
n$search_per_visit <- NULL
n$'% new users' <- n$`% of new users`
n$`% of new users` <- NULL
n$'% repeat users' <- n$`% of repeat users`
n$`% of repeat users` <- NULL
n$'% devices with language not set to English' <- n$`% of devices with lang not set to eng`
n$`% of devices with lang not set to eng` <- NULL
n$'% devices with tags manually set' <- n$`% of devices with tags manually set`
n$`% of devices with tags manually set` <- NULL
n$'% content played and rated' <- n$`% of content played and rated`
n$`% of content played and rated` <- NULL
n$'# content shared via transfers' <- n$content_share.sum.sum
n$content_share.sum.sum <- NULL
n$'# children setup per device for new installs' <- round(n$child_intake_new.sum / n$new_device.sum,2)
n$child_intake_new.sum <- NULL
n$'# children setup per device for all installs' <- round(n$child_intake.sum / n$did_count.sum,2)
n$child_intake.sum <- NULL
n$'% Genie shares per device' <- round(n$genie_share.sum.sum / n$did_count.sum,4)
n$genie_share.sum.sum <- NULL
n$'% of devices who played more than one content in one Genie Visit' <- round(n$more_than_one_content_played.sum / n$did_count.sum,4) 
n$more_than_one_content_played.sum <- NULL
n$'% of devices downloaded a content piece' <- round(n$atleast_one_download.sum / n$did_count.sum,4)
n$atleast_one_download.sum <- NULL
n$'% of content played and finished' <- round(n$content_complete.sum / n$`# of content played`,4)
n$content_complete.sum <- NULL
n$'# of content detail viewed' <- n$cont_detail_view.sum.sum
n$cont_detail_view.sum.sum <- NULL
n$'# of content downloaded' <- n$download.sum.sum.sum
n$download.sum.sum.sum <- NULL
n$'# of content rated' <- n$num_of_content_rated.sum
n$num_of_content_rated.sum <- NULL
n$'active devices D0' <- n$active_D0.sum
n$active_D0.sum <- NULL
n$'active devices D1' <- n$active_D1.sum
n$active_D1.sum <- NULL
n$'active devices D3' <- n$active_D3.sum
n$active_D3.sum <- NULL
n$'active devices D7' <- n$active_D7.sum
n$active_D7.sum <- NULL
n$'active devices D14' <- n$active_D14.sum
n$active_D14.sum <- NULL
n$'active devices D28' <- n$active_D28.sum
n$active_D28.sum <- NULL
n$'total usage in minutes D0' <- round(n$usage_D0.sum/60,2)
n$usage_D0.sum <- NULL
n$'total usage in minutes D1-D6' <- round(n$usage_D1_D6.sum/60,2)
n$usage_D1_D6.sum <- NULL
n$'total usage in minutes D7-D27' <- round(n$usage_D7_D27.sum/60,2)
n$usage_D7_D27.sum <- NULL
n$'total usage in minutes D28+' <- round(n$usage_D28.sum/60,2)
n$usage_D28.sum <- NULL
n$'unique devices D0' <- n$unique_users_D0
n$unique_users_D0 <- NULL
n$'unique devices D1-D6' <- n$unique_users_D1_D6
n$unique_users_D1_D6 <- NULL
n$'unique devices D7-D27' <- n$unique_users_D7_D27
n$unique_users_D7_D27 <- NULL
n$'unique devices D28+' <- n$unique_users_D28
n$unique_users_D28 <- NULL
n$'avg daily usage in minutes D0' <- round(n$avg_usage_D0.sum/60,2)
n$avg_usage_D0.sum <- NULL
n$'avg daily usage in minutes D1-D6' <- round(n$avg_usage_D1_D6.sum/60,2)
n$avg_usage_D1_D6.sum <- NULL
n$'avg daily usage in minutes D7-D27' <- round(n$avg_usage_D7_D27.sum/60,2)
n$avg_usage_D7_D27.sum <- NULL
n$'avg daily usage in minutes D28+' <- round(n$avg_usage_D28.sum/60,2)
n$avg_usage_D28.sum <- NULL
n <- subset(n,select = -c(search.sum,did_count.sum,new_device.sum,registered_users.sum,total_users.sum,total_time_played_sec.sum,gvisit_D0.sum,gvisit_D1_D6.sum,gvisit_D7_D27.sum,gvisit_D28.sum,cplay_D0.sum,cplay_D1_D6.sum,cplay_D7_D27.sum,cplay_D28.sum,cusage_D0.sum,cusage_D1_D6.sum,cusage_D7_D27.sum,cusage_D28.sum))
n[is.na(n)] <- 0

## For Existing Devices

e$'# of devices' <- e$did_count.sum
e$'# of visits' <- e$number_of_sessions.sum
e$number_of_sessions.sum <- NULL
e$'# of minutes' <- round(e$timespent.sum / 60,2)
e$timespent.sum <- NULL
e$'Minutes / Visit' <- e$minutes_per_visit
e$minutes_per_visit <- NULL
e$'# of content downloaded' <- e$download.sum.sum.sum
e$download.sum.sum.sum <- NULL
e$'# of content played' <- e$num_of_content_played.sum
e$num_of_content_played.sum <- NULL
e$'% searches per visit' <- e$search_per_visit
e$search_per_visit <- NULL
e$'% new users' <- e$`% of new users`
e$`% of new users` <- NULL
e$'% of registered users' <- round(e$registered_users.sum / e$total_users.sum,4)
e$'# registered users per device' <- round(e$registered_users.sum / e$did_count.sum,2)
e$'% content played and rated' <- e$`% of content played and rated`
e$`% of content played and rated` <- NULL
e$'% content shared' <- round(e$content_share.sum.sum / e$did_count.sum,4)
e$'% devices with language not set to English' <- e$`% of devices with lang not set to eng`
e$`% of devices with lang not set to eng` <- NULL
e$'# content shared via transfers' <- e$content_share.sum.sum
e$content_share.sum.sum <- NULL
e$'# children setup per device for new installs' <- round(e$child_intake_new.sum / e$new_device.sum,2)
e$child_intake_new.sum <- NULL
e$'# children setup per device for all installs' <- round(e$child_intake.sum / e$did_count.sum,2)
e$child_intake.sum <- NULL
e$'% Genie shares per device' <- round(e$genie_share.sum.sum / e$did_count.sum,4)
e$genie_share.sum.sum <- NULL
e$'% of devices who played more than one content in one Genie Visit' <- round(e$more_than_one_content_played.sum / e$did_count.sum,4) 
e$more_than_one_content_played.sum <- NULL
e$'% of devices downloaded a content piece' <- round(e$atleast_one_download.sum / e$did_count.sum,4)
e$atleast_one_download.sum <- NULL
e$'% of content played and finished' <- round(e$content_complete.sum / e$`# of content played`,4)
e$content_complete.sum <- NULL
e$'# of content detail viewed' <- e$cont_detail_view.sum.sum
e$cont_detail_view.sum.sum <- NULL
e$'# of content rated' <- e$num_of_content_rated.sum
e$num_of_content_rated.sum <- NULL
e$'active devices D0' <- e$active_D0.sum
e$active_D0.sum <- NULL
e$'active devices D1' <- e$active_D1.sum
e$active_D1.sum <- NULL
e$'active devices D3' <- e$active_D3.sum
e$active_D3.sum <- NULL
e$'active devices D7' <- e$active_D7.sum
e$active_D7.sum <- NULL
e$'active devices D14' <- e$active_D14.sum
e$active_D14.sum <- NULL
e$'active devices D28' <- e$active_D28.sum
e$active_D28.sum <- NULL
e$'total usage in minutes D0' <- round(e$usage_D0.sum/60,2)
e$usage_D0.sum <- NULL
e$'total usage in minutes D1-D6' <- round(e$usage_D1_D6.sum/60,2)
e$usage_D1_D6.sum <- NULL
e$'total usage in minutes D7-D27' <- round(e$usage_D7_D27.sum/60,2)
e$usage_D7_D27.sum <- NULL
e$'total usage in minutes D28+' <- round(e$usage_D28.sum/60,2)
e$usage_D28.sum <- NULL
e$'unique devices D0' <- e$unique_users_D0
e$unique_users_D0 <- NULL
e$'unique devices D1-D6' <- e$unique_users_D1_D6
e$unique_users_D1_D6 <- NULL
e$'unique devices D7-D27' <- e$unique_users_D7_D27
e$unique_users_D7_D27 <- NULL
e$'unique devices D28+' <- e$unique_users_D28
e$unique_users_D28 <- NULL
e$'avg daily usage in minutes D0' <- round(e$avg_usage_D0.sum/60,2)
e$avg_usage_D0.sum <- NULL
e$'avg daily usage in minutes D1-D6' <- round(e$avg_usage_D1_D6.sum/60,2)
e$avg_usage_D1_D6.sum <- NULL
e$'avg daily usage in minutes D7-D27' <- round(e$avg_usage_D7_D27.sum/60,2)
e$avg_usage_D7_D27.sum <- NULL
e$'avg daily usage in minutes D28+' <- round(e$avg_usage_D28.sum/60,2)
e$avg_usage_D28.sum <- NULL
e$`% of repeat users` <- NULL
e$`% of devices with tags manually set` <- NULL
e <- subset(e,select = -c(search.sum,minutes_per_content,did_count.sum,new_device.sum,registered_users.sum,total_users.sum,total_time_played_sec.sum,gvisit_D0.sum,gvisit_D1_D6.sum,gvisit_D7_D27.sum,gvisit_D28.sum,cplay_D0.sum,cplay_D1_D6.sum,cplay_D7_D27.sum,cplay_D28.sum,cusage_D0.sum,cusage_D1_D6.sum,cusage_D7_D27.sum,cusage_D28.sum))
e[is.na(e)] <- 0

rm(d4)
#####

## Setting Working Directory
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project Genie Funnel and Usage Metrics/")

##### Writing the output file #####

write.csv(t,file = "Tagged Devices - Genie Funnel and Usage Metrics.csv", row.names = F)
write.csv(u,file = "Untagged Devices - Genie Funnel and Usage Metrics.csv", row.names = F)
write.csv(n,file = "New Devices - Genie Funnel and Usage Metrics.csv", row.names = F)
write.csv(e,file = "Existing Devices - Genie Funnel and Usage Metrics.csv", row.names = F)

#####

################################## End of Code #########################################