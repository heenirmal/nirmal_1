############################### Old Device Usage Analysis ########################
library(plyr)
library(doBy)

########################## Finding tags and untags ###############################
#### Segregating Tagged and Untagged devices ####
setwd("D:/mafoi/ekstep/data/Final CSVs/ekstep data/Final CSVs/Final CSVs - GE_INTERACT")
#sep16 <- read.csv(file = "Sep2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
#oct16 <- read.csv(file = "Oct2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
nov16 <- read.csv(file = "Nov2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
#dec16 <- read.csv(file = "Dec2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
#jan17 <- read.csv(file = "Jan2017_GE_INTERACT.csv", header = T, stringsAsFactors = F)
#feb17 <- read.csv(file = "Feb2017_GE_INTERACT.csv", header = T, stringsAsFactors = F)
#mar17 <- read.csv(file = "Mar2017_GE_INTERACT(1Mar17-12Mar17).csv", header = T, stringsAsFactors = F)

## Getting all the tags information for all the data
#tags_sep16 <- sep16[, c(which(colnames(sep16) == "did"),grep("genie", colnames(sep16)))]
#tags_oct16 <- oct16[, c(which(colnames(oct16) == "did"),grep("genie", colnames(oct16)))]
tags_nov16 <- nov16[, c(which(colnames(nov16) == "did"),which(colnames(nov16) == "ts"),grep("genie", colnames(nov16)))]
#tags_dec16 <- dec16[, c(which(colnames(dec16) == "did"),grep("genie", colnames(dec16)))]
#tags_jan17 <- jan17[, c(which(colnames(jan17) == "did"),grep("genie", colnames(jan17)))]
#tags_feb17 <- feb17[, c(which(colnames(feb17) == "did"),grep("genie", colnames(feb17)))]
#tags_mar17 <- mar17[, c(which(colnames(mar17) == "did"),grep("genie", colnames(mar17)))]

#tags_sep16 <- tags_sep16[!duplicated(tags_sep16),]
#tags_oct16 <- tags_oct16[!duplicated(tags_oct16),]
tags_nov16 <- tags_nov16[!duplicated(tags_nov16),]
View(tags_nov16)
#tags_dec16 <- tags_dec16[!duplicated(tags_dec16),]
#tags_jan17 <- tags_jan17[!duplicated(tags_jan17),]
#tags_feb17 <- tags_feb17[!duplicated(tags_feb17),]
#tags_mar17 <- tags_mar17[!duplicated(tags_mar17),]


## Combining all the devices
library(plyr)
tags <- tags_nov16
  #rbind.fill(tags_sep16,tags_oct16,tags_nov16,tags_dec16,tags_jan17,tags_feb17,tags_mar17)
################ Date_filtering ################
tags$dummy <- strtrim(tags$ts, width = 19)
tags$date_time <- as.Date(tags$dummy, format = "%Y-%m-%dT%H:%M:%S")
tags$dummy <- NULL
tags <- subset(tags,tags$date_time >= "2016-11-01" & tags$date_time <= "2016-11-30")
range(tags$date_time)
tags$date_time <- NULL
tags$ts <- NULL
## Getting all the tags in single row
library(reshape2)
tags1 <- melt(tags, id.vars = c("did"), na.rm = T)
tags1$variable <- NULL
View(tags1)
tags2 <- tags1[!duplicated(tags1),]
names(tags2) <- c("did","tags")
tags3 <- arrange(tags2, did, tags)
#tags3 <- tags3[!duplicated(tags3$did),]

## Getting other did for tag status
setwd("D:/mafoi/ekstep/data/Final CSVs/ekstep data/Final CSVs/ME_GENIE_SESSION_SUMMARY")
me <- read.csv("ME_SESSION_SUMMARY_VISUALIZATION_1Sep16-19Mar17.csv", header = T, stringsAsFactors = F)
names(me)
me1 <- subset(me, select = c(dimensions.did..Descending,tags.genie..Descending,date_time))
colnames(me1) <- c("did","tags","date")
me2 <- subset(me1,me1$date >= "2016-11-01" & me1$date <= "2016-11-30")
me2 <- arrange(me2, did, tags)
me2$date <- NULL
rm(me1)
View(me2)
#me1 <- me1[!duplicated(me1$did),]

## Appending the two datasets
tags4 <- rbind(tags3,me2)
tags4 <- arrange(tags4, did, tags)
tags4 <- tags4[!duplicated(tags4),]
####################### Removing Office devices #############################
setwd("D:/mafoi/ekstep/works/old_deviceUsage/inputs")
office_did <- read.csv("ekstep_office_devices.csv",stringsAsFactors = F,header = T)

tags_f1 <- tags4
tags_f1 <- subset(tags_f1,tags_f1$did != office_did$Device.ID[1])
for(i in (2:nrow(office_did))){
  tags_f1 <- subset(tags_f1,tags_f1$did != office_did$Device.ID[i])
}
tags_df <- tags_f1
length(unique(tags_df$did))
####################### Tagging the devices #################################
setwd("D:/mafoi/ekstep/works/old_deviceUsage/inputs")
homeuse <- read.csv("homeuse.csv",header = T,stringsAsFactors = F)

tags_home <- subset(tags_df,tags_df$tags == homeuse$did[1])
tags_home_final <- rbind(tags_home)

for(i in (2:nrow(homeuse))){
  tags_home <- subset(tags_df,tags_df$tags == homeuse$did[i])
  tags_home_final <- rbind(tags_home_final,tags_home)
}
tags_home_final$tag_name <- "homeuse"

######################## merging tags and homeuse ########################
udevices <- unique(tags_home_final$did)
tags_final <- subset(tags_df,tags_df$did != udevices[1])
for(i in (2:length(udevices))){
  tags_final <- subset(tags_final,tags_final$did != udevices[i])
}
tags_final$tag_name <- ifelse(tags_final$tags == "","untagged","tagged")

tags_final1 <- rbind(tags_final,tags_home_final)
tags_final1$tags = NULL
tags_final1 <- tags_final1[!duplicated(tags_final1),]
tags_final1 <- tags_final1[order(tags_final1$did,tags_final1$tag_name,decreasing = F),]
did_final <- tags_final1[!duplicated(tags_final1$did),]
did_final$mode <- ifelse(did_final$tag_name == "tagged","B2B","B2C")

######################### Finding one month old devices ##############################
setwd("D:/mafoi/ekstep/data/Final CSVs/ekstep data/Final CSVs/Final CSVs - GE_INTERACT")
aug16 <- read.csv(file = "Aug2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
jul16 <- read.csv(file = "Jul_2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
sep16 <- read.csv(file = "Sep2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)

################# Loading ME_SESSION for one month old devices #############
setwd("D:/mafoi/ekstep/data/Final CSVs/ekstep data/Final CSVs/ME_GENIE_SESSION_SUMMARY")
me <- read.csv("ME_SESSION_SUMMARY_VISUALIZATION_1Sep16-19Mar17.csv", header = T, stringsAsFactors = F)
names(me)
me1 <- subset(me, select = c(dimensions.did..Descending,date_time))
colnames(me1) <- c("did","date")

me2 <- subset(me1,me1$date >= "2016-07-01" & me1$date <= "2016-09-30")

old1 <- jul16[,c("did"),drop = F]
old2 <- aug16[,c("did"),drop = F]
old3 <- sep16[,c("did"),drop = F]
old_me <- me2[,c("did"),drop = F]
old_df <- rbind(old1,old2,old3,old_me)
rm(aug16,jul16,sep16,old1,old2,old3)
old_df$age <- "one-monthold"
old_df <- old_df[!duplicated(old_df$did),]

did_mode_age <- merge(did_final,old_df,by="did",all.x = T)
did_mode_age[is.na(did_mode_age)] <- 0
did_mode_age$device_time <- ifelse(did_mode_age$age == "one-monthold","one-monthold","new-device")
table(did_mode_age$mode,did_mode_age$device_time)
mode_age <- did_mode_age[,c("did","mode","device_time")]
View(mode_age)
########################### Loading genie launch ###############################
setwd("D:/mafoi/ekstep/works/old_deviceUsage/inputs")
ge_launch <- read.csv("genie_launch.csv",stringsAsFactors = F,header = T)
ge_launch$context.date_range.to.per.second = NULL
library(stringi)
names(ge_launch) <- c("did","time","time_spent","visits")
data1 <- ge_launch
data1$dummy <- stri_sub(data1$time, 1, -5)
data1$dummy <- paste0(stri_sub(data1$dummy, 1, -18),stri_sub(data1$dummy, -15))
data1$date_time <- as.POSIXct(data1$dummy, format = "%B %d %Y, %H:%M:%S")
data1$dummy <- NULL
data1$date_time[1]
        ################# date-Filtering #######################
ge_launch_f1 <- subset(data1,data1$date_time >= "2016-11-01" & data1$date_time <= "2016-11-30")
ge_launch_f1$time_spent <- as.numeric(ge_launch_f1$time_spent)
ge_launch_f1[is.na(ge_launch_f1)] <- 0
ge_launch_f1 <- subset(ge_launch_f1,ge_launch_f1$time_spent >= 5)
View(ge_launch_f1)
ge_launch_f1$weeks <- format.Date(ge_launch_f1$date_time,"%W")
ge_mode_age <- merge(ge_launch_f1,mode_age,by="did")
########################## Metric1 : number of devices usage #############
devices_usage <- ge_mode_age[,c("did","weeks","mode","device_time")]
devices_usage <- devices_usage[!duplicated(devices_usage),]
devices_usage1 <- summaryBy(did ~ weeks+mode,data = devices_usage,FUN = length) 
devices_usage1$devices <- "all"
devices_usage2 <- summaryBy(did ~ weeks+mode,data = devices_usage[devices_usage$device_time == "one-monthold",],FUN = length) 
devices_usage2$devices <- "onemonth_old"
View(devices_usage1)
devices_usage1 <- rbind(devices_usage1,devices_usage2)
devices_usage_df <- dcast(devices_usage1,mode + devices ~ weeks,value.var = "did.length")
devices_usage_df$metric <- "number_devices"
View(devices_usage_df)
################### Metric2 : Genie visits ######################
library(reshape2)
genie_visits <- summaryBy(visits ~ weeks+mode,data = ge_mode_age,FUN = sum)
genie_visits$devices <- "all"
genie_visits1 <- summaryBy(visits ~ weeks+mode,data = ge_mode_age[ge_mode_age$device_time == "one-monthold",],FUN = sum)
genie_visits1$devices <- "onemonth_old"
genie_visits <- rbind(genie_visits,genie_visits1)
genie_visits_df <- dcast(genie_visits,mode + devices ~ weeks,value.var = "visits.sum")
genie_visits_df$metric <- "genie_visits"
View(genie_visits_df)
################## metric3 : content usage #######################
setwd("D:/mafoi/ekstep/data/Final CSVs/ekstep data/Final CSVs/session_lengths")
sess_len <- read.csv("session_length.csv",header = T,stringsAsFactors = F)
names(sess_len)
sess_len_fil <- subset(sess_len,sess_len$date_time >= "2016-11-01" & sess_len$date_time <= "2016-11-31")
sess_len_fil <- subset(sess_len_fil,session_length >= 5 & session_length <= 1800)
sess_len_fil$weeks <- format.Date(sess_len_fil$date_time,"%W")
sess_mode <- merge(sess_len_fil,mode_age,by="did")
rm(sess_len,sess_len_fil)
content_usage <- summaryBy(session_length ~ weeks+mode,data = sess_mode,FUN = sum)
content_usage$devices <- "all"
View(content_usage)
content_usage1 <- summaryBy(session_length ~ weeks+mode,data = sess_mode[sess_mode$device_time =="one-monthold",],FUN = sum)
content_usage1$devices <- "onemonth_old"
content_usage <- rbind(content_usage,content_usage1)
View(content_usage)
content_usage$session_length.sum <- as.integer(content_usage$session_length.sum/60)
#content_usage$ContentusageVsDevices <-  content_usage$session_length.sum/devices_usage1$did.length
content_usage_df <- dcast(content_usage,mode + devices ~ weeks,value.var = "session_length.sum")
content_usage_df$metric <- "content_usage"
View(content_usage_df)

######################## Metric4 : content Visits ################################
content_visits <- summaryBy(gdata.id ~ weeks+mode,data = sess_mode,FUN = length)
content_visits$devices <- "all"
content_visits1 <- summaryBy(gdata.id ~ weeks+mode,data = sess_mode[sess_mode$device_time == "one-monthold",],FUN = length)
content_visits1$devices <- "onemonth_old"
content_visits <- rbind(content_visits,content_visits1)
content_visits_df <- dcast(content_visits,devices + mode ~ weeks,value.var = "gdata.id.length")
content_visits_df$metric <- "content_visits"
View(content_visits_df)

###################### Metric5 : content_usage vs devices ###########################
content_usage$ContentusageVsDevices <-  content_usage$session_length.sum/devices_usage1$did.length
contVsdevices_df <- dcast(content_usage,mode + devices ~ weeks,value.var = "ContentusageVsDevices")
contVsdevices_df$metric <- "ContentusageVsDevices"
View(contVsdevices_df)

##################### Metric6 : genieVisit vs devices ###############################
genie_visits$genievisitsVsdevices <- genie_visits$visits.sum/devices_usage1$did.length
gevisitsVsdevices_df <- dcast(genie_visits,mode + devices ~ weeks,value.var = "genievisitsVsdevices")
gevisitsVsdevices_df$metric <- "genievisitsVsdevices"
View(gevisitsVsdevices_df)

#################### Metric7 : contentVisitsVsgenievisits #####################
content_visits$contentVisitsVsgenievisits <- content_visits$gdata.id.length/genie_visits$visits.sum
contentVisitsVsgenievisits_df <- dcast(content_visits,mode + devices ~ weeks,value.var = "contentVisitsVsgenievisits")
contentVisitsVsgenievisits_df$metric <- "contentVisitsVsgenievisits"


###################### Metric8 : contentusageVscontentvisit ###########################
content_usage$contentusageVscontentvisit <-  content_usage$session_length.sum/content_visits$gdata.id.length
contentusageVscontentvisit_df <- dcast(content_usage,mode + devices ~ weeks,value.var = "contentusageVscontentvisit")
contentusageVscontentvisit_df$metric <- "contentusageVscontentvisit"
View(contentusageVscontentvisit_df)

###################### Metric9 : Top10 contents,visits,timespent ######################
content_summ <- summaryBy(gdata.id ~ weeks + mode + gdata.id,data = sess_mode,FUN = length)
content_summ$devices <- "all"
content_summ <- content_summ[order(content_summ$gdata.id.length,decreasing = T),]
#content_summ <- content_summ[1:10,]
content_summ1 <- summaryBy(gdata.id ~ weeks + mode + gdata.id,data = sess_mode[sess_mode$device_time == "one-monthold",],FUN = length)
content_summ1$devices <- "onemonth_old"
content_summ1 <- content_summ1[order(content_summ1$gdata.id.length,decreasing = T),]
#content_summ1 <- content_summ1[1:10,]
content_summ_visits <- rbind(content_summ,content_summ1)
View(content_summ_visits)

content_summ <- summaryBy(session_length ~ weeks + mode + gdata.id,data = sess_mode,FUN = sum)
content_summ$devices <- "all"
content_summ <- content_summ[order(content_summ$session_length.sum,decreasing = T),]
#content_summ <- content_summ[1:10,]
content_summ1 <- summaryBy(session_length ~ weeks + mode + gdata.id,data = sess_mode[sess_mode$device_time == "one-monthold",],FUN = sum)
content_summ1$devices <- "onemonth_old"
content_summ1 <- content_summ1[order(content_summ1$session_length.sum,decreasing = T),]
#content_summ1 <- content_summ1[1:10,]
content_summ_sess <- rbind(content_summ,content_summ1)
content_summ_sess$session_length.sum <- as.integer(content_summ_sess$session_length.sum/60)
content_summ_sess
content_summary <- merge(content_summ_sess,content_summ_visits,by=c("weeks","mode","gdata.id","devices"),all.x = T)
############ Loading content_names ###################3
library(xlsx)
setwd("D:/mafoi/ekstep/works/old_deviceUsage/inputs")
content_names <- read.xlsx("Live Content 2nd March.xlsx",sheetName = "sheet1",encoding = "UTF-8")
names(content_names) <- c("gdata.id","name")
content_summary <- merge(content_summary,content_names,by=c("gdata.id"),all.x = T)

content_summary$topten <- ifelse(!is.na(content_summary$name),paste(content_summary$name,"TimeSpent","(",content_summary$session_length.sum,")","Visits","(",content_summary$gdata.id.length,")"),paste(content_summary$gdata.id,"TimeSpent","(",content_summary$session_length.sum,")","Visits","(",content_summary$gdata.id.length,")"))
View(content_summary)
##################### dcasting by weeks #############################
week_no <- c(unique(content_summary$weeks))
devices <- c(unique(content_summary$devices))
mode <- c(unique(content_summary$mode))
table(content_summary$devices,content_summary$mode)
content_int1 <- content_summary[content_summary$weeks == week_no[1] & content_summary$mode == mode[1] & content_summary$devices == devices[1],]
content_int1 <- content_int1[order(content_int1$session_length.sum,decreasing = T),]
content_int1 <- content_int1[c(1:10),]
content_int_final <- rbind(content_int1)
View(content_int_final)
for(k in c(1:5)){
  for(i in c(1:2)){
    for(j in c(1:2)){
      content_int1 <- content_summary[content_summary$weeks == week_no[k] & content_summary$mode == mode[i] & content_summary$devices == devices[j],]
      content_int1 <- content_int1[order(content_int1$session_length.sum,decreasing = T),]
      content_int1 <- content_int1[c(1:10),]
      content_int_final <- rbind(content_int_final,content_int1)
      
    }
  }
}
  
View(content_int_final)

#################### Merging by weeks ################################
content_weeks <- content_int_final[(11:nrow(content_int_final)),c("weeks","mode","devices","topten")]
content_weeks_final <- content_weeks[(1:40),c("mode","devices")]
for(i in (1:5)){
  content_weeks_final[[week_no[i]]] <- content_weeks[content_weeks$weeks == week_no[i],c("topten")]
}

content_weeks_final$metric <- "content_summary"
View(content_weeks_final)
#################### Metric 11 : Genie Sideloads and content sideloads ############
setwd("D:/mafoi/ekstep/data/Final CSVs/ekstep data/Final CSVs/Final CSVs - GE_TRANSFER")
ge_transfer <- read.csv("Nov2016_GE_TRANSFER.csv",header = T,stringsAsFactors = F)
names(ge_transfer)[1:20]
ge_transfer_f1 <- ge_transfer[,c("ts","did","edata.eks.datatype","edata.eks.direction")]
ge_transfer_f1$dummy <- strtrim(ge_transfer_f1$ts, width = 19)
ge_transfer_f1$date_time <- as.Date(ge_transfer_f1$dummy, format = "%Y-%m-%dT%H:%M:%S")
ge_transfer_f1$ts = NULL
ge_transfer_f1$dummy = NULL
ge_transfer_f1$weeks <- format.Date(ge_transfer_f1$date_time,"%W")
ge_transfer_f1 <- subset(ge_transfer_f1,ge_transfer_f1$date_time >= "2016-11-01" & ge_transfer_f1$date_time <= "2016-11-30")
table(ge_transfer_f1$weeks)
ge_transfer_fin <- merge(ge_transfer_f1,mode_age,by=c("did"))
################# Metric11 :summary Genie_sideloads ################################
ge_transfer_final <- subset(ge_transfer_fin,ge_transfer_fin$edata.eks.datatype == "PROFILE" & ge_transfer_fin$edata.eks.direction == "IMPORT")
sess_mode <- ge_transfer_final
content_visits <- summaryBy(edata.eks.direction ~ weeks+mode,data = sess_mode,FUN = length)
content_visits$devices <- "all"
content_visits1 <- summaryBy(edata.eks.direction ~ weeks+mode,data = sess_mode[sess_mode$device_time == "one-monthold",],FUN = length)
content_visits1$devices <- "onemonth_old"
content_visits <- rbind(content_visits,content_visits1)
genie_sideloads <- dcast(content_visits,devices + mode ~ weeks,value.var = "edata.eks.direction.length")
genie_sideloads$metric <- "genie_sideloads"
View(genie_sideloads)
table(ge_transfer$edata.eks.datatype,ge_transfer$edata.eks.direction)
################# Metric12 :summary content_sideloads ################################
ge_transfer_final <- subset(ge_transfer_fin,ge_transfer_fin$edata.eks.datatype == "CONTENT" & ge_transfer_fin$edata.eks.direction == "IMPORT")
sess_mode <- ge_transfer_final
content_visits <- summaryBy(edata.eks.direction ~ weeks+mode,data = sess_mode,FUN = length)
content_visits$devices <- "all"
content_visits1 <- summaryBy(edata.eks.direction ~ weeks+mode,data = sess_mode[sess_mode$device_time == "one-monthold",],FUN = length)
content_visits1$devices <- "onemonth_old"
content_visits <- rbind(content_visits,content_visits1)
content_sideloads <- dcast(content_visits,devices + mode ~ weeks,value.var = "edata.eks.direction.length")
content_sideloads$metric <- "content_sideloads"
View(content_sideloads)
table(ge_transfer$edata.eks.datatype,ge_transfer$edata.eks.direction)
################## merging all the metrics ###########################
olddevice_usage <- rbind.fill(devices_usage_df,content_usage_df,contVsdevices_df,gevisitsVsdevices_df,contentVisitsVsgenievisits_df,contentusageVscontentvisit_df,content_weeks_final,genie_sideloads,content_sideloads)
names(olddevice_usage)
oldDevice_analysis <- olddevice_usage[,c("metric",c(names(olddevice_usage))[1:7])]
oldDevice_analysis[is.na(oldDevice_analysis)] <- 0
View(oldDevice_analysis)
################# Writing the output #####################
setwd("D:/mafoi/ekstep/works/old_deviceUsage/inputs")
write.xlsx(oldDevice_analysis,"olddeviceusage_analysis(27-03-2017).xlsx",row.names = F,sheetName = "onemontholddevices-analysis")
