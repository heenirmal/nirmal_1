#####################################
##### Genie Retention Scorecard #####
#####################################

#######
setwd("D:/mafoi/ekstep/works/genie_retention/inputs")
# Data Requiredf
Genie_Launch <- read.csv(file = "ME_SESSION_SUMMARY_VISUALIZATION_1Sep16-26Feb17.csv", header = T, stringsAsFactors = F)
Genie_Launch <- subset(Genie_Launch,Genie_Launch$tags.genie..Descending == "dff9175fa217e728d86bc1f4d8f818f6d2959303" | Genie_Launch$tags.genie..Descending == "ee176639152bd418be786548f89ad6d05dc8d308")
devices_tags <- as.data.frame(unique(Genie_Launch$dimensions.did..Descending))
write.csv(devices_tags,"devices.csv",row.names = F)

setwd("D:/mafoi/ekstep/works/genie_retention/inputs")
# Data Required
Genie_Launch <- read.csv(file = "Visualization-ME_GENIE_LAUNCH_SUMMARY_1sep16-26feb17.csv", header = T, stringsAsFactors = F)
sess_len <- read.csv(file = "session_length.csv", header = T, stringsAsFactors = F)
devices_tags <- read.csv("devices.csv",header = T,stringsAsFactors = F)
View(Genie_Launch)
# Required libraries
library(doBy)
library(plyr)

#### Part 1 - Data Processing for Genie Launch ####

## creating date field for each did
Genie_Launch$date <- as.Date(as.character(Genie_Launch$date_time))
Genie_Launch$context.date_range.to.per.second <- NULL
colnames(Genie_Launch) <- c("did","date_range","time_spent","count","date_time","date")
View(Genie_Launch)
############ Date Filter #############
Genie_Launch <- subset(Genie_Launch,Genie_Launch$date >= "2017-01-27" & Genie_Launch$date <= "2017-02-26")
############ device Filter ############
Genie1 <- subset(Genie_Launch,Genie_Launch$did == devices_tags[1,1])
Genie_final <- rbind(Genie1)
for(i in (2:nrow(devices_tags))){
  Genie1 <- subset(Genie_Launch,Genie_Launch$did == devices_tags[i,1])
  Genie_final <- rbind(Genie_final,Genie1)
}

Genie_Launch <- Genie_final

############### Time duration buckets ####################
Genie_Launch$hour_time <- format.Date(Genie_Launch$date_time,format = "%H:%M:%S")
Genie_Launch$time_buckets <- ifelse(Genie_Launch$hour_time >= "09:00:00" & Genie_Launch$hour_time <= "13:00:00","9am-1pm",ifelse(Genie_Launch$hour_time > "13:00:00" & Genie_Launch$hour_time <= "18:00:00","1pm-6pm","7pm-9am"))

## converting time spent in numeric form and 
#             filtering out all the genie sessions of less than 5 sec
Genie_Launch$time_spent <- round(as.numeric(gsub(",","",Genie_Launch$time_spent)),0)
Genie_Launch <- subset(Genie_Launch, time_spent >= 5)

Genie_Launch_unq <- Genie_Launch[,c("did","time_buckets","date")]
Genie_Launch_unq <- Genie_Launch_unq[!duplicated(Genie_Launch_unq),]
View(Genie_Launch_unq)
## summarizing launch summary at date and did level

########## Metric 1: No.of devices used ############
require(doBy)
require(reshape2)
launch1 <- summaryBy(did ~ time_buckets + date, data = Genie_Launch_unq, FUN = length)
names(launch1) <- c("time_buckets","date","Noofdevicesused")
m1 <- dcast(launch1,time_buckets ~ date)
m1$metrics <- "Noofdevicesused"
View(m1)
########## Metric 2: Ginie visits ############
launch2 <- summaryBy(did ~ time_buckets + date, data = Genie_Launch, FUN = length)
names(launch2) <- c("time_buckets","date","Genie_visits")
m2 <- dcast(launch2,time_buckets ~ date)
m2$metrics <- "Genie_visits"
View(m2)

##### Part 2 - Data Processing for Session Length #####

# filtering data- removing all content which are played less than 5 seconds 
#                 capping anything above 30min to 30min
sess_len1 <- subset(sess_len, session_length >= 5)
sess_len1$session_length <- ifelse(sess_len1$session_length >= 1800,1800,sess_len1$session_length)
# creating date field for sess_len1
sess_len1$date <- as.Date(as.character(sess_len1$date_time))
################ Date_filtering ##############
sess_len1 <- subset(sess_len1,sess_len1$date >= "2017-01-27" & sess_len1$date <= "2017-02-26")
sess_len1$hour_time <- format.Date(sess_len1$date_time,format = "%H:%M:%S")
sess_len1$time_buckets <- ifelse(sess_len1$hour_time >= "09:00:00" & sess_len1$hour_time <= "13:00:00","9am-1pm",ifelse(sess_len1$hour_time > "13:00:00" & sess_len1$hour_time <= "18:00:00","1pm-6pm","7pm-9am"))

############ Device_filtering ##############
sess1 <- subset(sess_len1,sess_len1$did == devices_tags[1,1]) 
sess_final <- rbind(sess1)
for(i in (2:nrow(devices_tags))){
  sess1 <- subset(sess_len1,sess_len1$did == devices_tags[i,1]) 
  sess_final <- rbind(sess_final,sess1)
}

View(sess_final$session_length)

########### metric 3: Content Usage ################
met3 <- summaryBy(session_length ~ time_buckets + date,data = sess_final,FUN = sum)
names(met3) <- c("time_buckets","date","ContentUsage")
met3$ContentUsage <- as.integer(met3$ContentUsage/60)
m3 <- dcast(met3,time_buckets ~ date)
m3$metrics <- "ContentUsage"
########## metric 4: content visits ###############
sess_final$count <- 1
met4 <- summaryBy(count ~ time_buckets + date,data = sess_final,FUN = sum)
names(met4) <- c("time_buckets","date","content_visits")
View(met4)
m4 <- dcast(met4,time_buckets ~ date)
m4$metrics <- "content_visits"
els_final <- rbind.fill(m1,m2)
els_final <- rbind.fill(els_final,m3)
els_final <- rbind.fill(els_final,m4)
els_final[is.na(els_final)] <- 0
View(els_final)
write.csv(els_final,"esl_final.csv",row.names = F)

met5 <- summaryBy(session_length ~ time_buckets + date + gdata.id,data = sess_final,FUN = sum)
m5 <- dcast(met5,gdata.id + time_buckets ~ date)
m5[is.na(m5)] <- 0
write.csv(m5,"day_wise_content.csv",row.names = F)
View(m5)
####### derived_metric ###############
els_final$Genievisits_devices <- els_final$Genie_visits/els_final$`No.of devices used`
els_final$Contentvisit_ginievisit <- els_final$content_visits/els_final$Genie_visits
els_final$contentusage_contenvisit <- els_final$ContentUsage/els_final$content_visits
View(t(els_final))

write.csv(t(els_final),"esl.csv")
## getting data for This Week and the the Last Week
sess_len1$week <- as.numeric(format.Date(sess_len1$date,"%W"))
sess_len1 <- arrange(sess_len1, desc(date))
sess_len1$weeknum <- ifelse(sess_len1$week == unique(sess_len1$week)[1], "This Week", 
                          ifelse(sess_len1$week == unique(sess_len1$week)[2], "Last Week", "Others"))
tw2 <- subset(sess_len1, weeknum == "This Week")
lw2 <- subset(sess_len1, weeknum == "Last Week")

## getting content played and content usage for this week
tw2 <- arrange(tw2, did, day_num)
a <- tw2[!duplicated(tw2$did),]
a$cat <- ifelse(a$day_num < 7, "New Devices", 
                ifelse(a$day_num >= 7 & a$day_num < 28, "1 Week to 1 Month old",
                       ifelse(a$day_num >= 28, "More than one Month old", "Others")))
a <- subset(a, select = c(did,cat))
tw2 <- merge(tw2,a,by="did",all.x = T)

# summarizing the data at did and date level to get number of content played and content usage
require(doBy)
cp1 <- summaryBy(gdata.id ~ cat, data = tw2, FUN = length)
cu1 <- summaryBy(session_length ~ cat, data = tw2, FUN = sum)

## getting content played and content usage for last week
lw2 <- arrange(lw2, did, day_num)
b <- lw2[!duplicated(lw2$did),]
b$cat <- ifelse(b$day_num < 7, "New Devices", 
                ifelse(b$day_num >= 7 & b$day_num < 28, "1 Week to 1 Month old",
                       ifelse(b$day_num >= 28, "More than one Month old", "Others")))
b <- subset(b, select = c(did,cat))
lw2 <- merge(lw2,b,by="did",all.x = T)

# summarizing the data at did and date level to get number of content played and content usage
require(doBy)
cp2 <- summaryBy(gdata.id ~ cat, data = lw2, FUN = length)
cu2 <- summaryBy(session_length ~ cat, data = lw2, FUN = sum)

rm(a,b,tw2,lw2)
#####

## merging all the this week metrics
final <- merge(cu1,ud1,by="cat",all = T)
final <- merge(final,gv1,by="cat",all = T)
final <- merge(final,cp1,by="cat",all = T)
row.names(final) <- final$cat
final$cat <- NULL

colnames(final) <- c("CONTENT USAGE","#DEVICES","#GENIE VISITS","#CONTENT VISITS")

final["Overall_current",] <- colSums(final)

## merging all the last week metrics
last <- merge(cu2,ud2,by="cat",all = T)
last <- merge(last,gv2,by="cat",all = T)
last <- merge(last,cp2,by="cat",all = T)
row.names(last) <- last$cat
last$cat <- NULL

colnames(last) <- c("CONTENT USAGE","#DEVICES","#GENIE VISITS","#CONTENT VISITS")

last["Overall_last",] <- colSums(last)
last <- last["Overall_last",]

######## final check up if the data is correct

unique(launch2$week)[1]
unique(launch2$week)[2]
unique(sess_len1$week)[1]
unique(sess_len1$week)[2]

## final merged data frame
final <- rbind(final,last)
final$`CONTENT USAGE` <- round(final$`CONTENT USAGE` / 60,0)
final$'#GENIE VISITS / #DEVICES' <- round(final$`#GENIE VISITS` / final$'#DEVICES',1)
final$'#CONTENT VISITS / #GENIE VISITS' <- round(final$`#CONTENT VISITS` / final$'#GENIE VISITS',1)
final$'CONTENT USAGE / #CONTENT VISITS' <- round(final$`CONTENT USAGE` / final$`#CONTENT VISITS`,1)
final$'CONTENT USAGE / DEVICE' <- round(final$`CONTENT USAGE` / final$`#DEVICES`,1)
final$`#GENIE VISITS` <- NULL
final$`#CONTENT VISITS` <- NULL

final["%CHANGE",] <- round((final["Overall_current",] - final["Overall_last",]) / final["Overall_last",],2) 

#### writing the final file ####
write.csv(final, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project Genie Funnel and Usage Metrics/Project Genie Scorecard (Ikpreet)/Genie_Retention_Scorecard.csv", row.names = T)

################################# End of Code #####################################