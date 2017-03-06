#########################################
##### Devices more than 1 month old #####
#########################################

# Data Required
Genie_Launch <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/Visualization-ME_GENIE_LAUNCH_SUMMARY_1sep16-19feb17.csv", header = T, stringsAsFactors = F)
sess_len <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets/session_length.csv", header = T, stringsAsFactors = F)
off_devices <- read.csv(file = "")

# Filtering the Data
#Date Filter
#Device and Tag Filters



# Required libraries
library(doBy)
library(plyr)

#### Part 1 - Data Processing for Genie Launch ####

## creating date field for each did
Genie_Launch$date <- as.Date(as.character(Genie_Launch$date_time))
Genie_Launch$context.date_range.to.per.second <- NULL
colnames(Genie_Launch) <- c("did","date_range","time_spent","count","date_time","date")

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
launch1 <- summaryBy(count ~ did | date, data = Genie_Launch, FUN = sum)

## merge con with launch1 to get the day info for each did
launch2 <- merge(launch1, con, by = c("date","did"), all.x = T)
launch2$day_num <- as.numeric(substr(launch2$day,2,nchar(launch2$day))) 
launch2$date <- as.character(launch2$date)
launch2 <- subset(launch2, date >= as.Date("2016-09-01") & date < as.Date("2017-02-20"))

## getting data for This Week and Last Week
launch2$week <- as.numeric(format.Date(launch2$date,"%W"))
launch2 <- arrange(launch2, desc(date))
launch2$weeknum <- ifelse(launch2$week == unique(launch2$week)[1], "This Week", 
                          ifelse(launch2$week == unique(launch2$week)[2], "Last Week", "Others"))
tw1 <- subset(launch2, weeknum == "This Week")
lw1 <- subset(launch2, weeknum == "Last Week")

## getting active devices and genie vists for this week
tw1 <- arrange(tw1, did, day_num)
a <- tw1[!duplicated(tw1$did),]
a$cat <- ifelse(a$day_num < 7, "New Devices", 
                ifelse(a$day_num >= 7 & a$day_num < 28, "1 Week to 1 Month old",
                       ifelse(a$day_num >= 28, "More than one Month old", "Others")))
a <- subset(a, select = c(did,cat))
tw1 <- merge(tw1,a,by="did",all.x = T)
a$did_count <- 1

## getting required variables for this week
ud1 <- summaryBy(did_count ~ cat, data = a, FUN = sum)
gv1 <- summaryBy(count.sum ~ cat, data = tw1, FUN = sum)

## getting active devices and genie vists for last week
lw1 <- arrange(lw1, did, day_num)
b <- lw1[!duplicated(lw1$did),]
b$cat <- ifelse(b$day_num < 7, "New Devices", 
                ifelse(b$day_num >= 7 & b$day_num < 28, "1 Week to 1 Month old",
                       ifelse(b$day_num >= 28, "More than one Month old", "Others")))
b <- subset(b, select = c(did,cat))
lw1 <- merge(lw1,b,by="did",all.x = T)
b$did_count <- 1

## getting required variables for last week
ud2 <- summaryBy(did_count ~ cat, data = b, FUN = sum)
gv2 <- summaryBy(count.sum ~ cat, data = lw1, FUN = sum)

rm(a,b,launch1,tw1,lw1)
#####

##### Part 2 - Data Processing for Session Length #####

# filtering data- removing all content which are played less than 5 seconds 
#                 capping anything above 30min to 30min
sess_len1 <- subset(sess_len, session_length >= 5)
sess_len1$session_length <- ifelse(sess_len1$session_length >= 1800,1800,sess_len1$session_length)
# creating date field for sess_len1
sess_len1$date <- as.Date(as.character(sess_len1$date_time))
sess_len1 <- subset(sess_len1, date >= as.Date("2016-09-01") & date < as.Date("2017-02-20"))
sess_len1 <- merge(sess_len1,con, by = c("did","date"), all.x = T)
sess_len1$day_num <- as.numeric(substr(sess_len1$day,2,nchar(sess_len1$day)))
sess_len1$date <- as.character(sess_len1$date)

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