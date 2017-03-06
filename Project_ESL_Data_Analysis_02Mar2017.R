###########################################
########### ESL Data Analysis #############
###########################################

### Hypothesis 1 ###

##### Part A - Time of Play #####

# Input Data
ses1 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/ME_SESSION_SUMMARY_VISUALIZATION_1Sep16-26Feb17.csv", header = T, stringsAsFactors = F)
ses2 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/ME_SESSION_SUMMARY_VISUALIZATION_1Jul16-31Aug16.csv", header = T, stringsAsFactors = F)
ses1 <- rbind.fill(ses1,ses2)
Genie_Launch <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data/Visualization-ME_GENIE_LAUNCH_SUMMARY_7jul16-26feb17.csv", header = T, stringsAsFactors = F)
rm(ses2)

# Filtering for program tag - becb887fe82f24c644482eb30041da6d88bd8150
require(dplyr)
did1 <- filter(ses1, tags.genie..Descending == "becb887fe82f24c644482eb30041da6d88bd8150")
did2 <- unique(did1$dimensions.did..Descending)
d1 <- filter(ses1, dimensions.did..Descending %in% did2)
gl1 <- filter(Genie_Launch, dimensions.did..Descending %in% did2)

rm(did1)

# Filtering out relevant variables from d1
d1$ets..Descending <- NULL
d1$context.date_range.from..Descending <- NULL
d1$dimensions.group_user..Descending <- NULL
d1$dimensions.anonymous_user..Descending <- NULL
d1$edata.eks.contentType..Descending <- NULL
d1$tags.genie..Descending <- NULL
d1$Count <- NULL
d1$edata.eks.start_time..Descending <- NULL
names(d1)
colnames(d1) <- c("uid","gdata.id","did","timeSpent","date_time")

# Filtering out relevant variables from gl1
gl1$context.date_range.to.per.second <- NULL
gl1$context.date_range.from.per.second <- NULL
gl1$Sum.of.edata.eks.timeSpent <- NULL
colnames(gl1) <- c("did","count","date_time")

# converting timeSpent for d1 into numeric
d1$timeSpent <- as.numeric(as.character(gsub(",","",d1$timeSpent)))
# filtering timeSpent of less than 5seconds
d1 <- filter(d1, timeSpent >= 5)

# Adding field of count of content played
d1$count <- 1

### Adding Time of Play for contents
require(lubridate)
d1$top <- ifelse(hour(d1$date_time) >= 9 & hour(d1$date_time) < 16, "9am to 4pm",
                 ifelse(hour(d1$date_time) >= 16 & hour(d1$date_time) < 19, "4pm to 7pm",
                        ifelse(hour(d1$date_time) >= 19 & hour(d1$date_time) < 22, "7pm to 10pm",
                               ifelse(hour(d1$date_time) >= 22 | hour(d1$date_time) < 6, "10pm to 6am",
                                      ifelse(hour(d1$date_time) >= 6 & hour(d1$date_time) < 9, "6am to 9am", NA)))))

gl1$top <- ifelse(hour(gl1$date_time) >= 9 & hour(gl1$date_time) < 16, "9am to 4pm",
                 ifelse(hour(gl1$date_time) >= 16 & hour(gl1$date_time) < 19, "4pm to 7pm",
                        ifelse(hour(gl1$date_time) >= 19 & hour(gl1$date_time) < 22, "7pm to 10pm",
                               ifelse(hour(gl1$date_time) >= 22 | hour(gl1$date_time) < 6, "10pm to 6am",
                                      ifelse(hour(gl1$date_time) >= 6 & hour(gl1$date_time) < 9, "6am to 9am", NA)))))

# d1$topc_9to16 <- ifelse(hour(d1$date_time) >= 9 & hour(d1$date_time) < 16,1,0)
# d1$topt_9to16 <- ifelse(hour(d1$date_time) >= 9 & hour(d1$date_time) < 16,d1$timeSpent,0)
# d1$topc_16to19 <- ifelse(hour(d1$date_time) >= 16 & hour(d1$date_time) < 19,1,0)
# d1$topt_16to19 <- ifelse(hour(d1$date_time) >= 16 & hour(d1$date_time) < 19,d1$timeSpent,0)
# d1$topc_19to22 <- ifelse(hour(d1$date_time) >= 19 & hour(d1$date_time) < 22,1,0)
# d1$topt_19to22 <- ifelse(hour(d1$date_time) >= 19 & hour(d1$date_time) < 22,d1$timeSpent,0)
# d1$topc_22to6 <- ifelse(hour(d1$date_time) >= 22 | hour(d1$date_time) < 6,1,0)
# d1$topt_22to6 <- ifelse(hour(d1$date_time) >= 22 | hour(d1$date_time) < 6,d1$timeSpent,0)
# d1$topc_6to9 <- ifelse(hour(d1$date_time) >= 6 & hour(d1$date_time) < 9,1,0)
# d1$topt_6to9 <- ifelse(hour(d1$date_time) >= 6 & hour(d1$date_time) < 9,d1$timeSpent,0)

# Creating Date,Week,Month field
d1$date <- as.Date(d1$date_time)
d1$week <- week(d1$date_time)
d1$month <- paste0(month(d1$date_time,label = T),year(d1$date_time))
gl1$date <- as.Date(gl1$date_time)
gl1$week <- week(gl1$date_time)
gl1$month <- paste0(month(gl1$date_time,label = T),year(gl1$date_time))

# Summarizing Time of Play variables at did and date/week/month level
require(doBy)
#content visits + content usage by date and top
sum1a <- summaryBy(count + timeSpent ~ top | did | date, data = d1, FUN = sum)
sum1a$timeSpent.sum <- sum1a$timeSpent.sum/60
sum1a <- rename(sum1a, content_visits = count.sum, content_usage_minutes = timeSpent.sum)
#genie visits by date and top
sum1b <- summaryBy(count ~ top | did | date, data = gl1, FUN = sum)
sum1b <- rename(sum1b, genie_visits = count.sum)

#unique content played per device per day
sum1aa <- summaryBy(count ~ did | gdata.id | top, data = d1, FUN=sum)
# sum1aa <- unique(subset(d1,select = c(gdata.id,did,date,top)))

### Getting Active Devices and Total Devices per day
# Picking Relevant columns
gl2 <- unique(subset(gl1, select = c(did,date)))
gl2 <- arrange(gl2, did, date)
act_devices <- summaryBy(did ~ date, data = gl2, FUN = length)
gl2a <- gl2[!duplicated(gl2$did),]
gl2b <- summaryBy(did ~ date, data = gl2a, FUN = length)
# Creating Sequence of Dates and adding the number of devices which were added on a particular day
date_seq <- c()
date_seq$date <- seq(as.Date("2016/7/7"), as.Date("2017/2/26"), "days")
date_seq <- as.data.frame(date_seq)
date_seq <- merge(date_seq,gl2b,by="date",all = T)
date_seq[is.na(date_seq)] <- 0
date_seq$total_devices <- cumsum(date_seq$did.length)
date_seq$did.length <- NULL  
date_seq <- merge(date_seq,act_devices,by="date",all = T)
date_seq <- rename(date_seq, active_devices = did.length)
# Number of Active Devices vs Total Devices per day
sum1c <- date_seq
rm(gl2,gl2a,gl2b)

### Getting Active Days and Total Days per device
# Picking Relevant columns
gl2 <- unique(subset(gl1, select = c(did,date)))
gl2 <- arrange(gl2, did, date)
act_days <- summaryBy(date ~ did, data = gl2, FUN = length)
gl2a <- gl2[!duplicated(gl2$did),]
gl2a$last_date <- as.Date("2017/2/26")
gl2a$total_days <- gl2a$last_date - gl2a$date
gl2a$date <- NULL
gl2a$last_date <- NULL
# Number of Active Days vs Total Days per device
sum1f <- merge(gl2a,act_days,by="did",all = T)
sum1f <- rename(sum1f,active_days=date.length)
rm(gl2,gl2a,gl2b)

# Creating a Weekday activity for each device
gl2 <- gl1
gl2$weekday <- weekdays(gl2$date)
# Days of Week Devices are most active
sum1d <- summaryBy(count ~ did | weekday, data = gl2, FUN = sum)
sum1d <- rename(sum1d, genie_visits = count.sum)
# Month-wise activity of devices 
sum1e <- summaryBy(count ~ did | month, data = gl2, FUN = sum)
sum1e <- rename(sum1e, genie_visits = count.sum)
rm(gl2,act_devices,date_seq)

# sum1a <- summaryBy(topc_9to16 + topt_9to16 + topc_16to19 + topt_16to19 + topc_19to22 + topt_19to22 + topc_22to6 + topt_22to6 + topc_6to9 + topt_6to9 ~ did | date, data = d1, FUN = sum)
# sum1b <- summaryBy(topc_9to16 + topt_9to16 + topc_16to19 + topt_16to19 + topc_19to22 + topt_19to22 + topc_22to6 + topt_22to6 + topc_6to9 + topt_6to9 ~ did | week, data = d1, FUN = sum)
# sum1c <- summaryBy(topc_9to16 + topt_9to16 + topc_16to19 + topt_16to19 + topc_19to22 + topt_19to22 + topc_22to6 + topt_22to6 + topc_6to9 + topt_6to9 ~ did | month, data = d1, FUN = sum)
# sum1d <- summaryBy(topc_9to16 + topt_9to16 + topc_16to19 + topt_16to19 + topc_19to22 + topt_19to22 + topc_22to6 + topt_22to6 + topc_6to9 + topt_6to9 ~ did | gdata.id | date, data = d1, FUN = sum)
# sum1e <- summaryBy(topc_9to16 + topt_9to16 + topc_16to19 + topt_16to19 + topc_19to22 + topt_19to22 + topc_22to6 + topt_22to6 + topc_6to9 + topt_6to9 ~ did | gdata.id | week, data = d1, FUN = sum)
# sum1f <- summaryBy(topc_9to16 + topt_9to16 + topc_16to19 + topt_16to19 + topc_19to22 + topt_19to22 + topc_22to6 + topt_22to6 + topc_6to9 + topt_6to9 ~ did | gdata.id | month, data = d1, FUN = sum)

rm(d1)
#####

##### Part B - Frequency of Switching profiles #####

## Input Data
gi1 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT/Jul2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
gi2 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT/Aug2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
gi3 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT/Sep2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
gi4 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT/Oct2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
gi5 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT/Nov2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
gi6 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT/Dec2016_GE_INTERACT.csv", header = T, stringsAsFactors = F)
gi7 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT/Jan2017_GE_INTERACT.csv", header = T, stringsAsFactors = F)
gi8 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT/Feb2017_GE_INTERACT(1Feb17-12Feb17).csv", header = T, stringsAsFactors = F)

require(plyr)
gi <- rbind.fill(gi1,gi2,gi3,gi4,gi5,gi6,gi7,gi8)

rm(gi1,gi2,gi3,gi4,gi5,gi6,gi7,gi8)

## Filtering out the data for relevant dids
require(dplyr)
g1 <- filter(gi, did %in% did2)

## Creating date, date_time, weeknum, month field for g1
g1$date <- as.Date(strtrim(g1$ts, width = 19), format = "%Y-%m-%dT%H:%M:%S")
g1$date_time <- as.POSIXct(strtrim(g1$ts, width = 19), format = "%Y-%m-%dT%H:%M:%S")
g1$week <- week(g1$date)
g1$month <- paste0(month(g1$date,label = T),year(g1$date))

## Picking only relevant columns
g2 <- unique(subset(g1, select = c(did,uid,date,date_time,week,month)))
g2 <- arrange(g2, did, date_time)
g2 <- filter(g2, uid != "")

## Getting Unique users per device per date/week/month
g3a <- unique(subset(g2, select = c(did,uid,date,month)))
g3b <- unique(subset(g2, select = c(did,uid,week,month)))
sum2a <- summaryBy(uid ~ did | date, data = g3a, FUN = length)
sum2c <- summaryBy(uid ~ did | date | month, data = g3a, FUN = length)
#average unique users per device per active days
sum2aa <- summaryBy(uid.length ~ did, data = sum2a, FUN = mean)
sum2aa <- rename(sum2aa, avg_uniq_users_days = uid.length.mean)
sum2ca <- summaryBy(uid.length ~ did | month, data = sum2c, FUN = mean)
sum2ca <- rename(sum2ca, avg_uniq_users_days = uid.length.mean)
sum2b <- summaryBy(uid ~ did | week, data = g3b, FUN = length)
sum2d <- summaryBy(uid ~ did | week | month, data = g3b, FUN = length)
#average unique users per device per active weeks
sum2ba <- summaryBy(uid.length ~ did, data = sum2b, FUN = mean)
sum2ba <- rename(sum2ba, avg_uniq_users_weeks = uid.length.mean)
sum2da <- summaryBy(uid.length ~ did | month, data = sum2d, FUN = mean)
sum2da <- rename(sum2da, avg_uniq_users_weeks = uid.length.mean)

## Adding variable for switching uid by did and date
# g2$switch_uid_day <- c(0,as.numeric(with(g2,did[1:(length(did)-1)] == did[2:length(did)] & uid[1:(length(uid)-1)] != uid[2:length(uid)] & date[1:(length(date)-1)] == date[2:length(date)])))
# g2$switch_uid_week <- c(0,as.numeric(with(g2,did[1:(length(did)-1)] == did[2:length(did)] & uid[1:(length(uid)-1)] != uid[2:length(uid)] & week[1:(length(week)-1)] == week[2:length(week)])))
# g2$switch_uid_month <- c(0,as.numeric(with(g2,did[1:(length(did)-1)] == did[2:length(did)] & uid[1:(length(uid)-1)] != uid[2:length(uid)] & month[1:(length(month)-1)] == month[2:length(month)])))
# g2$active_days <- c(1,as.numeric(with(g2,did[1:(length(did)-1)] == did[2:length(did)] & date[1:(length(date)-1)] != date[2:length(date)])))
# g2$active_weeks <- c(1,as.numeric(with(g2,did[1:(length(did)-1)] == did[2:length(did)] & week[1:(length(week)-1)] != week[2:length(week)])))
# g2$active_months <- c(1,as.numeric(with(g2,did[1:(length(did)-1)] == did[2:length(did)] & month[1:(length(month)-1)] != month[2:length(month)])))

## Summarize switch of uid at did and date/week/month level
# sum2a <- summaryBy(switch_uid_day + active_days ~ did | date, data = g2, FUN = sum)
# sum2b <- summaryBy(switch_uid_week + active_weeks ~ did | week, data = g2, FUN = sum)
# sum2c <- summaryBy(switch_uid_month + active_months ~ did | month, data = g2, FUN = sum)
# sum2aa <- g2[!duplicated(g2[which(names(g2) %in% c("did","uid","date"))]),]
# sum2aa$count <- 1
# sum2bb <- g2[!duplicated(g2[which(names(g2) %in% c("did","uid","week"))]),]
# sum2bb$count <- 1
# sum2aa <- summaryBy(count ~ did | date, data = sum2aa, FUN = sum)
# sum2bb <- summaryBy(count ~ did | week, data = sum2bb, FUN = sum)
# 
rm(g1,g2,g3a,g3b,sum2a,sum2b)
#####

##### Part C - Content Time Spent between Anonymous and Profiled User & Group and Individual Users #####

# Filtering relevant devices for analysis
d1 <- filter(ses1, dimensions.did..Descending %in% did2)

# Filtering out relevant variables from d1
d1$ets..Descending <- NULL
d1$context.date_range.from..Descending <- NULL
d1$edata.eks.contentType..Descending <- NULL
d1$tags.genie..Descending <- NULL
d1$Count <- NULL
d1$edata.eks.start_time..Descending <- NULL
names(d1)
colnames(d1) <- c("uid","gdata.id","did","group_user","anonymous_user","timeSpent","date_time")

# converting timeSpent into numeric
d1$timeSpent <- as.numeric(as.character(gsub(",","",d1$timeSpent)))
# filtering timeSpent of less than 5seconds
d1 <- filter(d1, timeSpent >= 5)
# adding date,week,month field
d1$date <- as.Date(d1$date_time)
d1$week <- week(d1$date_time)
d1$month <- month(d1$date_time)

# Creating Time Spent fields for group, individual, anonymous and profiled users
d1$ts_g <- ifelse(d1$group_user == 1, d1$timeSpent, 0)
d1$ts_i <- ifelse(d1$group_user == 0, d1$timeSpent, 0)
d1$ts_a <- ifelse(d1$anonymous_user == 1, d1$timeSpent, 0)
d1$ts_p <- ifelse(d1$anonymous_user == 0, d1$timeSpent, 0)

# Getting Summarized results for Time Spent by did and date level
sum3 <- summaryBy(ts_g + ts_i + ts_a + ts_p ~ did, data = d1, FUN = sum)
# sum3a <- summaryBy(ts_g + ts_i + ts_a + ts_p ~ did | date, data = d1, FUN = sum)
# sum3b <- summaryBy(ts_g + ts_i + ts_a + ts_p ~ did | week, data = d1, FUN = sum)
# sum3c <- summaryBy(ts_g + ts_i + ts_a + ts_p ~ did | month, data = d1, FUN = sum)
sum3$'% of Time Spent by Anonymous User' <- round(sum3$ts_a.sum / (sum3$ts_a.sum + sum3$ts_p.sum),2)
sum3$'% of Time Spent by Individual Profiles' <- round(sum3$ts_i.sum / (sum3$ts_g.sum + sum3$ts_i.sum),2)
sum3$ts_g.sum <- NULL
sum3$ts_i.sum <- NULL
sum3$ts_a.sum <- NULL
sum3$ts_p.sum <- NULL

rm(d1)
#####

##### Part D - Content Usage Profile #####

## input files
gcp1 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Jul2016_GE_CREATE_PROFILE.csv", header = T, stringsAsFactors = F)
gcp2 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Aug2016_GE_CREATE_PROFILE.csv", header = T, stringsAsFactors = F)
gcp3 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Sep2016_GE_CREATE_PROFILE.csv", header = T, stringsAsFactors = F)
gcp4 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Oct2016_GE_CREATE_PROFILE.csv", header = T, stringsAsFactors = F)
gcp5 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Nov2016_GE_CREATE_PROFILE.csv", header = T, stringsAsFactors = F)
gcp6 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Dec2016_GE_CREATE_PROFILE.csv", header = T, stringsAsFactors = F)
gcp7 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Jan2017_GE_CREATE_PROFILE.csv", header = T, stringsAsFactors = F)
gcp8 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_CREATE_PROFILE/Feb2017_GE_CREATE_PROFILE(1Feb17-12Feb17).csv", header = T, stringsAsFactors = F)

require(plyr)
gcp <- rbind.fill(gcp1,gcp2,gcp3,gcp4,gcp5,gcp6,gcp7,gcp8)
rm(gcp1,gcp2,gcp3,gcp4,gcp5,gcp6,gcp7,gcp8)

## filter out the relevant devices
require(dplyr)
g1 <- filter(gcp, did %in% did2)

## picking only relevant columns
g2 <- unique(subset(g1, select = c(did,uid,ts)))

## creating date, week and month fields
g2$date <- as.Date(strtrim(g2$ts, width = 19), format = "%Y-%m-%dT%H:%M:%S")
g2$week <- week(g2$date)
g2$month <- month(g2$date)
g2$ts <- NULL

# Getting Summarized results for number of profiles created per did
sum4a <- summaryBy(uid ~ did, data = g2, FUN = length)
sum4a <- rename(sum4a, users_per_device = uid.length)
# sum4b <- summaryBy(uid ~ did | week, data = g2, FUN = length)
# sum4c <- summaryBy(uid ~ did | month, data = g2, FUN = length)

# Filtering relevant devices for analysis
d1 <- filter(ses1, dimensions.did..Descending %in% did2)

# Filtering out relevant variables from d1
d1$ets..Descending <- NULL
d1$context.date_range.from..Descending <- NULL
d1$edata.eks.contentType..Descending <- NULL
d1$tags.genie..Descending <- NULL
d1$Count <- NULL
d1$edata.eks.start_time..Descending <- NULL
d1$dimensions.group_user..Descending <- NULL
d1$dimensions.anonymous_user..Descending <- NULL
names(d1)
colnames(d1) <- c("uid","gdata.id","did","timeSpent","date_time")

# converting timeSpent into numeric
d1$timeSpent <- as.numeric(as.character(gsub(",","",d1$timeSpent)))
# filtering timeSpent of less than 5seconds
d1 <- filter(d1, timeSpent >= 5)
# adding date,week,month field
d1$date <- as.Date(d1$date_time)
d1$week <- week(d1$date_time)
d1$month <- month(d1$date_time)

sum5a <- summaryBy(timeSpent ~ did | uid, data = d1, FUN = sum)
sum5b <- summaryBy(timeSpent ~ did , data = d1, FUN = sum)
sum5 <- merge(sum5a,sum5b,by="did",all = T)
sum5$'perc_of_timeSpent' <- round(sum5$timeSpent.sum.x / sum5$timeSpent.sum.y, 4)

require(dplyr)
sum5 <- arrange(sum5, did, desc(perc_of_timeSpent))
sum5$timeSpent.sum.x <- NULL
sum5$timeSpent.sum.y <- NULL

rm(g1,g2,d1,sum5a,sum5b)
#####


##### Final Part - Getting Daily/Weekly/Monthly Average per did #####

# For Time of Play
# a1 <- summaryBy(topc_11to3.sum + topt_11to3.sum + topc_other.sum + topt_other.sum ~ did, data = sum1a, FUN = mean)
# a2 <- summaryBy(topc_11to3.sum + topt_11to3.sum + topc_other.sum + topt_other.sum ~ did, data = sum1b, FUN = mean)
# a3 <- summaryBy(topc_11to3.sum + topt_11to3.sum + topc_other.sum + topt_other.sum ~ did, data = sum1c, FUN = mean)
# colnames(a1) <- c("did","Daily Avg of Content Played 11am to 3pm","Daily Avg of Time of Content Played 11am to 3pm","Daily Avg of Content Played outside 11am to 3pm","Daily Avg of Time of Content Played outside 11am to 3pm")
# colnames(a2) <- c("did","Weekly Avg of Content Played 11am to 3pm","Weekly Avg of Time of Content Played 11am to 3pm","Weekly Avg of Content Played outside 11am to 3pm","Weekly Avg of Time of Content Played outside 11am to 3pm")
# colnames(a3) <- c("did","Monthly Avg of Content Played 11am to 3pm","Monthly Avg of Time of Content Played 11am to 3pm","Monthly Avg of Content Played outside 11am to 3pm","Monthly Avg of Time of Content Played outside 11am to 3pm")

# sum1 <- merge(a1,a2,by="did",all = T)
# sum1 <- merge(sum1,a3,by="did",all = T)
# rm(a1,a2,a3)

# For Switching Users
# a1 <- summaryBy(switch_uid_day.sum + active_days.sum ~ did, data = sum2a, FUN = mean)
# a2 <- summaryBy(switch_uid_week.sum + active_weeks.sum ~ did, data = sum2b, FUN = mean)
# a3 <- summaryBy(switch_uid_month.sum + active_months.sum ~ did, data = sum2c, FUN = mean)
# a4 <- summaryBy(active_days.sum ~ did, data = sum2a, FUN = sum)
# a5 <- summaryBy(active_weeks.sum ~ did, data = sum2b, FUN = sum)
# a6 <- summaryBy(active_months.sum ~ did, data = sum2c, FUN = sum)
# colnames(a1) <- c("did","Daily Avg of Frequency of Switching Profiles")
# colnames(a2) <- c("did","Weekly Avg of Frequency of Switching Profiles")
# colnames(a3) <- c("did","Monthly Avg of Frequency of Switching Profiles")
# colnames(a4) <- c("did","Active Days")
# colnames(a5) <- c("did","Active Weeks")
# colnames(a6) <- c("did","Active Months")

# sum2 <- merge(a1,a2,by="did",all = T)
# sum2 <- merge(sum2,a3,by="did",all = T)
# sum2 <- merge(sum2,a4,by="did",all = T)
# sum2 <- merge(sum2,a5,by="did",all = T)
# sum2 <- merge(sum2,a6,by="did",all = T)
# rm(a1,a2,a3,a4,a5,a6)

# For Content Usage
# a1 <- summaryBy(uid.length ~ did, data = sum4a, FUN = mean)
# a2 <- summaryBy(uid.length ~ did, data = sum4b, FUN = mean)
# a3 <- summaryBy(uid.length ~ did, data = sum4c, FUN = mean)
# colnames(a1) <- c("did","Daily Avg of Profiles created")
# colnames(a2) <- c("did","Weekly Avg of Profiles created")
# colnames(a3) <- c("did","Monthly Avg of Profiles created")
# 
# sum4 <- merge(a1,a2,by="did",all = T)
# sum4 <- merge(sum4,a3,by="did",all = T)
# rm(a1,a2,a3)
# 
# # Getting all together
# sum1[,-1] <- round(sum1[,-1],0)
# sum2[,-1] <- round(sum2[,-1],0)
# sum4[,-1] <- round(sum4[,-1],0)
# 
# fin <- merge(sum1,sum2,by="did",all = T)
# fin <- merge(fin,sum3,by="did",all = T)
# fin <- merge(fin,sum4,by="did",all = T)

# rm(sum1,sum2,sum3,sum4,sum1a,sum1b,sum1c,sum2a,sum2b,sum2c,sum4a,sum4b,sum4c)
#####

# write.csv(fin, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/hypo1a_20feb2017.csv", row.names = F)
# write.csv(sum5, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/hypo1b_20feb2017.csv", row.names = F)

## Combining a few Metrics
sum234 <- merge(sum2aa,sum2ba,by="did",all = T)
sum234 <- merge(sum234,sum3,by="did",all = T)
sum234 <- merge(sum234,sum4a,by="did",all = T)

sum2extra <- merge(sum2ca,sum2da,by=c("did","month"),all = T)

rm(sum2aa,sum2ba,sum3,sum4a,sum2ca,sum2da)

## Output files

#file1 - content visits + content usage by did and top
write.csv(sum1a, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file1.csv", row.names = F)
#file1a - unique contents played per device per date and top
write.csv(sum1aa, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file1a.csv", row.names = F)
#file2 - genie visits by did and top
write.csv(sum1b, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file2.csv", row.names = F)
#file3 - Number of Active Devices vs Total Devices per day
write.csv(sum1c, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file3.csv", row.names = F)
#file3a - Number of Active Days vs Total Days per device
write.csv(sum1f, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file3a.csv", row.names = F)
#file4 - Activty of Devices by Weekdays
write.csv(sum1d, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file4.csv", row.names = F)
#file5 - Activty of Devices by Months
write.csv(sum1e, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file5.csv", row.names = F)
#file6 - average unique users per device per active days/weeks, % of Time Spent by Anonymous User, % of Time Spent by Individual User, number of profiles created per did
write.csv(sum234, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file6.csv", row.names = F)
#file6 - average unique users per device-month per active days/weeks
write.csv(sum2extra, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file6a.csv", row.names = F)
#file7 - % of time spent by Individual profiles per did
write.csv(sum5, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project - ESL Data Analysis/op_file7.csv", row.names = F)




