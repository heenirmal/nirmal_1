##########################################################################
################### Piwik Analysis - Content Journey #####################
##########################################################################

### Required packages
library(plyr)
library(reshape2)
library(dplyr)
library(lubridate)
library(doBy)
library(stringi)

### Loading Piwik Data
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Piwik/Final CSVs/")
files <- list.files(pattern = ".csv")

con <- c()
i = files[1]

require(plyr)
for(i in files)
{
  data1 <- read.csv(file = i, header = T, stringsAsFactors = F)
  con <- rbind.fill(con,data1)
}
data1 <- con

### 
require(reshape2)

## Getting content journey info
d1 <- data1[, c(which(colnames(data1) %in% c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions")),grep(".type", colnames(data1)))]
dd1 <- melt(d1, id.vars = c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions"), na.rm = T)
dd1 <- subset(dd1, value != "")
require(dplyr)
dd1 <- rename(dd1,action_type = value)
dd1$variable <- sub("*\\.[^0-9]+", "", dd1$variable)

d2 <- data1[, c(which(colnames(data1) %in% c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions")),grep("eventCategory", colnames(data1)))]
dd2 <- melt(d2, id.vars = c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions"), na.rm = T)
dd2 <- subset(dd2, value != "")
require(dplyr)
dd2 <- rename(dd2,eventCategory = value)
dd2$variable <- sub("*\\.[^0-9]+", "", dd2$variable)

d3 <- data1[, c(which(colnames(data1) %in% c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions")),grep("eventAction", colnames(data1)))]
dd3 <- melt(d3, id.vars = c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions"), na.rm = T)
dd3 <- subset(dd3, value != "")
require(dplyr)
dd3 <- rename(dd3,eventAction = value)
dd3$variable <- sub("*\\.[^0-9]+", "", dd3$variable)

d4 <- data1[, c(which(colnames(data1) %in% c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions")),grep(".pageTitle", colnames(data1)))]
dd4 <- melt(d4, id.vars = c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions"), na.rm = T)
dd4 <- subset(dd4, value != "")
require(dplyr)
dd4 <- rename(dd4,pageTitle = value)
dd4$variable <- sub("*\\.[^0-9]+", "", dd4$variable)

d5 <- data1[, c(which(colnames(data1) %in% c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions")),grep(".siteSearchKeyword", colnames(data1)))]
dd5 <- melt(d5, id.vars = c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions"), na.rm = T)
dd5 <- subset(dd5, value != "")
require(dplyr)
dd5 <- rename(dd5,siteSearchKeyword = value)
dd5$variable <- sub("*\\.[^0-9]+", "", dd5$variable)

d6 <- data1[, c(which(colnames(data1) %in% c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions")),grep(".url", colnames(data1)))]
dd6 <- melt(d6, id.vars = c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions"), na.rm = T)
dd6 <- subset(dd6, value != "")
require(dplyr)
dd6 <- rename(dd6,url = value)
dd6$variable <- sub("*\\.[^0-9]+", "", dd6$variable)

d7 <- data1[, c(which(colnames(data1) %in% c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions")),grep(".eventName", colnames(data1)))]
dd7 <- melt(d7, id.vars = c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions"), na.rm = T)
dd7 <- subset(dd7, value != "")
require(dplyr)
dd7 <- rename(dd7,eventName = value)
dd7$variable <- sub("*\\.[^0-9]+", "", dd7$variable)

d8 <- data1[, c(which(colnames(data1) %in% c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions")),grep(".serverTimePretty", colnames(data1)))]
dd8 <- melt(d8, id.vars = c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions"), na.rm = T)
dd8 <- subset(dd8, value != "")
require(dplyr)
dd8 <- rename(dd8,action_serverTimePretty = value)
dd8$variable <- sub("*\\.[^0-9]+", "", dd8$variable)

d9 <- data1[, c(which(colnames(data1) %in% c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions")),grep(".timeSpent$", colnames(data1)))]
dd9 <- melt(d9, id.vars = c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions"), na.rm = T)
dd9 <- subset(dd9, value != "")
require(dplyr)
dd9 <- rename(dd9,action_timeSpent = value)
dd9$variable <- sub("*\\.[^0-9]+", "", dd9$variable)

## Combining the entire data of Visitor Journey
dd <- merge(dd1,dd2,by=c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions","variable"),all.x = T)
dd <- merge(dd,dd3,by=c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions","variable"),all.x = T)
dd <- merge(dd,dd4,by=c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions","variable"),all.x = T)
dd <- merge(dd,dd5,by=c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions","variable"),all.x = T)
dd <- merge(dd,dd6,by=c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions","variable"),all.x = T)
dd <- merge(dd,dd7,by=c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions","variable"),all.x = T)
dd <- merge(dd,dd8,by=c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions","variable"),all.x = T)
dd <- merge(dd,dd9,by=c("visitorId","actions","events","searches","visitorType","visitDuration","visitCount","serverDatePretty","serverTimePrettyFirstAction","visitConverted","goalConversions","variable"),all.x = T)

rm(con,d1,d2,d3,d4,d5,d6,d7,d8,d9,dd1,dd2,dd3,dd4,dd5,dd6,dd7,dd8,dd9,i,files)

## Creating Date time field for each entry
require(lubridate)
dd$date_time_action <- parse_date_time(dd$action_serverTimePretty, orders="mdy hms")
dd$date_time_visit <- parse_date_time(paste(dd$serverDatePretty,dd$serverTimePrettyFirstAction,sep=" "), orders="mdy hms")
dd$serverDatePretty <- NULL
dd$serverTimePrettyFirstAction <- NULL
dd$action_serverTimePretty <- NULL

## Arranging the visitor journey data
require(plyr)
dd <- arrange(dd, visitorId, visitCount, date_time_visit, date_time_action)

dd$action_timeSpent <- ifelse(is.na(dd$action_timeSpent),0,dd$action_timeSpent)

## Getting contentId for events: Authoring Tool and Content
dd$content_id <- ifelse(dd$eventCategory == "AuthoringTool", sub("[^a-zA-Z0-9_].*","",sub(".*/", "", dd$url)),
                        ifelse(dd$eventCategory == "Content", sub(".*:", "", dd$eventName), ""))

## Time spent and number of sessions for content to be in draft stage
draft <- subset(dd, select = c(date_time_action,date_time_visit,visitorId,variable,eventCategory,eventAction,content_id,action_timeSpent))
draft <- subset(draft, eventCategory == "AuthoringTool")
draft <- arrange(draft, visitorId, variable, date_time_action)

require(doBy)
a1 <- summaryBy(action_timeSpent ~ content_id, data = draft, FUN = sum)
a2 <- subset(draft, select = c(date_time_visit, visitorId, content_id))
a2 <- unique(a2)
a2 <- summaryBy(date_time_visit ~ content_id + visitorId, data = a2, FUN = length)
a2 <- summaryBy(date_time_visit.length ~ content_id, data = a2, FUN = sum)

draft_stage <- merge(a1,a2,by="content_id",all = T)
colnames(draft_stage) <- c("content_id","total_time_spent","number_of_sessions")

rm(a1,a2)

## Time spent for a content to move from Review stage to Live stage
# loading kibana data for stage shifts
stage_shift <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Piwik/Processed Data/Stage Shifts by ContentID.csv", header = T, stringsAsFactors = F)

# getting date_time field to stage_shift
require(stringi)
stage_shift$dummy <- stri_sub(stage_shift$Content.ID, 1, -5)
stage_shift$dummy <- paste0(stri_sub(stage_shift$dummy, 1, -18),stri_sub(stage_shift$dummy, -15))
stage_shift$date_time <- as.POSIXct(stage_shift$dummy, format = "%B %d %Y, %H:%M:%S")
stage_shift$dummy <- NULL
stage_shift$Content.ID <- NULL
stage_shift$Content.size <- NULL
colnames(stage_shift) <- c("content_id","previous_state","current_state","count","date_time")


# flagging draft, review and live for current stage
stage_shift$draft_stage <- ifelse(stage_shift$current_state == "Draft",1,0)
stage_shift$live_stage <- ifelse(stage_shift$current_state == "Live",1,0)
stage_shift$review_stage <- ifelse(stage_shift$current_state == "Review",1,0)

# number of times each content id has been moved to live, draft and review stage
require(doBy)
stage_shift1 <- summaryBy(draft_stage + live_stage + review_stage ~ content_id, data = stage_shift, FUN = sum)

# total time spent in review stage
review <- subset(stage_shift, previous_state == "Review" | current_state == "Review")
review <- subset(review, select = c(content_id,previous_state,current_state,date_time))
require(plyr)
review <- arrange(review, content_id,date_time)
review$time_taken_review <- 0

for(i in 1:(length(review$content_id)-1))
{
  if(review[i,1] == review[i+1,1])
  {
    if(review[i,3] == "Review" && review[i+1,3] == "Live")
    {
      review[i,5] <- difftime(review[i+1,4],review[i,4],units = "secs") 
    }
  } else {
    review[i,5] <- 0
  }
}

rm(i)

review <- subset(review, current_state == "Review")
rev <- summaryBy(time_taken_review ~ content_id, data = review, FUN = sum)

## Combining information for Review, draft stages
content_info <- merge(draft_stage,stage_shift1,by="content_id",all = T)
content_info <- merge(content_info,rev,by="content_id",all = T)
content_info[is.na(content_info)] <- 0

## Writing the final output ##
write.csv(content_info, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project Piwik Analysis/content_info.csv", row.names = F)

############################### End of Code #################################