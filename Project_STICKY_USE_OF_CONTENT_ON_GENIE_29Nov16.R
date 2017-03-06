################################################################
#################### EkStep - Project 2 ########################
############### STICKY USE OF CONTENT ON GENIE #################
################################################################

## Required libraries

## Setting the working directory for data played
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs - GE_INTERACT,OE_START,OE_END")

## Getting inputs for 1Sep'16 - 21Nov'16
Nov16 <- read.csv(file = "Nov2016.csv", header = T, stringsAsFactors = F)
Oct16 <- read.csv(file = "Oct2016.csv", header = T, stringsAsFactors = F)
Sep16 <- read.csv(file = "Sep2016.csv", header = T, stringsAsFactors = F)

## Filtering out relevant data
d1_nov <- subset(Nov16, eid == "OE_START")
d1_nov <- subset(d1_nov, select = c(did,ts,gdata.id))

d1_oct <- subset(Oct16, eid == "OE_START")
d1_oct <- subset(d1_oct, select = c(did,ts,gdata.id))

d1_sep <- subset(Sep16, eid == "OE_START")
d1_sep <- subset(d1_sep, select = c(did,ts,gdata.id))

## Combining the relevant data
d1 <- rbind(d1_sep,d1_oct)
d1 <- rbind(d1,d1_nov)

## creating date-time field
d1$dummy <- strtrim(d1$ts, width = 19)
d1$date_time <- as.POSIXct(d1$dummy, format = "%Y-%m-%dT%H:%M:%S")
d1$dummy <- NULL

## Setting the working directory for ME data (time period for OE_START event)
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/ME Data")

## Getting inputs for ME Data
ME_data <- read.csv(file = "ME_SESSION_SUMMARY_VISUALIZATION_1Sep16-21Nov16.csv", header = T, stringsAsFactors = F)

## Filtering out the relevant data from ME Data
ME_data1 <- subset(ME_data, select = c(dimensions.gdata.id..Descending,dimensions.did..Descending,edata.eks.start_time..Descending,edata.eks.timeSpent..Descending))
ME_data1 <- ME_data1[!duplicated(ME_data1),]

## creating date-time field
library(stringi)
ME_data1$dummy <- stri_sub(ME_data1$edata.eks.start_time..Descending, 1, -5)
ME_data1$dummy <- paste0(stri_sub(ME_data1$dummy, 1, -18),stri_sub(ME_data1$dummy, -15))
ME_data1$date_time <- as.POSIXct(ME_data1$dummy, format = "%B %d %Y, %H:%M:%S")
ME_data1$dummy <- NULL
ME_data1$edata.eks.start_time..Descending <- NULL
colnames(ME_data1) <- c("gdata.id","did","time_spent","date_time")

rm(ME_data)

## Getting time spent field for d1 dataset
d2 <- merge(d1,ME_data1, by = c("did","gdata.id","date_time"), all.x = T)

## Getting session length data for OE_START events
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/session_length_OE_START")
nov_ses <- read.csv(file = "Nov2016_session_length.csv", header = T, stringsAsFactors = F)
oct_ses <- read.csv(file = "Oct2016_session_length.csv", header = T, stringsAsFactors = F)
sep_ses <- read.csv(file = "Sep2016_session_length.csv", header = T, stringsAsFactors = F)

ses <- rbind(nov_ses,oct_ses)
ses <- rbind(ses,sep_ses)

rm(nov_ses,oct_ses,sep_ses)

## Filtering sess data
ses1 <- subset(ses, eid == "OE_START")
ses1 <- subset(ses1, select = c(gdata.id,did,ts,length))

## creating date-time field
ses1$dummy <- strtrim(ses1$ts, width = 19)
ses1$date_time <- as.POSIXct(ses1$dummy, format = "%Y-%m-%dT%H:%M:%S")
ses1$dummy <- NULL
ses1$ts <- NULL

## Getting session length to d2 from ses1
d3 <- merge(d2,ses1, by = c("did","gdata.id","date_time"), all.x = T)
d3$session_length <- ifelse(is.na(d3$time_spent),d3$length,d3$time_spent)

d3 <- d3[!is.na(d3$session_length),]
d3$time_spent <- NULL
d3$length <- NULL

d3$session_length <- round(as.numeric(gsub(",","",d3$session_length)),0)

# write.csv(d3, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/session_length_OE_START/session_length.csv")
