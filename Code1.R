#################################
###### Recursive Unzipping ######
#################################

### Required Functions

library(R.utils)
#### Function for Recursive Unzipping ####
rec_unzip <- function(level=2,unzip_type=".gz",output_file_type=".log"){
  
  ## Initial Level of Unzipping ##
  while(level>1)
  {
    files_dir <- list.dirs(path = dir())
    for(i1 in files_dir)
    {
      files_gz <- list.files(path = i1, pattern = unzip_type)
      if(length(files_gz) != 0) {
        for (i2 in files_gz) {
          unzip(paste(i1,i2,sep = "/"))
          file.remove(paste(i1,i2,sep = "/")) 
        }
      }
    }
    level <- level-1
  }
  
  
  ## Final Level of Unzipping ##
  files_dir <- list.dirs(path = dir())
  for(i3 in files_dir)
  {
    files_gz <- list.files(path = i3, pattern = unzip_type)
    if(length(files_gz) != 0) {
      for (i4 in files_gz) {
        gunzip(paste(i3,i4,sep = "/"),remove = T)
      }
    }
  }
  
  ## Saving all the log files to single destination folder ##
  files_dir <- list.dirs(path = dir())
  for(i5 in files_dir)
  {
    filestocopy <- list.files(path = i5, pattern = output_file_type)
    if(length(filestocopy) != 0)
    {
      file.copy(from = paste(i5,filestocopy,sep = "/"), to = getwd(), copy.mode = T)
      file.remove(paste(i5,filestocopy,sep = "/")) 
    }
  }
  
  return("Files Unzipped Successfully")
}
##########################################

#### Recursive Unzipping ####

### setting working directory
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/unzipping folder")

### calling unzip function
rec_unzip(level = 2,unzip_type = ".gz",output_file_type = ".log")

## Note: Manually delete all the empty folders present in the target folder

############################ End of Code ###############################