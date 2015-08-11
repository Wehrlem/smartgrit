#Author: Mario Gstrein, 05.05.2015, Fribourg (CH)
#functions needed to support the basic applicationss



# Install function for packages - list of reuqired packages are defined in ini.R   
checkPackages <- function(){
      
      for (packName in listReqPackages) {
            
            #install packages and load them 
            if (!is.element(packName, installed.packages()[,1])) {
                  install.packages(pkgs = packName, repos="http://cran.r-project.org")
                  require(packName, character.only=TRUE)
            } else {
                  #load packages if exists already
                  require(packName, character.only=TRUE)
                  
            }
      }
}

#function to kill al DB connections in the connect list
killAllDBConnects <- function() {

  all_cons <- dbListConnections(MySQL())
  
  for(con in all_cons) dbDisconnect(con)
  
    dbListConnections(MySQL())

}

#checks the consumption total of choosen months to find out if the calcualtion according the SLP is correct - this is for verification purposes
#requires dataframe: distrDataSet_C
testSumCons <- function() {
  
  test <- distrDataSet_C[which(distrDataSet_C$Month == 1 | distrDataSet_C$Month == 4 | distrDataSet_C$Month == 7),]
  
  test2 <- setNames(aggregate(test[, c('C_MWh_Week', 'C_MWh_Saturday', 'C_MWh_Sunday')], by = list(test$Anschlussobjekt), FUN = sum), c('Anschlussobjekt', 'C_MWh_Week', 'C_MWh_Saturday', 'C_MWh_Sunday'))
  
  j <- 1
  for(i in test2$Anschlussobjekt) {
    
    test2[j, 'Total'] <- sum(test2[j, 'C_MWh_Week'], test2[j, 'C_MWh_Saturday'], test2[j, 'C_MWh_Sunday'])
    j <- j+1
  }
  return(test2)
}



#create Folder structure for tmp file storage - not required anymore
checkDir <- function(subDir) {
  
  
  #setting up predefined folder struture
  tmpDir <- getwd()
  folders <- strsplit(subDir, "/")
  j <- 1
  
  #create working directory and set it as working directory until all folders are created
  #TODO: Still showing the warning messages that folder exist!
  for (i in folders[[1]]) {
    
    tmpDir <- file.path(tmpDir, folders[[1]][j])
    
    if(dir.exists(tmpDir) == TRUE) {
      
      setwd(tmpDir[[1]])
      
    } else {
      dir.create(tmpDir, showWarnings = FALSE)
      setwd(tmpDir[[1]])
      
    }
    
    j <- j + 1 
    
  }
  #reset the working directory
  tmpDir <- setwd(mainDir[[1]])
  
  
}