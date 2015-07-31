#functions needed to support the basic applicationss


# Install function for packages    
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



#create Folder structure for tmp
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



