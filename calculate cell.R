

#Operation:
#First call function 'readCellFile' to create a dataset with all necessary information about the cells
#Second call 'feedInPerCell' and/or 'consumptionPerCell' to calculate each cell's production and consumption

#calculation of hourly production - required input is the Anschlussobjekt and the production for each house
#The function requires as input a producion file which contains the columns "Anschlussobjekt" and the "yearly Production" numbers.
#the function generates per Anschlussobjekt a disribution of the yearly production according the sun radiation and the outcome is a 
#file (stored in an output folder) which contains a matrix with the (dimension month and time depending on the sun radiation file) production distribution according the sun radiation.
#calculation of the production for all cells using either the current production (MWh) or assigning a standardized profile (or according their potential depending on the location) 
#e.g. 
#TIME   January February
#12:00   20.40    10.52
#13:00   10.50    15.52

feedInPerCell <- function() {
      
      j <- 1
      
      #looping through the number of entries in the feed-in file to create the distribution of yearly production per cell according sun radition
      for (cellObject in aggrDataSetAll$Anschlussobjekt) { 
            
            #provisory or current production of an Anschlussobjekt (cell) - saving under different name
            switch(aggrDataSetAll$Produktion[j], 'Current' = {nameFile <- tmpFileNameProd}, 'Current' != {nameFile <- tmpFileNameProvisoryProd})
            
            #provisory or current production of an Anschlussobjekt (cell) - choosing different MWh (current: input data; provisory: standard profiles)
            #switch(aggrDataSetAll$Produktion[j], 'Current' = {tmpMWh <- aggrDataSetAll$MWh[j]}, 'Good' = {tmpMWh <- listProvisoryProd[1]}, 'names(listProvisoryProd[2])' = {tmpMWh <- listProvisoryProd[2]}, 'names(listProvisoryProd[3])' = {tmpMWh <- listProvisoryProd[3]})
            
            #add production (MWh) to the assign production potential
            #good production potential
            if (names(listProvisoryProd[1]) == aggrDataSetAll$Produktion[j]) {
                  tmpMWh <- listProvisoryProd[1] 
            #medium production potential
            } else if (names(listProvisoryProd[2]) == aggrDataSetAll$Produktion[j]) {
                  tmpMWh <- listProvisoryProd[2] 
            #bad production potential
            } else if (names(listProvisoryProd[3]) == aggrDataSetAll$Produktion[j]) {
                  tmpMWh <- listProvisoryProd[3] 
            #exisiting production
            } else if ('Current' == aggrDataSetAll$Produktion[j]) {
                  tmpMWh <- aggrDataSetAll$MWh[j] * controlProduction
            #for all houses with production assign "not applicable" to the consumption of the cell
            } else if ('Not applicable' == aggrDataSetAll$Produktion[j]) {
                  j <- j + 1 
                  next()
            }
      
            #calculation of the distribution
            singleCellFeedInYearly <- data.frame(mapply('*',sunRadiationYearly[listMonths], tmpMWh))
            singleCellFeedInYearly[, 'Time'] <- timeSlots
            singleCellFeedInYearly[is.na(singleCellFeedInYearly)] <- 0
            
            #storing Anschlussobjekt distribution in a file
            pathToFileProd <- paste(folderOutProdTmp, paste(aggrDataSetAll$Anschlussobjekt[j], nameFile , sep="", collapse = ""), sep = '', collapse='')
            write.table(singleCellFeedInYearly, file = pathToFileProd, sep = ';', row.names = FALSE)
            
            j <- j + 1
            
      }
      
      
}


#calculation of hourly consumption
#Input is a file with Anschlussobjekt, Verbrauch, and Clustering (for distributing the hourly consumption). The consumption profiles (H0, G0 etc.) are in % of the total over the year.
#At the moment profiles are genralized which means only 3 profiles for each category (H, G, L) is provided. For a more detailed definition, tis part must be redone (it needs another cateogrisation criterias as BKW Clustering column) 
#e.g. 
#TIME   'Week_S', 'Saturday_S', 'Sunday_S','Week_W', 'Saturday_W', 'Sunday_W', 'Week_TP', 'Saturday_TP', 'Sunday_TP'
#12:00   20.40       10.52          20.40    10.52          ...
#13:00   10.50       15.52          19.55     9.50          ...      
consumptionPerCell <- function() {
      
      #load data from global dataset
      aggrCons <- subset(aggrDataSetAll, aggrDataSetAll$Produktion != 'Current', select = listColNamesInputFiles)
      
      
      j <- 1
      
      #Calculation for distribution of consumption:
      #to generate a house (cell) consumption, different clusters (H0, G1) can exist. In general, if there are only business or households entries for a house, it gets aggregated to one variable.
      #However, if there are two or three entries of the same cell (because households and business exists), each unit in the house gets calculated according different distibution patterns and summarized in one file afterwards.
      #looping through the number of entries in the consumption file to create the distribution of yearly consumption per cell according VDEW 
      for (cellID in aggrCons$Anschlussobjekt) { 
            
            
            #creating data.frame for each Anschlussobjekt
            nameCellCons <- paste(aggrCons$Anschlussobjekt[j], tmpFileNameCons, sep="", collapse = "")
            
            #implementing here the different consumption profiles according H and G. For further separation adjust here
            switch(aggrCons$Clustering[j], H0 = {consProfile <- consDistrH0[listColNamesConProfile]}, G0 = {consProfile <- consDistrG0[listColNamesConProfile]}, L0 = {consProfile <- consDistrL0[listColNamesConProfile]})
            
            #neigbhouring cell IDs required to test if there are same IDs in the list. This is required to create one file out of an Anschlussobjekt matrix (ID & Clustering) with several entries
            prevCellID <- aggrCons$Anschlussobjekt[j - 1]
            nextCellID <- aggrCons$Anschlussobjekt[j + 1]
            
            #to avoid error messages in the beginning and in the end of the list
            if (length(aggrCons$Anschlussobjekt[j - 1]) == 0) prevCellID <- "999"
            if (is.na(aggrCons$Anschlussobjekt[j + 1])) nextCellID <- "999"
                  
            #adjustment on the total consumption
            adjustedMWh <- aggrCons$MWh[j] * controlConsumption
          
                  
            #if there are more Anschlussobjekte with have the same ID exist in the list
            if( aggrCons$Anschlussobjekt[j] == nextCellID ) {
                   
                  cellConsYearlyAddOn <- data.frame(mapply('*',consProfile, adjustedMWh))
                 
                  cellConsYearlyTmp <- data.frame(mapply('+',cellConsYearlyAddOn, cellConsYearlyTmp))
                  
                  j <- j + 1
                  next()
      
                  #if the next ID is not the same number but the current ID is the same as the previous ID (last ID of a serie), the calculated distributions are aggregated and stored in a file      
                  } else if( aggrCons$Anschlussobjekt[j] == prevCellID && aggrCons$Anschlussobjekt[j] != nextCellID ) {
                        #create another distribution of consumption file and add it to the 
                        cellConsYearlyAddOn <- data.frame(mapply('*',consProfile, adjustedMWh))
                        
                        cellConsYearlyMultiple <- data.frame(mapply('+',cellConsYearlyTmp[listColNamesConProfile], cellConsYearlyAddOn[listColNamesConProfile]))
                        cellConsYearlyMultiple[, 'Time'] <- timeSlots

                        #storing Anschlussobjekt distribution in a file
                        pathFileProd <- paste(folderOutConsTmp, nameCellCons, sep = '', collapse='')
                        write.table(cellConsYearlyMultiple, file = pathFileProd, sep = ';', row.names = FALSE)
            
                  
                  #check if the next ID in the list is not equal to the current one which means, if not, the ID exist only once and the distribution is calculated and a csv file will be generated
                  } else {
                        #calculating distribution file
                        cellConsYearly <- data.frame(mapply('*',consProfile, aggrCons$MWh[j]))
                        cellConsYearly[, 'Time'] <- timeSlots
                        
                        #storing Anschlussobjekt distribution in a file
                        pathFileProd <- paste(folderOutConsTmp, nameCellCons, sep = '', collapse='')
                        write.table(cellConsYearly, file = pathFileProd, sep = ';', row.names = FALSE)
            }
      
      
            j <- j + 1
     
      }

}

#calculating the hourly status per month (current). Input are the consumption and production files generated by the functions "consumptionPerCell" and "feedInPerCell"
#any adjustments on the consumption or production figures are happened after this step
#the result are postivie for a surplus and negative for a shortage
#e.g. 
#TIME       Period      January     February
#12:00      week        -2.40      -1.52
#13:00      week         1.50       5.52
#...
#12:00      winter      -2.10      -1.52
#13:00      winter      -3.50      -5.52
calculateCellStatus <- function() {
      
      #load functions needed for the calculation of the status
      if(is.null(aggrDataSetAll$Anschlussobjekt)) readCellFile()
      if(calculateConsumption == TRUE) consumptionPerCell()
      if(calculateProduction == TRUE) feedInPerCell()
      
      listOfcellIDs <- c(11100151188, 11100151173, 11100153361) #unique(aggrDataSetAll$Anschlussobjekt)

      #for which period of the week would I like to calculate the status: week (aka workdays) or saturaday or sunday
      day <- 'Sunday'
      month <- 'August'
      
      
      colNameCons <- paste(day, '_', listMonthsAssigned[month], sep = '', collapse='')
      
      for(idCell in unique(listOfcellIDs)) {
           
            #read tmp consumption file
            tmpConsCell <- read.csv(paste(folderOutConsTmp, idCell, tmpFileNameCons, sep = '', collapse=''), sep = ';', colClasses = listColNamesLocCell, stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("NA",""))
            
            #read tmp production file
            nameProdFile <- if (idCell %in% listCellProdIDs) tmpFileNameProd else tmpFileNameProvisoryProd
            
            tmpProdCell <- read.csv(paste(folderOutProdTmp, idCell, nameProdFile, sep = '', collapse=''), sep = ';', colClasses = listColNamesLocCell, stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("NA",""))
            
           
            diffConsProdPerCell <- data.frame(tmpProdCell[ , c(month)] - tmpConsCell[ , c(colNameCons)])
            diffConsProdPerCell[ , 'Time'] <- timeSlots
            
            #reshape(diffConsProdPerCell, direction = 'wide', idvar = 'Time', timevar = 'Anschlussobjekt')
            print(diffConsProdPerCell)
            
            # rename score to student and append studentno column
            test2 <- transform(diffConsProdPerCell, student = Time, Time = NULL)
            reshape(test2, dir = "wide", idvar = "Time", timevar = "studentno")
            
            
            
      }
      
      #switch off calculation steps for production and consumption - it is just for speed, later it is a control on the panel
      calculateConsumption <<- FALSE
      calculateProduction <<- FALSE
      
}


#Re-clustering loop for the list of Anschlussobjekte - make H1, H2 etc. to H0. same with G and L
reclusteringCell <- function(data) {
     
       for (i in unique(data$Clustering)) {
            data[data == i] <- as.character(listClusteringCell[i])

       }
      
      return(data)
}


#does the Anschlussobjekt currently possess a production? - Additional column with Y, N according a list of tarifs
existProdOfCell <- function(data) {
      
      data[,'Produktion'] <-  data$Tarif
      data  <- subset(data, select=-Tarif)
      
      for (elementList in listTarifsProd) {
            #assign "Current" to existing PV production 
            data[data == elementList] <- 'Current'
           
      }
      
      data$Produktion[data$Produktion != 'Current']  <- 'Not applicable'
      
      return(data)
}


#assign production status either current MWh with "Current" or the potential (currently "Good", "Medium", and "Bad" - see listProvisoryProd)
#ToDo: according geodata assign potential of production 
#data$Produktion[data$Produktion != 'Current']  <- sample(names(listProvisoryProd), 1, replace = TRUE)
assignProvisoryPV <- function() {
      #get a list with Anschlussobjekte only consumption
      cellOnlyConsIDs <- aggrDataSetAll[!(aggrDataSetAll$Anschlussobjekt %in% listCellProdIDs),]
      
    
      for (j in unique(cellOnlyConsIDs$Anschlussobjekt)) {
            #assignment of randomly generated PV potential to Anschlussobjekte without production
            aggrDataSetAll$Produktion[aggrDataSetAll$Anschlussobjekt == j] <<- sample(names(listProvisoryProd), 1, replace = TRUE)
            
      }
      
}



#reads the entire csv file of and creates a list of all IDs. This is the starting point!
#provides a clean dataset of production and consumption
readCellFile <- function (rawFile = 'input/Data Spiez, Haselweg -  AnschOb, Tarif, MWh, Clustering.csv') {
      
      rawData <- read.csv(rawFile, sep = ';', colClasses = listColNamesReadFiles)

      
      #remove all NA (Verbrauch) and tarifs which should not be considered in the calculations
      rawData <- na.omit(rawData)
      rawData <- rawData[!(rawData$Clustering %in% listClusteringForRemoving),]
      
      #re-clustering of BKW's Clustering column (currently it is a distinguished between private = H0, business = G0, and agriculture = L0). Any detailled clustering adapt HERE
      rawData <- reclusteringCell(rawData)
     
      #add column to state if the ID produces currently electricity or not
      rawData <- existProdOfCell(rawData)
      
      #Aggregating all "Messpunkte" of a house to an Anschlussobjekt (house) level - Still multiple entries possible if a house includes households and business as well as has a production  
      aggrData <- setNames(aggregate(x = rawData$MWh, by = list(rawData$Anschlussobjekt, rawData$Clustering, rawData$Produktion), sum), c('Anschlussobjekt', 'Clustering', 'Produktion', 'MWh'))
      aggrData <- aggrData[order(aggrData$Anschlussobjekt),]
      
      #storing in global variable
      aggrDataSetAll <<- aggrData
      
      #store all Anschlussobjekte IDs in a list
      listCellProdIDs <<- aggrDataSetAll$Anschlussobjekt[aggrDataSetAll$Produktion == 'Current']
      #assign randomly provisory PV production to Anschlussobjekte without a production
      assignProvisoryPV()
      
}

