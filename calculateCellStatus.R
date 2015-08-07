#Author: Mario Gstrein, 05.05.2015, Fribourg (CH)
#Operation:
#call generateAnschlussobjektTable() to provide basic dataset for distribution calculation
#call calculatingDistribution(sqlProfileStatement, selectColumn, typDistr) to generate the distribution for consumption and production
#call mergeDistribution() to generate one dataset with 'Anschlussobjekt', 'Time', 'Month', 'C_MWh', 'P_MWh' and store it in the DB


#TODO: 1) calculation of distribution is sequential (per month), however it only should do it when there are no entries in the DB or the distribution (sum of entries) has
#      2) in function exitProvProd randomly production potential is assigned, however, it is done each time (should do once, if id doesn't exist in the production_potential_cell table)


#the function reads the original data on Messpunkte level and generates a raw table in which each a house (=Anschlussobjekt).
#There are not unique IDs as a house can have different profiles (H0, G0).
#output: is a dataframe aoDataSet (columns: Anschlussobjekt, Clustering, GEM_Nr, C_MWh, P_MWh, Potential)
# function call: 
#calculatingDistribution(sqlLoadConsProfile, c('Anschlussobjekt', 'Clustering', 'C_MWh'), 'C')
#calculatingDistribution(sqlLoadProdProfile, c('Anschlussobjekt', 'GEM_Nr', 'P_MWh'), 'P')
generateAnschlussobjektTable <- function () {
  
    #load all Messpunkte
    rs <- dbSendQuery(connectDB, sqlLoadTest)
    rawData <- fetch(rs, n = -1)
    
    #remove all NA (Verbrauch) and tarifs which should not be considered in the calculations
    rawData <- na.omit(rawData)
    
    #storing in global variable
    aoDataSet <<- rawData
    
    #rename columns to general terms
    names(aoDataSet)[names(aoDataSet) =="BKW_Clustering"] <<- "Clustering"
    names(aoDataSet)[names(aoDataSet) =="MWh_2013"] <<- "C_MWh"
    
    #re-clustering of all different H0...H6 and G0...G6 to H0 respectively to G0 (currently it is a distinguishment of private = H0, business = G0, and agriculture = L0). Any detailled clustering adapt HERE
    reclusteringCell()
    
    #add columns "P_MWh" and "Potential" of PV to state if the ID produces currently electricity or not
    existProdOfCell()
    
    #Aggregating all "Messpunkte" of a house to an Anschlussobjekt (house) level - Still multiple entries possible if a house includes households and business as well as has a production  
    aoDataSet <<- setNames(aggregate(aoDataSet[, c('C_MWh', 'P_MWh')], by = list(aoDataSet$Anschlussobjekt, aoDataSet$Clustering, aoDataSet$GEM_Nr), FUN = sum), c('Anschlussobjekt', 'Clustering', 'GEM_Nr', 'C_MWh', 'P_MWh'))
    
    aoDataSet <<- aoDataSet[order(aoDataSet$Anschlussobjekt),]
    
    #assign MWh according the "Potential" column to Anschlussobjekte without a production
    assignProvisoryPV()
  
  
}

#calculation of hourly distribution for consumption and production by loading "profiles".
#variables: sqlStatement (string of a sql statement loading of the profiles); selectColumn (vector of column names which should be choosen from aoDataSet); typDistr (P or C, refers to what type of distribution is calculated) 
#input: is the aoDataSet (columns: Anschlussobjekt, Clustering, GEM_Nr, C_MWh, P_MWh, Potential)
#output: is a dataframe with the columns: 'Anschlussobjekt', 'Time', 'Month', 'C_MWh' (or) 'P_MWh'. The results are stored in DB "XXX"
calculatingDistribution <- function(sqlProfileStatement, selectColumn, typDistr) {
  
    #load consumption or sun radiation profiles etc.
    rs <- dbSendQuery(connectDB, sqlProfileStatement)
    profiles <- fetch(rs, n = -1)
    
    #which column for MWh should be choosen from C_MWh, or P_MWh
    columnNameMWh <- paste(typDistr, '_MWh', sep = '', collapse = '')
    
    #selecting only columns for the consumption calculation 
    tmpDataSet <- aoDataSet[selectColumn]
    
    #two datasets required to merge them afterwards (only consumption or production can be calculated at once)
    if(typDistr == 'C') distrDataSet_C <<- 0 else if(typDistr == 'P') distrDataSet_P <<- 0
    
    distrDataSet <- data.frame(list('Anschlussobjekt' = 'character', 'Time' = 'integer', 'Month' = 'integer', columnNameMWh = 'double'))
    distrDataSet <- 0
    
    j <- 1
    
    for (cellID in tmpDataSet$Anschlussobjekt) { 
      
        #load profiles (matching either Clustering for consumption or GEM_Nr for production)
        if(typDistr == 'C') tmpProfile <- profiles[which (profiles$Profile == tmpDataSet$Clustering[j]), ] else if(typDistr == 'P') tmpProfile <- profiles[which (profiles$GEM_Nr == tmpDataSet$GEM_Nr[j]), ]
        
        #calculating the distribution per ID according the profile (either sun radiation or consumption pattern)
        res <- data.frame(mapply('*',tmpProfile[, 4:27], tmpDataSet[j, columnNameMWh] ))
        
        #create a data frame for distribution per Anschlussobjekt (hourly) and add columns
        res[, 'Anschlussobjekt'] <- cellID
        res[, 'Time'] <- rownames(res)
        res[, 'Month'] <- settings['month','Value']
        res[, columnNameMWh] <- res
        res[, 1] <- NULL
        
        #create a dataframe with all consumption/production distributions
        distrDataSet <- rbind(res, distrDataSet) 
        
        j <- j + 1
    }
    
    #aggregate distribution dataset so only each row represents a single Anschlussobjekt to a specific time and month. (e.g. aggregate different load profiles H0 and G0 of one ID)
    distrDataSet <- setNames(aggregate(distrDataSet[, columnNameMWh], by = list(distrDataSet$Anschlussobjekt, distrDataSet$Time, distrDataSet$Month), FUN = sum), c('Anschlussobjekt', 'Time', 'Month', columnNameMWh))
    distrDataSet <- subset(distrDataSet, distrDataSet$Anschlussobjekt != 0)
    
    if(typDistr == 'C') distrDataSet_C <<- distrDataSet else if(typDistr == 'P') distrDataSet_P <<- distrDataSet
    
}




#merges the datasets 'distrDataSet_C' and 'distrDataSet_C' to generate one dataset with 'Anschlussobjekt', 'Time', 'Month', 'C_MWh', 'P_MWh' and store it in the DB
mergeDistribution <- function () {
  
    totalDistrDataSet <<- data.frame(merge(distrDataSet_C, distrDataSet_P , by = c('Anschlussobjekt', 'Time', 'Month')), stringsAsFactors=FALSE)
    totalDistrDataSet <<- totalDistrDataSet[order(totalDistrDataSet$Anschlussobjekt, as.numeric(totalDistrDataSet$Time), as.numeric(totalDistrDataSet$Month)),]
    
    #building the INSERT statement out of the totalDistrDataSet
    insertStatementBeg <- 'INSERT INTO `content_anschlussobjekte` (`Anschlussobjekt`, `Time`, `Month`, `C_MWh`, `P_MWh`) VALUES'
    
    valueInsertRow <- ''
  
    j <- 1
    for (i in totalDistrDataSet$Anschlussobjekt) {
      
        sqlCheckEntry <- paste('SELECT count(*) FROM `content_anschlussobjekte` WHERE `Anschlussobjekt` = ', totalDistrDataSet[j, 'Anschlussobjekt'] ,' AND  `Time` = ', totalDistrDataSet[j, 'Time'],' AND `Month` = ', totalDistrDataSet[j, 'Month'], sep = '', collapse = NULL)
        
        #checks if the entry already exists in the database
        rs <- dbSendQuery(connectDB, sqlCheckEntry)
        entryExists <- fetch(rs, n = -1)

        #first update entries if they exist or insert them
        if(entryExists > 0) {
          
            sqlUpdateEntry <- paste('UPDATE `content_anschlussobjekte` SET C_MWh = ', totalDistrDataSet[j, 'C_MWh'], ', P_MWh = ', totalDistrDataSet[j, 'P_MWh'] ,' WHERE `Anschlussobjekt` = ', totalDistrDataSet[j, 'Anschlussobjekt'] ,' AND  `Time` = ', totalDistrDataSet[j, 'Time'],' AND `Month` = ', totalDistrDataSet[j, 'Month'], sep = '', collapse = NULL)
            dbSendQuery(connectDB, sqlUpdateEntry)
            
        } else {
        
            #create value for each row in the dataframe
            valueInsertRow <- paste( valueInsertRow, '(', totalDistrDataSet[j, 'Anschlussobjekt'], ',', totalDistrDataSet[j, 'Time'], ',', totalDistrDataSet[j, 'Month'], ',', totalDistrDataSet[j, 'C_MWh'], ',', totalDistrDataSet[j, 'P_MWh'], ')', sep = '', collapse = NULL)

            #adding a , between the values except for the last one
            if(length(totalDistrDataSet$Anschlussobjekt) > j) valueInsertRow <- paste(valueInsertRow, ', ', sep = '', collapse = NULL)
          
        }
        
        j <- j + 1
    
    }
    
    
    #Finalize the insert statement and send query (insert only executed if there are values to be insert otherwise query is not executed)
    insertStatement <- paste(insertStatementBeg, valueInsertRow, sep = '', collapse = '')
    #insertStatement <- substr(insertStatement,1,nchar(insertStatement)-1)

    if (insertStatement != insertStatementBeg) dbSendQuery(connectDB, insertStatement) 
    
    
}


#calculating the status of a cell by comparing an intervall with the production and consumption at a given time point
#the intervall is calculated from the max production and max consumption divided by the number of steps (settings). The step size is divided afterwards with the consumption and production of a specific time point
calculatingCellStatus <- function() {
  
  #get min and max per Anschlussobjekt and calculating
  rs <- dbSendQuery(connectDB, sqlMaxConsCell)
  maxCons <- fetch(rs, n = -1)
  rs <- dbSendQuery(connectDB, sqlMaxProdCell)
  maxProd <- fetch(rs, n = -1)
  
  #select difference and assign
  rs <- dbSendQuery(connectDB, sqlCalDiffProdConsCell)
  diffPvCDataSet <- fetch(rs, n = -1)
  
  
  tmpStatus <- apply(diffPvCDataSet['Total_P_C'], 2, function(x) diffPvCDataSet$Total_P_C/as.numeric(settings['rangeStatusSize', 'Value'] ))
  
  diffPvCDataSet <- merge(diffPvCDataSet, tmpStatus, by=0, all=TRUE) 
  
  #rename columns to general terms
  names(diffPvCDataSet)[names(diffPvCDataSet) =="Total_P_C.x"] <- "Total_P_C"
  names(diffPvCDataSet)[names(diffPvCDataSet) =="Total_P_C.y"] <- "Status"
  
  #update DB
  j <- 1
  for (i in diffPvCDataSet$Anschlussobjekt) {
    
    sqlUpdateEntry <- paste('UPDATE `content_anschlussobjekte` SET Total_P_C = ', diffPvCDataSet[j, 'Total_P_C'], ', Status = ', diffPvCDataSet[j, 'Status'] ,' WHERE `Anschlussobjekt` = ', diffPvCDataSet[j, 'Anschlussobjekt'] ,' AND  `Time` = ', diffPvCDataSet[j, 'Time'],' AND `Month` = ', diffPvCDataSet[j, 'Month'], sep = '', collapse = NULL)
    dbSendQuery(connectDB, sqlUpdateEntry)
      
    
    j <- j + 1
    
  }
  
  
  #update settings if maxP and maxC changed
  if(as.numeric(settings['maxP', 'Value']) != maxProd || as.numeric(settings['maxC', 'Value']) != maxCons)  {
    
    sqlUpdateMaxC <- paste('UPDATE `settings` SET `Value` = ', maxCons, ' WHERE `Variable` = "maxC"', sep = '', collapse = NULL)
    sqlUpdateMaxP <- paste('UPDATE `settings` SET `Value` = ', maxProd, ' WHERE `Variable` = "maxP"', sep = '', collapse = NULL)
    dbSendQuery(connectDB, c(sqlUpdateMaxP, sqlUpdateMaxC))
    calcRanges()
    

  }
  
  
  

}


#calculates range size according the max values in C_MWh and P_MWh
calcRanges <- function() {
  
  maxP <- round(as.numeric(settings['maxP','Value']), digits = 0)
  maxC <- round(as.numeric(settings['maxC','Value']), digits = 0)
  rangeSteps <- as.numeric(settings['rangeCellStatus','Value'])
  tmpP <- as.numeric(0) 
  
  #calculate the range size
  res <- (maxP + maxC) / rangeSteps
  
  sqlUpdateRanges <- paste('UPDATE `settings` SET `Value` = ', res, ' WHERE `Variable` = "rangeStatusSize"', sep = '', collapse = NULL)
  dbSendQuery(connectDB, sqlUpdateRanges)
  
  #refresh settings
  rs <- dbSendQuery(connectDB, 'SELECT * FROM `settings` WHERE 1')
  settings <<- fetch(rs, n = -1)
  rownames(settings) <<- settings[,'Variable']
}





#generates the storage status (in MWh)
calculatingCellStorage <- function() {
  
  
}


#Re-clustering loop for the list of Anschlussobjekte - make H1, H2 etc. to H0. same with G and L
reclusteringCell <- function() {
  
    for (i in unique(aoDataSet$Clustering)) {
        aoDataSet[aoDataSet == i] <<- as.character(listClusteringCell[i])
      
    }
    
}



#creates an additional column "P_MWh" according a list of production tarifs. (Splitting MWH of consumption and production into different columns)
existProdOfCell <- function() {
  
    aoDataSet[,'P_MWh'] <<- as.integer(0)
    
    
    q <- 1
    for (elementList in aoDataSet$Tarif) {
    
        #acreate new column for current production. Assign value in MWh
        if(aoDataSet[q, 'Tarif'] %in% as.list(listTarifsProd$Tarif)) {
            aoDataSet[q, 'P_MWh'] <<- aoDataSet[q, 'C_MWh']
            aoDataSet[q, 'C_MWh'] <<- 0
        }
      
        q <- q + 1
        
    }
    
    #saving all Anschlussobjekte IDs for later reference
    listCellProdIDs <<- subset(aoDataSet, aoDataSet$P_MWh > 0)
    
    aoDataSet$MWh_2013[aoDataSet$Tarif %in% as.list(listTarifsProd$Tarif)] <<- as.integer(0)
    aoDataSet <<- subset(aoDataSet, select=-Tarif)
    
}


#assign production status either current MWh with "Current" or the potential (currently "Good", "Medium", and "Bad" - see listProvisoryProd)
#ToDo:  potential of production assign according geodata
assignProvisoryPV <- function() {

    #get a list with Anschlussobjekte without a production
    listCellConsIDs <<- subset(aoDataSet, aoDataSet$P_MWh == 0)
    
    #assign "current" value to all active production units
    for (j in listCellProdIDs$Anschlussobjekt) {
      
        #assign "current" status to current/active PV production
        aoDataSet$Potential[j == aoDataSet$Anschlussobjekt] <<- 'Current'
      
      
    }
    
    
    #assign randomly potential PV production to all non-current production according the column "Potential"
    for (k in unique(listCellConsIDs$Anschlussobjekt)) {
      
        #assignment of (randomly generated) PV potential to Anschlussobjekte without production
        aoDataSet$Potential[aoDataSet$Anschlussobjekt == k] <<- sample(rownames(listProvisoryProd), 1, replace = TRUE)
      
    }
    
    #assign MWh according the potential PV production
    aoDataSet$P_MWh[aoDataSet$Potential == 'Good'] <<- listProvisoryProd['Good', 'MWh']
    aoDataSet$P_MWh[aoDataSet$Potential == 'Medium'] <<- listProvisoryProd['Medium', 'MWh']
    aoDataSet$P_MWh[aoDataSet$Potential == 'Bad'] <<- listProvisoryProd['Bad', 'MWh']
    
    #save results in DB (Potential per Anschlussobjekt)
    listProvProd <- unique(aoDataSet[, c('Anschlussobjekt', 'Potential')], incomparables = FALSE)
    
    #building the INSERT statement out of the totalDistrDataSet
    insertStatementBeg <- 'INSERT INTO `production_potential_per_cell` (`Anschlussobjekt`, `Potential`) VALUES'
    
    valueInsertRow <- ''
    
    j <- 1
    for (i in listProvProd$Anschlussobjekt) {
      
      sqlCheckEntry <- paste('SELECT count(*) FROM `production_potential_per_cell` WHERE `Anschlussobjekt` = ', listProvProd[j, 'Anschlussobjekt'], sep = '', collapse = NULL) 
      #checks if the entry already exists in the database
      rs <- dbSendQuery(connectDB, sqlCheckEntry)
      entryExists <- fetch(rs, n = -1)

      #first update entries if they exist or insert them
      if(entryExists > 0) {
        
        sqlUpdateEntry <- paste('UPDATE `production_potential_per_cell` SET Potential = ', paste0("'", listProvProd[j, 'Potential'], "'"), ' WHERE `Anschlussobjekt` = ', listProvProd[j, 'Anschlussobjekt'], sep = '', collapse = NULL)
        dbSendQuery(connectDB, sqlUpdateEntry)
        
      } else {
        
        #create value for each row in the dataframe
        valueInsertRow <- paste( valueInsertRow, '(', listProvProd[j, 'Anschlussobjekt'], ',', paste0("'", listProvProd[j, 'Potential'], "'"), ')', sep = '', collapse = NULL)
        
        #adding a , between the values except for the last one
        if(length(listProvProd$Anschlussobjekt) > j) valueInsertRow <- paste(valueInsertRow, ', ', sep = '', collapse = NULL)
        
      }
      
      j <- j + 1
      
    }
    
    
    #Finalize the insert statement and send query (insert only executed if there are values to be insert otherwise query is not executed)
    insertStatement <- paste(insertStatementBeg, valueInsertRow, sep = '', collapse = '')
    #insertStatement <- substr(insertStatement,1,nchar(insertStatement)-2)

    if (insertStatement != insertStatementBeg) dbSendQuery(connectDB, insertStatement) 
    
    
  
}


