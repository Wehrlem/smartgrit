#Author: Mario Gstrein, 05.05.2015, Fribourg (CH)
#Operation:
# 1) call generateAnschlussobjektTable() to provide basic dataset for distribution calculation
# 2) call calculatingDistribution() to generate the distribution for consumption and production (the consumption is generated for Week, Saturday and Sunday)
# 3) call mergeDistribution() to generate one dataset with 'Anschlussobjekt', 'Time', 'Month', 'C_MWh_Week', 'C_MWh_Saturday', 'C_MWh_Sunday', 'P_MWh' and store it in the DB
# 4) call calculatingCellStatus to generate two columns in the DB a) a difference between production and consumption and b) the intervall value for assigning the status to the intervall
# 5) call caluclatingCellStorage to generate the available amount of electricity to a given time-point

#IMPORTANT: steps 1 to 3 should only be done, if there is a new set of raw data. It generates the basic table (content_anschlussobjekte) which requires a lot of computation time. Any further adaptation, e.g. consumption, is done afterwards or based on this data. 

#TODO: - in function exitProvProd randomly production potential is assigned, however, it is done each time (should do once, if id doesn't exist in the production_potential_cell table)


#the function reads the original data on Messpunkte level and generates a raw table in which each a house (=Anschlussobjekt).
#There are not unique IDs as a house can have different profiles (H0, G0).
#output: is a dataframe aoDataSet (columns: Anschlussobjekt, Clustering, GEM_Nr, C_MWh, P_MWh, Potential)
#Anschlussobjekt Clustering GEM_Nr C_MWh P_MWh Potential
#2     11100108498         H0    768  5228  4000    Medium
#3     11100151173         H0    768  8902  4031   Current
#4     11100151188         H0    768  1392 10028   Current
#1     11100153358         G0    768 16133  3500       Bad
#5     11100153358         H0    768  6851  3500       Bad
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

#calculation of hourly distribution for production by loading "profiles". In this case it is the daily (average) sun radiation from January to December
#input: is the aoDataSet (columns: Anschlussobjekt, Clustering, GEM_Nr, C_MWh, P_MWh, Potential)
#output: is a dataframe with the columns: 'Anschlussobjekt', 'Time', 'Month', 'P_MWh'. The results are stored in DB "XXX"
calculatingDistrProd <- function() {
    

    #load consumption or sun radiation profiles etc.
    rs <- dbSendQuery(connectDB, sqlLoadProdProfile)
    profiles <- fetch(rs, n = -1)
    
    #selecting only columns for the consumption or production calculation 
    tmpDataSet <- aoDataSet[c('Anschlussobjekt', 'GEM_Nr', 'P_MWh')]
    
    #remove all double entries whichare caused by different profiles (H0, G0) as a house has only one production capacity
    tmpDataSet <- unique(tmpDataSet)
    
    #two datasets required to merge them afterwards (only consumption or production can be calculated at once)
    distrDataSet_P <<- 0
    tmpdistrDataSet <- data.frame(list('Anschlussobjekt' = 'character', 'Time' = 'integer', 'Month' = 'integer', 'P_MWh' = 'double'))
    tmpdistrDataSet <- 0
    
    j <- 1

    for (cellID in tmpDataSet$Anschlussobjekt) { 
        
      
        #load profiles (matching either Clustering for consumption or GEM_Nr for production)
        tmpProfile <- profiles[which (profiles$GEM_Nr == tmpDataSet$GEM_Nr[j]), 4:15]

        #loop through months
        for (mth in 1:12) {
          
            #the required profile looks up  the DB where they are stored per months (columns in table)
            res <- data.frame(mapply('*',tmpProfile[mth], tmpDataSet[j, 'P_MWh'] ))
            
            #create a data frame for distribution per Anschlussobjekt (hourly) and add columns
            res[, 'Anschlussobjekt'] <- cellID
            res[, 'Time'] <- rownames(res)
            res[, 'Month'] <- mth
            res[, 'P_MWh'] <- res[]
            res[, 1] <- NULL
            
            #create a dataframe with all consumption/production distributions
            tmpdistrDataSet <- rbind(res, tmpdistrDataSet) 
            
            
        }
        
        j <- j + 1
    }
    
    
    #aggregate distribution dataset so only each row represents a single Anschlussobjekt to a specific time and month. (e.g. aggregate different load profiles H0 and G0 of one ID)
    tmpdistrDataSet <- setNames(aggregate(tmpdistrDataSet[, 'P_MWh'], by = list(tmpdistrDataSet$Anschlussobjekt, tmpdistrDataSet$Time, tmpdistrDataSet$Month), FUN = sum), c('Anschlussobjekt', 'Time', 'Month', 'P_MWh'))
    tmpdistrDataSet <- subset(tmpdistrDataSet, tmpdistrDataSet$Anschlussobjekt != 0)
    
    distrDataSet_P <<- tmpdistrDataSet
    
}


#calculation of hourly distribution for consumption aaccording standard load profiles (SLP - VDEW. for week, saturday, sunday each of summer, winter, and transition period)
#Thus, the function mutliplies the yearly consumption of an Anschlussobjekt with the SLP and asigns for each month the specific SLP (e.g. for January the winter profile)
#input: is the aoDataSet (columns: Anschlussobjekt, Clustering, GEM_Nr, C_MWh, P_MWh, Potential)
#output: is a dataframe with the columns: 'Anschlussobjekt', 'Time', 'Month', 'C_MWh_Week', 'C_MWh_Saturday', 'C_MWh_Sunday')
calculatingDistrCons <- function() {
  
  
  #load consumption or sun radiation profiles etc.
  rs <- dbSendQuery(connectDB, sqlLoadConsProfile)
  profiles <- fetch(rs, n = -1)
  
  #selecting only columns for the consumption or production calculation 
  tmpDataSet <- aoDataSet[c('Anschlussobjekt', 'Clustering', 'C_MWh')]
  
  #two datasets required to merge them afterwards (only consumption or production can be calculated at once)
  distrDataSet_C <<- 0 
  tmpdistrDataSet <- data.frame(list('Anschlussobjekt' = 'character', 'Time' = 'integer', 'Month' = 'integer', 'C_MWh_Week' = 'double', 'C_MWh_Saturday' = 'double', 'C_MWh_Sunday' = 'double'))
  tmpdistrDataSet <- 0
  
  rs <- dbSendQuery(connectDB, sqlLoadAssignedMonth)
  assignedPeriod <- fetch(rs, n = -1)
  #assignedPeriod <- as.character(assignedPeriod)
  
  j <- 1
  for (cellID in tmpDataSet$Anschlussobjekt) { 
    
    
    #load profiles (matching either Clustering for consumption)
    tmpProfile <- profiles[which (profiles$Profile == tmpDataSet$Clustering[j]), ]
  
    #calculate the consumption for summer, winter and transition phase according VDEW SLP
    res <- data.frame(tmpProfile[1:3], mapply('*',tmpProfile[c('Week', 'Saturday', 'Sunday')], tmpDataSet[j, 'C_MWh'] ))
    
    #assign consumption pattern per month (see assigned periods to month, e.g. W = 1, W = 2, TP = 3)
    for (mth in 1:12) {
      
      #which period (W, S, TP) is used for the consumption pattern
      period <- assignedPeriod$Period[assignedPeriod$Month == mth]
      period <- as.character(period)
      
      tmpConsMonthly <- as.data.frame(res[which(period == res['Period']),  ])
     
      #create a data frame for distribution per Anschlussobjekt (hourly) and add columns
      tmpConsMonthly[, 'Anschlussobjekt'] <- cellID
      tmpConsMonthly[, 'Month'] <- mth
        
      #create a dataframe with all consumption/production distributions
      tmpdistrDataSet <- rbind(tmpConsMonthly, tmpdistrDataSet) 
      
      
    }
    j <-j+1
  }
  
  #rename columns to general terms
  names(tmpdistrDataSet)[names(tmpdistrDataSet) =="Week"] <- "C_MWh_Week"
  names(tmpdistrDataSet)[names(tmpdistrDataSet) =="Saturday"] <- "C_MWh_Saturday"
  names(tmpdistrDataSet)[names(tmpdistrDataSet) =="Sunday"] <- "C_MWh_Sunday"

  #aggregate distribution dataset so only each row represents a single Anschlussobjekt to a specific time and month. (e.g. aggregate different load profiles H0 and G0 of one ID)
  tmpdistrDataSet <- setNames(aggregate(tmpdistrDataSet[, c('C_MWh_Week', 'C_MWh_Saturday', 'C_MWh_Sunday')], by = list(tmpdistrDataSet$Anschlussobjekt, tmpdistrDataSet$Time, tmpdistrDataSet$Month), FUN = sum), c('Anschlussobjekt', 'Time', 'Month', 'C_MWh_Week', 'C_MWh_Saturday', 'C_MWh_Sunday'))
  tmpdistrDataSet <- subset(tmpdistrDataSet, tmpdistrDataSet$Anschlussobjekt != 0)
  
  distrDataSet_C <<- tmpdistrDataSet
  
}



#merges the datasets 'tmpdistrDataSet_C' and 'tmpdistrDataSet_C' to generate one dataset with 'Anschlussobjekt', 'Time', 'Month', 'C_MWh_Week', 'C_MWh_Saturday', 'C_MWh_Sunday' 'P_MWh' and store it in the DB
mergeDistribution <- function () {
  
    totaldistrDataSet <<- data.frame(merge(distrDataSet_C, distrDataSet_P , by = c('Anschlussobjekt', 'Time', 'Month')), stringsAsFactors=FALSE)
    totaldistrDataSet <<- totaldistrDataSet[order(totaldistrDataSet$Anschlussobjekt, as.numeric(totaldistrDataSet$Month), as.numeric(totaldistrDataSet$Time)),]
    
    #create different tables for different days (week, Sunday, Sturday)
    
    #building the INSERT statement out of the totaldistrDataSet
    insertStatementBeg <- 'INSERT INTO `content_anschlussobjekte` (`Anschlussobjekt`, `Time`, `Month`, `C_MWh_Week`, `C_MWh_Saturday`, `C_MWh_Sunday`, `P_MWh`) VALUES'
    
    valueInsertRow <- ''
  
    j <- 1
    for (i in totaldistrDataSet$Anschlussobjekt) {
      
        sqlCheckEntry <- paste('SELECT count(*) FROM `content_anschlussobjekte` WHERE `Anschlussobjekt` = ', totaldistrDataSet[j, 'Anschlussobjekt'] ,' AND  `Time` = ', totaldistrDataSet[j, 'Time'],' AND `Month` = ', totaldistrDataSet[j, 'Month'], sep = '', collapse = NULL)
        
        #checks if the entry already exists in the database
        rs <- dbSendQuery(connectDB, sqlCheckEntry)
        entryExists <- fetch(rs, n = -1)

        #first update entries if they exist or insert them
        if(entryExists > 0) {
          
            sqlUpdateEntry <- paste('UPDATE `content_anschlussobjekte` SET `C_MWh_Week` = ', totaldistrDataSet[j, 'C_MWh_Week'], ', `C_MWh_Saturday` = ', totaldistrDataSet[j, 'C_MWh_Saturday'], ', `C_MWh_Sunday` = ', totaldistrDataSet[j, 'C_MWh_Sunday'], ', `P_MWh` = ', totaldistrDataSet[j, 'P_MWh'] ,' WHERE `Anschlussobjekt` = ', totaldistrDataSet[j, 'Anschlussobjekt'] ,' AND  `Time` = ', totaldistrDataSet[j, 'Time'],' AND `Month` = ', totaldistrDataSet[j, 'Month'], sep = '', collapse = NULL)
            #print(sqlUpdateEntry)
            dbSendQuery(connectDB, sqlUpdateEntry)
            
        } else {
        
            #create value for each row in the dataframe
            valueInsertRow <- paste( valueInsertRow, '(', totaldistrDataSet[j, 'Anschlussobjekt'], ',', totaldistrDataSet[j, 'Time'], ',', totaldistrDataSet[j, 'Month'], ',', totaldistrDataSet[j, 'C_MWh_Week'], ',', totaldistrDataSet[j, 'C_MWh_Saturday'], ',', totaldistrDataSet[j, 'C_MWh_Sunday'], ',', totaldistrDataSet[j, 'P_MWh'], ')', sep = '', collapse = NULL)

            #adding a , between the values except for the last one
            if(length(totaldistrDataSet$Anschlussobjekt) > j) valueInsertRow <- paste(valueInsertRow, ', ', sep = '', collapse = NULL)
          
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
  
  #get min and max of the table
  rs <- dbSendQuery(connectDB, sqlMaxConsCell)
  maxCons <- fetch(rs, n = -1)
  maxCons <- as.numeric(maxCons)
  rs <- dbSendQuery(connectDB, sqlMaxProdCell)
  maxProd <- fetch(rs, n = -1)
  maxProd <- as.numeric(maxProd)
  rs <- dbSendQuery(connectDB, sqlLoadIntervallSteps)
  intervSteps <- fetch(rs, n = -1)
  intervSteps <- as.numeric(intervSteps)
  
  #looks up if the range size for the status has changed
  calcRanges(maxProd, maxCons, intervSteps)
  
  #calculating the intervall level of a sepcific time point 
  #TODO: Ask if this is okay over the entire table
  #dbSendQuery(connectDB, paste('UPDATE `content_anschlussobjekte` SET `Status` = `Diff_P_C` / ', as.numeric(settings['intervallSize', 'Value']), sep = '', collapse = ''))


}


#calculates range size according the max values in C_MWh and P_MWh
calcRanges <- function(maxProd, maxCons, intervSteps) {
  
    if(as.numeric(settings['maxP', 'Value']) != maxProd || sum(as.numeric(settings[c('maxC_Week', 'maxC_Saturday', 'maxC_Sunday'), 'Value'])) != sum(maxCons) || as.numeric(settings['intervallSteps', 'Value']) != intervSteps)  {
    
        maxP <- round(as.numeric(settings['maxP','Value']), digits = 0)
        
        j <- 1
        for(maxDay in c('maxC_Week', 'maxC_Saturday', 'maxC_Sunday' )) {
          
          maxC <- round(as.numeric(settings[maxDay,'Value']), digits = 0)
          #calculate the range size
          res <- (maxP + maxC) / intervSteps
        
          #update settings
          sqlUpdateRanges <- paste('UPDATE `settings` SET `Value` = ', res, ' WHERE `Variable` = ', paste0("'intervallSize", substr(maxDay, 5, nchar(maxDay)), "'"), sep = '', collapse = '')
          sqlUpdateMaxC <- paste('UPDATE `settings` SET `Value` = ', maxCons[j], ' WHERE `Variable` = ', paste0("'", maxDay, "'; "), sep = '', collapse = '')
          dbSendQuery(connectDB, sqlUpdateRanges)
          dbSendQuery(connectDB, sqlUpdateMaxC)
          
          j <- j + 1 
          
        }
        
        #update max production
        sqlUpdateMaxP <- paste('UPDATE `settings` SET `Value` = ', maxProd, ' WHERE `Variable` = ', paste0("'", 'maxP', "'"), sep = '', collapse = '')
        dbSendQuery(connectDB, sqlUpdateMaxP)

        
        #refresh settings
        rs <- dbSendQuery(connectDB, 'SELECT * FROM `settings` WHERE 1')
        settings <<- fetch(rs, n = -1)
        rownames(settings) <<- settings[,'Variable']
    
    }
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
    
    aoDataSet$C_MWh[aoDataSet$Tarif %in% as.list(listTarifsProd$Tarif)] <<- as.integer(0)
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
    
    #building the INSERT statement out of the totaldistrDataSet
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


test <- function() {
  
  #select difference from table
  rs <- dbSendQuery(connectDB, sqlCalDiffProdConsCell)
  diffPvCDataSet <- fetch(rs, n = -1)
  
  #calculating the cell status (according the number of intervalls)
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
  
  
  
}

checkSumDistr <- function() {
  rs <- dbSendQuery(connectDB, sqlCheckSumDistr)
  res <- fetch(rs, n = -1)
  
  return(res)
}
