#Creation of a network structure to define the next neigbhours of an Anschlussobjekt
#It works like the longest or shortest distance of  the Latitude/Longitude point of the Anschlussobjekt to the next Trafostation

#calculate the distance and save it to the list
#order andidentify list members 
#building a grid??


#read data sets, clear of NA or zero coordinates, merge trafostation location with Anschlussobjekt
#outcome: list of Anschlussobjekte with
createNeighbourListFromNextCell <- function () {
      
      chooseTrafo <- c(10783, 10377)
      
      #choose only the test environement SPIEZ, HaselWeg Trafo Nr 10783
      rawCellData <- cellLocationData[cellLocationData$Trafo.Nr %in% chooseTrafo, ] 
      rawTrafoData <- trafoLocationData[trafoLocationData$Trafo.Nr %in% chooseTrafo, ] 
     
      #merging cell and trafo information 
      aggrLocationData <<- merge.data.frame(x = rawCellData, y = rawTrafoData, by = 'Trafo.Nr', all.x = TRUE)
      
      #remove all 0 distances
      aggrLocationData <<- subset(aggrLocationData, aggrLocationData$C.Latitude != 0 | aggrLocationData$C.Longitude != 0)
      
      
      j <- 1
      #list with the distance of each Anschlussobjekt to the trafostations
      for (i in aggrLocationData$Anschlussobjekt) {
            
            #calculation according geosphere package
            aggrLocationData[j,'Distance Trafo'] <<- distGeo(c(aggrLocationData[j,'C.Latitude'],aggrLocationData[j,'C.Longitude']), c(aggrLocationData[j,'T.Latitude'], aggrLocationData[j,'T.Longitude']))
                   
            j <- j +1
      }
      
      #list of Trafo numbers
      listTrafoNumbers <- unique(aggrLocationData$Trafo.Nr)
      #sort list according Trafo Nr and distance where the nearest comes first
      aggrLocationData <<- aggrLocationData[order(aggrLocationData$Trafo.Nr, aggrLocationData$`Distance Trafo`),]
      
      
      j <- 1
      #creating edge ids to define the next neigbours. Starting with the closest to the trafostation 
      for(i in listTrafoNumbers) {
            
            tmpLocationData <- aggrLocationData[aggrLocationData$Trafo.Nr %in% i, ]
            
            
            for (h in tmpLocationData$Anschlussobjekt) {
                  #set Latitude and Longitude for the nearest point to the trafostation
                  currentLatitude <- tmpLocationData[1,'C.Latitude']
                  currentLongitude <- tmpLocationData[1,'C.Longitude']
                  currentAnschlussobjekt <- tmpLocationData[1, 'Anschlussobjekt']
                  
                 
                  q <- 1
                  #calculation of the geospherial distance between the Anschlussobjekts where the Latitude and Longitude alters
                  for (k in tmpLocationData$Anschlussobjekt) {
                        #calculation according geosphere package
                       tmpLocationData[q,'Distance'] <- distGeo(c(currentLatitude, currentLongitude), c(tmpLocationData[q,'C.Latitude'], tmpLocationData[q,'C.Longitude']))
                       q <- q + 1
                  }
                  
                  #sort list according Trafo Nr and distance where the nearest comes first
                  tmpLocationData <- tmpLocationData[order(tmpLocationData$Trafo.Nr, tmpLocationData$Distance),]
                  
                  
                  #add the Anschlussobjekt ID of the next entry to the current ID
                  if (is.na(tmpLocationData$Anschlussobjekt[2])) {
                        aggrLocationData[currentAnschlussobjekt == aggrLocationData$Anschlussobjekt, 'Edge Prev'] <<- tmpLocationData$Anschlussobjekt[1] 
                  
                        } else {
                              aggrLocationData[currentAnschlussobjekt == aggrLocationData$Anschlussobjekt, 'Edge Prev'] <<- tmpLocationData$Anschlussobjekt[2]
                  
                        } 
                  
                  #remove current Anschlussobjekt
                  tmpLocationData <- tmpLocationData[!tmpLocationData$Anschlussobjekt %in% currentAnschlussobjekt, ] 
                  j <- j+1
            }
            
      }

      
      #create the edge list for the graph 
      edgeListLocData <<- subset(aggrLocationData, select = c('Edge Next', 'Anschlussobjekt'))
      

      #ToDO:
      #create diff of cons and prod per hour and anschlussobjekt
      #calculate the status per hour (diff of cons - prod) and assign color to it
      #add hourly based (columnname: status_j where j is iterative) status and difference to the edgelist
      #assign color to the vertex according the anschlussobjekt see http://stackoverflow.com/questions/15999877/correctly-color-vertices-in-r-igraph
      #http://christophergandrud.github.io/d3Network/
      #the trafo doesn't only feed one street, so for the example you can use the trafo and subdivided by street? Or only use a street as acrowd
      
      #nice graph plot 
      graph <- graph_from_data_frame(edgeListLocData, directed = TRUE, vertices = NULL)
      grFrame <- get.data.frame(graph, what = 'edges') 
      simpleNetwork(grFrame, fontSize = 12)
      
}




#read data sets, clear of NA or zero coordinates, merge trafostation location with Anschlussobjekt
#outcome: list of Anschlussobjekte with
createNeighbourListFromTrafo <- function () {
      
      #choose only the test environement SPIEZ, HaselWeg Trafo Nr 10783
      rawCellData <- subset(cellLocationData, Trafo.Nr == 10783)
      rawTrafoData <- subset(trafoLocationData, Trafo.Nr == 10783)
      
      
      #merging cell and trafo information 
      aggrLocationData <<- merge.data.frame(x = rawCellData, y = rawTrafoData, by = 'Trafo.Nr', all.x = TRUE)
      
      #remove all 0 distances
      aggrLocationData <<- subset(aggrLocationData, aggrLocationData$C.Latitude != 0 | aggrLocationData$C.Longitude != 0)

      
      j <- 1
      
      for (i in aggrLocationData$Anschlussobjekt) {
            
            if(aggrLocationData[j,'C.Latitude'] != 0 && aggrLocationData[j,'C.Longitude'] != 0) {
            
                  #calculation according geosphere package
                  aggrLocationData[j,'Distance'] <<- distGeo(c(aggrLocationData[j,'C.Latitude'],aggrLocationData[j,'C.Longitude']), c(aggrLocationData[j,'T.Latitude'], aggrLocationData[j,'T.Longitude']))
                  value <- distGeo(c(aggrLocationData[j,'C.Latitude'],aggrLocationData[j,'C.Longitude']), c(aggrLocationData[j,'T.Latitude'], aggrLocationData[j,'T.Longitude']))
                 
                  #Calculation according formmular: dist = 6378.388 * acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1))
                  #see http://www.kompf.de/gps/distcalc.html
                  #aggrLocationData[j,'Distance Kugeldreieck'] <<- 6378.388 * acos(sin(aggrLocationData[j,'C.Latitude']) * sin(aggrLocationData[j,'T.Latitude']) + cos(aggrLocationData[j,'C.Latitude']) * cos(aggrLocationData[j,'T.Latitude']) * cos(aggrLocationData[j,'T.Longitude'] - aggrLocationData[j,'C.Longitude']))
                  
                  #Verbesserte Methode 
                  #lat <- (aggrLocationData[j,'C.Latitude'] + aggrLocationData[j,'T.Latitude']) / 2 * 0.01745
                  #dx <- 111.3 * cos(lat) * (aggrLocationData[j,'C.Longitude'] - aggrLocationData[j,'T.Longitude'])
                  #dy <- 111.3 * (aggrLocationData[j,'C.Latitude'] - aggrLocationData[j,'T.Latitude'])
                  
                  #aggrLocationData[j,'Distance Verbesserte M'] <<- sqrt(dx * dx + dy * dy)
                  
                  #Satz des Pythagoras
                  #dx = 71.5 * (aggrLocationData[j,'C.Longitude'] - aggrLocationData[j,'T.Longitude'])
                  #dy = 111.3 * (aggrLocationData[j,'C.Latitude'] - aggrLocationData[j,'T.Latitude'])
                  #aggrLocationData[j,'Distance Pythagoras'] <<- sqrt(dx * dx + dy * dy)
            
                  } else {
                        aggrLocationData[j,'Distance Trafo'] <<- 0
                  
            }            
            j <- j +1
      }
      
      #sort list according Trafo Nr and distance where the nearest comes first
      aggrLocationData <<- aggrLocationData[order(aggrLocationData$Trafo.Nr, aggrLocationData$Distance),]
      
      q <- 1
      #creating edge ids to define the neigbours
      for(i in aggrLocationData$Anschlussobjekt) {
            
            #first element of a list is connected with the TrafoNr
            if (length(aggrLocationData$Anschlussobjekt[q - 1]) == 0 || aggrLocationData$Trafo.Nr[q] != aggrLocationData$Trafo[q - 1]) {
                  edgePrev <- aggrLocationData$Trafo.Nr[q]
                  
            } else {
                  edgePrev <- aggrLocationData$Anschlussobjekt[q - 1]
            
            }
            

            aggrLocationData[q, 'Edge Prev'] <<- edgePrev
            #aggrLocationData[q, 'Edge Next'] <<- edgeNext
            
            q <- q + 1
            
      }

      #sort list according Trafo Nr and distance where the nearest comes first
      aggrLocationData <<- aggrLocationData[order(aggrLocationData$Trafo.Nr, aggrLocationData$Distance),]
      
      #create the edge list for the graph 
      edgeListLocData <- subset(aggrLocationData, select = c('Edge Prev', 'Anschlussobjekt'))
      

      #nice graph plot 
      graph <- graph_from_data_frame(edgeListLocData, directed = TRUE, vertices = NULL)
      grFrame <- get.data.frame(graph, what = 'edges') 
      simpleNetwork(grFrame, fontSize = 12)
      
}






#not used as it should calculate the shortest path of a set of coordinates - gdistance
calculatingNeighbours <- function() {
      
      spg <- subset(aggrLocationData, select = c('C.Latitude', 'C.Longitude'))
      spg <- unique.data.frame(spg)
      
      coordinates(spg) <- c('C.Latitude', 'C.Longitude')
      # coerce to SpatialPixelsDataFrame
      #gridded(spg) <- TRUE
      points2grid(spg, tolerance=0.9, round=1)
      # coerce to raster
      rasterDF <- raster((spg))
      proj4string(rasterDF) <- CRS("+proj=lcc +lat_1=48 +lat_2=33 +lon_0=-100 +ellps=WGS84")
      #crs(rasterDF) <- "+proj=longlat +datum=WGS84"
      
      
      #Create a Transition object from the raster
      tr <- transition(rasterDF,mean,4)
      
      
      #define the start and the end point to calculate the shortest way - currently according the shortes and the longest distance from Trafo to each Anschlussobjekt
      c1 <- c(aggrLocationData[which.max(aggrLocationData$Distance),'C.Latitude'], aggrLocationData[which.max(aggrLocationData$Distance),'C.Longitude'])
      c2 <- c(aggrLocationData[which.min(aggrLocationData$Distance),'C.Latitude'], aggrLocationData[which.min(aggrLocationData$Distance),'C.Longitude'])
      
      #Calculate the RSP distance between the points
      #rSPDistance(tr, sP1, sP2, 1)
      
      #make a SpatialLines object for visualization
      sPath1 <- shortestPath(tr, c1, c2)
      print(sPath1)
      lines(sPath1)
      
      #make a TransitionLayer for further calculations
      sPath2 <- shortestPath(tr, c1, c2)
      plot(rasterDF(sPath2))
     
}
