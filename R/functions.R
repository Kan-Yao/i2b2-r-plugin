#' i2b2ExportData
#' 
#' In this package, you will find functions for exporting data
#' 
#' @name i2b2ExportData
#' @docType package

#' dbConnexion
#' 
#' Connection function to the database
#'
#' @param directoryConfig Config file directorys
#' 

NULL_VALUE <- "'[protected]'"

dbConnexion <- function(directoryConfig){
  connectionFile <- file.path(directoryConfig, "connectionFile.cfg")
  con <- RPostgres::dbConnect(RPostgres::Postgres(), 
                   dbname = getValueFromConfFile(connectionFile,"dbname"),
                   host = getValueFromConfFile(connectionFile,"host"),
                   port = getValueFromConfFile(connectionFile,"port"),
                   user = getValueFromConfFile(connectionFile,"user"),
                   password = getValueFromConfFile(connectionFile,"password"))
  
}


#' dbDisconnection
#' 
#' Disconnect function to the database
#'
#' @param conn connection information
#'
dbDisconnection <- function(conn){
  RPostgres::dbDisconnect(conn)
}


#' getValueFromConfFile
#'
#' Reading the connection file 
#' 
#' @param The read file path
#' @param pattern The field to read
#' 
getValueFromConfFile <- function(file, pattern){
  gsub(paste0(pattern,"="),"",grep(paste0("^",pattern,"="), scan(file,what="",quiet=T),value=T))
}



#' makePreparedQuery
#'
#' Function for prepared statements
#' 
#' @param conn connection information
#' 
makePreparedQuery <- function(conn) {
  function(statement, ...) {
      options(warn=-1)
    params = list(...)
    
    rs <- RPostgres::dbSendQuery(conn, statement)
    on.exit(RPostgres::dbClearResult(rs))
    
    if (length(params) > 0) {
      RPostgres::dbBind(rs, params)
    }
    df <- RPostgres::dbFetch(rs, n = -1, ...)
    if (!RPostgres::dbHasCompleted(rs)) {
    RPostgres::dbClearResult(rs)
      warning("Pending rows", call. = FALSE)
    }
    
    options(warn=0)
    df
  }
}

#' verifIdProject
#'
#' Checks if project ID is valid
#'
#' @param conn connection information
#' @param idProjet L'idProjet a verifier
#'
#' @return True if idProjet exists
#'
verifIdProject <- function(conn,idProject){
  out <- tryCatch({
    query <- "SELECT COUNT(DISTINCT x.project_id) line 
    FROM i2b2pm.pm_project_user_roles x
    WHERE x.project_id=($1::varchar)"
    
    verifId <- makePreparedQuery(conn)
    verifId(query,idProject)
    r <- strtoi(verifId(query,idProject),base=0L) == 1
    if(r == TRUE){
      return(TRUE)
    }else{
      print("idProjet non valide !")
    }
  }, error = function(err){
    print(err)
    
  },finally = {
    r <- strtoi(verifId(query,idProject),base=0L) == 1
  }
  )
}


#' verifIdSession
#'
#' Checks if session ID is valid
#'
#' @param conn connection information
#' @param idSession Session ID 
#'
#'@return True if idSession exists
#'
verifIdSession <- function(conn,idSession){
  out <- tryCatch({
    query <- "SELECT COUNT(DISTINCT x.session_id) line 
    FROM i2b2pm.pm_user_session x
    WHERE x.session_id=($1::varchar)"
    
    verifSession <- makePreparedQuery(conn)
    verifSession(query,idSession)
    r <- strtoi(verifSession(query,idSession),base=0L) == 1
    
    if(r == TRUE){
      return(TRUE)
    }else{
      print("idSession non valide")
    }
  }, error = function(err){
    print(err)
    
  },finally = {
    return(strtoi(verifSession(query,idSession),base=0L) == 1)
  }
  )
}


#' verifIdResult
#'
#' Checks if project ID is valid
#'
#' @param conn connection information
#' @param idResult Result ID
#'
#' @return True if idResult exists
#'
verifIdResult <- function(conn,idResult){
  out <- tryCatch({
    query <- "SELECT COUNT(DISTINCT x.result_instance_id) line 
    FROM i2b2data_multi_nomi.qt_query_result_instance x
    WHERE x.result_instance_id=($1::numeric)"
    
    verifResult <- makePreparedQuery(conn)
    verifResult(query,idResult)
    r <- strtoi(verifResult(query,idResult),base=0L) == 1
    if(r == TRUE){
      return(TRUE)
    }else{
      print("idResult non valide")
    }
  }, error = function(err){
    print(err)
    
  },finally = {
    return(strtoi(verifResult(query,idResult),base=0L) == 1)
  }
  )
}


#' getEndSessionDate
#' 
#' Gets the session end date of the current user
#' 
#' @param conn connection information
#' @param idProject Project ID
#' @param idSession Session ID 
#' @param idResult Result ID
#' @param idUser User ID
#'
#' @return The session end date 
#'
getEndSessionDate <- function(conn, idProject, idSession,idResult,idUser){
  if(verifQueryOwner(conn, idSession, idProject, idResult, idUser) == TRUE){
    if(verifIdProject(conn,idProject) == TRUE){
      if(verifIdSession(conn,idSession)== TRUE){
        out <- tryCatch({
          query <- "WITH tmp AS ( SELECT DISTINCT x.user_id, x.user_role_cd, project_id, y.session_id, y.expired_date 
          FROM i2b2pm.pm_project_user_roles x, i2b2pm.pm_user_session y
          WHERE x.user_id=y.user_id 
          and y.session_id=($1::varchar)
          and project_id=($2::varchar)
        )
          SELECT DISTINCT tmp.expired_date FROM tmp"
          
          endSessionDate <- makePreparedQuery(conn)
          endSessionDate(query,idSession,idProject)
          
          return(endSessionDate(query,idSession,idProject))
          
        }, error = function(err){
          print(err)
          
        },finally = {
        }
        )
      }
    }
  }else{
    print("Not permit to execute this query !")
    
  }
}


#' getSetTableFromIdResult
#' 
#' This function get the table name to ask for data extraction
#'
#' @param conn connection information
#' @param idResult Result ID
#' @param exportTable Table number to ask
#'
#' @return The table name to ask
#' 
getSetTableFromIdResult <- function(conn, idResult, exportTable){
  if(verifIdResult(conn,idResult) == TRUE){
    out <- tryCatch({
      query <- "SELECT x.result_type_id typeResult
      FROM i2b2data_multi_nomi.qt_query_result_instance x, i2b2data_multi_nomi.qt_query_result_type y
      WHERE x.result_instance_id = ($1::numeric)
      AND x.result_type_id = y.result_type_id"
      
      quizTable <- makePreparedQuery(conn)
      quizTable(query,idResult)
      table <- exportTable[exportTable$V1 %in% quizTable(query,idResult),]$V2
      print(table)
      return(table)
      
    },error = function(err){
      print(err)
      
    },finally = {
      
    }
    )
  }
}


#' activeSession
#' 
#' Verifies that the user's session is on
#' 
#' @param conn connection information
#' @param idProject Project ID
#' @param idSession Session ID 
#' @param idResult Result ID
#' @param idUser User ID
#'
#' @return TRUE if session active and FALSE else
#' 
activeSession <- function(conn,idProject, idSession,idResult, idUser){
  dat <- getEndSessionDate(conn,idProject, idSession,idResult, idUser)
  #curentDate <- format(Sys.time(), "%Y-%m-%d %H:%M:%S");
  curentDate <-'2015-09-29 13:15:58'
  endSession <- dat[1,"expired_date"]
  
  if(curentDate > endSession){
    print("lost session !")
    
    
  }else{
      print("Active session ")
    return(TRUE)
  }
  
}



#' actifUser
#' 
#' Verifies that the user is active and authorized to export the database
#' 
#' @param conn connection information
#' @param idSession Session ID 
#' @param idUser User ID
#'
#' @return True if the user is active and False else     
#'                                            
actifUser <- function(conn, idSession, idUser){
  if(verifIdSession(conn,idSession)== TRUE){
    out <- tryCatch({
      query <-"SELECT DISTINCT x.status_cd 
      FROM i2b2pm.pm_user_data x, i2b2pm.pm_user_session y
      WHERE y.session_id = ($1::varchar)
      AND x.user_id = ($2::varchar)"
      
      userActif <- makePreparedQuery(conn)
      
      if(identical(paste(userActif(query,idSession, idUser)),"A")){
          print("active user !")
        return(TRUE)
        
      }else{
        print("User not active !")
        
      }
    },error = function(err){
      print(err)
      
    },finally = {
      
    }
    )
  }
}


#' verifQueryOwner
#' 
#' This function ensures that the current user is the author of the petition.
#' 
#' @param conn connection information
#' @param idSession Session ID                                                                  
#' @param idProject Project ID
#' @param idResult Result ID
#' @param idUser User ID
#'
#' @return True if the user is the owner of the query false else
#' 
verifQueryOwner <- function(conn, idSession, idProject, idResult, idUser){
  if(verifIdSession(conn,idSession)== TRUE){
    if(verifIdProject(conn,idProject) == TRUE){
      if(verifIdResult(conn,idResult) == TRUE){
        out <- tryCatch({
          query <- "SELECT count(*)
          FROM i2b2data_multi_nomi.qt_query_master x, i2b2data_multi_nomi.qt_query_result_instance y, i2b2pm.pm_user_session z
          WHERE x.user_id = z.user_id
          AND x.query_master_id = y.query_instance_id
          AND z.session_id = ($1::varchar)
          AND y.result_instance_id = ($2::numeric)
          AND x.group_id = ($3::varchar)
          AND x.user_id = ($4::varchar)"
          
          queryOwner <- makePreparedQuery(conn)
          res <- queryOwner(query,idSession,idResult,idProject,idUser)$count == 1
          if(res){print("user owns set")}else{print("user don't owns set")}
          return(res)
          
        },error = function(err){
          print(err)
          
        },finally = {
          
        }
        )
        
      }
    }
  }
}


#' getRole
#' 
#' Determines the user's privileges. Columns to display depend on this privilege.
#' 
#' @param conn connection information
#' @param idProject Project ID
#' @param idSession Session ID
#'
#' @return 0 when the user is not authorized to view all the fields and 1 else.   
#' 
getRole <- function(conn, idProject, idSession){
  if(verifIdSession(conn,idSession) == TRUE){
    if(verifIdProject(conn,idProject) == TRUE){
      out <- tryCatch({
        query <- "
        SELECT x.user_role_cd AS user_role_cd
        FROM i2b2pm.pm_project_user_roles x, i2b2pm.pm_user_session y
        WHERE x.user_id=y.user_id 
        and y.session_id=($1::varchar)
        and project_id=($2::varchar)
        "
        
        role <- makePreparedQuery(conn)
        
        return(role(query,idSession,idProject)$user_role_cd)
      },error = function(err){
        print(err)
        
      },finally = {
        
      }
      )
    }
  }
}


#' buildColumnEncounter
#'
#' builds columns to display for exporting visit data set
#'                                
#' @param conn connection information                                        
#' @param idProject Project ID
#' @param idSession Session ID
#' @param colonneEncounter A display field list
#'
#' @return List of columns to display
#' 
buildColumnEncounter <- function(con,idProject,idSession,colonneEncounter){
  role <- getRole(con,idProject,idSession)
  paste0(sprintf("%s AS %s",ifelse(sapply(colonneEncounter$Profile,function(x){any(role%in%x)}),paste0("v.",colonneEncounter$Column),NULL_VALUE),colonneEncounter$Alias),collapse=", ")
}


#' buildColumnFact
#'
#' builds columns to display for exporting encounter data set  
#'     
#' @param conn connection information
#' @param idProject Project ID
#' @param idSession Session ID
#' @param colonneFact A display field list
#'
#' @return List of columns to display
#'                                                               
buildColumnFact <- function(con,idProject,idSession,colonneFact){
  role <- getRole(con,idProject,idSession)
  paste0(sprintf("%s AS %s",ifelse(sapply(colonneFact$Profile,function(x){any(role%in%x)}),paste0("f.",colonneFact$Column),NULL_VALUE),colonneFact$Alias),collapse=", ")
}


#' buildColumnPatient
#' 
#' Builds columns to display for exporting patient data set
#' 
#' @param conn connection information
#' @param idProject Project ID
#' @param idSession Session ID
#' @param colonnePatient A display field list
#'
#' @return List of columns to display 
#' 
buildColumnPatient <- function(con,idProject,idSession,colonnePatient){
  role <- getRole(con,idProject,idSession)
  paste0(sprintf("%s AS %s",ifelse(sapply(colonnePatient$Profile,function(x){any(role%in%x)}),paste0("p.",colonnePatient$Column),NULL_VALUE),colonnePatient$Alias),collapse=", ")
}


#' getIdQuery
#' 
#' Return the query number
#' 
#' @param conn connection information
#' @param idResult Result number
#'
#' @return idQuery Query ID
#'
getIdQuery <- function(conn,idResult){
  if(verifIdResult(conn,idResult) == TRUE){
    out <- tryCatch({
      query <- "SELECT query_instance_id 
      FROM i2b2data_multi_nomi.qt_query_result_instance x
      WHERE x.result_instance_id = ($1::numeric)"
      
      idQuery <- makePreparedQuery(conn)
      return(strtoi(idQuery(query,idResult),base=0L))
      
    },error = function(err){
      print(err)
      
    },finally = {
      
    }
    )
    
  }
}


#' getAction
#' 
#'  Return the purpose of this package
#' 
getAction <- function(){
  return("Export")
}

#' getColumnFile
#' 
#' Reads a file from a directory
#' 
#' @param directoryConfig Config file directory
#' @param file Name of CSV file
#' 
getColumnFile <- function(directoryConfig,file){
    read.csv2(file.path(directoryConfig,file), stringsAsFactors=FALSE)
}

#' factCollection
#'
#' From the result of the query i2b2, this function returns a list of facts matching.

#' @param idResult Result number
#' @param idProject Projet ID
#' @param idSession Session ID
#' @param directoryConfig Config file directory
#' @param idUser User ID
#'
#' @return La list of encounters
#' 
factCollection <- function(idResult, idProject, idSession,directoryConfig, idUser){
  
  if(activeSession(conn, idProject, idSession,idResult, idUser) == TRUE){
    if(actifUser(conn, idSession, idUser) == TRUE){
      if(verifQueryOwner(conn, idSession, idProject, idResult, idUser) == TRUE){
          fact_cols <- buildColumnFact(conn,idProject,idSession,getColumnFile(directoryConfig,"obsFactColumn.csv"))
          i2b2DataSchema <- getValueFromConfFile(file.path(directoryConfig,"connectionFile.cfg"),"i2b2DataSchema")
          set_table <- getSetTableFromIdResult(conn, idResult, getColumnFile(directoryConfig,"exportTable.csv"))
        if(identical(set_table,"qt_patient_set_collection") == TRUE){
          query <- sprintf("SELECT  %s
                           FROM %s.%s f, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND f.patient_num = c.patient_num",
                           fact_cols,
                           i2b2DataSchema,
                           "observation_fact",
                           i2b2DataSchema,
                           set_table)
          fact <- makePreparedQuery(conn)
          fact(query,idResult)
          
        }else if(identical(set_table,"qt_patient_enc_collection") == TRUE){
          query <- sprintf("SELECT  %s
                           FROM %s.%s f, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND f.patient_num = c.patient_num
                           AND f.encounter_num = c.encounter_num",
                           fact_cols,
                           i2b2DataSchema, 
                           "observation_fact",
                           i2b2DataSchema,
                           set_table)
          fact <- makePreparedQuery(conn)
          fact(query,idResult)
          
        }
      }else{
        print('No permit to execute this query !')
        
      }
    }else{
      print('Wrong user !')
      
    }
  }else{
    print('Thank you to reconnect !')
    
  }
}


#' visitCollection
#' 
#' From the result of the query i2b2, this function returns a list of visits matching.
#'
#' @param idResult Result number
#' @param idProject Projet ID
#' @param idSession Session ID
#' @param directoryConfig Config file directory
#' @param idUser User ID
#'
#' @return list of visits
#'
visitCollection <- function(idResult, idProject, idSession,directoryConfig, idUser){
  if(activeSession(conn,idProject, idSession,idResult,idUser) == TRUE){
    if(actifUser(conn, idSession, idUser) == TRUE){
      if(verifQueryOwner(conn, idSession, idProject, idResult, idUser) == TRUE){
          enc_cols <- buildColumnEncounter(conn,idProject,idSession,getColumnFile(directoryConfig,"encounterColumn.csv"))
          i2b2DataSchema <- getValueFromConfFile(file.path(directoryConfig,"connectionFile.cfg"),"i2b2DataSchema")
          set_table <- getSetTableFromIdResult(conn, idResult, getColumnFile(directoryConfig,"exportTable.csv"))
        if(identical(set_table,"qt_patient_enc_collection") == TRUE){
          query <- sprintf("SELECT  %s
                           FROM %s.%s v, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND v.patient_num = c.patient_num
                           AND v.encounter_num = c.encounter_num",
                           enc_cols,
                           i2b2DataSchema,
                           "visit_dimension",
                           i2b2DataSchema,
                           set_table)
          
          encounter <- makePreparedQuery(conn)
          encounter(query,idResult)
          
        }else if(identical(set_table,"qt_patient_set_collection") == TRUE){
          query <- sprintf("SELECT  %s
                           FROM %s.%s v, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND v.patient_num = c.patient_num",
                           enc_cols,
                           i2b2DataSchema,
                           "visit_dimension",
                           i2b2DataSchema,
                           set_table)
          
          encounter <- makePreparedQuery(conn)
          encounter(query,idResult)
          
        }
      }else{
        print('No permit is execute this query !')
        
      }
    }else{
      print('Wrong user !')
      
    }
  }else{
    print('Thank you to reconnect !')
    
  }
}



#' patientCollection
#' 
#' From the result of the query i2b2, this function returns a list of matching patients.   
#'
#' @param idResult Result number
#' @param idProject Projet ID
#' @param idSession Session ID
#' @param directoryConfig Config file directory
#' @param idUser User ID
#'
#' @return list of patients
#' 
patientCollection <- function(idResult, idProject, idSession, directoryConfig, idUser){
  if(activeSession(conn,idProject, idSession,idResult, idUser) == TRUE){
    if(actifUser(conn, idSession, idUser) == TRUE){
      if(verifQueryOwner(conn, idSession, idProject, idResult, idUser) == TRUE){
            set_table <- getSetTableFromIdResult(conn,idResult, getColumnFile(directoryConfig,"exportTable.csv"))
            i2b2DataSchema <- getValueFromConfFile(file.path(directoryConfig,"connectionFile.cfg"),"i2b2DataSchema")
            patient_cols <- buildColumnPatient(conn,idProject,idSession,getColumnFile(directoryConfig,"patientColumn.csv"))
          query <- sprintf("SELECT  %s
                           FROM %s.%s p, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND p.patient_num = c.patient_num",
                           patient_cols,
                           i2b2DataSchema,
                           "patient_dimension",
                           i2b2DataSchema,
                           set_table)
          patient <- makePreparedQuery(conn)
          patient(query,idResult)
          
              }else{
        print('No owner of the query!')
      }
    }else{
      print('Wrong user !')
    }
  }else{
    print('Thank you to reconnect !')
  }
  
}


#' exportData
#' 
#' Creates different CSV file
#' 
#' @param data Data type to export
#' @param file Name of CSV file
#'
exportData <- function(data,file){
write.table(data, file, sep=";", quote=T, na="", fileEncoding="latin1", col.names=T, row.names=F)
}

#' downloadAsCsv
#' 
#' Exporte les données sous forme de fichier CSV.
#' This function creates files by each type of data to export.
#' 
#' @param idUser User ID
#' @param idResult Result number
#' @param idProject Projet ID
#' @param idSession Session ID
#' @param researchGoal Study goal
#' @param exportData data type to export (1: , 2: patients et visits, 3: patients, visits and encounters)
#' @param directoryConfig Config file directory
#' 
#' @export
#'
downloadAsCsv <- function(idUser, idResult, idProject, idSession, researchGoal, exportData, directoryConfig){
  out <- tryCatch({
    conn <<- dbConnexion(directoryConfig)
    
    if(exportData == 1){
      patient <- patientCollection(idResult,idProject,idSession,directoryConfig,idUser)
      exportData(patient, file = "patient.csv")
      
    }else if(exportData == 2){
      patient <- patientCollection(idResult,idProject,idSession,directoryConfig,idUser)
      encounter <- visitCollection(idResult,idProject,idSession,directoryConfig,idUser)
      
      exportData(patient, file = "patient.csv")
      exportData(encounter, file = "encounter.csv")
      
    }else if(exportData == 3){
      patient <- patientCollection(idResult,idProject,idSession,directoryConfig,idUser)
      fact <- factCollection(idResult,idProject,idSession,directoryConfig,idUser)
      encounter <- visitCollection(idResult,idProject,idSession,directoryConfig,idUser)
      
      exportData(patient, file = "patient.csv")
      exportData(encounter, file = "encounter.csv")
      exportData(fact, file = "observation.csv")
      
    }
  },error = function(err){
    print(err)
    
  }, finally={
    query <- sprintf("INSERT INTO i2b2pm.pm_rplugin 
                     (user_id,session_id,project_id,query_instance_id,result_instance_id,user_role_cd,rplugin_action,rplugin_status,rplugin_research_goal,rplugin_date,rplugin_export_type)
                     VALUES ('%s','%s','%s',%d, %d,'%s','%s','ok','%s',Now(),'%s')",idUser,idSession,idProject,getIdQuery(conn,idResult),idResult,paste0(getRole(conn,idProject,idSession),collapse=";"),getAction(),researchGoal,exportData)
    
    send <- RPostgres::dbSendQuery(conn, query)
    res <- RPostgres::dbFetch(send)
    RPostgres::dbClearResult(send)
    dbDisconnection(conn)
  })
}
