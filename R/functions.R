#' i2b2ExportData
#' 
#' Dans ce package, vous trouverez des fonctions pour l'export des données des requêtes 
#' 
#' @name i2b2ExportData
#' @docType package

#' dbConnexion
#' 
#' Fonction de connexion à la base de donnees.
#' Cette fonction se connecte à l'aide du driver de la base de donnees a interroger, du nom de la base, de l'adresse du hote, du numero de port
#' de l'identifiant utilisateur et du mot de passe.
#' Elle fact appel à la fonction dbConnect du package DBI.
#' 
#' @param connexionInfo datatable contenant tous les informations de connexion
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
#' Fonction de deconnexion a la base de donnees
#'
#' @param connexion en cours
#'
dbDisconnection <- function(conn){
  RPostgres::dbDisconnect(conn)
}


#' getValueFromConfFile
#'
#' Fonction pour la lecture de fichier de connexion
#' 
#' @param file le chemin du fichier a lire
#' @param pattern le champ a lire
#' 
getValueFromConfFile <- function(file, pattern){
  gsub(paste0(pattern,"="),"",grep(paste0("^",pattern,"="), scan(file,what="",quiet=T),value=T))
}



#' makePreparedQuery
#'
#' Fonction pour les requetes preparees
#' 
#' @param conn informations de connexion
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
#'Verifie si l'identifiant du projet est valide.
#'
#'@param conn Information de connexion
#'@param idProjet L'idProjet a verifier
#'@return True si l'idProjet existe
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
#'Verifie si l'identifiant du projet est valide.
#'
#'@param conn Information de connexion
#'@param idSession L'idSession a verifier
#'@return True si l'idSession existe
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
#'Verifie si l'identifiant du projet est valide.
#'
#'@param conn Information de connexion
#'@param idResult L'idResult a verifier
#'@return True si l'idResult existe
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
#' Permet de recuperer la date de la fin de session de l'utilisateur courant.
#' Elle fact appel aux differentes fonctions de verification : verifIdSession(...), verifIdProject(...), verifIdResult(...)
#' 
#' @param idProject Identifiant du projet 
#' @param idSession Identifiant de la session de l'utilisateur
#' @return renvoie la date de la fin de session de l'utilisateur   
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
    print("Pas autoriser a executer cette requete !")
    
  }
}


#' getSetTableFromIdResult
#' 
#' cette fonction permet de recuperer le nom de la table a interroger, pour l'extraction des donnees.
#' Elle fact appel a la fonction verifIdResult(...).
#' 
#' @param conn Information de connexion
#' @param idResult Numero du resultat
#' @param exportTable Numero de la table a interroger
#' @return le nom de la table a interroger 
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
#' Verifie que la session de l'utilisateur est bien active.
#' Elle fact appel a la fonction getEndSessionDate(...) pour la comparaison avec la date courante.
#' 
#' @param idProject Identifiant du projet 
#' @param idSession Identifiant de la session de l'utilisateur
#' @return TRUE if session active ou FALSE si session inactive
#' 
activeSession <- function(conn,idProject, idSession,idResult, idUser){
  dat <- getEndSessionDate(conn,idProject, idSession,idResult, idUser)
  #curentDate <- format(Sys.time(), "%Y-%m-%d %H:%M:%S");
  curentDate <-'2015-09-29 13:15:58'
  endSession <- dat[1,"expired_date"]
  
  if(curentDate > endSession){
    print("session perdu")
    
    
  }else{
      print("session active")
    return(TRUE)
  }
  
}



#' actifUser
#' 
#' Permet de verifier que l'utilisateur est bien actif et donc autorise a exporter des donnees de la base.
#' Elle fact appel a la fonction verifIdSession(...), et verifie dans la table des usager, si l'utilisateur est actif.
#' 
#' @param idSession Identifiant de la session de l'utilisateur 
#' @return True si l'utilisateur est actif et False sinon     
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
          print("user active")
        return(TRUE)
        
      }else{
        print("user pas actif")
        
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
#' Cette fonction permet de verifier que l'utilisateur courant est bien l'auteur de la requete.
#' Avant cette verification, cette fonction fact appel aux differentes fonctions de verification : verifIdSession(...), verifIdProject(...). verifIdResult(...)
#' 
#' @param idSession Identifiant de la session de l'utilisateur                                                                    
#' @param idProject Identifiant du projet
#' @param idResult Numéro du résultat
#' @return True si l'utilisateur est le proprietaire de la requete
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
#' Fonction qui determine l'autorisation dont jouit l'utilisateur.                  
#' De cette autorisation dependra la liste des colonnes a afficher au moment de l'export des donnees.
#' 
#' 
#' @param idProject Identifiant du projet
#' @param idSession Identifiant de la session de l'utilisateur  
#' @return 0 lorsque l'utilisateur n'est pas autorisé a visualiser l'ensemble des champs et 1 sinon.   
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
#' Fonction qui construit les colonnes a afficher lors de l'export de la liste des encounters des patients
#'                                                                        
#' @param idProject identifiant du projet
#' @param idSession identifiant de la session de l'utilisateur
#' @param colonneEncounter Liste des champs a afficher
#' @return La liste des champs a afficher
#' 
buildColumnEncounter <- function(con,idProject,idSession,colonneEncounter){
  role <- getRole(con,idProject,idSession)
  paste0(sprintf("%s AS %s",ifelse(sapply(colonneEncounter$Profile,function(x){any(role%in%x)}),paste0("v.",colonneEncounter$Column),NULL_VALUE),colonneEncounter$Alias),collapse=", ")
}


#' buildColumnFact
#'
#' Fonction qui construit les colonnes a afficher lors de l'export des facts   
#'     
#' @param idProject Identifiant du projet
#' @param idSession Identifiant de la session de l'utilisateur
#' @param colonneFact Liste des champs a afficher
#' @return La liste des champs a afficher
#'                                                               
buildColumnFact <- function(con,idProject,idSession,colonneFact){
  role <- getRole(con,idProject,idSession)
  paste0(sprintf("%s AS %s",ifelse(sapply(colonneFact$Profile,function(x){any(role%in%x)}),paste0("f.",colonneFact$Column),NULL_VALUE),colonneFact$Alias),collapse=", ")
}


#' buildColumnPatient
#' 
#' Fonction qui construit les colonnes a afficher lors de l'export des patients 
#' 
#' @param idProject Identifiant du projet
#' @param idSession Identifiant de la session de l'utilisateur
#' @param colonnePatient Liste des champs a afficher
#' @return La liste des champs a afficher 
#' 
#' 
buildColumnPatient <- function(con,idProject,idSession,colonnePatient){
  role <- getRole(con,idProject,idSession)
  paste0(sprintf("%s AS %s",ifelse(sapply(colonnePatient$Profile,function(x){any(role%in%x)}),paste0("p.",colonnePatient$Column),NULL_VALUE),colonnePatient$Alias),collapse=", ")
}


#' getIdQuery
#' 
#' Fonction qui retourne le numero de la requete facte par l'utilisateur.
#' 
#' @param con Informations de connexion a la base de donnees
#' @param idResult Numero du resultat
#' @return idQuery Le numero de la requete
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
#' Retourne le type d'application utilise
#' 
#' 
getAction <- function(){
  return("Export")
}

#' getAction
#' 
#' Retourne le type d'application utilise
#' 
#' 
getColumnFile <- function(directoryConfig,file){
    read.csv2(file.path(directoryConfig,file), stringsAsFactors=FALSE)
}

#'factCollection
#'
#' Cette fonction interroge la base de données et retourne la liste des facts realisee durant le sejour du patient.
#'
#' @param idResult Numero du resultat
#' @param idProject Identifiant du projet
#' @param idSession Identifiant de la session
#' @param directoryConfig le chemin du fichier de connexion
#' @return La liste  des sejours correspondant
#' 
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
        print('Pas autoriser a executer cette requete !')
        
      }
    }else{
      print('User non valide !')
      
    }
  }else{
    print('Merci de vous reconnecter !')
    
  }
}


#' visitCollection
#' 
#' A partir du numero du resultat de la requete d'i2b2, cette fonction renvoie la liste des encounters correspondant.                           
#'                                                                        
#' @param idResult Numero du resultat
#' @param idProject Identifiant du projet
#' @param idSession Identifiant de la session
#' @param directoryConfig le chemin du fichier de connexion
#' @return La liste  des sejours correspondant
#' 
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
        print('Pas autoriser a executer cette requete !')
        
      }
    }else{
      print('User non valide !')
      
    }
  }else{
    print('Merci de vous reconnecter !')
    
  }
}



#' patientCollection
#' 
#' A partir du numero du resultat de la requete d'i2b2, cette fonction renvoie la liste des patients correspondant.    
#'
#' @param idResult Numero du resultat
#' @param idProject Identifiant du projet
#' @param idSession Identifiant de la session
#' @param directoryConfig le chemin du fichier de connexion
#' @return La liste des sejours correspondant
#' 
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
        print('Pas proprietaire de la requete !')
      }
    }else{
      print('User non valide !')
    }
  }else{
    print('Merci de vous reconnecter !')
  }
  
}


#' exportData
#' 
#' Creation des datatables en fonction du type de donnees a exporter et insertion dans la table de tracage des informations concernant le telechargement.
#' Cette fonction cree des fichiers selon chaque type de donnees a exporter.
#' 
#' @param idResult Numero du resultat
#' @param idProject Identifiant du projet
#' @param idSession IdSession Identifiant de la session
#' @param researchGoal But de l'etude
#' @param exportData Le type de donnees a exporter (1: Patients, 2: patients et facts, 3: patients, facts, et encounters)
#' @param directoryConfig le chemin du fichier de connexion
#' 
#'
#'
exportData <- function(data,file){
write.table(data, file, sep=";", quote=T, na="", fileEncoding="latin1", col.names=T, row.names=F)
}

#' downloadAsCsv
#' 
#' Creation des datatables en fonction du type de donnees a exporter et insertion dans la table de tracage des informations concernant le telechargement.
#' Cette fonction cree des fichiers selon chaque type de donnees a exporter.
#' 
#' @param idResult Numero du resultat
#' @param idProject Identifiant du projet
#' @param idSession IdSession Identifiant de la session
#' @param researchGoal But de l'etude
#' @param exportData Le type de donnees a exporter (1: Patients, 2: patients et facts, 3: patients, facts, et encounters)
#' @param directoryConfig le chemin du fichier de connexion
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
