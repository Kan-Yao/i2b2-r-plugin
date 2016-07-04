#' i2b2ExportData
#' 
#' Dans ce package, vous trouverez des fonctions de connexion a la base de donnees, de requetage et de restitution de resultats de requete
#' 
#' @name i2b2ExportData
#' @docType package
NULL

#' dbConnexion
#' 
#' Fonction de connexion à la base de donnees.
#' Cette fonction se connecte à l'aide du driver de la base de donnees a interroger, du nom de la base, de l'adresse du hote, du numero de port
#' de l'identifiant utilisateur et du mot de passe.
#' Elle fait appel à la fonction dbConnect du package DBI.
#' 
#' @param connexionInfo datatable contenant tous les informations de connexion
#' @export
#' 


#'
#'
#'
getValueFromConfFile <- function(file, pattern){
  gsub(paste0(pattern,"="),"",grep(paste0("^",pattern,"="), scan(file,what="",quiet=T),value=T))
}


#' Fonction de connexion a la base de donnees
#' 
#' @param connexionInfo datatable contenant tous les informations de connexion
#' @export
#' 
dbConnexion <- function(connexionFile){
  con <- dbConnect(RPostgres::Postgres(), 
                   dbname = getValueFromConfFile(connexionFile,"dbname"),
                   host = getValueFromConfFile(connexionFile,"host"),
                   port = getValueFromConfFile(connexionFile,"port"),
                   user = getValueFromConfFile(connexionFile,"user"),
                   password = getValueFromConfFile(connexionFile,"password"))
  
}



#' Fonction de deconnexion a la base de donnees
#'
#' @param connexion en cours
#'
dbDisconnection <- function(conn){
  dbDisconnect(conn)
}


#' 
#'
#' 
#' 
makePreparedQuery <- function(conn) {
  function(statement, ...) {
    params = list(...)
    
    rs <- dbSendQuery(conn, statement)
    on.exit(dbClearResult(rs))
    
    if (length(params) > 0) {
      dbBind(rs, params)
    }
    
    df <- dbFetch(rs, n = -1, ...)
    if (!dbHasCompleted(rs)) {
      warning("Pending rows", call. = FALSE)
    }
    
    df
  }
}

#' verifIdProject
#'
#'Verifie si l'identifiant du projet est valide.
#'
#'@param conn information de connexion
#'@param idProjet L'idProjet a verifier
#'@return True si l'idProjet existe
#'@export
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
    q()
  },finally = {
    r <- strtoi(verifId(query,idProject),base=0L) == 1
  }
  )
}


#' verifIdSession
#'
#'Verifie si l'identifiant du projet est valide.
#'
#'@param conn information de connexion
#'@param idSession L'idSession a verifier
#'@return True si l'idSession existe
#'@export
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
    q()
  },finally = {
    return(strtoi(verifSession(query,idSession),base=0L) == 1)
  }
  )
}


#' verifIdResult
#'
#'Verifie si l'identifiant du projet est valide.
#'
#'@param conn information de connexion
#'@param idResult L'idResult a verifier
#'@return True si l'idResult existe
#'@export
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
    q()
  },finally = {
    return(strtoi(verifResult(query,idResult),base=0L) == 1)
  }
  )
}


#' getEndSessionDate
#' 
#' Permet de recuperer la date de la fin de session de l'utilisateur courant.
#' Elle fait appel aux differentes fonctions de verification : verifIdSession(...), verifIdProject(...), verifIdResult(...)
#' 
#' 
#' @param idProject identifiant du projet 
#' @param idSession identifiant de la session de l'utilisateur
#' @return renvoie la date de la fin de session de l'utilisateur   
#' @export                                                               
#' 
getEndSessionDate <- function(conn, idProject, idSession,idResult){
  if(verifQueryOwner(conn, idSession, idProject, idResult) == TRUE){
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
          q()
        },finally = {
        }
        )
      }
    }
  }else{
    print("Pas autoriser a executer cette requete !")
    q()
  }
}


#' nameQuizTable
#' 
#' cette fonction permet de recuperer le nom de la table a interroger, pour l'extraction des donnees.
#' Elle fait appel a la fonction verifIdResult(...).
#' 
#' @param conn Information de connexion
#' @param idResult Numero du resultat
#' @param exportTable Numero de la table a interroger
#' @return le nom de la table a interroger 
#' @export
#' 
nameQuizTable <- function(conn,idResult, exportTable){
  if(verifIdResult(conn,idResult) == TRUE){
    out <- tryCatch({
      query <- "SELECT x.result_type_id typeResult
      FROM i2b2data_multi_nomi.qt_query_result_instance x, i2b2data_multi_nomi.qt_query_result_type y
      WHERE x.result_instance_id = ($1::numeric)
      AND x.result_type_id = y.result_type_id"
      
      quizTable <- makePreparedQuery(conn)
      quizTable(query,idResult)
      
      return(exportTable[exportTable$V1 %in% quizTable(query,idResult),]$V2)
      
    },error = function(err){
      print(err)
      q()
    },finally = {
      
    }
    )
  }
}


#' activeSession
#' 
#' Verifie que la session de l'utilisateur est bien active.
#' Elle fait appel a la fonction getEndSessionDate(...) pour la comparaison avec la date courante.
#' 
#' @param idProject identifiant du projet 
#' @param idSession identifiant de la session de l'utilisateur
#' @return TRUE if session active ou FALSE si session inactive
#' @export
#' 
activeSession <- function(conn,idProject, idSession,idResult){
  dat <- getEndSessionDate(conn,idProject, idSession,idResult)
  #curentDate <- format(Sys.time(), "%Y-%m-%d %H:%M:%S");
  curentDate <-'2015-09-29 13:15:58'
  endSession <- dat[1,"expired_date"]
  
  if(curentDate > endSession){
    print("session perdu")
    q()
    
  }else{
    return(TRUE)
  }
  
}



#' actifUser
#' 
#' Permet de verifier que l'utilisateur est bien actif et donc autorise a exporter des donnees de la base.
#' Elle fait appel a la fonction verifIdSession(...), et verifie dans la table des usager, si l'utilisateur est actif.
#' 
#' @param idSession identifiant de la session de l'utilisateur 
#' @return True si l'utilisateur est actif et False sinon     
#' @export  
#'                                            
actifUser <- function(conn, idSession){
  if(verifIdSession(conn,idSession)== TRUE){
    out <- tryCatch({
      query <-"SELECT DISTINCT x.status_cd 
      FROM i2b2pm.pm_user_data x, i2b2pm.pm_user_session y
      WHERE y.session_id = ($1::varchar)"
      
      userActif <- makePreparedQuery(conn)
      
      if(identical(paste(userActif(query,idSession)),"A")){
        return(TRUE)
        
      }else{
        print("user pas actif")
        q()
      }
    },error = function(err){
      print(err)
      q()
    },finally = {
      
    }
    )
  }
}


#' verifQueryOwner
#' 
#' Cette fonction permet de verifier que l'utilisateur courant est bien l'auteur de la requete.
#' Avant cette verification, cette fonction fait appel aux differentes fonctions de verification : verifIdSession(...), verifIdProject(...). verifIdResult(...)
#' 
#' @param idSession identifiant de la session de l'utilisateur                                                                    
#' @param idProject identifiant du projet
#' @param idResult Numéro du résultat
#' @return True si l'utilisateur est le proprietaire de la requete
#' @export
#' 
verifQueryOwner <- function(conn, idSession, idProject, idResult){
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
          AND x.group_id = ($3::varchar)"
          
          queryOwner <- makePreparedQuery(conn)
          return(queryOwner(query,idSession,idResult,idProject)$count == 1)
          
        },error = function(err){
          print(err)
          q()
        },finally = {
          
        }
        )
        
      }
    }
  }
}


#' checkRole
#' 
#' Fonction qui determine l'autorisation dont jouit l'utilisateur.                  
#' De cette autorisation dependra la liste des colonnes a afficher au moment de l'export des donnees.
#' 
#' 
#' @param idProject identifiant du projet
#' @param idSession identifiant de la session de l'utilisateur  
#' @return 0 lorsque l'utilisateur n'est pas autorisé a visualiser l'ensemble des champs et 1 sinon.   
#' @export
#' 
checkRole <- function(conn,idProject,idSession){
  if(verifIdSession(conn,idSession)== TRUE){
    if(verifIdProject(conn,idProject) == TRUE){
      out <- tryCatch({
        query <- "WITH tmp AS ( 
        SELECT DISTINCT x.user_id, x.user_role_cd, project_id, y.session_id, y.expired_date 
        FROM i2b2pm.pm_project_user_roles x, i2b2pm.pm_user_session y
        WHERE x.user_id=y.user_id 
        and y.session_id=($1::varchar)
        and project_id=($2::varchar)
      )
        SELECT count(*) FROM tmp 
        WHERE tmp.user_role_cd LIKE \'DATA_DEID' OR
        tmp.user_role_cd LIKE \'DATA_PROT' 
        LIMIT 1"
        
        role <- makePreparedQuery(conn)
        
        return(role(query,idSession,idProject))
      },error = function(err){
        print(err)
        q()
      },finally = {
        
      }
      )
    }
  }
}


#' buildColonneVisite
#'
#' Fonction qui construit les colonnes a afficher lors de l'export de la liste des visites des patients
#'                                                                        
#' @param idProject identifiant du projet
#' @param idSession identifiant de la session de l'utilisateur
#' @param colonneVisite Liste des champs a afficher
#' @return La liste des champs a afficher
#' @export
#' 
buildColonneVisite <- function(con,idProject,idSession,colonneVisite){
  role <- strtoi(checkRole(con,idProject,idSession),base=0L)
  
  paste0(sprintf("%s AS %s",ifelse(role>=colonneVisite$Anon,paste0("v.",colonneVisite$Colonne),"'[protected]'"),colonneVisite$Alias),collapse=", ")
  
}


#' buildColonneFait
#'
#' Fonction qui construit les colonnes a afficher lors de l'export des faits   
#'     
#' @param idProject identifiant du projet
#' @param idSession identifiant de la session de l'utilisateur
#' @param colonneFait Liste des champs a afficher
#' @return La liste des champs a afficher
#' @export      
#'                                                               
buildColonneFait <- function(con,idProject,idSession,colonneFait){
  role <- strtoi(checkRole(con,idProject,idSession),base=0L)
  
  paste0(sprintf("%s AS %s",ifelse(role>=colonneFait$Anon,paste0("f.",colonneFait$Colonne),"'[protected]'"),colonneFait$Alias),collapse=", ")
  
}


#' buildColonnePatient
#' 
#' Fonction qui construit les colonnes a afficher lors de l'export des patients 
#' 
#' @param idProject identifiant du projet
#' @param idSession identifiant de la session de l'utilisateur
#' @param colonnePatient Liste des champs a afficher
#' @return La liste des champs a afficher 
#' @export
#' 
buildColonnePatient <- function(con,idProject,idSession,colonnePatient){
  role <- strtoi(checkRole(con,idProject,idSession),base=0L)
  
  paste0(sprintf("%s AS %s",ifelse(role>=colonnePatient$Anon,paste0("p.",colonnePatient$Colonne),"'[protected]'"),colonnePatient$Alias),collapse=", ")
  
}



#' Fonction qui retourne le numero de la requete faite par l'utilisateur.
#' 
#' @param con Informations de connexion a la base de donnees
#' @param idResult
#' @return idQuery
#' @export
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
      q()
    },finally = {
      
    }
    )
    
  }
}


#' Fonction qui retourne le user_id
#' 
#' @param con
#' @param idProject
#' @return user_id
#' @export
#' 
getIdUser <- function(conn, idSession){
  if(verifIdSession(conn,idSession) == TRUE){
    out <- tryCatch({
      query <- "SELECT user_id 
      FROM i2b2pm.pm_user_session x
      WHERE x.session_id=($1::varchar)"
      
      idUser <- makePreparedQuery(conn)
      return(paste(idUser(query,idSession)))
      
    },error = function(err){
      print(err)
      q()
    },finally = {
      
    }
    )
  }
}



#' Verifie les droits de l'utilisateur
#' 
#' @param con Informations de connexion a la base de donnees
#' @param idProject identifiant du projet
#' @param idSession identifiant de la session de l'utilisateur  
#' 
getRoleUser <- function(conn,idProject,idSession){
  role <- strtoi(checkRole(conn,idProject,idSession),base=0L)
  if(role > 0){
    return("De-identified Data")
  }else{
    return("Limited Data Set")
  }
}



#' Retourne le type d'application utilise
#' 
#' 
getAction <- function(){
  return("Export")
}




#'factCollection
#'
#' Cette fonction interroge la base de données et retourne la liste des faits realisee durant le sejour du patient.
#'
#'@param idResult Numero du resultat
#'@param idProject Ndentifiant du projet
#'@param idSession Identifiant de la session
#'@return La liste  des sejours correspondant
#'@export
#'
factCollection <- function(idResult, idProject, idSession,connexionFile){
  #dbConnexion
  conn <- dbConnexion(connexionFile)
  
  if(activeSession(conn, idProject, idSession,idResult) == TRUE){
    print('Session active !')
    if(actifUser(conn, idSession) == TRUE){
      print('User actif')
      if(verifQueryOwner(conn, idSession, idProject, idResult) == TRUE){
        if(identical(nameQuizTable(conn, idResult,exportTable),"qt_patient_enc_collection") == TRUE){
          query <- sprintf("SELECT DISTINCT %s
                           FROM %s.%s f, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND f.patient_num = c.patient_num
                           AND f.encounter_num = c.encounter_num",
                           buildColonneFait(conn,idProject,idSession,colonneFait),
                           "i2b2data_multi_nomi",
                           "observation_fact",
                           "i2b2data_multi_nomi",
                           nameQuizTable(conn, idResult, exportTable))
          
          fact <- makePreparedQuery(conn)
          fact(query,idResult)
          
        }else if(identical(nameQuizTable(conn, idResult,exportTable),"qt_patient_set_collection") == TRUE){
          query <- sprintf("SELECT DISTINCT %s
                           FROM %s.%s f, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND f.patient_num = c.patient_num",
                           buildColonneFait(conn,idProject,idSession,colonneFait),
                           "i2b2data_multi_nomi",
                           "observation_fact",
                           "i2b2data_multi_nomi",
                           nameQuizTable(conn, idResult, exportTable))
          
          fact <- makePreparedQuery(conn)
          fact(query,idResult)
          
        }
      }else{
        print('Pas autoriser a executer cette requete !')
        q()
      }
    }else{
      print('User non valide !')
      q()
    }
  }else{
    print('Merci de vous reconnecter !')
    q()
  }
}


#' visitCollection
#' 
#' A partir du numero du resultat de la requete d'i2b2, cette fonction renvoie la liste des visites correspondant.                           
#'                                                                        
#'@param idResult Numero du resultat
#'@param idProject Ndentifiant du projet
#'@param idSession Identifiant de la session
#'@return La liste  des sejours correspondant
#'@export
#'
visitCollection <- function(idResult, idProject, idSession,connexionFile){
  #dbConnexion
  conn <- dbConnexion(connexionFile)
  
  if(activeSession(conn,idProject, idSession,idResult) == TRUE){
    print('Session active !')
    if(actifUser(conn, idSession) == TRUE){
      print('User actif')
      if(verifQueryOwner(conn, idSession, idProject, idResult) == TRUE){
        if(identical(nameQuizTable(conn, idResult,exportTable),"qt_patient_enc_collection") == TRUE){
          query <- sprintf("SELECT DISTINCT %s
                           FROM %s.%s v, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND v.patient_num = c.patient_num
                           AND v.encounter_num = c.encounter_num",
                           buildColonneVisite(conn,idProject,idSession,colonneVisite),
                           "i2b2data_multi_nomi",
                           "visit_dimension",
                           "i2b2data_multi_nomi",
                           nameQuizTable(conn, idResult, exportTable))
          
          visite <- makePreparedQuery(conn)
          visite(query,idResult)
          
        }else if(identical(nameQuizTable(conn, idResult,exportTable),"qt_patient_set_collection") == TRUE){
          query <- sprintf("SELECT DISTINCT %s
                           FROM %s.%s v, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND v.patient_num = c.patient_num",
                           buildColonneVisite(conn,idProject,idSession,colonneVisite),
                           "i2b2data_multi_nomi",
                           "visit_dimension",
                           "i2b2data_multi_nomi",
                           nameQuizTable(conn, idResult, exportTable))
          
          visite <- makePreparedQuery(conn)
          visite(query,idResult)
          
        }
      }else{
        print('Pas autoriser a executer cette requete !')
        q()
      }
    }else{
      print('User non valide !')
      q()
    }
  }else{
    print('Merci de vous reconnecter !')
    q()
  }
}



#' patientCollection
#' 
#' A partir du numero du resultat de la requete d'i2b2, cette fonction renvoie la liste des patients correspondant.    
#'
#'@param idResult Numero du resultat
#'@param idProject Identifiant du projet
#'@param idSession Identifiant de la session
#'@return La liste  des sejours correspondant
#'@export
#'
patientCollection <- function(idResult, idProject, idSession,connexionFile){
  #dbConnexion
  
  if(activeSession(conn,idProject, idSession,idResult) == TRUE){
    print('Session active !')
    if(actifUser(conn, idSession) == TRUE){
      print('User actif')
      if(verifQueryOwner(conn, idSession, idProject, idResult) == TRUE){
        if(identical(nameQuizTable(conn,idResult,exportTable),"qt_patient_enc_collection") == TRUE){
          query <- sprintf("SELECT DISTINCT %s
                           FROM %s.%s p, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND p.patient_num = c.patient_num",
                           buildColonnePatient(conn,idProject,idSession,colonnePatient),
                           "i2b2data_multi_nomi",
                           "patient_dimension",
                           "i2b2data_multi_nomi",
                           nameQuizTable(conn,idResult, exportTable))
          
          patient <- makePreparedQuery(conn)
          patient(query,idResult)
          
        }else if(identical(nameQuizTable(conn,idResult,exportTable),"qt_patient_set_collection") == TRUE){
          query <- sprintf("SELECT DISTINCT %s
                           FROM %s.%s p, %s.%s c
                           WHERE c.result_instance_id = ($1::numeric)
                           AND p.patient_num = c.patient_num",
                           buildColonnePatient(conn,idProject,idSession,colonnePatient),
                           "i2b2data_multi_nomi",
                           "patient_dimension",
                           "i2b2data_multi_nomi",
                           nameQuizTable(conn,idResult, exportTable))
          
          patient <- makePreparedQuery(conn)
          patient(query,idResult)
          
        }
      }else{
        print('Pas proprietaire de la requete !')
        q()
      }
    }else{
      print('User non valide !')
      q()
    }
  }else{
    print('Merci de vous reconnecter !')
    q()
  }
  
}


# Creation des datatables en fonction du type de donnees a exporter

#' getDataTable
#'
#' @export
#'
getDataTable <- function(idResult,idProject,idSession,researchGoal,exportData,connexionFile){
  out <- tryCatch({
    conn <<- dbConnexion(connexionFile)
    
    if(exportData == 1){
      patient <- patientCollection(idResult,idProject,idSession,connexionFile)
      
      write.csv2(patient, file = "patientCollection.csv")
      
    }else if(exportData == 2){
      patient <- patientCollection(idResult,idProject,idSession,connexionFile)
      fait <- factCollection(idResult,idProject,idSession,connexionFile)
      
      write.csv2(patient, file = "patientCollection.csv")
      write.csv2(fait, file = "observationfactCollection.csv")
      
    }else if(exportData == 3){
      patient <- patientCollection(idResult,idProject,idSession,connexionFile)
      fait <- factCollection(idResult,idProject,idSession,connexionFile)
      visite <- visitCollection(idResult,idProject,idSession,connexionFile)
      
      write.csv2(patient, file = "patientCollection.csv")
      write.csv2(fait, file = "observationfactCollection.csv")
      write.csv2(visite, file = "visitCollection.csv")
      
    }
  },error = function(err){
    print(err)
    q()
  }, finally={
    query <- sprintf("INSERT INTO i2b2pm.pm_rplugin 
                     (user_id,session_id,project_id,query_instance_id,result_instance_id,user_role_cd,rplugin_action,rplugin_status,rplugin_research_goal,rplugin_date,rplugin_export_type)
                     VALUES ('%s','%s','%s',%d, %d,'%s','%s','ok','%s',(SELECT CURRENT_TIMESTAMP),'%s')",getIdUser(conn,idSession),idSession,idProject,getIdQuery(conn,idResult),idResult,getRoleUser(conn,idProject,idSession),getAction(),researchGoal,paste0("_",exportData,"_",collapse =""))
    
    send <- dbSendQuery(conn, query)
    res <- dbFetch(send, n = -1)
    # dbDisconnect(conn)
  })
}