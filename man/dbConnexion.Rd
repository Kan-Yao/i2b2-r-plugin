% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/functions.R
\name{dbConnexion}
\alias{dbConnexion}
\title{dbConnexion}
\usage{
dbConnexion(connexionFile)
}
\arguments{
\item{connexionInfo}{datatable contenant tous les informations de connexion}
}
\description{
Fonction de connexion à la base de donnees.
Cette fonction se connecte à l'aide du driver de la base de donnees a interroger, du nom de la base, de l'adresse du hote, du numero de port
de l'identifiant utilisateur et du mot de passe.
Elle fait appel à la fonction dbConnect du package DBI.
}

