% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/functions.R
\name{nameQuizTable}
\alias{nameQuizTable}
\title{nameQuizTable}
\usage{
nameQuizTable(conn, idResult, exportTable)
}
\arguments{
\item{conn}{Information de connexion}

\item{idResult}{Numero du resultat}

\item{exportTable}{Numero de la table a interroger}
}
\value{
le nom de la table a interroger
}
\description{
cette fonction permet de recuperer le nom de la table a interroger, pour l'extraction des donnees.
Elle fait appel a la fonction verifIdResult(...).
}

