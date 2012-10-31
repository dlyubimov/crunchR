#' crunchR package
#' 
#' @docType package
#' @name crunchR
#' @import RProtoBuf
#' 
NULL


##########################
# generic initialization #
##########################



.onLoad <- function (libname=NULL,pkgname=NULL) { 
	require(RProtoBuf)
	a <<- new.env()
}
