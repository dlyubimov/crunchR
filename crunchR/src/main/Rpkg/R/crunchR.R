#' crunchR package
#' 
#' @docType package
#' @name crunchR
#' @exportPattern "^crunchR\\."
#' @import rJava
#' 
NULL


##########################
# generic initialization #
##########################

.onLoad <- function (libname=NULL,pkgname=NULL) .crunchR.init(libname,pkgname, pkgInit=T)
.onUnload <- function(libpath) rm(crunchR) 

.crunchR.init <- function(libname=NULL, pkgname=NULL, pkgInit = F) {
	
	library(rJava)
#	require(RProtoBuf)
	
	if ( length(pkgname) == 0 ) pkgname <- "crunchR"
	
	crunchR <- new.env()
	
	hadoopcp <- crunchR.hadoopClassPath()
	crunchR$hadoopcp <- hadoopcp
	
	if ( pkgInit ) {
		options(error=quote(dump.frames("errframes", F)))
		
		.jpackage(pkgname, morePaths = hadoopcp, lib.loc = libname)
		cp <- list.files(system.file("java",package=pkgname,lib.loc=libname),
				full.names=T, pattern ="\\.jar$")
		crunchR$cp <- cp
	} else {
		# DEBUG mode: package not installed.
		# look files in a maven project tree 
		# denoted by RCRUNCH_HOME
		rcrunchHome <- Sys.getenv("RCRUNCH_HOME")
		if ( nchar(rcrunchHome)==0 )
			stop ("for initializing from maven tree, set RCRUNCH_HOME variable.")
		
		libdir <- file.path ( rcrunchHome, "target")
		pkgdir <- list.files(libdir, pattern= "^crunchR-.*-rpkg$", full.names=T)
		cp <- c ( list.files( file.path(pkgdir, "inst", "java"),pattern="\\.jar$",full.names=T),
				file.path(libdir,"test-classes"))
		.jinit(classpath = c(hadoopcp,cp))
		
		crunchR$cp <- cp
	}
	
	# make sure all classpath entries exists, 
	# it may cause problems later.
	crunchR$cp <- crunchR$cp[file.exists(crunchR$cp)]
	
	crunchR <<- crunchR
	
}


#' hadoop class path
#' 	
#' @description Find local hadoop client and return classpath
#' 
#' @return a character vector containing hadoop classpath.
#' 
#' @author dmitriy
crunchR.hadoopClassPath <- function () {
	tryCatch({
				cp <- strsplit(system("hadoop classpath", intern=T),.Platform$path.sep)[[1]]
				cp <- cp[file.exists(cp)]
				cp
			}, error = function (e) {
				cat(as.character(e))
				warning("Hadoop classpath cannot be established")
				character(0)
			})
}


