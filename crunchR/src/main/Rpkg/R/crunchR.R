#' crunchR package
#' 
#' @docType package
#' @name crunchR
#' @exportPattern "^crunchR\\."
#' @import rJava
#' @import RProtoBuf
#' 
#' @include zzzClasses.R
#' @include Pipeline.R
#' @include TwoWayPipe.R
#' @include DoFn.R
#' @include IO.R
#' @include SerializationHelper.R
#' 
NULL


##########################
# generic initialization #
##########################

.onLoad <- function (libname=NULL,pkgname=NULL) .crunchR.init(libname,pkgname, pkgInit=T)
.onUnload <- function(libpath) rm(crunchR) 

.crunchR <- new.env(parent=emptyenv())

.crunchR.init <- function(libname=NULL, pkgname=NULL, pkgInit = F) {
	
	library(rJava)
	require(RProtoBuf)
	
	if ( length(pkgname) == 0 ) pkgname <- "crunchR"
	
	
	hadoopcp <- crunchR.hadoopClassPath()
	
	if ( pkgInit ) {
		options(error=quote(dump.frames("errframes", F)))
		
		.jpackage(pkgname, morePaths = hadoopcp, lib.loc = libname)
		cp <- list.files(system.file("java",package=pkgname,lib.loc=libname),
				full.names=T, pattern ="\\.jar$")
		.crunchR$cp <- cp
		jobJar <- list.files(system.file("hadoop-job", package=pkgname, lib.loc=libname), full.names=T, pattern="^crunchR-.*-hadoop-job.jar$")
		
		if (length(jobJar)!=1) stop ("cannot find hadoop job jar")
		
	} else {
		# DEBUG mode: package not installed.
		# look files in a maven project tree 
		# denoted by RCRUNCH_HOME
		rcrunchHome <- Sys.getenv("RCRUNCH_HOME","~/projects/github/crunchR/crunchR")
		if ( nchar(rcrunchHome)==0 )
			stop ("for initializing from maven tree, set RCRUNCH_HOME variable.")
		
		libdir <- file.path ( rcrunchHome, "target")
		pkgdir <- list.files(libdir, pattern= "^crunchR-.*-rpkg$", full.names=T)
		cp <- c ( list.files( file.path(pkgdir, "inst", "java"),pattern="\\.jar$",full.names=T),
				file.path(libdir,"test-classes"))
		.jinit(classpath = c(hadoopcp,cp))
		
		.crunchR$cp <- cp
		
		jobJar <- list.files(libdir, pattern="^crunchR-.*-hadoop-job.jar$")
	}
	
	# make sure all classpath entries exists, 
	# it may cause problems later.
	.crunchR$hadoopcp <- hadoopcp
	.crunchR$cp <- .crunchR$cp[file.exists(.crunchR$cp)]
	.crunchR$jobJar <- jobJar[1]

	.crunchR$PipelineJClass <- J("org/apache/crunch/Pipeline")
	.crunchR$PipelineResultJClass <- J("org/apache/crunch/PipelineResult")
	.crunchR$MRPipelineJClass <- J("org/apache/crunch/impl/mr/MRPipeline")
	.crunchR$DistCacheJClass <- J("org/apache/crunch/util/DistCache")
	.crunchR$TextFileTargetJClass <- J("org/apache/crunch/io/text/TextFileTarget")
	.crunchR$FileJClass <- J("java/io/File")
	.crunchR$RDoFnJClass <- J("org/crunchr/fn/RDoFn")
	
	#finding job jar 
#	crunchR <<- crunchR
	
	# init inclusions 
	.pipeline.init(pkgname)
	
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


