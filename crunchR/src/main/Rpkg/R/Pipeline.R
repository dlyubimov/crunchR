
.pipeline.init <- function(pkgname) {
}

Pipeline.initialize <- function (jpipeline = NULL) {
	
	if ( !is.null(jpipeline))  {
		stopifnot ( is(jpipeline,"jobjRef") & 
						jpipeline %instanceof% .crunchR$PipelineJClass )
		jobj <<- jpipeline
	}
}

Pipeline.getConfiguration <- function () {
	.jcall(jobj,"Lorg/apache/hadoop/conf/Configuration;","getConfiguration")
}

MRPipeline.initialize <- function (name = "crunchR-pipe") {
	jp <- new(.crunchR$MRPipelineJClass, .crunchR$MRPipelineJClass@jobj, name )
	callSuper(jpipeline=jp)
}


#' @title Run MR pipeline.
#' 
#' @description MRPipeline run
#' 
#' @details
#' 
#' This calls Crunch's MRPipeline#run() call. 
#' it also makes sure that all rcrunchR jars are added to the job 
#' using DistCache class. 
#' 
#' 
MRPipeline.run <- function () { 
	'Runs MR pipeline	
	' 
	
	.equipWithJobJars(.self)
	jpr <- .jcall(jobj,"Lorg/apache/crunch/PipelineResult;","run")
	crunchR.PipelineResult$new (jpr)
}

.equipWithJobJars <- function (mrPipeline ) {
	jConf <- mrPipeline$getConfiguration()
	stopifnot (!is.null(jConf))
	
	# we also need standard rJava/jri stuff here. Mostly, jri.
	libFiles <- c(.crunchR$cp,
			list.files(system.file("jri",package="rJava"),pattern="\\.jar$", full.names=T))
	
	sapply(libFiles,function (jarPath) {
				file <- new( .crunchR$FileJClass, jarPath )
				.jcall(.crunchR$DistCacheJClass,"V","addJarToDistributedCache", jConf, file)
			})	
	NULL
}		




