
.pipeline.init <- function(pkgname) {
}

Pipeline.initialize <- function (jpipeline = NULL) {
	
	if ( !is.null(jpipeline))  {
		stopifnot ( is(jpipeline,"jobjRef") & 
						jpipeline %instanceof% crunchR$PipelineJClass )
		jobj <<- jpipeline
	}
}

Pipeline.getConfiguration <- function () {
	.jcall(jobj,"org/apache/hadoop/conf/Configuraton;","getConfiguration")
}

MRPipeline.initialize <- function (name = "crunchR-pipe") {
	jp <- new(crunchR$MRPipelineJClass, crunchR$MRPipelineJClass@jobj, name )
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
	jpr <- .jcall(jobj,"org/apache/crunch/PipelineResult;","run")
	crunchR.PipelineResult$new (jpr)
}

.equipWithJobJars <- function (mrPipeline ) {
	jConf <- mrPipeline$getConfiguration()
	stopifnot (!is.null(jConf))
	
	sapply(crunchR$cp,function (jarPath) {
				file <- new( crunchR$FileJClass, jarPath )
				.jcall(crunchR$DistCacheJClass,"addJarToDistributedCache", jConf, file)
			})	
	NULL
}		


#'
#' crunchR's pipeline R5 class wrapper
#' 
crunchR.Pipeline <- setRefClass("Pipeline", 
		fields = list( 
				jobj = "jobjRef"
		),
		methods = list(
				initialize = Pipeline.initialize,
				getConfiguration = Pipeline.getConfiguration
		)
)

#'
#' crunchR's MRPipeline R5 class wrapper
#' 
crunchR.MRPipeline <- setRefClass("MRPipeline", contains=crunchR.Pipeline,
		methods = list (
				initialize = MRPipeline.initialize,
				run = MRPipeline.run
		)
)

#'
#' PipelineResult R5 class
#' 
crunchR.PipelineResult <- setRefClass("PipelineResult",
		fields = list( 
				jobj = "jobjRef"
		),
		methods = list (
				initialize = function( jpipelineResult ) {
					stopifnot (is(jpipelineResult,"jobjRef") & 
									jpipelineResult %instanceof% crunchR$PipelineResultJClass)
					jobj <<- jpipelineResult
				}
		)
)

