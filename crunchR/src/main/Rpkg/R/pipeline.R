
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

#' Run MR pipeline.
#' 
#' MRPipeline run
#' 
MRPipeline.run <- function () { 
	'Runs MR pipeline.
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
crunchR.Pipeline <- setRefClass("Pipeline", fields = c("jobj"),
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
#' @S3class PipelineResult
#' 
crunchR.PipelineResult <- setRefClass("PipelineResult",fields=c("jobj"),
		methods = list (
				initialize = function( jpipelineResult ) {
					stopifnot (is(jpipelineResult,"jobjRef") & 
									jpipelineResult %instanceof% crunchR$PipelineResultJClass)
					jobj <<- jpipelineResult
				}
		)
)

