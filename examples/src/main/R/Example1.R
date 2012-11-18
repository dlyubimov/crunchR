
wordCountExample <- function() { 
	
	# this is WIP wordcount example in R, crunchR
	
	library(crunchR)
	
	pipeline <- crunchR.MRPipeline$new("test-pipeline")
	
	inputPCol <- pipeline$readTextFile("/crunchr-examples/input")
	
	outputPCol <- inputPCol$parallelDo(	
			function(line) emit( strsplit(tolower(line),"[^[:alnum:]]")[[1]] )
	)
	
	outputPCol$writeTextFile("/crunchr-examples/output")
	
	result <- pipeline$run()
	
	if ( !result$succeeded() ) stop ("pipeline failed.")
	
}

wordCountExample()


#####################
### quick scratchpad
#rpipe <- crunchR.TwoWayPipe$new(new(J("java/lang/String")))
#doFn <- crunchR.DoFn$new(function(x) emit(x),customizeEnv=T)
#doFn$rpipe <- rpipe
#doFn$callProcess(c("A","B","C"))	
#
#line <- "this is a line"
#class(strsplit(tolower(line),"[^[:alnum:]]")[[1]])