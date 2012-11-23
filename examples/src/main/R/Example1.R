
wordCountExample <- function() { 
	
	# this is WIP wordcount example in R, crunchR
	
	library(crunchR)
	
	pipeline <- crunchR.MRPipeline$new("test-pipeline")
	
	inputPCol <- pipeline$readTextFile("/crunchr-examples/input")

#   the following locks up. TODO: figure how it locks up, probably during 
#   cleanup. 
	
#	wordsPCol <- inputPCol$parallelDo(	
#			function(line) emit( strsplit(tolower(line),"[^[:alnum:]]+")[[1]] )
#	)
#	
#	wordsPTab <- wordsPCol$parallelDo(function(word) emit(word,1),
#			keyType = crunchR.RString$new(),
#			valueType = crunchR.RUint32$new())
	
#   this works:
	wordsPTab <- inputPCol$parallelDo(	
			function(line) { 
				words<- strsplit(tolower(line),"[^[:alnum:]]+")[[1]]
				sapply(words, function(x) emit(x,1))
			},
			keyType = crunchR.RString$new(),
			valueType = crunchR.RUint32$new()
	)
	
	wordsPTab$writeTextFile("/crunchr-examples/output")
	
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

#ptable <- crunchR.RPTableType$new()