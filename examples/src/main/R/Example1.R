
wordCountComplete <- function () {
	
	pipeline <- crunchR.MRPipeline$new("test-pipeline")
	inputPCol <- pipeline$readTextFile("/crunchr-examples/input")

	# this ptable contains pairs word, 1
	wordsPTab <- inputPCol$parallelDo(	
			function(line) { 
				words<- strsplit(tolower(line),"[^[:alnum:]]+")[[1]]
				sapply(words, function(x) emit(x,1))
			},
			keyType = crunchR.RString$new(),
			valueType = crunchR.RVarUint32$new()
	)

	groupedPTab <- wordsPTab$groupByKey()
	
	key <- NULL
	count <- 0
	
	wordCountsPTab <- groupedPTab$parallelDo(
			FUN_INIT_GROUP = function (key) { 
				key<<-key 
				count <<- 0 
			},
			FUN_PROCESS = function (values) count <<- count + sum(values),
			FUN_CLEANUP_GROUP = function() emit(key,count),
			keyType = crunchR.RString$new(),
			valueType = crunchR.RVarUint32$new()
	)
	
	wordCountsPTab$writeTextFile("/crunchr-examples/output")
	
	result <- pipeline$run()
	if ( !result$succeeded() ) stop ("pipeline failed.")
	
}

library(crunchR)

wordCountComplete()

# end of wordCount example


################################################
# same as worldCountExample1 but decomposing   
# mapper task into 2 do functions.
################################################

wordCountExample2 <- function() { 
	pipeline <- crunchR.MRPipeline$new("test-pipeline")
	inputPCol <- pipeline$readTextFile("/crunchr-examples/input")
	
	
	# Currently, R part is agnostic of doFn() merging done by crunch; 
	# so even that these two functions can be merged on the R side,
	# right now it dispatches emit() of the first function back to java 
	# and crunch and then Crunch feeds it back to the R side. Which results 
	# intermediate output being serialized twice in addition to just task total 
	# input/output serialization. My hope is that in the future we might hack 
	# into crunch optimizer enough to figure that certain doFn emitters 
	# could be short-circuited to process() calls of other doFn's without 
	# sending the data thru java/Crunch side.
	
	
	wordsPCol <- inputPCol$parallelDo(	
			function(line) emit( strsplit(tolower(line),"[^[:alnum:]]+")[[1]] )
	)
	
	wordsPTab <- wordsPCol$parallelDo(function(word) emit(word,1),
			keyType = crunchR.RString$new(),
			valueType = crunchR.RVarUint32$new())
	
	groupedPTab <- wordsPTab$groupByKey()
	
	key <- NULL
	count <- 0
	
	wordCountsPTab <- groupedPTab$parallelDo(
			FUN_INIT_GROUP = function (key) { 
				key<<-key 
				count <<- 0 
			},
			FUN_PROCESS = function (values) count <<- count + sum(values),
			FUN_CLEANUP_GROUP = function() emit(key,count),
			keyType = crunchR.RString$new(),
			valueType = crunchR.RVarUint32$new()
	)
	
	wordCountsPTab$writeTextFile("/crunchr-examples/output")
	
	result <- pipeline$run()
	if ( !result$succeeded() ) stop ("pipeline failed.")
}

wordCountExample2()


#####################
### quick scratchpad


#rpipe=crunchR.TwoWayPipe$new(new(J("java/lang/String")))
#ls(environment(rpipe$doFnMap$`-2`$FUN_PROCESS))
#
#doFn <- crunchR.DoFn$new()
#doFn$init(list(process=function(x) emit(x)),customizeEnv=T,rpipe=rpipe)
#doFn$callProcess("WORD","KDLS")
#doFn$femit

