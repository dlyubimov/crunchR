
TwoWayPipe.ADD_DOFN <- -1
TwoWayPipe.CLEANUP_DOFN <- -2
TwoWayPipe.ADD_GROUPED_DOFN <- -3

TwoWayPipe.initialize <- function (jpipe, outputBuffCapacity = 4096 ) { 
	
	jpipe <<- jpipe
	doFnMap <<- list()
	
	# special function #1: create another serialized DoFn instance.
	addDoFnFn <- crunchR.DoFn$new()
	addDoFnFn$init(list(process=function(doFn) .self$addDoFn(doFn) ), rpipe =.self)
	addDoFnFn$srtype <- crunchR.DoFnRType$new(.self)
	addDoFnFn$doFnRef <- TwoWayPipe.ADD_DOFN
	addDoFn(addDoFnFn)
	
	# special function #3: create GroupedDoFn instance.
	addGroupedDoFnFn <- crunchR.DoFn$new()
	addGroupedDoFnFn$init(list(process=function(groupedDoFn) .self$addDoFn(doFn)),rpipe=.self)
	addGroupedDoFnFn$srtype <- crunchR.GroupedDoFnRType$new(.self)
	addGroupedDoFnFn$doFnRef <- TwoWayPipe.ADD_GROUPED_DOFN
	addDoFn(addGroupedDoFnFn)
	
	# special function #2: computational cleanup of doFns: 
	cleanupDoFn <- crunchR.DoFn$new()
	cleanupDoFn$init( list(process=function(fnRef){
						doFn <- .self$doFnMap[[as.character(fnRef)]]
						if ( is.null(doFn))
							stop(sprintf("Unknown doFnRef %d.",x))
						doFn$callCleanup()
						emit(fnRef)
						.self$flushOutput()
					}),
			customizeEnv=T,
			rpipe=.self
	)
	cleanupDoFn$srtype <- crunchR.RVarInt32$new()
	cleanupDoFn$trtype <- crunchR.RVarInt32$new()
	cleanupDoFn$doFnRef <- TwoWayPipe.CLEANUP_DOFN
	addDoFn(cleanupDoFn)
	
	# pipe fields
	outputBuffCapacity <<- outputBuffCapacity
	outputBuff <<- raw(outputBuffCapacity)
	outputBuffOffset <<- 1
	outputMsgCnt <<- 0
	
	.crunchR$twoWayPipe <- .self
}

#'
#' Add a DoFn
#' 
#' @param doFn a DoFn to add
TwoWayPipe.addDoFn <- function (doFn) {
	# unfortunately, numeric keys to the list 
	# index must be nonnegative sequential
	# rather than associative. So i convert 
	# them to character to force them to be 
	# associative.
	fnRefKey <- as.character(doFn$doFnRef)
	doFnMap[[fnRefKey]] <<- doFn
	srType[[fnRefKey]] <<- doFn$getSRType()
	trType[[fnRefKey]] <<- doFn$getTRType()
	doFn$rpipe <- .self
	doFn$callInitialize()
}

TwoWayPipe.run <- function () {
	repeat {
		rawbuff <- .jcall(jpipe,"[B","rcallbackNextBuff")
		if ( is.null(rawbuff) | length(rawbuff)==0) {
			if ( .jlogging) .jlogInfo("received input close, exiting dispatch loop.")	
			closeOutput()
			break
		}
		
		dispatch(rawbuff)
		
		.jcall(jpipe,"V","rcallbackBufferConsumed")
	}
}


TwoWayPipe.dispatch <- function (rawbuff) {
	stopifnot(mode(rawbuff)=="raw")
	count <- .getShort(rawbuff)
	
	if ( .jlogging ) .jlogInfo(sprintf("got input data with %d messages.",count))
	
	offset <- 3L	
	i <- 1L
	while (i <= count ) {
		
		doFnRef <- .getVarInt32(rawbuff,offset)
		offset <- offset + doFnRef[2]
		fnRefKey <- as.character(doFnRef[1])
		
		if ( .jlogging ) .jlogInfo(sprintf("got fnref %s.",fnRefKey))
		
		
		srt <- srType[[fnRefKey]]
		doFn <- doFnMap[[fnRefKey]]

		if ( is.null(doFn) )
			stop (sprintf("Unknown doFnRef %s, no such function found", fnRefKey))
		if ( is.null(srt) )
			stop (sprintf("Source type not found for function doFnRef %s ",fnRefKey))
		
		robj <- srt$get(rawbuff,offset)
		offset <- robj$offset
		
		# call doFn's process 
		doFn$callProcess(robj$value)
		
		i <- i + 1L
	}
}

TwoWayPipe.sendEmit <- function (doFn,...) {
	if ( outputBuffOffset >= outputBuffCapacity ) flushOutput()
	packedEmission <- doFn$trtype$set(...)
	
	# 1. writing function reference
	fnRef <- .setVarInt32(doFn$doFnRef)
	fnRefLen <- length(fnRef)
	outputBuff[outputBuffOffset:(outputBuffOffset+fnRefLen-1)]<<-fnRef
	outputBuffOffset <<- outputBuffOffset+fnRefLen
	
	# 2. writing emited value
	len <- length(packedEmission)
	outputBuff[outputBuffOffset:(outputBuffOffset+len-1)] <<- packedEmission
	outputBuffOffset <<- outputBuffOffset + len
	
	outputMsgCnt <<- outputMsgCnt + 1
}

TwoWayPipe.flushOutput <- function() {
	if ( outputMsgCnt == 0 ) return()
	cntRaw <- .setShort(outputMsgCnt)
	.jcall(jpipe,"V","rcallbackEmitBuffer",c(cntRaw,outputBuff[1:outputBuffOffset-1]))
	outputBuffOffset <<- 1
	outputMsgCnt <<- 0
	
}

TwoWayPipe.closeOutput <- function ()  {
	flushOutput()
	.jcall(jpipe,"V","rcallbackEmitBuffer",raw(0))
	
}



		