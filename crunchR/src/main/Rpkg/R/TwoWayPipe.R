
TwoWayPipe.ADD_DO_FN = -1;

TwoWayPipe.initialize <- function (jpipe ) { 
	
	jpipe <<- jpipe
	doFnMap <<- list()
	
	addDoFnFn <- crunchR.DoFn$new(function(x) .self$addDoFn(x))
	addDoFnFn$srtype <- crunchR.DoFnRType$new(.self)
	addDoFnFn$doFnRef <- 0xFFFFFFFF
	addDoFn(addDoFnFn)
	
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
}

TwoWayPipe.run <- function () {
	repeat {
		rawbuff <- .jcall(jpipe,"[B","rcallbackNextBuff")
		if ( is.null(rawbuff) | length(rawbuff)==0) break
		
		dispatch(rawbuff)
		
		.jcall(jpipe,"V","rcallbackBufferConsumed")
	}
}


TwoWayPipe.dispatch <- function (rawbuff) {
	stopifnot(mode(rawbuff)=="raw")
	count <- .getShort(rawbuff)
	offset <- 3L	
	i <- 1L
	while (i <= count ) {
		
		doFnRef <- .getVarUint32(rawbuff,offset)
		offset <- offset + doFnRef[2]
		fnRefKey <- as.character(doFnRef[1])
		
		
		srt <- srType[[fnRefKey]]
		doFn <- doFnMap[[fnRefKey]]

		if ( is.null(srt) | is.null(doFn) )
			stop ("unknown doFnRef or unintialized RType")
		
		robj <- srt$get(rawbuff,offset)
		offset <- robj$offset
		
		# call doFn's process 
		doFn$FUN_PROCESS(x=robj$value)
		
		i <- i + 1L
	}
	
}




		