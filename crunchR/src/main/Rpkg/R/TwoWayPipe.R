
TwoWayPipe.initialize <- function (jpipe ) { 
	
	jpipe <<- jpipe
	doFnMap <<- list()
	
	.crunchR$twoWayPipe <- .self
}

#'
#' Add a DoFn
#' @param doFn a DoFn to add
#' @return the doFn's ref value
TwoWayPipe.addDoFn <- function ( doFn ) {
	stopifnot(is(doFn,crunchR.DoFn))
	
	i <- length(doFnMap)+1
	doFnMap[[i]] <<- doFn
	rtypeNames <- doFn$getRTypeClassNames()
	
	s <- .rTypeRegistry[[rtypeNames[1]]]
	t <- .rTypeRegistry[[rtypeNames[2]]]
	
	stopifnot(!is.null(s) & !is.null(t))
	srType[[i]] <<- s
	trType[[i]] <<- t
}

TwoWayPipe.run <- function () {
	repeat {
		rawbuff <- jpipe$rcallbackNextBuff()
		if ( is.null(rawbuff)) break
		dispatch(rawbuff)
		jpipe$rcallbackBufferConsumed()
	}
}

TwoWayPipe.dispatch <- function (rawbuff) {
	stopifnot(mode(rawbuff)=="raw")
	count <- .getShort(rawbuff)
	offset <- 2	
	i <- 1
	while (i <= count ) {
		doFnRef <- .getVarUint32(rawbuff,offset)
		srt <- srtype[[doFnRef]]
		doFn <- doFnMap[[doFnRef]]
		
		if ( is.null(srt) | is.null(doFn) )
			stop ("unknown doFnRef or unitialized RType")
		
		offset <- offset + doFnRef[2]
		robj <- rtype$get(rawbuff,offset)
		offset <- robj$offset
		
		# call doFn's process 
		doFn$FUN_PROCESS(x=robj$value)
		
		i <- i + 1
	}
	
}

crunchR.TwoWayPipe <- setRefClass("TwoWayPipe",
		fields = list (
				jpipe="jobjRef",
				doFnMap="list",
				srType="list",
				trType="list"
		),
		methods = list (
				initialize = TwoWayPipe.initialize,
				addDoFn = TwoWayPipe.addDoFn,
				run = TwoWayPipe.run,
				dispatch = TwoWayPipe.dispatch
		)
)




		