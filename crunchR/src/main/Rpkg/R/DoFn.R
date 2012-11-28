
#' DoFn$initialize method, mainly, to initialize closure environments with proper emit() 
#' function.
#' 
#' @param closures a list with fields $process = process function, $initalize= initialize function, 
#'   		$cleanup= cleanup function
DoFn.init <- function ( closures,
		customizeEnv=F, rpipe = NULL, femit = NULL, ...) {
	doFnRef <<- 0
	
	# prep call environment
	if (is.null(femit))
		femit <- function(...) .self$rpipe$sendEmit(doFn=.self,...)
	
	femit <<- femit
	
	if ( !is.null(rpipe))
		rpipe <<- rpipe
	
	stopifnot (!is.null(closures$process))
	FUN_PROCESS <<- .customizeEnv(closures$process,customizeEnv)
	if (!is.null(closures$initialize)) FUN_INITIALIZE <<- .customizeEnv(closures$initialize,customizeEnv)
	if (!is.null(closures$cleanup)) FUN_CLEANUP <<- .customizeEnv(closures$cleanup,customizeEnv)
	
	srtype <<- crunchR.RString$new()
	trtype <<- crunchR.RStrings$new()
	
}

DoFn..customizeEnv <- function(FUN, customizeEnv) { 
	if ( customizeEnv ) { 
		fenv <- new.env(parent=environment(FUN))
		fenv$emit <- femit
		environment(FUN)<-fenv
	} 
	FUN
}

GroupedDoFn.init <- function (closures, customizeEnv=F, ...) {
	callSuper(closures,customizeEnv,...)
	
	if (!is.null(closures$initGroup)) FUN_INIT_GROUP <<- 
				.customizeEnv(closures$initGroup,customizeEnv)
	if (!is.null(closures$cleanupGroup)) FUN_CLEANUP_GROUP <<- 
				.customizeEnv(closures$cleanupGroup,customizeEnv)
}

GroupedDoFn.callProcess <- function(groupChunk) {
	if (groupChunk$firstChunk) 
		callInitGroup(groupChunk$key)
	fcall(FUN_PROCESS,groupChunk$vv)
	if ( groupChunk$lastChunk) 
		callCleanupGroup()
}


DoFnRType.set <- function (value ) {
	stopifnot (is(value,crunchR.DoFn$className))
	
	doFn <- value 
	
	fnRef <- .setVarUint32(doFn$doFnRef)
	
	closures <- RRaw.set(serialize( doFn$getClosures(),	connection=NULL)); 
	
	rTypeState <- RTypeStateRType.set(doFn$srtype$getState())
	tTypeState <- RTypeStateRType.set(doFn$trtype$getState())
	
	# in R2Java serialization, we also attach java class names 
	# so that RType can be properly instantiated.
	c(fnRef, closures, rTypeState, tTypeState)
	
}

DoFnRType.get <- function (rawbuff,offset=1, holder = NULL ) {
	
	fnRef <- .getVarUint32(rawbuff,offset)
	offset <- offset + fnRef[2]
	
	closures <- RRaw.get(rawbuff,offset)
	offset <- closures$offset
	closures <- unserialize(closures$value)
	
	sTypeState <- RTypeStateRType.get(rawbuff,offset)
	offset <- sTypeState$offset
	sTypeState <- sTypeState$value
	
	tTypeState <- RTypeStateRType.get(rawbuff,offset)
	offset <- tTypeState$offset
	tTypeState <- tTypeState$value
	
	doFn <- if (is.null(holder)) crunchR.DoFn$new() else holder
	doFn$init(closures,	customizeEnv=T, rpipe = rpipe)
	
	doFn$doFnRef <- fnRef[1]
	
	# RType classes must have default constructor for generic DoFn'ss
	doFn$srtype <- getRefClass(sTypeState$rClassName)$new()
	doFn$srtype$setState(sTypeState)
	
	doFn$trtype <- getRefClass(tTypeState$rClassName)$new()
	doFn$trtype$setState(tTypeState)
	
	list(value=doFn,offset=offset)
}



