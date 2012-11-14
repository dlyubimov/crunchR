
#' DoFn$initialize method
#' 
#' @param FUN_PROCESS the process method, R-serialized into raw
#' @param FUN_INITIALIZE optional initialize R function
#' @param FUN_CLEANUP optional cleanup R function
DoFn.initialize <- function ( FUN_PROCESS, FUN_INITIALIZE=NULL, FUN_CLEANUP=NULL) {
	FUN_PROCESS <<- FUN_PROCESS
	if (!is.null(FUN_INITIALIZE)) FUN_INITIALIZE <<- FUN_INITIALIZE
	if (!is.null(FUN_CLEANUP)) FUN_CLEANUP <<- FUN_CLEANUP
	srtype <<- crunchR.RString$new()
	trtype <<- crunchR.RString$new()
}


DoFn.getSRType <- function () srtype

DoFn.getTRType <- function () trtype

DoFnRType.set <- function (value ) stop ("Not implemented")

DoFnRType.get <- function (rawbuff,offset=1 ) { 
	fnRef <- .getVarUint32(rawbuff,offset)
	offset <- offset + fnRef[2]
	
	typeClassNames <- RStrings.get(rawbuff,offset)
	offset <- typeClassNames$offset
	
	f_init <- .unserializeFun(rawBuff,offset)
	f_process <- .unserializeFun(rawBuff,f_init$offset)
	f_cleanup <- .unserializeFun(rawBuff,f_process$offset)

	offset <- f_cleanup$offset
	
	doFn <- crunchR.DoFn$new(f_process$value,f_init$value,f_cleanup$value)
	doFn$rpipe <- rpipe
	doFn$rtypeClassNames <- typeClassNames$value
	
	list(value=doFn,offset=offset)
}

DoFnRType.initialize <- function (rpipe) {
	rpipe <<- rpipe
}


