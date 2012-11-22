
#' DoFn$initialize method
#' 
#' @param FUN_PROCESS the process method, R-serialized into raw
#' @param FUN_INITIALIZE optional initialize R function
#' @param FUN_CLEANUP optional cleanup R function
DoFn.initialize <- function ( FUN_PROCESS, FUN_INITIALIZE=NULL, FUN_CLEANUP=NULL,
		customizeEnv=F) {
	doFnRef <<- 0
	
	# prep call environment
	femit <- function(x) .self$rpipe$emit(x,.self)
	fcustEnv <- function(f) {
		if ( customizeEnv ) { 
			fenv <- new.env(parent=environment(f))
			fenv$emit<-femit
			environment(f)<-fenv
		} 
		f
	}
	
	stopifnot (!is.null(FUN_PROCESS))
	FUN_PROCESS <<- fcustEnv(FUN_PROCESS)
	if (!is.null(FUN_INITIALIZE)) FUN_INITIALIZE <<- fcustEnv(FUN_INITIALIZE)
	if (!is.null(FUN_CLEANUP)) FUN_CLEANUP <<- fcustEnv(FUN_CLEANUP)
	
	srtype <<- crunchR.RString$new()
	trtype <<- crunchR.RStrings$new()
	
}


DoFn.getSRType <- function () srtype

DoFn.getTRType <- function () trtype

DoFnRType.set <- function (value ) {
	stopifnot (is(value,crunchR.DoFn$className))
	
	doFn <- value 
	
	fnRef <- .setVarUint32(doFn$doFnRef)
	
	closures <- RRaw.set(serialize( list (doFn$FUN_PROCESS,doFn$FUN_INITIALIZE,doFn$FUN_CLEANUP),
			connection=NULL)); 
	
	rClassNames <- RStrings.set(
			c(
					doFn$srtype$getRefClass()$className,
					doFn$trtype$getRefClass()$className
			)
	)
	
	rJavaClassNames <- RStrings.set(
			c(
					doFn$srtype$getJavaClassName(),
					doFn$trtype$getJavaClassName()
			)
	)
	
	# in R2Java serialization, we also attach java class names 
	# so that RType can be properly instantiated.
	c(fnRef, closures, rClassNames,rJavaClassNames)
	
}

DoFnRType.get <- function (rawbuff,offset=1 ) { 
	fnRef <- .getVarUint32(rawbuff,offset)
	offset <- offset + fnRef[2]
	
	closures <- RRaw.get(rawbuff,offset)
	offset <- closures$offset
	closures <- unserialize(closures$value)

	typeClassNames <- RStrings.get(rawbuff,offset)
	offset <- typeClassNames$offset
	stopifnot ( length(typeClassNames$value)==2 )
	
	doFn <- crunchR.DoFn$new(closures[[1]],closures[[2]],
			closures[[3]],customizeEnv=T)
	doFn$doFnRef <- fnRef[1]
	doFn$rpipe <- rpipe
	
	# RType classes must have default constructor for generic DoFn'ss
	doFn$srtype <- getRefClass(typeClassNames$value[1])$new()
	doFn$trtype <- getRefClass(typeClassNames$value[2])$new()
	
	list(value=doFn,offset=offset)
}

DoFnRType.initialize <- function (rpipe) {
	rpipe <<- rpipe
}


