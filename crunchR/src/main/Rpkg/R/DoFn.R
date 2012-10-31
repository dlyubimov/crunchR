
#'
#' 
#' @param rDoFn RDoFn (java class) owner
#' @param FUN_PROCESS the process method, R-serialized into raw
#' @param FUN_INITIALIZE optional initialize R function
#' @param FUN_CLEANUP optional cleanup R function
DoFn.initialize <- function (rDoFn, FUN_PROCESS, FUN_INITIALIZE=NULL, FUN_CLEANUP=NULL) {
	rDoFn <<- rDoFn
	FUN_PROCESS <<- FUN_PROCESS
	if (!is.null(FUN_INITIALIZE)) FUN_INITIALIZE <<- FUN_INITIALIZE
	if (!is.null(FUN_CLEANUP)) FUN_CLEANUP <<- FUN_CLEANUP
}

DoFn.run <- function() { 
	eOption <- getOption("error")
	options(error=quote(dump.frames("errframes",F)))
	on.exit(options( error=eOption))
	
	if ( !is.null(FUN_INITIALIZE ))
		FUN_INITIALIZE()
	
	tryCatch({
				
				# todo .. process
			},
			finally = {
				try( if ( !is.null(FUN_CLEANUP)) FUN_CLEANUP());
			})
	
}


crunchR.DoFn <- setRefClass("DoFn",
		fields = list(
				rDoFn = "jobjref",
				FUN_INITIALIZE="function",
				FUN_PROCESS="function",
				FUN_CLEANUP="function"
		),
		methods = list (
				initialize = DoFn.initialize,
				run = DoFn.run
		
		)
)