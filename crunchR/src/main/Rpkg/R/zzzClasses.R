# all class definitions
# because roxygen fails to load sources 
# in the order defined in @import but rather in the alphabet order 
# of the file names.


######################### IO ###############################

crunchR.RTypeState <- setRefClass("RTypeState",
		fields = list (
				rClassName = "character",
				javaClassName = "character",
				specificState = "raw"
		)
)

# this kind of all assumes Writables type family for now. 
# Support for distinct type families (Avro comes to mind) 
# is not modeled here but it is not terribly unthinkable to add.
crunchR.RType <- setRefClass("RType",
		methods=list (
				set = function ( value ) { stop ("Not implemented") },
				get = function (rawbuff, offset = 1 ) { stop ("Not implemented")},
				setState = function (typeState ) { },
				getState = function () {
					state <- crunchR.RTypeState$new()
					state$rClassName <- getClass()@className
					state$javaClassName <- getJavaClassName()
					state
				},
				getJavaClassName = function() { stop ("Not implemented") },
				getPType = function() { stop ("Not implemented")}
		)
)

crunchR.RTypeStateRType <- setRefClass("RTypeStateRType", contains="RType",
		methods = list (
				getJavaClassName = function() "org.crunchr.types.io.RTypeStateRType",
				set = RTypeStateRType.set,
				get = RTypeStateRType.get
		)
)

crunchR.RString <- setRefClass("RString",contains = "RType",
		methods = list (
				getJavaClassName = function () "org.crunchr.types.io.RString",
				getPType = function() .crunchR$WritablesJClass$strings(),	
				set = RString.set,
				get = RString.get
		)
)

crunchR.RRaw <- setRefClass("RRaw",contains = "RType",
		methods = list (
				getJavaClassName = function () "org.crunchr.types.io.RRaw",
				set = RRaw.set,
				get = RRaw.get
		)
)

crunchR.RUint32 <- setRefClass("RUint32", contains="RType",
		methods = list (
				set = RUint32.set,
				get = RUint32.get,
				getJavaClassName = function() "org.crunchr.types.io.RInteger",
				getPType = function() .crunchR$WritablesJClass$ints()
		)
)

crunchR.RInt32 <- setRefClass("RInt32", contains="RType",
		methods = list (
				set = RInt32.set,
				get = RInt32.get,
				getJavaClassName = function() "org.crunchr.types.io.RInteger",
				getPType = function() .crunchR$WritablesJClass$ints()
		)
)

crunchR.RVarUint32 <- setRefClass("RVarUint32", contains = "RType",
		methods = list (
				set = RVarUint32.set,
				get = RVarUint32.get,
				getJavaClassName = function() "org.crunchr.types.io.RVarUint32",
				getPType = function() .crunchR$WritablesJClass$ints()
		)
)

crunchR.RVarInt32 <- setRefClass("RVarInt32", contains = "RType",
		methods = list (
				set = RVarInt32.set,
				get = RVarInt32.get,
				getJavaClassName = function() "org.crunchr.types.io.RVarInt32",
				getPType = function() .crunchR$WritablesJClass$ints()
		)
)

#' RStrings
#' 
#' @description
#' this communicates a character vector between R and java side
#' (There's probably no direct correspondence to a pType here though at the 
#' moment, so it is not to be used for emitting purposes, only for source type
#' perhaps)
crunchR.RStrings <- setRefClass("RStrings",contains = "RString",
		methods = list (
				getJavaClassName = function () "org.crunchr.types.io.RStrings",
				getPType = function() J("org/apache/crunch/types/writable/Writables")$strings(),	
				set = RStrings.set,
				get = RStrings.get
		)
)

crunchR.RPTableType <- setRefClass("RPTableType", contains =  "RType",
		fields = list (
				keyType = "RType",
				valueType = "RType"
		),
		methods = list (
				initialize = function (
						keyRType = crunchR.RString$new(), 
						valueRType = crunchR.RString$new(),...) {
					callSuper(...)
					keyType <<- keyRType
					valueType <<- valueRType
				},
				set = function(key,value) c( keyType$set(key), valueType$set(value) ),
				get = function(rawbuff, offset=1) {
					key <- keyType$get(rawbuff,offset)
					offset <- key$offset
					value <- valueType$get(rawbuff,offset)
					offset <- value$offset
					list(value=list(key=key$value,value=value$value),offset=offset)
				},
				setState = RPTableType.setState,
				getState = RPTableType.getState,
				getJavaClassName = function () "org.crunchr.types.io.RPTableType",
				getPType = function() .crunchR$WritablesJClass$tableOf(
							keyType$getPType(),
							valueType$getPType())
		)
)

############################ TwoWayPipe.R ###############################

crunchR.TwoWayPipe <- setRefClass("TwoWayPipe",
		fields = list (
				jpipe="jobjRef",
				doFnMap="list",
				srType="list",
				trType="list",
				outputBuff="raw",
				outputBuffOffset="numeric",
				outputMsgCnt="numeric",
				outputBuffCapacity="numeric"
		),
		methods = list (
				initialize = TwoWayPipe.initialize,
				addDoFn = TwoWayPipe.addDoFn,
				run = TwoWayPipe.run,
				dispatch = TwoWayPipe.dispatch,
				emit = TwoWayPipe.emit,
				flushOutput = TwoWayPipe.flushOutput,
				closeOutput = TwoWayPipe.closeOutput
		)
)

############################# DoFn ####################################

crunchR.DoFn <- setRefClass("DoFn",
		fields = list(
				rpipe="ANY",
				doFnRef="numeric",
				srtype="RType", 
				trtype="RType",
				FUN_INITIALIZE="function",
				FUN_PROCESS="function",
				FUN_CLEANUP="function"
		),
		methods = list (
				initialize = DoFn.initialize,
				getSRType = DoFn.getSRType,
				getTRType = DoFn.getTRType,
				fcall = function (f,...) if (!is.null(f)) f(...),
				callInitialize = function(...) fcall(FUN_INITIALIZE),
				callProcess = function(...) fcall(FUN_PROCESS,...),
				callCleanup = function(...) fcall(FUN_CLEANUP)
		)
)

crunchR.DoFnRType <- setRefClass("RDoFnRType", contains = "RType", 
		fields = list (
				rpipe = "ANY"
		),
		methods = list (
				initialize = DoFnRType.initialize,
				set = DoFnRType.set,
				get = DoFnRType.get
		)
)


############################ Pipeline.R ################################
#'
#' crunchR's pipeline R5 class wrapper
#' 
crunchR.Pipeline <- setRefClass("Pipeline", 
		fields = list( 
				jobj = "jobjRef"
		),
		methods = list(
				initialize = Pipeline.initialize,
				getConfiguration = Pipeline.getConfiguration,
				readTextFile = function(...) stop("abstract method")
		)
)

#'
#' crunchR's MRPipeline R5 class wrapper
#' 
crunchR.MRPipeline <- setRefClass("MRPipeline", contains=crunchR.Pipeline,
		methods = list (
				initialize = MRPipeline.initialize,
				run = MRPipeline.run,
				readTextFile = function (pathName) 	
					crunchR.PCollection$new(jobj$readTextFile(pathName))
		)
)

#'
#' PipelineResult R5 class
#' 
crunchR.PipelineResult <- setRefClass("PipelineResult",
		fields = list( 
				jobj = "jobjRef"
		),
		methods = list (
				initialize = function( jpipelineResult ) {
					stopifnot (is(jpipelineResult,"jobjRef") & 
									jpipelineResult %instanceof% .crunchR$PipelineResultJClass)
					jobj <<- jpipelineResult
				},
				succeeded = function() jobj$succeeded()
		
		)
)

crunchR.PCollection <- setRefClass ("PCollection", 
		fields = list (
				jobj = "jobjRef",
				rtype = "RType"
		),
		methods = list (
				
				initialize = function (jobjRef,rtype = crunchR.RString$new() ) {
					jobj <<- jobjRef
					rtype <<- rtype
				},
				
				parallelDo = function ( FUN_PROCESS, trtype=crunchR.RStrings$new(), 
						FUN_INITIALIZE=NULL,FUN_CLEANUP=NULL ) {
					
					doFn <- crunchR.DoFn$new(FUN_PROCESS,FUN_INITIALIZE,FUN_CLEANUP)
					doFn$srtype <- rtype
					doFn$trtype <- trtype
					
#					jDoFn <- J("org/crunchr/fn/RDoFn")$fromBytes(DoFnRType.set(doFn))
					jDoFn <- .jcall(.crunchR$RDoFnJClass ,"Lorg/crunchr/fn/RDoFn;","fromBytes",DoFnRType.set(doFn))
					jpcollection <- jobj$parallelDo(jDoFn,trtype$getPType())
					crunchR.PCollection$new(jpcollection,trtype)
				},
				
				writeTextFile = function (pathname ) {
					stopifnot (is(pathname,"character"))
					stopifnot (length(pathname)==1)
					jTextFileTarget <- new(.crunchR$TextFileTargetJClass,pathname)
					jobj$write(jTextFileTarget)
				} 
		)
)

