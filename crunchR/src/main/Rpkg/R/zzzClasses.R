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
				getPType = function() { stop ("Not implemented")},
				as.singleEmit = function() .self
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
				get = RStrings.get,
				as.singleEmit=function() crunchR.RString$new()
		)
)

crunchR.RPTableType <- setRefClass("RPTableType", contains =  "RType",
		fields = list (
				keyType = "RType",
				valueType = "RType"
		),
		methods = list (
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

crunchR.RPGroupedTableType = setRefClass("RPGroupedTableType", contains = "RType",
		fields = list (
				keyType = "RType",
				valueType = "RType"
		),
		methods = list (
				get = RPGroupedTableType.get,
				getState = RPGroupedTableType.getState,
				setState = RPGroupedTableType.setState,
				getJavaClassName = function() "org.crunchr.types.io.RPGropedTableType"
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
				sendEmit = TwoWayPipe.sendEmit,
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
				femit="ANY",
				FUN_INITIALIZE="ANY",
				FUN_PROCESS="ANY",
				FUN_CLEANUP="ANY"
		),
		methods = list (
				initialize=function(...) initFields(
							FUN_INITIALIZE=NULL,
							FUN_PROCESS=NULL,
							FUN_CLEANUP=NULL,
							femit=NULL),
				init = DoFn.init,
				getSRType = function () srtype,
				getTRType = function () trtype,
				.customizeEnv = DoFn..customizeEnv,
				fcall = function (f,...) if (!is.null(f)) f(...),
				callInitialize = function(...) fcall(FUN_INITIALIZE),
				callProcess = function(...) fcall(FUN_PROCESS,...),
				callCleanup = function(...) fcall(FUN_CLEANUP),
				getClosures = function() 
					list(initialize=FUN_INITIALIZE,process=FUN_PROCESS,cleanup=FUN_CLEANUP)
		)
)

crunchR.GroupedDoFn <- setRefClass("GroupedDoFn",
		fields = list (
				FUN_INIT_GROUP = "function",
				FUN_CLEANUP_GROUP = "function"
		),
		methods = list (
				init = GroupedDoFn.init,
				callProcess = GroupedDoFn.callProcess,
				callInitGroup = function(...) fcall(FUN_INIT_GROUP,...),
				callCleanupGroup = function (...) fcall(FUN_CLEANUP_GROUP,...),
				getClosures = function () 
					c (callSuper(),groupInit=FUN_INIT_GROUP,groupCleanup=FUN_CLEANUP_GROUP)
		)
)

crunchR.DoFnRType <- setRefClass("DoFnRType", contains = "RType", 
		fields = list (
				rpipe = "ANY"
		),
		methods = list (
				initialize = function(rpipe=NULL) rpipe <<- rpipe,
				set = DoFnRType.set,
				get = DoFnRType.get
		)
)
crunchR.GroupedDoFnRType <- setRefClass("GroupedDoFnRType", contains="DoFnRType",
		methods = list (
				initialize = function(...) callSuper(...),
				get = function(rawbuff,offset=1) callSuper(rawbuff,offset,crunchR.groupedDoFn$new())
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
					crunchR.PCollection$new(jobj=jobj$readTextFile(pathName),rtype=crunchR.RString$new())
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
				rtype = "RType",
				doFnGen = "refObjectGenerator"
		),
		methods = list (
				initialize = function (...) {
					initFields(...)
					# roxygen process seems to invoke constructors of 
					# base classes with no proper environment setup ..
					# workaround
					if ( exists("crunchR.DoFn")) doFnGen <<- crunchR.DoFn
				},
				.jparallelDo = function (closures,trtype ) {
					doFn <- doFnGen$new()
					doFn$init(closures)
					doFn$srtype <- rtype$as.singleEmit()
					doFn$trtype <- trtype
					
					jDoFn <- .jcall(.crunchR$RDoFnJClass ,"Lorg/crunchr/fn/RDoFn;","fromBytes",
							DoFnRType.set(doFn))
					jobj$parallelDo(jDoFn,trtype$getPType())
				},
				# this automatically detects if 
				# keyType argument supplied, then result is PTable instance; 
				# otherwise, it is a PCollection one.
				parallelDo = function ( 
						FUN_PROCESS, 
						FUN_INITIALIZE=NULL,
						FUN_CLEANUP=NULL,
						valueType = crunchR.RStrings$new(),
						...
					) 
					.parallelDo(closures=
									list(process=FUN_PROCESS,
											initialize=FUN_INITIALIZE,
											cleanup=FUN_CLEANUP),
							valueType = valueType,
							...),
				
				.parallelDo = function ( closures, keyType, ... ) { 
					if (missing(keyType)) { 
						.parallelDo.PCollection(closures,...)
					} else {
						.parallelDo.PTable(closures,keyType,...)
					}
				},
				.parallelDo.PCollection = function ( closures, valueType ) {
					crunchR.PCollection$new(
							jobj = .jparallelDo( closures, valueType),
							rtype = valueType
					)
				},
				.parallelDo.PTable = function (closures, keyType, valueType ) {
					ptableType <- crunchR.RPTableType$new(keyType=keyType,valueType=valueType)
					crunchR.PTable$new(
							jobj=.jparallelDo( closures, ptableType),
							rtype=ptableType
					)
				},
				writeTextFile = function (pathname ) {
					stopifnot (is(pathname,"character"))
					stopifnot (length(pathname)==1)
					jTextFileTarget <- new(.crunchR$TextFileTargetJClass,pathname)
					jobj$write(jTextFileTarget)
					NA
				} 
		)
)

crunchR.PTable <- setRefClass("PTable", contains = "PCollection",
		fields = list ( 
		),
		methods = list (
				groupByKey <- function() {
					jgtable <- .jobj$groupByKey()
					gtype=crunchR.RPGroupedTableType$new(
							keyType=rtype$keyType,
							valueType = type$valueType)
					gtable <- crunchR.PGroupedTable$new(jobj=jgtable,
							rtype = gtype
					)
				}
		)
)

crunchR.PGroupedTable <- setRefClass("PGroupedTable", contains = "PCollection",
		methods = list (
				initialize = function (...) {
					callSuper( ...)
					doFnGen <<- crunchR.GroupedDoFn 
				},
				# this automatically detects if 
				# keyType argument supplied, then result is PTable instance; 
				# otherwise, it is a PCollection one.
				parallelDo = function ( 
						FUN_PROCESS, 
						FUN_INITIALIZE=NULL,
						FUN_CLEANUP=NULL,
						FUN_INIT_GROUP,
						FUN_CLEANUP_GROUP=NULL,
						valueType = crunchR.RStrings$new(),
						...
					) 
					.parallelDo(closures=
									list(process=FUN_PROCESS,
											initialize=FUN_INITIALIZE,
											cleanup=FUN_CLEANUP,
											groupInit=FUN_GROUP_INIT,
											groupCleanup=FUN_CLEANUP_GROUP
									),
							valueType,
							...)
		)
)

