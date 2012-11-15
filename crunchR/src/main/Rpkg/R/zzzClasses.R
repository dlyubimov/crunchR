# all class definitions
# because roxygen fails to load sources 
# in the order defined in @import but rather in the alphabet order 
# of the file names.


######################### IO ###############################
crunchR.RType <- setRefClass("RType",
		methods=list (
				set = function ( value ) { stop ("Not implemented") },
				get = function (rawbuff, offset = 1 ) { stop ("Not implemented")},
				getJavaClassName = function() { stop ("Not implemented") }
		)
)

crunchR.RString <- setRefClass("RString",contains = "RType",
		methods = list (
				getJavaClassName = function () "org.crunchr.io.RString",
				set = RString.set,
				get = RString.get
		)
)

crunchR.RRaw <- setRefClass("RString",contains = "RType",
		methods = list (
				getJavaClassName = function () "org.crunchr.io.RRaw",
				set = RRaw.set,
				get = RRaw.get
		)
)

crunchR.RStrings <- setRefClass("RStrings",contains = "RString",
		methods = list (
				getJavaClassName = function () "org.crunchr.io.RStrings",
				set = RStrings.set,
				get = RStrings.get
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
				getTRType = DoFn.getTRType
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

############################ TwoWayPipe.R ###############################

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

