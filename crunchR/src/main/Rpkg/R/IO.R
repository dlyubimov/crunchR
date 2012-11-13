

crunchR.RType <- setRefClass("RType",
		methods=list (
				set = function ( value ) { stop ("Not implemented") },
				get = function (rawbuff, offset ) { stop ("Not implemented")}
		)
)

#'
#' 
#' @returns list l: l$value is the R object, l$offset next offset to use.
#' 
RString.get <- function (rawbuff, offset = 1 ) {
	len <- .getVarUint32(rawbuff[offset:length(rawbuff)])
	if ( len[1] == 0 ) return (character(0))
	offset <- offset + len[2]
	list(value=iconv(list(rawbuff[offset:(offset+len[1]-1)]),from="UTF8")[[1]],offset=offset+len[1])
}

RString.set <- function (value ) {
	#rawstring <- charToRaw(value)
	rawstring <- iconv(value,to="UTF8",toRaw=T)[[1]]
	len <- length(rawstring)
	c(.setVarUint32(len),rawstring)
}

RStrings.get <- function (rawbuff, offset = 1 ) {
}

RStrings.set <- function (rawbuff,offset,value ) {
	
}


crunchR.RStrings <- setRefClass("RString",contains = "RType",
		methods = list (
				set = RString.set,
				get = RString.get
		)
)


crunchR.RStrings <- setRefClass("RStrings",contains = "RString",
		methods = list (
				set = RStrings.set,
				get = RStrings.get
				)
		)

.rTypeRegistry <- list(
		org.crunchr.io.RStrings = crunchR.RStrings
)
