
# We probably should be doing a lot of this serialization code in Rcpp.
# it is ok for prototyping purposes though.


#' get
#' 
#' @returns list l: l$value is the R object, l$offset next offset to use.
#' 
RString.get <- function (rawbuff, offset = 1 ) {
	len <- .getVarUint32(rawbuff[offset:length(rawbuff)])
	offset <- offset + len[2]
	if ( len[1] == 0 ) { 
		value <- (character(0))
	} else { 
		value <- iconv(list(rawbuff[offset:(offset+len[1]-1)]),from="UTF8")[[1]]
	}
	list(value=value,offset=offset+len[1])
}

RString.set <- function (value ) {
	#rawstring <- charToRaw(value)
	rawstring <- iconv(value,to="UTF8",toRaw=T)[[1]]
	len <- length(rawstring)
	c(.setVarUint32(len),rawstring)
}

RRaw.get <- function (rawbuff,offset=1) { 
	len <- .getVarUint32(rawbuff,offset)
	offset <- offset+len[2]
	if ( len[1] == 0 ) { 
		value <- raw(0)
	} else { 
		value <- rawbuff[offset:(offset+len[1]-1)] 
	}
	list(value=value,offset=offset+len[1])
}

RRaw.set <- function (value) {
	stopifnot (class(value)=="raw")
	len <- .setVarUint32(length(value))
	c(len,value)
}

RVarUint32.get <- function (rawbuff,offset=1 ) {
	val <- .getVarUint32(rawbuff,offset)
	list(value=val[1],offset=offset+val[2])
}

RVarUint32.set <- function (value ) {
	.setVarUint32(value)
}

RVarInt32.get <- function (rawbuff,offset=1 ) {
	val <- .getVarInt32(rawbuff,offset)
	list(value=val[1],offset=offset+val[2])
}

RVarInt32.set <- function (value ) {
	.setVarInt32(value)
}

RUint32.get <- function (rawbuff,offset=1 ) {
	list(value=sum(bitShiftL(as.numeric(rawbuff[offset:(offset+3)]),(0:3)*8)),
			offset=offset+4)
}

RUint32.set <- function ( value ) { 
	as.raw(bitAnd(0xff, bitShiftR(value,(0:3)*8)))
}

RInt32.get <- function (rawbuff,offset=1 ) {
	r <- RUint32.get(rawbuff,offset)
	if (r$value>=0x80000000 ) r$value <- r$value - 0x100000000
	r
}

RInt32.set <- function ( value ) RUint32.set(value)


RStrings.get <- function (rawbuff, offset = 1 ) {
	count <- .getVarUint32(rawbuff,offset)
	offset <- offset + count[2]
	count <- count[1]
	valueVec <- 
			if ( count >0 ) supply(1:count, function(x) { 
							len <- .getVarUint32(rawbuff[offset:length(rawbuff)])
							offset <<- offset + len[2]
							if ( len[1] == 0 ) { 
								value <- ""
							} else { 
								value <- unlist(iconv(list(rawbuff[offset:(offset+len[1]-1)]),from="UTF8"))
								offset <<- offset+len[1]
							}
							value
						}
				) else character(0)
	list(value=valueVec,offset=offset)
}

RStrings.set <- function (value ) {
	stopifnot(class(value)=="character")
	
	# this is probably not the best practice 
	# (multiple c() invocation). Better practice 
	# would be lazy extension and final trim.
	# but should do for now.
	r <- .setVarUint32(length(value))
	i<- 1L
	while ( i <= length(value)) { 
		
		rawstring <- iconv(value[i],to="UTF8",toRaw=T)[[1]]
		len <- length(rawstring)
		r<- c(r, .setVarUint32(len),rawstring)
		i <- i + 1
	}
	r
}

RTypeStateRType.get <- function (rawbuff, offset = 1 ) {
	
	state <- crunchR.RTypeState$new()
	
	v <- RString.get(rawbuff,offset)
	offset <- v$offset
	state$rClassName <- v$value
	
	v <- RString.get(rawbuff,offset)
	offset <- v$offset
	state$javaClassName <- v$value
	
	v <- RRaw.get(rawbuff,offset)
	offset <- v$offset
	state$specificState <- v$value
	
	list(offset=offset,value=state)
}

RTypeStateRType.set <- function ( value ) {
	c(
			RString.set(value$rClassName),
			RString.set(value$javaClassName),
			RRaw.set(value$specificState)
	)
}

RPTableType.setState <- function (typeState ) {
	callSuper(typeState)
	
	rawbuff <- typeState$specificState
	
	keyTypeState <- RTypeStateRType.get(rawbuff)
	offset <- keyTypeState$offset
	keyTypeState <- keyTypeState$value
	
	valueTypeState <- RTypeStateRType.get(rawbuff,offset)
	offset <- valueTypeState$offset
	valueTypeState <- valueTypeState$value
	
	keyType <<- getRefClass(keyTypeState$rClassName)$new()
	keyType$setState(keyTypeState)
	
	valueType <<- getRefClass(valueTypeState$rClassName)$new()
	valueType$setState(valueTypeState)
	
}

RPTableType.getState <-  function () {
	state <- callSuper()
	state$specificState <- c(
			RTypeStateRType.set(keyType$getState()),
			RTypeStateRType.set(valueType$getState())
	)
	state
}

RPGroupedTableType.getState <- function () {
	state <- callSuper()
	state$specificState <- c(
			RTypeStateRType.set(keyType$getState()),
			RTypeStateRType.set(valueType$getState())
	)
	state
}

RPGroupedTableType.setState <- function (typeState ) {
	
	callSuper(typeState)
	
	rawbuff <- typeState$specificState
	
	keyTypeState <- RTypeStateRType.get(rawbuff)
	offset <- keyTypeState$offset
	keyTypeState <- keyTypeState$value
	
	valueTypeState <- RTypeStateRType.get(rawbuff,offset)
	offset <- valueTypeState$offset
	valueTypeState <- valueTypeState$value
	
	keyType <<- getRefClass(keyTypeState$rClassName)$new()
	keyType$setState(keyTypeState)
	
	valueType <<- getRefClass(valueTypeState$rClassName)$new()
	valueType$setState(valueTypeState)
	
}


.FIRST_CHUNK    <- 0x01
.LAST_CHUNK     <- 0x02

#' unserialize grouped chunk of data
#' @return a list of value and offset of the next message in the buffer.
#' value is a list with the following attributes: 
#' firstChunk, boolean, indicates this is a first chunk in a group.
#' lastChunk, boolean, indicates if this is a lust chunk of values in the group.
#' key -- is the grouped key, if firstChunk==TRUE, or NULL otherwise.
#' vv -- list of values in chunk
#' 
RPGroupedTableType.get <- function (rawbuff, offset=1) {
	
	if ( .jlogging ) 
		.jlogInfo(paste(as.character(rawbuff[offset:min(c(offset+20,length(rawbuff)))]),collapse = " "))
	
	
	# flags
	flags <- as.integer(rawbuff[offset])
	offset <- offset + 1
	
	# key
	if ( bitAnd(flags,.FIRST_CHUNK)!= 0) { 
		key	<- keyType$get(rawbuff,offset)
		offset <- key$offset
		key <- key$value
		firstChunk <- T
	} else {
		key <- NULL
		firstChunk <- F
	}
	
	# value count 
	count <- .getShort(rawbuff,offset)
	offset <- offset + 2
	
	if (count ==0 ) { 
		vv <- NULL
	} else { 
		vv <- sapply(1:count, function(x) {
					v <- valueType$get(rawbuff,offset)
					offset <<- v$offset
					v$value
				}
		)
	}
	list(offset = offset,
			value = list(
					key=key,
					vv=vv,
					firstChunk=firstChunk,
#					lastChunk=(bitAnd(flags,.LAST_CHUNK)!=0
					lastChunk=(count==0)
			)
	)
	
}

#' unserialize a function packed using RRaw RType
#' @return a list with value=value and offset= next offset.
.unserializeFun <- function (rawbuff,offset = 1) { 
	f_bytes <- RRaw.get(rawbuff,offset)
	offset <- f_bytes$offset
	if ( length(f_bytes$value)==0) {
		f <- NULL
	} else {
		f <- unserialize(f_bytes$value)
	}
	list(value=f,offset=offset)
}
