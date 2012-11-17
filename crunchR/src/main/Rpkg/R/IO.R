
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
	sum(bitShiftL(as.numeric(rawbuff[offset:(offset+3)]),(0:3)*8))
}

RUint32.set <- function ( value ) { 
	as.raw(bitAnd(0xff, bitShiftR(value,(0:3)*8)))
}

RInt32.get <- function (rawbuff,offset=1 ) {
	r <- RUint32.get(rawbuff,offset)
	if (r>=0x80000000 ) r<- r - 0x100000000
	r
}

RInt32.set <- function ( value ) RUint32.set(value)


RStrings.get <- function (rawbuff, offset = 1 ) {
	count <- .getVarUint32(rawbuff,offset)
	offset <- offset + count[2]
	count <- count[1]
	valueVec <- character(0)
	while (count > 0 ) { 
		len <- .getVarUint32(rawbuff[offset:length(rawbuff)])
		offset <- offset + len[2]
		if ( len[1] == 0 ) { 
			value <- ""
		} else { 
			value <- iconv(list(rawbuff[offset:(offset+len[1]-1)]),from="UTF8")[[1]]
			offset <- offset+len[1]
		}
		valueVec[length(valueVec)+1] <- value
		count <- count -1 
	}
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