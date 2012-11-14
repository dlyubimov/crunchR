
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
		value <- iconv(list(rawbuff[offset:offset+len[1]-1]),from="UTF8")[[1]]
		list(value=,offset=offset+len[1])
	}
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

RStrings.get <- function (rawbuff, offset = 1 ) {
	count <- .getVarUint32(rawbuff,offset)
	offset <- offset + count[2]
	valueVec <- character(0)
	while (count > 0 ) { 
		len <- .getVarUint32(rawbuff[offset:length(rawbuff)])
		offset <- offset + len[2]
		if ( len[1] == 0 ) { 
			value <- (character(0))
		} else { 
			value <- iconv(list(rawbuff[offset:offset+len[1]-1]),from="UTF8")[[1]]
			offset <- offset+len[1]
			count <- count -1 
			valueVec[length(valueVec)+1] <- value
		}
		list(value=valueVec,offset=offset)
	}
}

RStrings.set <- function (rawbuff,offset,value ) {
	
}



#' unserialize a function packed using RRaw RType
#' @return a list with value=value and offset= next offset.
.unserializeFun <- function (rawBuff,offset) { 
	f_bytes <- RRaw.get(rawbuff,offset)
	offset <- f_bytes$offset
	if ( length(f_bytes$value)==0) {
		f <- NULL
	} else {
		f <- unserialize(f_bytes$value)
	}
	list(value=f,offset=offset)
}
