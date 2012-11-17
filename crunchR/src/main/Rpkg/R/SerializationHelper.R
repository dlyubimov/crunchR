
# serialization helpers

library(bitops)

.getShort <- function (rawbuff, offset = 1 ) {
	# assume little endian packing 
	as.integer(sum(bitShiftL(rawbuff[offset:(offset+1)],8*(0:1))))
}

.setShort <- function (x) {
	stopifnot(x<0xFFFF & x >=0 )
	as.raw(bitAnd(bitShiftR(x,(0:1)*8),0xFF))
}

.setVarUint32V1 <- function (val ) {
	
	b7 <- bitAnd(bitShiftR(val,7*(0:4)),0x7f)
	l <- length(which(cumsum(b7[5:1])[5:1]>0))
	if( l>1 ) b7[1:(l-1)] <- bitOr(0x80,b7[1:(l-1)])
	if ( l == 0 ) l <-  1
	as.raw(b7[1:l])
}

.setVarUint32V2 <- function (val ) {
	r <- raw(5)
	i <- 1
	mask <- bitFlip(0x7f)
	repeat {
		if ( bitAnd(val,mask)==0 ) {
			r[i] <- as.raw(val)
			break
		} else { 
			r[i] <- as.raw(bitOr(bitAnd(val,0x7f),0x80))
		}
		i <- i+1
		val <- bitShiftR(val,7)
	}
	r[1:i]	
}

#' get variable length 32bit integer
#' @return vector of 2 integers. 1st integer is the value. 
#'  2nd value is number of bytes packed representation 
#' was taking in the input buffer. We actually return 
#' value as numeric because integer mode in R can't 
#' represent 0xFFFFFFFF. on the java side, this would
#' have been -1.
.getVarUint32V1 <- function (rawbuff, offset = 1L ) {
	l <- offset + 4L
	if ( l > length(rawbuff) ) l <- length(rawbuff)
	r <- as.integer(rawbuff[offset:l])
	l <- which(bitAnd(r,0x80)==0)
	if ( length(l)==0) l <- 1L else l <- l[1]
	# trim to actual variable length detected.
	r<- r[1:l]
	if ( l > 1 ) 
		r[1:l-1L] <- bitAnd(r[1:l-1L],0x7f)
	
	c( sum(bitShiftL(r,7*(0:(length(r)-1)))), l)
}


# looks like even when compiled, 
# the loop-free version runs faster.
# if there's a better way to do it 
# then we will do it later...

.setVarUint32 <- .setVarUint32V1 
.getVarUint32 <- .getVarUint32V1

.setVarInt32 <- function (x) {
	.setVarUint32(bitXor(bitShiftL(x,1),(sign(x)<0)*0xFFFFffff))
}

.getVarInt32 <- function (rawbuff, offset=1L) {
	r <- .getVarUint32(rawbuff,offset)
	neg <- bitAnd(r[1],1)!=0
	r[1]<- bitShiftR(bitXor(r[1],0xFFFFffff*neg),1)-neg*0x80000000
	r
}
