# TODO: Add comment
# 
# Author: dmitriy
###############################################################################



test1 <- function () {
	
	
}


#context("rcrunchtests")
#test_that("",test1))


################## my scratchpad 

fname <- "/home/dmitriy/projects/github/crunchR/crunchR/simfun.dat"
fbytes <- file.info(fname)$size 
		
f <- file ( fname ,"rb")
a<- readBin(f,"raw",fbytes)
close(f)

library(crunchR)
rpipe <- crunchR.TwoWayPipe$new(new(J("java/lang/String"),"AAA") )
rtype <- crunchR.DoFnRType$new(rpipe)
doFn <- rtype$get(a)

doFn <- DoFnRType.get(a)$value 
		
rawbuff <- a
offset <- 1
