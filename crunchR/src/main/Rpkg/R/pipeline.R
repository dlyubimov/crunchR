
#'
#' crunchR's pipeline R5 class
#' 
crunchR.Pipeline <- setRefClass("Pipeline", fields = c("jpipeline"))

Pipeline.initialize <- function (jpipeline){
	stopifnot (class(jpipeline)!="")
	jpipeline <<-jpipeline
}

crunchR.Pipeline$methods (
		initialize = Pipeline.initialize
		)