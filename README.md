Introduction
===============
CrunchR is an R wrapper for Apache Crunch that allows you to write MapReduce pipelines in R.

Prerequisites
===============
For starters, you want to be running Linux-- we're not quite ready for OS X.

To use R Crunch, you need to have the following R packages installed locally:
* rJava
* roxygen2
* bitops
* RProtoBuf (optional)

You will also need the protocol buffer compiler, `protoc`, version 2.4.1 installed on your path.

Getting Started
================
We're still in the phase of getting everything to play nicely together. You can try everything out
by running the following:

	cd crunchR
	./install-snapshot-rpkg.sh

Assuming that works well, there are some example R scripts under the `examples` directory that
you can use to test out loading the `crunchR` library and running simple pipelines.

License
=======
Apache 2.0
