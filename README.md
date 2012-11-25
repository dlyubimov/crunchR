Introduction
===============
CrunchR is an R wrapper for Apache Crunch that allows you to write MapReduce pipelines in R.

Prerequisites
===============
For starters, you want to be running Linux-- we're not quite ready for OS X.

To use Crunch R, you need to have the following R packages installed locally:
* rJava
* roxygen2
* bitops
* RProtoBuf (optional)

You will also need the protocol buffer compiler, `protoc`, version 2.4.1 installed on your path.

Another thing is that your map/reducer tasks on the cluster will have to have access to the same as above 
plus they need to be able to load JRI library. One way to do it is to supply -Djava.library.path 
to children as in follows 

    <property>
       <name>mapred.child.java.opts</name>
       <value>-Djava.library.path=/home/dmitriy/R/x86_64-pc-linux-gnu-library/2/rJava/jri </value>
       <final>false</final>
    </property>

Perhaps another possible way to install it is just to soft-link the libjri.so into your hadoop native libs folder.
You can figure location of libjri.so by running 

    system.file("jri",package="rJava")

after installing rJava package.


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
