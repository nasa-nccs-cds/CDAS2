###                                CDS2 Project

_Climate Data Analytic Service provider built on scala, Spark, Akka, Haddop, and python tools such as UVCDAT, etc._

####  Prerequisite: Install the Scala develpment tools:

    1) Scala:                     http://www.scala-lang.org/download/install.html                   
                        
    
    2) Scala Build Tool (sbt):    http://www.scala-sbt.org/0.13/docs/Setup.html
                        

####  Install and run CDS2:

    1) Checkout the CDS2 sources:

        >> git clone https://github.com/nasa-nccs-cds/CDAS2.git 

    2) Build the application:

        >> cd CDAS2
        >> sbt publish-local

     3) Run unit tests:

        >> sbt test

     4) Access demos:

        Designed to be deployed with the CDWPS framework (https://github.com/nasa-nccs-cds/CDWPS)

####  Python/NetCDF support through Conda::

    1) Install Anaconda: https://github.com/UV-CDAT/uvcdat/wiki/Install-using-Anaconda
    
    2) Install packages using conda:
        
        >> conda create -n uvcdat-2.8 -c uvcdat -c conda-forge uvcdat
        >> conda install -c conda-forge nco
        >> source activate uvcdat-2.8
        >> conda install pyzmq


####  Code development:

    1) Install IntelliJ IDEA CE from https://www.jetbrains.com/idea/download/ with Scala plugin enabled.
    
    2) Start IDEA and import the CDAS2 Project from Version Control (github) using the address https://github.com/nasa-nccs-cds/CDAS2.git.
    
    

    

