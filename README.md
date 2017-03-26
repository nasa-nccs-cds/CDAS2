###                                CDS2 Project

_Climate Data Analytic Service provider built on scala, Spark, Akka, Haddop, and python tools such as UVCDAT, etc._

####  Prerequisite: Install the Java/Scala develpment tools:

    1) Java SE Platform (JDK) 1.8:   http://www.oracle.com/technetwork/indexes/downloads/index.html
    2) Scala:                        http://www.scala-lang.org/download/install.html
    3) Scala Build Tool (sbt):       http://www.scala-sbt.org/0.13/docs/Setup.html

####  Install and run CDS2:

    1) Checkout the CDS2 sources:

        >> cd <prefix>
        >> git clone https://github.com/nasa-nccs-cds/CDAS2.git 

    2) Build the application:

        >> cd CDAS2
        >> sbt publish
        >> sbt stage

     3) Run unit tests:

        >> sbt test

     4) Source the setup file to configure the runtime environment:

        >> source <prefix>/CDAS2/bin/setup_runtime.sh

     6) Access demos:

        Designed to be deployed with the CDWPS framework (https://github.com/nasa-nccs-cds/CDWPS)

####  Python/NetCDF support through Conda::

    1) Install Anaconda: https://github.com/UV-CDAT/uvcdat/wiki/Install-using-Anaconda
    
    2) Create cdas2 conda environment:
        
        >> conda create -n cdas2  -c conda-forge -c uvcdat/label/nightly -c uvcdat uvcdat nco pyzmq psutil
        
    3) Initialize shell enviromnment for cdas:
    
        >> source <prefix>/CDAS2/bin/setup_runtime.sh
        >> source activate cdas2

####  Code development:

    1) Install IntelliJ IDEA CE from https://www.jetbrains.com/idea/download/ with Scala plugin enabled.
    
    2) Start IDEA and import the CDAS2 Project from Version Control (github) using the address https://github.com/nasa-nccs-cds/CDAS2.git.
    
    

