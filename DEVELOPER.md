##                                CDS2 Project Developer Notes

_Climate Data Analytic Service provider built on scala, Spark, Akka, Haddop, and python tools such as UVCDAT, etc._

###  Configuration

The CDAS environment is initialized by sourcing the **{CDAS2}/bin/setup_runtime.sh** script.

####  Environment variables:
The following environment variables can be set to customize the environment:

    * CDAS_CACHE_DIR: Sets the location of the CDAS cache directory (default: ~/.cdas/cache).
    * CDWPS_HOME_DIR: Sets the location of the CDWPS home directory (default: {CDAS2}/../CDWPS).
    * CDSHELL_HOME_DIR: Sets the location of the CDSHELL home directory (default: {CDAS2}/../CDASClientConsole).

####  Configuration parameters:
During the CDAS build process a copy of the file _cdas.properties_ is copied to the CDAS cache directory.
    Edit this file to customize the CDAS installation. 
    
Here are descriptions of the currently active parameters:
     
     * wps.response.syntax: Determines the syntax of the WPS responses.  The possibilities are:
        - wps: Use syntax conforming to the wps schema.
        - generic:  Use a simpler (and easier to parse) generic format.
     * wps.server.proxy.href: Http address used to query the server for results (e.g. http://localhost:9001)
     * max.procs: The maximum number of processers that CDAS ia allowed to utilize.
     * ncml.recreate: When set to true the server will recreate the NCML files associated with all registered collections.  When false (the default) it will use the existing NCML files.
     

###  Kernel Development

The use may contribute new analysis modules (kernels) developed in java, scala, or python.  

#### Python Kernels
Here are some pointers on developing new python kernels. Some example code is displayed below.

    1. Create a new python file called {moduleName}.py under {CDAS2}/src/pycdas/kernels/internal. All kernels defined in this file will be automatically registered in WPS under a KernelModule named {moduleName}. In a future version it will be possible to locate this file outside of {CDAS}.
    2. Create a class that extends either Kernel (for numpy operations) of CDMSKernel (for cdms2 operations) from pycdas.kernels.Kernel.  
    3. An example Kernel definition, and a corresponding WPS request, are shown below.  One can also take a look at any of the existing python files in the pycdas/kernels/internal directory.
    4. Configure the Kernel by passing a pycdas.kernels.Kernel.KernelSpec instance to the __init__ method.  
    5. The arguments of the KernelSpec define the kernelId, title, description, and configuration parameters for the Kernel.  The configuration parameters are discussed below in the parallelization section.
    6. Define the kernel's executeOperation method to define an analytic operation with a single input.  Alternately, override the kernel's executeOperations method to define an analytic operation with multiple inputs.
    7. The execute method's 'input' argument provides input data arrays in various formats:
         input.array():        numpy ndarray
         input.getVariable():  cdms2 Variable instance
    8. Parameters from the WPS 'operation' specification (e.g. "axes":"xy") can be accessed using the task.metadata dictionary.
    9. The createResult method (on either CDMSKernel or npArray) creates a properly formatted kernel result.
    10. The Kernel is referenced in the WPS request (see below) using the id "python.{moduleName}.{kernelId}"
    
##### Python kernel example code (from file {CDAS2}/src/pycdas/kernels/internal/cdmsModule.py).

```python
from pycdas.kernels.Kernel import CDMSKernel, KernelSpec
import cdutil

class AverageKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average", "Averages the inputs using UVCDAT with area weighting by default", parallize=True ) )

    def executeOperation(self, task, input):
        variable = input.getVariable()
        axis = task.metadata.get("axis","xy")
        weights = task.metadata.get("weights","generate").split(",")
        if( len(weights) == 1 ): weights = weights[0]
        action = task.metadata.get("action","average")
        returned = 0
        result_var = cdutil.averager( variable, axis=axis, weights=weights, action=action, returned=returned )
        return self.createResult( result_var, _input, task )

```
##### Corresponding WPS request
```
...datainputs=[
    domain=[ {"name":"d0","time":{"start":0,"end":10,"system":"indices"}} ],
    variable=[ {"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"} ],
    operation=[ {"name":"python.cdmsModule.ave","input":"v1","domain":"d0","axes":"xy"} ]
    ]
    
```

##### Python Kernel Parallelization
The configuration parameters defined in the KernelSpec specify how CDAS will handle the parallelization of the Kernel.   Python Kernels can be 
either parallelizable _(parallize=True)_ or non-parallelizable _(parallize=False)_.  If a kernel is non-parallelizable then CDAS assumes that the kernel will either run serially or handle 
its own parallelization internally.  If a kernel is parallelizable then CDAS will handle the parallelization.  CDAS parallelization occurs as follows:

    1. CDAS partitions the input into N fragments by splitting the data over time into N non-overlapping continuous time segments of approx equal length.
    2. CDAS create N copies of the kernel and calls the N executeOperation(s) methods in parallel.  Each method call is passed a different input fragment of the same variable(s).
    3. The kernel executions produce N result fragments whcih are then combined to produce the final result.
    4. The combination of fragments proceeds as follows:
        - 

###  Rebuilding

After modifying the CDAS source code (or pulling a new snapshot from github), the framework can be rebuilt using some or all of the 
commands in the **{CDAS2}/bin/update.sh** script.

###  Distribution

####  Updating the python distribution:

    1) Push a github tag for version x.x:
    
        >> git push origin HEAD                   # Push any existing commits
        >> git push origin :refs/tags/x.x         # Delete the remote tag if it already exists
        >> git tag -fa x.x                        # Tag the current HEAD
        >> git push --tags origin master          # Pust the tag to origin
        
    2) Edit <version> and <download_url> in setup.py with new version tag x.x
    
    3) Upload new dist to pypi:
     
        >> python setup.py sdist upload -r pypi
        
    4) Build and upload conda package:
    
        >> conda skeleton pypi pycdas       # Update the conda build skeleton with the new pycdas version number
        >> conda build pycdas               # Builds the conda package and prints the <build-path>
        >> anaconda login
        >> anaconda upload <build-path>
        >> anaconda logout
        
  
        
    