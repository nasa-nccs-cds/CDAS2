###                                CDS2 Project

_Climate Data Analytic Service provider built on scala, Spark, Akka, Haddop, and python tools such as UVCDAT, etc._

####  To Update python dist:

    1) Push new github tag:
    
        >> git push origin HEAD
        >> git tag x.x -m "Adds a new tag so we can push this dist to PyPI."
        >> git push --tags origin master
        
    2) Edit <version> and <download_url> in setup.py with new version tag x.x
    
    3) Upload new dist to pypi:
     
        >> python setup.py sdist upload -r pypi
        
    4) Build and upload conda package:
    
        >> conda skeleton pypi pycdas       # Update the conda build skeleton with the new pycdas version number
        >> conda build pycdas               # Builds the conda package and prints the <build-path>
        >> anaconda login
        >> anaconda upload <build-path>
        >> anaconda logout
        
  
        
    