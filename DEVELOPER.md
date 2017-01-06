###                                CDS2 Project

_Climate Data Analytic Service provider built on scala, Spark, Akka, Haddop, and python tools such as UVCDAT, etc._

####  To Update python dist:

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
        
  
        
    