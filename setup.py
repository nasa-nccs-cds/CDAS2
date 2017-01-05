from distutils.core import setup
setup(
  name = 'pycdas',
  packages = ['pycdas'],
  version = '1.2',
  package_dir = {'': 'python'},
  description = 'Python portal and worker components of the Climate Data Analytic Services (CDAS) framework',
  author = 'Thomas Maxwell',
  author_email = 'thomas.maxwell@nasa.gov',
  url = 'https://github.com/nasa-nccs-cds/CDAS2.git',
  download_url = 'https://github.com/nasa-nccs-cds/CDAS2/tarball/1.2',
  keywords = ['climate', 'data', 'analytic', 'services'],
  classifiers = [],
)