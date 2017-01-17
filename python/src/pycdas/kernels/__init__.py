import logging, os

lname = "worker"
log_file = os.path.expanduser('~/.cdas/' + lname + "-" + str(os.getpid()) +'.log')
logger = logging.getLogger( lname )
formatter = logging.Formatter(lname + ': %(asctime)s %(levelname)s %(message)s')
handler = logging.FileHandler( log_file )
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)