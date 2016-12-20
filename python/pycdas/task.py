from messageParser import mParse

class Task:
    def __init__( self, task_header ):
        headerToks = task_header.split('|')
        taskToks = headerToks[1].split('-')
        opToks = taskToks[0].split('.')
        self.module = ".".join( opToks[0:2] )
        self.op = opToks[-1]
        self.rId = taskToks[1]
        self.inputs = headerToks[2].split(',')
        self.metadata = mParse.s2m( headerToks[3] )

    def __str__(self): return ":".join( [ self.module, self.op ] )