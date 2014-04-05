
from iov import getIOV

class TableIOV:

    def __init__(self,db,iov):
        self.iov_snapshot=0
        self.db=db
        self.updateIOV(iov)
        self.outputs=[]

    def updateIOV(self,iov):
        self.iov_snapshot=getIOV(iov)
        
    def prepareOutput(self,rows):
        output={}
        output['iov']=self.iov_snapshot
        output['headers']=self.outputs[:]
        output['rows']=[]
        for row in rows:
            obj={'values':[]}
            for key in self.outputs:
                obj['values'].append(row[key])
            output['rows'].append(obj)
        return output
    
