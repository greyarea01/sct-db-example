#!/usr/bin/env python

import coral
from tableiov import TableIOV

class MurIOV(TableIOV):
    def __init__(self,db,iov=None):
        TableIOV.__init__(self,db,iov)
        self.rodmurAlias='r'
        self.outputs=[]
        
    def updateIOV(self,iov):
        TableIOV.updateIOV(self,iov)
        (self.fkRODMUR,self.tableRODMUR,self.keyNameRODMUR)= self.db.getCOOLInfo('/SCT/DAQ/Configuration/RODMUR',self.iov_snapshot,0)
        
    def buildOutputList(self,query,reducedForm):
        query.addToTableList(self.tableRODMUR,self.rodmurAlias)
        
        outputsRODMUR=[ 'crate',
                        'rod',
                        'position',
                        'MUR' ]
        self.outputs=outputsRODMUR[:]
        
        for output in outputsRODMUR:
            query.addToOutputList(self.rodmurAlias+'.'+output,output)



    def buildConditions( self, query, constraints):
        condition=coral.AttributeList()
        conditionString=""
        
        #RODMUR conditions
        condition.extend('ct','int')
        condition['ct'].setData(constraints['crate'])
        
        condition.extend('fk','int')
        condition['fk'].setData(self.fkRODMUR)
        
        condition.extend('sl','int')
        condition['sl'].setData(constraints['slot'])
        conditionString+=self.rodmurAlias+'.'+self.keyNameRODMUR+'=:fk'
        conditionString+=' AND '+self.rodmurAlias+'.crate=:ct'
        conditionString+=' AND '+self.rodmurAlias+'.rod=:sl'
        if constraints['position']>=0:
            condition.extend('ps','int')
            condition['ps'].setData(constraints['position'])
            conditionString+=' AND '+self.rodmurAlias+'.position=:ps'
        print conditionString
        query.setCondition(conditionString,condition)

