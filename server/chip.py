#!/usr/bin/env python


import coral
from tableiov import TableIOV

class ChipIOV(TableIOV):
    def __init__(self,db,iov=None):
        TableIOV.__init__(self,db,iov)
        self.chipAlias='chp'

        

    def updateIOV(self,iov,module='none'):
        TableIOV.updateIOV(self,iov)
        if module=='none':
            return
        print '#####',module
        module=int(module)
        channel = int( (module%1000000000)+1)
        (fk,table,name) = self.db.getCOOLInfo('/SCT/DAQ/Configuration/Chip',self.iov_snapshot,channel)
        self.tableCHP=table
        self.keyNameCHP=name
        self.fkCHP=fk
        print '#### Chip:', channel, self.fkCHP, self.tableCHP, self.keyNameCHP


    def buildOutputList(self,query,reducedForm):
        query.addToTableList(self.tableCHP,self.chipAlias)
        outputsCHP = ['chip',
                      'active',
                      'address',
                      'config',
                      'mask0',
                      'mask1',
                      'mask2',
                      'mask3',
                      'vthr',
                      'vcal',
                      'delay',
                      'preamp',
                      'shaper',
                      'rc_function',
                      'rc_args',
                      'c_factor',
                      'target',
                      'trim']
        self.outputs = outputsCHP[:]
        for output in outputsCHP:
            query.addToOutputList(self.chipAlias+'.'+output,output)

    
        

    def buildConditions(self,query,constraints):
        condition=coral.AttributeList()
        conditionString=""

        #CHP conditions
        #condition.extend('fk','int')
        #condition['fk'].setData(self.fkCHP)

        #conditionString+=self.chipAlias+'.'+self.keyNameCHP+'=:fk'
        conditionString+=self.chipAlias+'.'+self.keyNameCHP+'='+str(self.fkCHP)
        
        if constraints['chip'] >=0:
            condition.extend('ch','int')
            condition['ch'].setData(constriants['chip'])
            conditionString+=' AND '+self.chipAlias+'.chip'


        print conditionString    
        query.setCondition(conditionString,condition)

        

