#!/usr/bin/env python

import coral
from tableiov import TableIOV
    
class CrateIOV(TableIOV):
    def __init__(self,db,iov=None):
        TableIOV.__init__(self,db,iov)
        self.tableAlias='cr'
        self.outputs=[ 'crate',
                  'triggerFrequency',
                  'resetFrequency',
                  'triggerTypeTTC',
                  'triggerTypeTIM',
                  'BCIDOffset',
                  'TrigEnablesInt',
                  'TrigEnablesTTC',
                  'UseRunMode',
                  'TriggerDelay',
                  'ClockDelay']
        
    def updateIOV(self,iov):
        TableIOV.updateIOV(self,iov)
        (self.fk,self.table,self.keyName)= self.db.getCOOLInfo('/SCT/DAQ/Configuration/TIM',self.iov_snapshot,0)
        
    def buildOutputList(self,query,reducedForm):
        query.addToTableList(self.table,self.tableAlias)

        for output in self.outputs:
            query.addToOutputList(self.tableAlias+'.'+output,output)


    def buildConditions(self,query, constraints):
        condition=coral.AttributeList()
        conditionString=""

        condition.extend('fk','int')
        condition['fk'].setData(self.fk)
        conditionString+=self.tableAlias+'.'+self.keyName+'=:fk'

        if constraints['crate']>=0:
            condition.extend('ct','int')
            condition['ct'].setData(constraints['crate'])
            conditionString+=' AND '+self.tableAlias+'.crate=:ct'

        query.setCondition(conditionString,condition)

