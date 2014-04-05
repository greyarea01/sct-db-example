#!/usr/bin/env python


import coral
from tableiov import TableIOV


class RodIOV(TableIOV):
    def __init__(self,db,iov=None):
        TableIOV.__init__(self,db,iov)
        self.tableAlias='rd'
        self.slvAlias='sl'

    def updateIOV(self,iov):
        TableIOV.updateIOV(self,iov)
        (self.fkROD,self.tableROD,self.keyNameROD)= self.db.getCOOLInfo('/SCT/DAQ/Configuration/ROD',self.iov_snapshot,0)
        (self.fkSLV,self.tableSLV,self.keyNameSLV)= self.db.getCOOLInfo('/SCT/DAQ/Configuration/Slave',self.iov_snapshot,0)
        
    def buildOutputList(self,query,reducedForm):
        query.addToTableList(self.tableROD,self.tableAlias)
        query.addToTableList(self.tableSLV,self.slvAlias+'0')
        query.addToTableList(self.tableSLV,self.slvAlias+'1')
        query.addToTableList(self.tableSLV,self.slvAlias+'2')
        query.addToTableList(self.tableSLV,self.slvAlias+'3')
        
        
        outputsROD=[ 'crate',
                     'slot',
                     'ROB',
                     'SRCid',
                     'BCIDOffset']
        self.outputs=outputsROD[:]
        
        for output in outputsROD:
            query.addToOutputList(self.tableAlias+'.'+output,output)
            
        outputSLV=[ 'ipramFile',
                    'idramFile',
                    'extFile']
        for output in outputSLV:
            query.addToOutputList(self.slvAlias+'0.'+output,output+'0')
            query.addToOutputList(self.slvAlias+'1.'+output,output+'1')
            query.addToOutputList(self.slvAlias+'2.'+output,output+'2')
            query.addToOutputList(self.slvAlias+'3.'+output,output+'3')
            self.outputs.append(output+'0')
            self.outputs.append(output+'1')
            self.outputs.append(output+'2')
            self.outputs.append(output+'3')
            
            


    def buildConditions( self, query, constraints):
        condition=coral.AttributeList()
        conditionString=""
        
        #ROD conditions
        condition.extend('ct','int')
        condition['ct'].setData(constraints['crate'])
        condition.extend('fkROD','int')
        condition['fkROD'].setData(self.fkROD)

        conditionString+=self.tableAlias+'.crate=:ct AND '+self.tableAlias+'.'+self.keyNameROD+'=:fkROD'

        if constraints['slot']>=0:
            condition.extend('sl','int')
            condition['sl'].setData(constraints['slot'])
            conditionString+=' AND '+self.tableAlias+'.slot=:sl'
            
        #SLV conditions

        condition.extend('fkSLV','int')
        condition['fkSLV'].setData(self.fkSLV)
        
        conditionString+=' AND '+self.slvAlias+'0.'+self.keyNameSLV+'=:fkSLV'
        conditionString+=' AND '+self.slvAlias+'1.'+self.keyNameSLV+'=:fkSLV'
        conditionString+=' AND '+self.slvAlias+'2.'+self.keyNameSLV+'=:fkSLV'
        conditionString+=' AND '+self.slvAlias+'3.'+self.keyNameSLV+'=:fkSLV'
        
        # ROD<->SLV join conditions
        conditionString+=' AND '+self.slvAlias+'0.id='+self.tableAlias+'.slave0'
        conditionString+=' AND '+self.slvAlias+'1.id='+self.tableAlias+'.slave1'
        conditionString+=' AND '+self.slvAlias+'2.id='+self.tableAlias+'.slave2'
        conditionString+=' AND '+self.slvAlias+'3.id='+self.tableAlias+'.slave3'
        
        query.setCondition(conditionString,condition)

