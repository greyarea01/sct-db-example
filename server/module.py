#!/usr/bin/env python


import coral
from tableiov import TableIOV

class ModuleIOV(TableIOV):
    def __init__(self,db,iov=None):
        TableIOV.__init__(self,db,iov)
        self.rodmurAlias='r'
        self.murAlias='m'
        self.modAlias='mod'
        self.rmodAlias='rmod'
        self.pmurAlias='pm'
        self.outputs=[]
        self.modKeys={}
        

    def updateIOV(self,iov,modules=None):
        TableIOV.updateIOV(self,iov)
        (self.fkRODMUR,self.tableRODMUR,self.keyNameRODMUR)= self.db.getCOOLInfo('/SCT/DAQ/Configuration/RODMUR',self.iov_snapshot,0)
        (self.fkMUR,self.tableMUR,self.keyNameMUR)= self.db.getCOOLInfo('/SCT/DAQ/Configuration/MUR',self.iov_snapshot,0)
        (self.fkPMUR,self.tablePMUR,self.keyNamePMUR)=self.db.getCOOLInfo('/SCT/DAQ/Configuration/PowerMUR',self.iov_snapshot,0)
        self.modKeys={}
        if modules==None:
            return
        for module in modules:
            channel = int( (module%1000000000)+1)
            (fk,table,name) = self.db.getCOOLInfo('/SCT/DAQ/Configuration/Module',self.iov_snapshot,channel)
            self.tableMOD=table
            self.keyNameMOD=name
            self.modKeys[module]=fk
            print module,channel,fk


    def buildOutputList(self,query,reducedForm):
        query.addToTableList(self.tableRODMUR,self.rodmurAlias)
        query.addToTableList(self.tableMUR,self.murAlias)
        query.addToTableList(self.tablePMUR,self.pmurAlias)
        
        outputsRODMUR=[ 'crate',
                     'rod',
                     'position']
        for output in outputsRODMUR:
            query.addToOutputList(self.rodmurAlias+'.'+output,output)
        outputsMUR=['module',
                    'moduleID',
                    'rmoduleID',
                    'rx0Fibre',
                    'rx1Fibre',
                    'txFibre']
        self.outputs=outputsMUR[:]

        
        for output in outputsMUR:
            query.addToOutputList(self.murAlias+'.'+output,output)

        query.addToOrderList('module')
        
        if len(self.modKeys)>0:
            outputsMOD=['group',
                        'active',
                        'select']

            query.addToTableList(self.tableMOD,self.modAlias)
            query.addToTableList(self.tableMOD,self.rmodAlias)
            
            for output in outputsMOD:
                query.addToOutputList(self.modAlias+'.'+output,output)
                query.addToOutputList(self.rmodAlias+'.'+output,'r'+output)
                self.outputs.append(output)
                self.outputs.append('r'+output)
            
                # PMUR outputs
        query.addToOutputList(self.pmurAlias+'.'+'crate','PowerSupply')
        query.addToOutputList(self.pmurAlias+'.'+'channel','PowerChannel')
        self.outputs.append('PowerSupply')
        self.outputs.append('PowerChannel')


    def buildConditions(self,query,constraints):
        condition=coral.AttributeList()
        conditionString=""
        
        #RODMUR conditions
        condition.extend('ct','int')
        condition['ct'].setData(constraints['crate'])
        condition.extend('fkRODMUR','int')
        condition['fkRODMUR'].setData(self.fkRODMUR)
        condition.extend('sl','int')
        condition['sl'].setData(constraints['slot'])
        conditionString=self.rodmurAlias+'.'+self.keyNameRODMUR+'=:fkRODMUR'
        conditionString+=' AND '+self.rodmurAlias+'.crate=:ct'
        conditionString+=' AND '+self.rodmurAlias+'.rod=:sl'

        condition.extend('ps','int')
        condition['ps'].setData(constraints['position'])
        conditionString+=' AND '+self.rodmurAlias+'.position=:ps'

        condition.extend('fkPMUR','int')
        condition['fkPMUR'].setData(self.fkPMUR)
        conditionString+=' AND '+self.pmurAlias+'.'+self.keyNamePMUR+'=:fkPMUR'
        
        if constraints['module']>=0:
            condition.extend('md','int')
            condition['md'].setData(constraints['module'])
            conditionString+=' AND '+self.murAlias+'.module=:md'
        #PMUR JOIN conditions
        conditionString+=' AND '+self.rodmurAlias+'.MUR='+self.pmurAlias+'.MUR'
        conditionString+=' AND '+self.pmurAlias+'.module='+self.murAlias+'.module'
        #MUR JOIN conditions
         
        conditionString+=' AND '+self.rodmurAlias+'.MUR='+self.murAlias+'.MUR'

        if len(self.modKeys) > 0:
            
            # MOD JOIN conditions
            # JOIN on the moduleID
            conditionString+=' AND '+self.murAlias+'.moduleID='+self.modAlias+'.id'
            conditionString+=' AND '+self.murAlias+'.rmoduleID='+self.rmodAlias+'.id'
            # JOIN on moduleID + fk
            conditionString+=' AND ('
            first=True
            for key in self.modKeys.keys():
                value=self.modKeys[key]
                print key,self.modKeys[key]
                if first:
                    first = False
                else:
                    conditionString+=' OR '
                conditionString+=' ( '+self.modAlias+'.id='+str(key)+' AND '+self.modAlias+'.'+self.keyNameMOD+'='+str(value)+' ) '
            conditionString+=')'
            # JOIN on rmoduleID + fk
            conditionString+=' AND ('
            first=True
            for key in self.modKeys.keys():
                value=self.modKeys[key]
                print key,self.modKeys[key]
                if first:
                    first = False
                else:
                    conditionString+=' OR '
                conditionString+=' ( '+self.rmodAlias+'.id='+str(key)+' AND '+self.rmodAlias+'.'+self.keyNameMOD+'='+str(value)+' ) '
            conditionString+=')'
        print conditionString    
        query.setCondition(conditionString,condition)

        

