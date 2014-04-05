#!/usr/bin/env python

from crate import CrateIOV
from rod import RodIOV
from mur import MurIOV
from module import ModuleIOV
from chip import ChipIOV


def attributeListToDict(attributeList):
    dictionary = {}
    for attribute in iter(attributeList):
        name=attribute.specification().name()
        value=attribute.data()
        dictionary[name]=value
        
    return dictionary


class CrateApi:
    def __init__(self,db):
        self.session=db.session
        self.db=db

    def getInfo(self,iov='now',crate='list',slot='none',position='none',module='none',chip='none'):
        if crate in ['none','list','all']:
            return self.getCrateInfo(iov,crate)
        if slot=='none':
            return self.getCrateInfo(iov,crate)
        elif slot in ['list','all']:
            return self.getRODInfo(iov,crate,slot)

        if position=='none':
            return self.getRODInfo(iov,crate,slot)
        elif position in ['list','all']:
            return self.getMURInfo(iov,crate,slot,position)

        if module=='none':
            return self.getMURInfo(iov,crate,slot,position)

        if chip=='none':
            return self.getMODInfo(iov,crate,slot,position,module)

        return self.getChipInfo(iov,crate,slot,position,module,chip)
        

    def getCrateInfo(self,iov,crate='list'):
        reducedForm=False
        if crate=='none':
            return None
        elif crate=='list':
            reducedForm=True
            crate=-1
        elif crate=='all':
            reducedForm=False
            crate=-1
        constraints={ 'crate':int(crate)} 
        return self.getRows(CrateIOV(self.db,iov),constraints,reducedForm)
    
    
    def getRODInfo(self,iov,crate='none',slot='list'):
        reducedForm=False
        print '*****',crate,slot
        if crate in ['none','list','all']:
            return None
        if slot is 'none':
            return None
        elif slot=='list':
            reducedForm=True
            slot=-1
        elif slot=='all':
            print 'changing slot to -1'
            reducedForm=False
            slot=-1
        print '*****',crate,slot
        constraints={ 'crate':int(crate),'slot':int(slot)}
        return self.getRows(RodIOV(self.db,iov),constraints,reducedForm)

    def getMURInfo(self,iov,crate='none',slot='none',position='list'):
        reducedForm=False
        if crate in ['none','list','all']:
            return None
        if slot in ['none','list','all']:
            return None
        if position=='none':
            return None
        elif position=='list':
            position=-1
            reducedForm=True
        elif position=='all':
            reducedForm=False
            position=-1
        constraints={ 'crate':int(crate), 'slot':int(slot), 'position':int(position)}
        return self.getRows(MurIOV(self.db,iov),constraints,reducedForm)
    
    def getMODInfo(self,iov,crate='none',slot='none',position='none',module='list'):
        reducedForm=False
        if crate in ['none','list','all']:
            return None
        if slot in ['none','list','all']:
            return None
        if position in ['none','list','all']:
            return None

        if module == 'none':
            return None
        elif module=='list':
            module=-1
            reducedForm=True
        elif module=='all':
            reducedForm=False
            module=-1
            
        constraints={ 'crate':int(crate), 'slot':int(slot), 'position':int(position), 'module':int(module)}
        modIOV=ModuleIOV(self.db,iov)
        
        rows=self.getRows(modIOV,constraints,reducedForm)

        # now need to do second query to get extra information
        # weird structure of COOL + foreign tables means can't do a single JOIN to get this info
        # bizarre structure
        headers=rows['headers']
        index1=headers.index('moduleID')
        index2=headers.index('rmoduleID')
        modules=set()
        print rows['rows']
        for row in rows['rows']:
            values=row['values']
            print '*** : ',values[index1],values[index2]
            modules.add(values[index1])
            modules.add(values[index2])
        for module in modules:
            print module

        modIOV.updateIOV(iov,modules) #this triggers the extended form

        rows = self.getRows(modIOV,constraints,reducedForm)
        
        return rows
        #return self.getRows(ModuleIOV(self.db,iov),constraints,reducedForm)
        
    # not a clean API: suddenly module index becomes moduleID
    # oh well...
    def getChipInfo(self,iov,crate='none',slot='none',position='none',moduleID='none',chip='list'):
        reducedForm=False
        if crate in ['none','list','all']:
            return None
        if slot in ['none','list','all']:
            return None
        if position in ['none','list','all']:
            return None
        if moduleID in ['none','list','all']:
            return None
        if chip == 'none':
            return None
        elif chip=='list':
            chip=-1
            reducedForm=True
        elif chip=='all':
            reducedForm=False
            chip=-1
        constraints={'chip':chip}
        chipIOV=ChipIOV(self.db,iov)
        chipIOV.updateIOV(iov,moduleID)
        rows = self.getRows(chipIOV,constraints,reducedForm)
        return rows
        


    def getRows(self,module,constraints,reducedForm):
        schema=self.session.nominalSchema()
        self.session.transaction().start(True)
        query=schema.newQuery()

        module.buildConditions(query,constraints)
        module.buildOutputList(query,reducedForm)

        cursor=query.execute()
        rows=[]
        for row in iter(cursor):
            dict=attributeListToDict(row)
            rows.append(dict)
        del query
        self.session.transaction().commit()
        return module.prepareOutput(rows)
    
