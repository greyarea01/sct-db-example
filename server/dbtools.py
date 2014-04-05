#!/usr/bin/env python

from PyCool import cool
import coral
from CoolConvUtilities.AtlCoolLib import indirectOpen

class DBTools:
    def __init__(self):

        self.connected=False
        
    def getCOOLInfo(self,folderName,iov,channel):
        table = self.db.getFolder(folderName)
        description=table.description()
        begin=description.find('coracool')+9
        end=description.find('coracool',begin)-2
        fields=description[begin:end].split(':')
        coralTableName='COMP200_'+fields[0]
        coolKeyName=fields[1]
        coralKeyName=fields[2]
        obj=table.findObject(iov,channel)
        coolKeyValue=obj.payload()[coolKeyName]
        coolInfo={'keyName': coralKeyName,
                  'keyValue': coolKeyValue,
                  'tableName': coralTableName,
                  'begin':obj.since(),
                  'end':obj.until()}
        # coolInfo for future expansion
        return (coolKeyValue,coralTableName,coralKeyName)

    def initDB(self,file=''):
        if file == '':
            self.db = indirectOpen('COOLONL_SCT/COMP200',oracle=True)
            svc=coral.ConnectionService()
            self.session = svc.connect('oracle://ATLAS_COOLPROD/ATLAS_COOLONL_SCT')
            self.connected=True
        else:
            self.db = indirectOpen('sqlite://;schema='+file+';dbname=COMP200')
            svc = coral.ConnectionService()
            self.session = svc.connect('sqlite:///'+file)
            self.connected=True
        
        
