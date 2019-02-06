#################################################################
# Autor: Manuel Carpente
# Version: 1.0
# Objective: These script read one Hive table and send this data to Atlas as entity
# This script need one configFile that contains these variables:
# - hiveHost
# - hivePost
# - database
# - tablename
# - atlasHost
# - atlasPort
# - atlasUser
# - atlasUserPass
# - clusterName
# ################################################################

# Example configFile:
########################################
# hiveHost=hdps00.pro.torusware.com
# hivePort=10000
# database= liberbank_consumption
# tablename= precos_tablon
# atlasHost = hdps00.pro.torusware.com
# atlasPort= 21000
# atlasUser=admin
# atlasUserPass=admin
# clusterName=pbd_18016_liberbank_dg
######################################

from pyhive import hive
from dateutil import parser
import sys
import os
import json
from atlasclient.client import Atlas

def getParameters(configFile):
  f = open(configFile)
  result={}
  # Save parameters into diccionary
  for line in f:
    if line.startswith("hiveHost"):
      result.update({'hiveHost':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("hivePort"):
      result.update({'hivePort':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("database"):
      result.update({'database':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("tablename"): 
      result.update({'tablename':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("atlasHost"): 
      result.update({'atlasHost':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("atlasPort"): 
      result.update({'atlasPort':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("atlasUser"):
      result.update({'atlasUser':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("atlasUserPass"):
      result.update({'atlasUserPass':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("clusterName"):
      result.update({'clusterName':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
  return result

def checkParameters(params):
# Check that configFile contains parameters and parameters are not null
  if ('hiveHost' in params and 'hivePort' in params and 'database' in params and 'tablename' in params and 'atlasHost' in params and 'atlasPort' in params and 'atlasUser' in params and 'atlasUserPass' in params and 'clusterName' in params):
    return (params['hiveHost'] != '' and params['hivePort'] != '' and params['database'] != '' and params['tablename'] != ''and params['atlasHost'] != '' and params['atlasPort'] != '' and params['atlasUser'] != '' and params['atlasUserPass'] != ''  and params['clusterName']!='')

##### Start define auxiliar funtions to create AtlasEntity Json ###########
def createAtlasEntity(database,tablename,parentGuid,createTime,comment,columns,clusterName):
  # Define template entity
  entityModel={"typeName":"Liberbank_Dataset", "status":"ACTIVE", "version":0}
  entity=entityModel
  # Add guid 
  entity.update({"guid":str(parentGuid)})
  attributes={}
  # Insert into attributes values that read on hive table
  attributes.update({"name":tablename})
  attributes.update({"qualifiedName":"Liberbank_Dataset."+database+"."+tablename})
  attributes.update({'datasetRef': [{'typeName':'hive_table',"uniqueAttributes":{"qualifiedName":database+"."+tablename+"@"+clusterName}}]})
  attributes.update({"createTime":createTime})
  attributes.update({"comment":comment})
  attributes.update({"columns":columns})
  entity.update({"attributes":attributes})
  
  return entity

def createColumnField(guid):
  # Add guid column and typeName foreach column on Hive Table
  column_model= {"typeName":"Liberbank_Column"}
  column=column_model
  column.update({"guid":str(guid)})
  return column
  

def createReferredEntity(guid,parentGuid,columnName,columnType,comment,database,tableName):
  # Create section referrerEntity that describe columns on Hive table
  referredEntityModel= {"status":"ACTIVE","version":0,"typeName":"Liberbank_Column"}
  datasetModel= {"typeName":"Liberbank_Dataset"}
  entityField=referredEntityModel
  # Add guid
  entityField.update({"guid":str(guid)})
  attributes={}
  # Add required fields on this section, name, qualifiedName, type and comment
  attributes.update({"name":columnName})
  attributes.update({"qualifiedName":"Liberbank_Dataset."+database+"."+tableName+"."+columnName})
  attributes.update({"type":columnType})
  attributes.update({"comment":comment})
  dataset=datasetModel
  dataset.update({"guid":str(parentGuid)})
  attributes.update({"dataset":dataset})
  entityField.update({"attributes":attributes})
  return entityField
  
def sendAtlasEntity(body, atlasHost, atlasPort, atlasUser,atlasUserPass):
  # Get connection to Atlas server
  client=Atlas(atlasHost,port=atlasPort,username=atlasUser,password=atlasUserPass)
  # Convert string to json 
  entity_dict= json.loads(body.replace('\'','"'))
  # send petition to atlas
  client.entity_post.create(data=entity_dict)
##### End define auxiliar funtions to create AtlasEntity Json ###########

# Function to create AtlasEntity from hive Table
def hiveTableToAtlasEntity(params):
    # Get connection to Hive
    cursor=hive.connect(host=params['hiveHost'],port=params['hivePort'],auth='KERBEROS',kerberos_service_name='hive',database=params['database']).cursor()
    parentGuid=-12
    cmd = 'DESCRIBE FORMATTED '+params['tablename']
    cursor.execute(cmd)
    info=cursor.fetchall()
    # Find createTime field
    for tupla in info:
      if "CreateTime:" in tupla[0]:
        createTime=tupla[1]
        dt=parser.parse(createTime)
        createTime=dt.strftime("%Y-%m-%dT%H:%M:%S.%sZ")
      if tupla[1] is not None and "comment" in tupla[1] and tupla[0]=='':
        comment = tupla[2]   
    # Parse table columns
    guid=parentGuid
    columns = []
    referredEntity= {}
    comment = ''
    cursor.execute("DESCRIBE "+params['tablename'])
    columnInfo=cursor.fetchall()
    for tupla in columnInfo:
      if tupla[0]!= '' and '#' not in tupla[0]:
        guid = guid - 1
        columns.append(createColumnField(guid))
        refEnt=createReferredEntity(guid,parentGuid,tupla[0],tupla[1],tupla[2],params['database'],params['tablename'])
        referredEntity.update({str(guid):refEnt})
      else:
        break;
  
    resEntity=createAtlasEntity(params['database'],params['tablename'],parentGuid,createTime,comment,columns,params['clusterName'])
    result={"entity":resEntity,"referredEntities":referredEntity}
    sendAtlasEntity(str(result),params['atlasHost'],params['atlasPort'],params['atlasUser'],params['atlasUserPass'])
  
if __name__ == "__main__":
  if len(sys.argv)==2 and os.path.isfile(sys.argv[1]):
    # Capture params from configFile
    params=getParameters(sys.argv[1])
    if checkParameters(params):
      hiveTableToAtlasEntity(params)
    else:
      print ("Make sure that parameters exists and not empty value. Params: hiveHost, hivePort, database, tablename, atlasHost, atlasPort,atlasUser, atlasUserPass, clusterName") 
  else:
    print ("Make sure that config file exists and path to file is correct")
