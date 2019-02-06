#################################################################
# Autor: Manuel Carpente
# Version: 1.0
# Objective: These script receive a json with operation name (create, delete, update) and params needed in each case.
# This script need one configFile that contains these variables:
# - atlasHost
# - atlasPort
# - atlasUser
# - atlasUserPass
# ################################################################

# Example configFile:
########################################
# Example configFile:
########################################
# atlasHost = hdps00.pro.torusware.com
# atlasPort= 21000
# atlasUser=admin
# atlasUserPass=admin
######################################

import sys
import os
import json
import json
from atlasclient.client import Atlas

def getParameters(configFile):
  f = open(configFile)
  result={}
  # Save parameters into diccionary
  for line in f:
    if line.startswith("atlasHost"): 
      result.update({'atlasHost':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("atlasPort"): 
      result.update({'atlasPort':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("atlasUser"):
      result.update({'atlasUser':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
    if line.startswith("atlasUserPass"):
      result.update({'atlasUserPass':(line[line.find('=')+1:len(line)-1]).lstrip().rstrip()})
  return result
  
def checkParameters(params):
  if ('atlasHost' in params and 'atlasPort' in params and 'atlasUser' in params and 'atlasUserPass' in params):
    return (params['atlasHost'] != '' and params['atlasPort'] != '' and params['atlasUser'] != '' and params['atlasUserPass'] != '' )
    
def sendAtlasEntity(body, atlasHost, atlasPort, atlasUser,atlasUserPass):
  # Get connection to Atlas server
  client=Atlas(atlasHost,port=atlasPort,username=atlasUser,password=atlasUserPass)
  # Convert string to json 
  entity_bulk= json.loads(body.replace('\'','"'))
  print (entity_bulk)
  # send petition to atlas
  client.entity_bulk.create(data=entity_bulk)

def setAttributes():
  attributes={}
  # Set optional fields if exists
  if "createTime" in data['fields']: 
    attributes.update({"createTime":data['fields']['createTime']})
  if "periodicity" in data['fields']: 
    attributes.update({"periodicity":data['fields']['periodicity']})
  if "artifact" in data['fields']: 
    attributes.update({"artifact":data['fields']['artifact']})
  if "service" in data['fields']: 
    attributes.update({"service":data['fields']['service']})
  if "version" in data['fields']: 
    attributes.update({"version":data['fields']['version']})
  if "executionTimes" in data['fields']: 
    attributes.update({"executionTimes":data['fields']['executionTimes']})
  # Set non optional fields
  attributes.update({"name":data['fields']['name']})
  # Type on parameters are Liberbank_Process_Gen or Liberbank_Process_Exec 
  attributes.update({"qualifiedName": data['fields']['typeName']+'.'+data['fields']['name']})
  # Depend typeName, Process_Gen has executionDates and Process_Exect has startTime and endTime
  if data['fields']['typeName'] == 'Liberbank_Process_Gen':
    attributes.update({"executionDates":data['fields']['executionDates']})
  else:
    if data['fields']['typeName'] == 'Liberbank_Process_Exec':
      attributes.update({"startTime":data['fields']['startTime']})
      attributes.update({"endTime":data['fields']['endTime']})
  attributes.update({"userName":data['fields']['userName']})
  attributes.update({"operations":data['fields']['operations']})
  # Create inputs array for send to Atlas with params
  paramsInputs=data['fields']['inputs']
  processInputs=[]
  for paramInput in paramsInputs:
     processInputs.append({"typeName":paramInput['typeName'], 'uniqueAttributes':{
                    "qualifiedName":paramInput['qualifiedName']}})
                    
  attributes.update({"inputs":processInputs})
  # Create outputs array for send to Atlas with params
  paramsOutputs=data['fields']['outputs']
  processOutputs=[]
  for paramOutput in paramsOutputs:
     processOutputs.append({"typeName":paramOutput['typeName'], 'uniqueAttributes':{
                    "qualifiedName":paramOutput['qualifiedName']}})
  attributes.update({"outputs":processOutputs})
  return attributes
  
def createGenProcess(data,atlasHost, atlasPort, atlasUser,atlasUserPass):
  # Set guid (Non optional field)
  entityFields={"guid":str(-123),"status":"ACTIVE","version":0,"typeName":data['fields']['typeName']}
  attributes=setAttributes()
  # Print result, to test
  # Add attributes field to entity Definition
  entityFields.update({"attributes":attributes})
  # Create body to atlas Petition
  body={"entities":[entityFields]}
  # Call function to create process
  sendAtlasEntity(str(body), atlasHost, atlasPort, atlasUser,atlasUserPass)


def updataGenProcces(data,atlasHost, atlasPort, atlasUser,atlasUserPass):
  # Get connection to Atlas
  client=Atlas(atlasHost,port=atlasPort,username=atlasUser,password=atlasUserPass)
  # Find Process by type and qualifiedName
  entity = client.entity_unique_attribute(data['fields']['typeName'], qualifiedName=data['fields']['typeName']+"."+data['fields']['name'])
  # Save executionDates on array
  entityExecutionDates=entity.entity['attributes']['executionDates']
  # Obtain new executionDates from params file
  for dates in data['fields']['executionDates']:
    entitiExecutionDates.append(dates)
  # Assign new value to attribute
  entity['attributes']['executionDates']= entityExecutionDates
  # Call atlas to update attribute (executionDates)
  entity.update(attribute='executionDates')
  
def deleteGenProcess(data,atlasHost, atlasPort, atlasUser,atlasUserPass):
  # Get connection to Atlas
  client=Atlas(atlasHost,port=atlasPort,username=atlasUser,password=atlasUserPass)
  # Find object by type and qualifiedName
  entity = client.entity_unique_attribute(data['fields']['typeName'], qualifiedName=data['fields']['typeName']+"."+data['fields']['name'])
  # Do delete
  entity.delete()
  
if __name__ == "__main__":
  if len(sys.argv)==3 and os.path.isfile(sys.argv[1]) and os.path.isfile(sys.argv[2]):
    params=getParameters(sys.argv[1])
    if (checkParameters(params):
      with open(sys.argv[2]) as f:
        data = json.load(f)
      # Read operation and call function to manage that operation
      if data['operation'] == 'create':
        createGenProcess(data, params['atlasHost'],params['atlasPort'],params['atlasUser'], params['atlasUserPass'])
      if data['operation'] == 'update':
        updateGenProcess(data,params['atlasHost'],params['atlasPort'],params['atlasUser'], params['atlasUserPass'])
      if data ['operation'] == 'delete':
        deleteGenProcess(data,params['atlasHost'],params['atlasPort'],params['atlasUser'], params['atlasUserPass'])
    else: 
      print ("Make sure that parameters exists and not empty value. Params: atlasHost, atlasPort,atlasUser, atlasUserPass") 
  else:
    print ("Make sure that configFile exists and ProccesParams file exists and path to them is correct")
