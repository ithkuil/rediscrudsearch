## Simple redis-backed database with text search on specified fields.
## Provides a simplified API for basic database operations and exposes redis object
## as e.g. myDB.red if specific redis calls are needed.
## Search proc uses set intersection to efficiently find matching records.
## 
## Example usage:

## .. code-block:: nimrod
##   myDB = newRedisDB("documents")  
##                                             
##   myDB.createOrUpdate("item1", 
##                       newStringTable({"name": "personA", 
##                                       "description" : "Item for test",
##                                       "tags":"thing"})
##   myDB.createOrUpdate("addkey", 
##                       newStringTable({"name":"addkey",
##                                       "owner":"bob", 
##                                       "description": "Add a public SSH Key",
##                                       "tags": "add,ssh,key,thing"}))
##   
##   var found = myDB.search("thing") # returns @["item1", "addkey"]
##   
##   if myDB.checkOwner("addkey", "eddie"):
##     # eddie is not the 'owner' so the following line doesn't get executed
##     myDB.createOrUpdate("addkey", "tags": "add,eddiewashere,ssh,key,thing")
##   
##   var items = myDB.listAll()
##   echo "Items count is " & $items.len
##   for i in 0..items.len-1:
##     echo "Item #" & $(i+1)
##     for k, v in items[i]:
##       echo k & ": " & v   
##
##   # Output is
##   # Item #1
##   # id: item1
##   # name: personA
##   # description: Item for test
##   # tags: thing
##   # Item #2
##   # id: addkey
##   # name: addkey
##   # owner: bob
##   # description: Add a public SSH Key
##   # tags: add,ssh,key,thing

import redis, strutils, strtabs, sockets, re

type
  TRedisDB* = object
    red*: TRedis
    conf: PStringTable 
    initialized: bool    
    name*: string

type 
  TKeyValPair = tuple[field, value:string]

proc config(db: TRedisDB, data: varargs[string]) =
  ## Set config values like port and host for redis.
  var i = 0
  while data.len > 1 and true:
    db.conf[data[i]] = data[i+1]
    inc(i)
    if i == data.len-1:
      break

proc newRedisDB*(dbname:string, confData:varargs[string]):TRedisDB =
  ## Only first arg (dbname) is necessary, these are the defaults if the rest aren't specified.
  ##   .. code-block:: nimrod
  ##     myDB = newRedisDB("mydbname", "host", "localhost", "port", "6379", 
  ##                       "searchFields", "name,description,tags"
  ##                       "ownerField", "owner")  
  ##                                             
  var db = TRedisDB(name:dbname)
  db.conf = newStringTable(modeCaseInsensitive)
  db.conf["host"] = "localhost"
  db.conf["port"] = "6379"
  db.conf["searchFields"]="name,description,tags"
  db.conf["ownerField"]="owner"
  db.config(confData)
  db.initialized = false
  db.red = redis.open(db.conf["host"], TPort(parseInt(db.conf["port"])))
  db.initialized = true
  return db

proc checkInit(db:TRedisDB) =
  var x=1

proc read*(db:TRedisDB, id:string):PStringTable =
  ## Read hash (stringtable) with a particular id.
  var list = db.red.hGetAll db.name&":obj:"&id
  var strTable = newStringTable()
  if not isNil(list):
    var i = 0
    while i < list.len and not isNil(list[i]):
      strTable[list[i]] = list[i+1]
      i = i + 2
  return strTable
  
proc checkOwner*(db:TRedisDb, id, val:string):bool =
  ## Verify that val matches the value of the ownerField
  ## (specified in the config, defaults to 'owner')
  ## for object with this id.
  var data = db.red.hGet(db.name&":obj:"&id, db.conf["ownerField"])
  if isNil(data):
    return false 
  else:
    return (data == val)

proc delete*(db:TRedisDB, id:string) =
  ## Delete object.
  var x = 1
  
proc listAll*(db:TRedisDB): seq[PStringTable] = 
  ## List all objects.
  var prefix = db.name & ":obj:"
  var ids = db.red.keys(prefix & "*")
  var list:seq[PStringTable] = @[]
  if isNil(ids):
    return @[]
  else:
    db.red.multi()
    for id in ids:
      discard read(db, id[prefix.len..id.len-1])
      discard db.red.echoServ("{{REC}}")                  
    var ret = db.red.exec()
   
    var table = newStringTable()
    var i = 0 
    var itemNum = 0
    while i<ret.len:
      var item = ret[i]
      if item != nil:      
        if item == "{{REC}}":
          var idStr = ids[itemNum]
          table["id"] = idStr[9..idStr.len-1]
          list.add(table)
          itemNum += 1
          table = newStringTable()
          i += 1
        else:
          table[ret[i]] = ret[i+1]
          i += 2
    return list
   
proc splitWords(str:string):seq[string] =
  return str.findAll(re"((\b[^\s]+\b)((?<=\.\w).)?)")

var ignoreStr = """a,able,about,across,after,all,almost,also,am,
among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,
dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,
have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,
least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not
,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,
should,since,so,some,than,that,the,their,them,then,there,these,
they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,
which,while,who,whom,why,will,with,would,yet,you,your  
"""
var ignore = ignoreStr.replace("\n","").split(',') 

proc keepRelevant(words:seq[string]):seq[string] =
  var ret:seq[string] = @[]
  for word in words:
    if not (word in ignore):
      ret.add word
  return ret

proc index(db:TRedisDB, id:string, keyvals:PStringTable) =
  var fields = db.conf["searchFields"].split(',')
  var words:seq[string] = @[]
  for field in fields:
    if not isNil(keyvals[field]): 
      var words = keyvals[field].splitWords()
      words = keepRelevant(words)
      for word in words:
        var numAdded = db.red.sAdd(db.name&":index:"&word.toLower(), id)

proc search*(db: TRedisDB, query:string):seq[PStringTable] =
  ## Search for an object with searchFields including all 
  ## keywords in query (separate with spaces).
  var keywords = query.split(' ')
  var keys:seq[string] = @[]
  for word in keywords:
    keys.add db.name&":index:"&word.toLower()
  var ret = db.red.sInter(keys)
  return @[]

proc createOrUpdate*(db: TRedisDB, id:string, data:PStringTable) =
  ## Create or update existing fields of an object
  ## If object with same id exists, only updates fields specified.
  checkInit(db)
  for key, val in data:
    var reply = db.red.hSet(db.name&":obj:"&id, key, val)
  db.index(id, data)

when isMainModule:
  var myDB = newRedisDB("mydb") 
  var item1 = newStringTable({"name": "personA", "description" : "Item for test",
                              "tags":"thing", "owner": "bob"})
  myDB.createOrUpdate("item2", item1)
  myDB.createOrUpdate("addkey", newStringTable({"name":"addkey", "owner": "tom", "description":
                                 "Add a public SSH Key", "tags": "add,ssh,key,thing"}))
  discard myDB.search("thing")
  
  var items = myDB.listAll()
  echo "Items count is " & $items.len
  for i in 0..items.len-1:
    echo "Item #" & $(i+1)
    for k, v in items[i]:
      echo k & ": " & v      
