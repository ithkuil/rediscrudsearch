import redis, os, parsecfg, strutils, streams, strtabs, sockets, re

type
  TRedisDB* = object
    red: TRedis
    conf: PStringTable 
    initialized: bool    
    name*: string

type 
  TKeyValPair = tuple[field, value:string]
     


proc config(db: TRedisDB, data: varargs[string]) =
  ## Set config values like port and host for redis
  var i = 0
  while data.len > 1 and true:
    db.conf[data[i]] = data[i+1]
    inc(i)
    if i == data.len-1:
      break

proc newRedisDB*(dbname:string, confData:varargs[string]):TRedisDB =
  var db = TRedisDB(name:dbname)
  db.conf = newStringTable(modeCaseInsensitive)
  db.conf["host"] = "localhost"
  db.conf["port"] = "6379"
  db.conf["searchFields"]="name,description,tags"
  db.conf["ownerField"]="name"
  db.config(confData)
  db.initialized = false
  db.red = redis.open(db.conf["host"], TPort(parseInt(db.conf["port"])))
  db.initialized = true
  return db

proc checkInit(db:TRedisDB) =
  var x=1

proc read*(db:TRedisDB, id:string):PStringTable =
  var list = db.red.hGetAll db.name&":"&id
  var strTable = newStringTable()
  if not isNil(list):
    var i = 0
    while i < list.len and not isNil(list[i]):
      strTable[list[i]] = list[i+1]
      i = i + 2
  return strTable
  
proc checkOwner*(db:TRedisDb, id, val:string):bool =
  # verify that name matches the name in the config
  var data = db.red.hGet(db.name&":"&id, db.conf["ownerField"])
  if isNil(data):
    return false 
  else:
    return (data == val)

proc delete*(db:TRedisDB, id:string) =
  var x = 1
  
proc listAll*(db:TRedisDB): seq[PStringTable] =
  var ids = db.red.keys(db.name&":*")
  var list:seq[PStringTable] = @[]
  if isNil(ids):
    return @[]
  else:
    for id in ids:
      db.red.multi()
      var valRead = read(db, id)
      for k, v in valRead:
        echo k &": " & v
      
    discard """ var returned = db.red.exec()
    var strTable = newStringTable()
    if not isNil(returned):
      var i = 0
      while i < list.len and not isNil(list[i]):
        strTable[list[i]] = list[i+1]
        i = i + 2
    return strTable
 """
   
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

proc index*(db:TRedisDB, id:string, keyvals:PStringTable) =
  var fields = db.conf["searchFields"].split(',')
  var words:seq[string] = @[]
  for field in fields:
    if not isNil(keyvals[field]): 
      var words = keyvals[field].splitWords()
      words = keepRelevant(words)
      for word in words:
        var numAdded = db.red.sAdd(db.name&":index:"&word.toLower(), id)

proc search*(db: TRedisDB, query:string):seq[PStringTable] =
  var keywords = query.split(' ')
  var keys:seq[string] = @[]
  for word in keywords:
    keys.add db.name&":index:"&word.toLower()
  var ret = db.red.sInter(keys)
  for item in ret:
    echo("search return value:")
    echo(item)
  return @[]

proc createOrUpdate*(db: TRedisDB, id:string, data:PStringTable) =
  checkInit(db)
  for key, val in data:
    echo "Id is " & db.name&":"&id& " Key is " & key & " val is " & val
    var reply = db.red.hSet(db.name&":"&id, key, val)
  db.index(id, data)

when isMainModule:
  echo "build ok"
  var myDB = newRedisDB("mydb") 
  var item1 = newStringTable({"name": "personA", "description" : "Item for test"}, 
                              modeCaseInsensitive)
  myDB.createOrUpdate("item1", item1)
  discard myDB.search("test")  
  
