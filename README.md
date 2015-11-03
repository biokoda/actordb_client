
This is an erlang thrift client to actordb.

Check actordb_client:test/0 for an example how to connect.

With actordb_client:config/0,1,2,3 You can specify configuration options for queries:
- which worker pool to use (if you have more than one)
- gen_server timeout to a worker (default is infinity)
- key_type select statements return list of maps. By default keys are atoms. Setting this value to list or binary will set key type.

Value returned from actordb_client:config/0,1,2,3 is an optional first parameter to any query.

For a list of possible error results check actordb_client:error1 function.

Successful results are:
{ok,{changes,LastInsertRowid,RowsChanged}}
{ok,{HasMore,ListOfMaps}}

At the moment HasMore is always false.
