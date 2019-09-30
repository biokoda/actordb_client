%% @hidden
-module(actordb_client_trace).
-author('Biokoda d.o.o.').

-callback connection_event(Format::string(), Args::list()) -> ok.
% QueryTime: how long between sending query over wire and receiving response
% PoolTime: how long to find gen_server to execute query
% WaitTime: how long from sending query to gen_server and start execution
-callback query_time(QueryTime::integer(), 
    PoolTime::integer(),
    WaitTime::integer(),
    FromPid::pid(),
    CallFunc::atom(),
    Args::list()) -> ok.
