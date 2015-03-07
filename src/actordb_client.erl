-module(actordb_client).
-compile([{parse_transform, lager_transform}]).
% API
-export([test/0,start/1, login/3, exec/4, exec/5, exec_multi/4,exec_multi/5, exec_all/3, exec_all/4, exec/2]).
-behaviour(gen_server).
-behaviour(poolboy_worker).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

test() ->
	PoolInfo = [{size, 10}, {max_overflow, 20}],
	WorkerInfo = [{hostname, "127.0.0.1"},
        % {database, "db1"},
        {username, "db1"},
        {password, "abc123"},
        {port,33306}
    ],
	start([{pool,PoolInfo,WorkerInfo}]).

% [{PoolName,PoolParams,WorkerParams}]
start([_|_] = Pools) ->
	ok = application:set_env(actordb_client, pools,Pools),
	application:start(?MODULE).


login(PoolName,U,P) ->
	poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {call, login, [U,P]})
    end).

exec(PoolName,Actor,Type,Sql) ->
	exec(PoolName,Actor,Type,Sql,[]).
exec(PoolName,Actor,Type,Sql,Flags) ->
	poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {call, exec_single, [Actor,Type,Sql,Flags]})
    end).

exec_multi(PoolName,Actors, Type, Sql) ->
	exec_multi(PoolName,Actors,Type,Sql,[]).
exec_multi(PoolName,[_|_] = Actors, Type, Sql, Flags) ->
	poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {call, exec_multi, [Actors,Type,Sql,Flags]})
    end).

exec_all(PoolName,Type,Sql) ->
	exec_all(PoolName,Type,Sql,[]).
exec_all(PoolName,Type,Sql,Flags) ->
	poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {call, exec_all, [Type,Sql,Flags]})
    end).

exec(PoolName,Sql) ->
	poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {call, exec_sql, [Sql]})
    end).



-record(dp, {conn, username, password}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    % process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args),
    % Database = proplists:get_value(database, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    Port = proplists:get_value(port, Args),
    
    {ok,C} = thrift_client_util:new(Hostname, Port, adbt_thrift, []),
    {C1,_R} = thrift_client:call(C, login, [Username,Password]),
    {ok, #dp{conn=C1, username = Username, password = Password}}.

handle_call({call, Func,Params}, _From, P) ->
	{C,R} = thrift_client:call(P#dp.conn, Func, Params),
    {reply, R, P#dp{conn = C}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, P) ->
	thrift_client:close(P#dp.conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

