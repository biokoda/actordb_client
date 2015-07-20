% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_client).
-compile([{parse_transform, lager_transform}]).
-include("adbt_types.hrl").
% API
-export([test/0, start/2, start/1,
exec_single/4, exec_single/5,
exec_single_prepare/5, exec_single_prepare/6,
exec_multi/4,exec_multi/5,
exec_multi_prepare/5,exec_multi_prepare/6,
exec_all_prepare/4,exec_all_prepare/5,
exec_prepare/2,exec_prepare/3,
exec_all/3, exec_all/4,
exec/1,exec/2]).
-behaviour(gen_server).
-behaviour(poolboy_worker).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3]).
-export([resp/1]).

% Usage example
test() ->
	PoolInfo = [{size, 10}, {max_overflow, 5}],
	% Single host in worker params. Every worker in pool will connect to it.
	WorkerParams = [{hostname, "127.0.0.1"},
		% {database, "db1"},
		{username, "db1"},
		{password, "abc123"},
		{port,33306}
	],
	% Multiple hosts in worker params. Every worker will pick one random host and connect to that.
	% If connection to DB is lost, it will try to find a working host.
	% WorkerParams =
	% [
	%    [
	%      {hostname, "192.168.1.2"},
	%      {username, "db1"},
	%      {password, "abc123"},
	%      {port,33306}
	%    ],
	%    [
	%      {hostname, "192.168.1.3"},
	%      {username, "db1"},
	%      {password, "abc123"},
	%      {port,33306}
	%    ]
	% ],
	start(PoolInfo,WorkerParams).

% Single pool is most likely sufficient for most situations.
% WorkerParams can be a single property list (connect to one host)
% or a list of property lists (worker randomly connects to one of the hosts).
start(PoolParams,WorkerParams) ->
	start([{default_pool,PoolParams,WorkerParams}]).
start([{_Poolname,_PoolParams, _WorkerParams}|_] =  Pools) ->
	ok = application:set_env(actordb_client, pools,Pools),
	application:start(?MODULE).


exec_single(Actor,Type,Sql,Flags) ->
	exec_single(default_pool,Actor,Type,Sql,Flags).
exec_single(PoolName,Actor,Type,Sql,Flags) ->
	R = poolboy:transaction(PoolName, fun(Worker) ->
		gen_server:call(Worker, {call, exec_single, [Actor,Type,Sql,Flags]})
	end),
	resp(R).

exec_single_prepare(Actor,Type,Sql,Flags,BindingVals) ->
	exec_single_prepare(default_pool,Actor,Type,Sql,Flags,BindingVals).
exec_single_prepare(PoolName,Actor,Type,Sql,Flags,BindingVals) ->
	R = poolboy:transaction(PoolName, fun(Worker) ->
		gen_server:call(Worker, {call, exec_single_prepare, [Actor,Type,Sql,Flags,BindingVals]})
	end),
	resp(R).

exec_multi(Actors, Type, Sql,Flags) ->
	exec_multi(default_pool,Actors,Type,Sql,Flags).
exec_multi(PoolName,[_|_] = Actors, Type, Sql, Flags) ->
	R = poolboy:transaction(PoolName, fun(Worker) ->
		gen_server:call(Worker, {call, exec_multi, [Actors,Type,Sql,Flags]})
	end),
	resp(R).

exec_multi_prepare(Actors, Type, Sql,Flags,BindingVals) ->
	exec_multi_prepare(default_pool,Actors,Type,Sql,Flags,BindingVals).
exec_multi_prepare(PoolName,[_|_] = Actors, Type, Sql, Flags,BindingVals) ->
	R = poolboy:transaction(PoolName, fun(Worker) ->
		gen_server:call(Worker, {call, exec_multi_prepare, [Actors,Type,Sql,Flags,BindingVals]})
	end),
	resp(R).

exec_all(Type,Sql,Flags) ->
	exec_all(default_pool,Type,Sql,Flags).
exec_all(PoolName,Type,Sql,Flags) ->
	R = poolboy:transaction(PoolName, fun(Worker) ->
		gen_server:call(Worker, {call, exec_all, [Type,Sql,Flags]})
	end),
	resp(R).

exec_all_prepare(Type,Sql,Flags,BindingVals) ->
	exec_all_prepare(default_pool,Type,Sql,Flags,BindingVals).
exec_all_prepare(PoolName,Type,Sql,Flags,BindingVals) ->
	R = poolboy:transaction(PoolName, fun(Worker) ->
		gen_server:call(Worker, {call, exec_all_prepare, [Type,Sql,Flags,BindingVals]})
	end),
	resp(R).

exec(Sql) ->
	exec(default_pool,Sql).
exec(PoolName,Sql) ->
	R = poolboy:transaction(PoolName, fun(Worker) ->
		gen_server:call(Worker, {call, exec_sql, [Sql]})
	end),
	resp(R).

exec_prepare(Sql,BindingVals) ->
	exec_prepare(default_pool,Sql,BindingVals).
exec_prepare(PoolName,Sql,BindingVals) ->
	R = poolboy:transaction(PoolName, fun(Worker) ->
		gen_server:call(Worker, {call, exec_sql_prepare, [Sql,BindingVals]})
	end),
	resp(R).

resp({ok,R}) ->
	{ok,resp(R)};
resp([M|T]) when is_map(M) ->
	[resp(X) || X <- [M|T]];
resp(M) when is_map(M) ->
	maps:from_list([{binary_to_atom(K,latin1),resp(V)} || {K,V} <- maps:to_list(M)]);
resp(#'Val'{bigint = V}) when is_integer(V) ->
	V;
resp(#'Val'{integer = V}) when is_integer(V) ->
	V;
resp(#'Val'{smallint = V}) when is_integer(V) ->
	V;
resp(#'Val'{real = V}) when is_float(V) ->
	V;
resp(#'Val'{bval = V}) when V == true; V == false ->
	V;
resp(#'Val'{text = V}) when is_binary(V); is_list(V) ->
	V;
resp(#'Val'{isnull = true}) ->
	undefined;
resp(#'Result'{rdRes = undefined, wrRes = Write}) ->
	resp(Write);
resp(#'Result'{rdRes = Read, wrRes = undefined}) ->
	resp(Read);
resp(#'ReadResult'{hasMore = More, rows = Rows}) ->
	{More,resp(Rows)};
resp(#'WriteResult'{lastChangeRowid = LC, rowsChanged = NChanged}) ->
	{changes,LC,NChanged};
resp(#'InvalidRequestException'{code = C, info = I}) ->
	{error,{C,I}};
resp(R) ->
	R.


-record(dp, {conn, hostinfo = [], otherhosts = [], callqueue, tryconn}).

start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

init(Args) ->
	% process_flag(trap_exit, true),
	random:seed(os:timestamp()),
	case Args of
		[{_,_}|_] = Props ->
			ok;
		[[{_,_}|_]|_] ->
			Props = randelem(Args)
	end,
	{ok,C1} = do_connect(Props),
	{ok, #dp{conn=C1, hostinfo = Props, otherhosts = Args -- [Props], callqueue = queue:new()}}.

handle_call(_Msg, _From, #dp{conn = undefined} = P) ->
	% We might delay response a bit for max 1s to see if we can reconnect?
	{reply,{error,closed},P};
	% {noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue)}};
handle_call({call, Func,Params}, _From, P) ->
	Result = (catch thrift_client:call(P#dp.conn, Func, Params)),
	case Result of
		{C,{ok, Reply}} ->
			{reply, {ok, Reply}, P#dp{conn = C}};
		{C,{exception,Msg}} ->
			{reply, {ok, Msg}, P#dp{conn = C}};
		{_,{error,Msg}} when Msg == closed; Msg == econnrefused ->
			(catch thrift_client:close(P#dp.conn)),
			self() ! reconnect,
			% lager:error("Connection lost to ~p",[proplists:get_value(hostname, P#dp.hostinfo)]),
			{reply,{error,Msg},P#dp{conn = undefined}};
		{_, {E, Msg}} when E == error ->
			(catch thrift_client:close(P#dp.conn)),
			self() ! reconnect,
			{reply,{E, Msg}, P#dp{conn = undefined}};
		Other ->
			(catch thrift_client:close(P#dp.conn)),
			self() ! reconnect,
			{reply, Other, P#dp{conn = undefined}}
	end.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(reconnect,#dp{conn = undefined} = P) ->
	{ok,C} = do_connect(P#dp.hostinfo),
	{noreply,P#dp{conn = C}};
handle_info(reconnect,P) ->
	(catch thrift_client:close(P#dp.conn)),
	handle_info(reconnect,P#dp{conn = undefined});
handle_info(connect_other,#dp{otherhosts = []} = P) ->
	erlang:send_after(500,self(),reconnect),
	{noreply,P};
handle_info(connect_other, #dp{conn = undefined} = P) ->
	Props = randelem(P#dp.otherhosts),
	case do_connect(Props) of
		{ok,undefined} ->
			% Cant connect to other host. Wait a bit and
			% start again with our assigned host.
			erlang:send_after(500,self(),reconnect),
			{noreply,P};
		{ok,C} ->
			% We found a new connection to some other host.
			% Still periodically try to reconnect to original host if it comes back up.
			{noreply,P#dp{conn = C, tryconn = tryconn(P#dp.hostinfo)}}
	end;
handle_info(connect_other,P) ->
	(catch thrift_client:close(P#dp.conn)),
	handle_info(connect_other,P#dp{conn = undefined});
handle_info({'DOWN',_Monitor,_,Pid,Reason}, #dp{tryconn = Pid} = P) ->
	case Reason of
		{ok,C} when C /= undefined ->
			% Original host seems to be up,
			handle_info(reconnect,P);
		_ ->
			{noreply, P#dp{tryconn = tryconn(P#dp.hostinfo)}}
	end.

terminate(_Reason, P) ->
	(catch thrift_client:close(P#dp.conn)),
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


tryconn(Props) ->
	{Pid,_} = spawn_monitor(fun() ->
		timer:sleep(1000),
		exit(do_connect(Props))
	end),
	Pid.
% Return {ok,Connection} if ok.
% If connection closed retry later.
% If some other error (like invalid login info) throw exception
do_connect(Props) ->
	Hostname = proplists:get_value(hostname, Props),
	% Database = proplists:get_value(database, Args),
	Username = proplists:get_value(username, Props),
	Password = proplists:get_value(password, Props),
	Port = proplists:get_value(port, Props),

	case catch thrift_client_util:new(Hostname, Port, adbt_thrift, []) of
		{ok,C} ->
			case catch thrift_client:call(C, login, [Username,Password]) of
				{C1,{ok,_}} ->
					{ok,C1};
				{_,{error,Err}} when Err == closed; Err == econnrefused ->
					self() ! connect_other,
					{ok,undefined};
				{_,{exception,Msg}} ->
					throw(Msg);
				{_,Err} ->
					throw(Err)
			end;
		{_,{error,Err}} when Err == closed; Err == econnrefused ->
			self() ! connect_other,
			{ok,undefined}
	end.

randelem(Args) ->
	case lists:keyfind(crypto,1,application:which_applications()) of
		false ->
			Num = random:uniform(1000000);
		_ ->
			Num = binary:first(crypto:rand_bytes(1))
	end,
	lists:nth((Num rem length(Args)) + 1,Args).
