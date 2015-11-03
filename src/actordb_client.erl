% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_client).
-include_lib("adbt/src/adbt_types.hrl").
-include_lib("adbt/src/adbt_constants.hrl").
% API
-export([test/0,test/2, start/2, start/1,
config/0, config/1,config/2, config/3,
exec_config/1,exec_config/2,
exec_schema/1,exec_schema/2,
exec_single/4, exec_single/5,
exec_single_param/5, exec_single_param/6,
exec_multi/4,exec_multi/5,
% exec_multi_prepare/5,exec_multi_prepare/6,
% exec_all_prepare/4,exec_all_prepare/5,
exec_param/2,exec_param/3,
exec_all/3, exec_all/4,
exec/1,exec/2,salt/0,salt/1,
uniqid/0,uniqid/1,
actor_types/0, actor_types/1,
actor_tables/1, actor_tables/2,
actor_columns/2, actor_columns/3]).
-behaviour(gen_server).
-behaviour(poolboy_worker).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3]).
-export([resp/1,resp/2]).

% Usage example
test() ->
	test("myuser","mypass").
test(U,Pw) ->
	PoolInfo = [{size, 10}, {max_overflow, 5}],
	% Single host in worker params. Every worker in pool will connect to it.
	WorkerParams = [{hostname, "127.0.0.1"},
		% {database, "db1"},
		{username, U},
		{password, Pw},
		{port,33306},
		{recv_timeout, infinity}
	],
	% Multiple hosts in worker params. Every worker will pick one random host and connect to that.
	% If connection to DB is lost, it will try to find a working host.
	% WorkerParams =
	% [
	%    [
	%      {username, "db1"},
	%      {password, "abc123"},
	%      {hostname, "192.168.1.2"},
	%      {recv_timeout, infinity},
	%      {port,33306}
	%    ],
	%    [
	%      {username, "db1"},
	%      {password, "abc123"},
	%      {hostname, "192.168.1.3"},
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
	case application:ensure_all_started(?MODULE) of
		{ok,_} ->
			% Check in every pool if connection succeeded.
			% If any ok, return ok (workers try other connections if their set one fails)
			R = [poolboy:transaction(PoolName, fun(Worker) ->
				gen_server:call(Worker, status) end) || {PoolName,_,_} <- Pools],
			case [ok || ok <- R] of
				[ok|_] ->
					ok;
				[] ->
					[poolboy:stop(PoolName) || {PoolName,_,_} <- Pools],
					application:stop(?MODULE),
					case hd(R) of
						{error,closed} ->
							{error,connection_failed};
						R1 ->
							R1
					end
			end;
		{error,closed} ->
			{error,connection_failed};
		Err ->
			Err
	end.

-record(adbc,{key_type = atom, pool_name = default_pool, query_timeout = infinity}).

config() ->
	#adbc{}.
config([{_,_}|_] = L) when is_list(L) ->
	#adbc{pool_name = proplists:get_value(pool_name, L, default_pool),
		key_type = proplists:get_value(key_type, L, atom),
		query_timeout = proplists:get_value(query_timeout, L, infinity)};
config(PoolName) ->
	#adbc{pool_name = PoolName}.
config(PoolName, QueryTimeout) ->
	#adbc{pool_name = PoolName, query_timeout = QueryTimeout}.
config(PoolName, QueryTimeout, KeyType) ->
	#adbc{pool_name = PoolName, query_timeout = QueryTimeout, key_type = KeyType}.

exec_config(Sql) ->
	exec_config(#adbc{},Sql).
exec_config(C, Sql) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_config, [Sql]})
	end, C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

uniqid() ->
	uniqid(#adbc{}).
uniqid(C) ->
	poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, uniqid, []})
	end,C#adbc.query_timeout).

actor_types() ->
	actor_types(#adbc{}).
actor_types(C) ->
	poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, actor_types, []})
	end,C#adbc.query_timeout).

actor_tables(ActorType) ->
	actor_tables(#adbc{},ActorType).
actor_tables(C,ActorType) when is_atom(ActorType) ->
	actor_tables(C,atom_to_binary(ActorType,utf8));
actor_tables(C,ActorType) ->
	poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, actor_tables, [ActorType]})
	end,C#adbc.query_timeout).

actor_columns(ActorType, Table) ->
	actor_columns(#adbc{},ActorType, Table).
actor_columns(C, ActorType, Table) when is_atom(ActorType) ->
	actor_columns(C,atom_to_binary(ActorType,utf8), Table);
actor_columns(C, ActorType, Table) when is_atom(Table) ->
	actor_columns(C,ActorType,atom_to_binary(Table,utf8));
actor_columns(C,ActorType, Table) ->
	poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, actor_columns, [ActorType, Table]})
	end,C#adbc.query_timeout).

salt() ->
	salt(#adbc{}).
salt(C) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, salt, []})
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

exec_schema(Sql) ->
	exec_schema(#adbc{},Sql).
exec_schema(C,Sql) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_schema, [Sql]})
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

exec_single(Actor,Type,Sql,Flags) ->
	exec_single(#adbc{},Actor,Type,Sql,Flags).
exec_single(C,Actor,Type,Sql,Flags) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_single, [Actor,tostr(Type),Sql,flags(Flags)]})
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% Example: actordb_client:exec_single_param("myactor","type1","insert into tab values (?1,?2,?3);",[create],[[20000000,"bigint!",3]]).
exec_single_param(Actor,Type,Sql,Flags,BindingVals) ->
	exec_single_param(#adbc{},Actor,Type,Sql,Flags,BindingVals).
exec_single_param(C,Actor,Type,Sql,Flags,BindingVals) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_single_param, [Actor,tostr(Type),Sql,flags(Flags),fix_binds(BindingVals)]})
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

exec_multi(Actors, Type, Sql,Flags) ->
	exec_multi(#adbc{},Actors,Type,Sql,Flags).
exec_multi(C,[_|_] = Actors, Type, Sql, Flags) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_multi, [Actors,Type,Sql,Flags]})
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% exec_multi_prepare(Actors, Type, Sql,Flags,BindingVals) ->
% 	exec_multi_prepare(default_pool,Actors,Type,Sql,Flags,BindingVals).
% exec_multi_prepare(PoolName,[_|_] = Actors, Type, Sql, Flags,BindingVals) ->
% 	R = poolboy:transaction(PoolName, fun(Worker) ->
% 		gen_server:call(Worker, {call, exec_multi_prepare, [Actors,tostr(Type),Sql,flags(Flags),fix_binds(BindingVals)]})
% 	end),
% 	resp(R).

exec_all(Type,Sql,Flags) ->
	exec_all(#adbc{},Type,Sql,Flags).
exec_all(C,Type,Sql,Flags) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_all, [Type,Sql,Flags]})
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% exec_all_prepare(Type,Sql,Flags,BindingVals) ->
% 	exec_all_prepare(default_pool,Type,Sql,Flags,BindingVals).
% exec_all_prepare(PoolName,Type,Sql,Flags,BindingVals) ->
% 	R = poolboy:transaction(PoolName, fun(Worker) ->
% 		gen_server:call(Worker, {call, exec_all_prepare, [tostr(Type),Sql,flags(Flags),fix_binds(BindingVals)]})
% 	end),
% 	resp(R).

exec(Sql) ->
	exec(#adbc{},Sql).
exec(C, Sql) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_sql, [Sql]})
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

exec_param(Sql,BindingVals) ->
	exec_param(#adbc{},Sql,BindingVals).
exec_param(C,Sql,BindingVals) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_sql_param, [Sql,fix_binds(BindingVals)]})
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

tostr(H) when is_atom(H) ->
	atom_to_binary(H,latin1);
tostr(H) ->
	iolist_to_binary(H).

flags([H|T]) when is_atom(H) ->
	[atom_to_binary(H,latin1)|flags(T)];
flags([H|T]) ->
	[H|flags(T)];
flags([]) ->
	[].

fix_binds([H|T]) ->
	[fix_binds1(H)|fix_binds(T)];
fix_binds([]) ->
	[].
fix_binds1([H|T]) when is_list(H); is_binary(H) ->
	[#'Val'{text = iolist_to_binary(H)}|fix_binds1(T)];
fix_binds1([H|T]) when H >= -32768, H =< 32767 ->
	[#'Val'{smallint = H}|fix_binds1(T)];
fix_binds1([H|T]) when H >= -2147483648, H =< 2147483647 ->
	[#'Val'{integer = H}|fix_binds1(T)];
fix_binds1([H|T]) when is_integer(H) ->
	[#'Val'{bigint = H}|fix_binds1(T)];
fix_binds1([H|T]) when is_float(H) ->
	[#'Val'{real = H}|fix_binds1(T)];
fix_binds1([H|T]) when H == true; H == false ->
	[#'Val'{bval = H}|fix_binds1(T)];
fix_binds1([undefined|T]) ->
	[#'Val'{isnull = true}|fix_binds1(T)];
fix_binds1([H|T]) when is_atom(H) ->
	[#'Val'{text = atom_to_binary(H,latin1)}|fix_binds1(T)];
fix_binds1([]) ->
	[].

resp(R) ->
	resp(atom,R).
resp(KeyType,{ok,R}) ->
	case resp(KeyType,R) of
		{error,E} ->
			{error,E};
		_ ->
			{ok,resp(KeyType,R)}
	end;
resp(KeyType,[M|T]) when is_map(M) ->
	[resp(KeyType,X) || X <- [M|T]];
resp(atom,M) when is_map(M) ->
	maps:from_list([{binary_to_atom(K,latin1),resp(V)} || {K,V} <- maps:to_list(M)]);
resp(binary,M) when is_map(M) ->
	maps:from_list([{K,resp(V)} || {K,V} <- maps:to_list(M)]);
resp(list,M) when is_map(M) ->
	maps:from_list([{binary_to_list(K),resp(V)} || {K,V} <- maps:to_list(M)]);
resp(_,#'Val'{bigint = V}) when is_integer(V) ->
	V;
resp(_,#'Val'{integer = V}) when is_integer(V) ->
	V;
resp(_,#'Val'{smallint = V}) when is_integer(V) ->
	V;
resp(_,#'Val'{real = V}) when is_float(V) ->
	V;
resp(_,#'Val'{bval = V}) when V == true; V == false ->
	V;
resp(_,#'Val'{text = V}) when is_binary(V); is_list(V) ->
	V;
resp(_,#'Val'{isnull = true}) ->
	undefined;
resp(TT,#'Result'{rdRes = undefined, wrRes = Write}) ->
	resp(TT,Write);
resp(TT,#'Result'{rdRes = Read, wrRes = undefined}) ->
	resp(TT,Read);
resp(TT,#'ReadResult'{hasMore = More, rows = Rows}) ->
	{More,resp(TT,Rows)};
resp(_,#'WriteResult'{lastChangeRowid = LC, rowsChanged = NChanged}) ->
	{changes,LC,NChanged};
resp(_,{error,E}) ->
	error1(E);
resp(_,#'InvalidRequestException'{} = R) ->
	error1(R);
resp(_,R) ->
	R.


-record(dp, {conn, hostinfo = [], otherhosts = [], callqueue = queue:new(), tryconn}).

start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

init(Args) ->
	% process_flag(trap_exit, true),
	random:seed(os:timestamp()),
	case Args of
		[{_,_}|_] = Props ->
			Other = [];
		[[{_,_}|_]|_] ->
			Props = randelem(Args),
			Other = Args -- [Props]
	end,
	case catch do_connect(Props) of
		{ok,C1} ->
			{ok, #dp{conn=C1, hostinfo = Props, otherhosts = Other}};
		{error,closed} when Other /= [] ->
			self() ! connect_other,
			{ok, #dp{conn={error,closed}, hostinfo = Props, otherhosts = Other}};
		{error,E} ->
			erlang:send_after(500,self(),reconnect),
			{ok, #dp{conn={error,E}, hostinfo = Props, otherhosts = Other}}
		% {'InvalidRequestException',_,<<"Username and/or password incorrect.">>} = Err ->
		% 	{stop,Err};
		% {error,E} ->
		% 	{stop,{error,E}}
	end.

handle_call(stop,_,P) ->
	{stop,normal,P};
handle_call(_Msg, _From, #dp{conn = {error,E}} = P) ->
	% We might delay response a bit for max 1s to see if we can reconnect?
	{reply,error1({error,E}),P};
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
			{reply,error1({error,Msg}),P#dp{conn = {error,closed}}};
		{_, {error, Msg}} ->
			(catch thrift_client:close(P#dp.conn)),
			self() ! reconnect,
			{reply,error1({error, Msg}), P#dp{conn = {error,Msg}}};
		{error,E} ->
			(catch thrift_client:close(P#dp.conn)),
			self() ! reconnect,
			{reply, error1({error,E}), P#dp{conn = {error,E}}};
		{'EXIT',{badarg,_}} ->
			{reply,badarg,P}
	end;
handle_call(status,_,P) ->
	case P#dp.conn of
		{error,_} ->
			{reply,P#dp.conn,P};
		_ ->
			{reply,ok,P}
	end.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(reconnect,#dp{conn = {error,_}} = P) ->
	case do_connect(P#dp.hostinfo) of
		{ok,C} ->
			{noreply,P#dp{conn = C}};
		{error,closed} ->
			self() ! connect_other,
			{noreply,P#dp{conn = {error,closed}}};
		{error,E} ->
			{noreply,P#dp{conn = {error,E}}}
	end;
handle_info(reconnect,P) ->
	(catch thrift_client:close(P#dp.conn)),
	handle_info(reconnect,P#dp{conn = {error,error}});
handle_info(connect_other,#dp{otherhosts = []} = P) ->
	erlang:send_after(500,self(),reconnect),
	{noreply,P};
handle_info(connect_other, #dp{conn = {error,_}} = P) ->
	Props = randelem(P#dp.otherhosts),
	case do_connect(Props) of
		{error,E} ->
			% Cant connect to other host. Wait a bit and
			% start again with our assigned host.
			erlang:send_after(500,self(),reconnect),
			{noreply,P#dp{conn = {error,E}}};
		{ok,C} ->
			% We found a new connection to some other host.
			% Still periodically try to reconnect to original host if it comes back up.
			{noreply,P#dp{conn = C, tryconn = tryconn(P#dp.hostinfo)}}
	end;
handle_info(connect_other,P) ->
	{noreply,P};
	% (catch thrift_client:close(P#dp.conn)),
	% handle_info(connect_other,P#dp{conn = {error,error}});
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

	case catch thrift_client_util:new(Hostname, Port, actordb_thrift, []) of
		{ok,C} ->
			case (catch thrift_client:call(C, salt, [])) of
				{CS,{ok,Salt}} ->
					<<Num1:160>> = HashBin = crypto:hash(sha, Password),
					<<Num2:160>> = crypto:hash(sha, <<Salt/binary, (crypto:hash(sha, HashBin))/binary>>),
					case catch thrift_client:call(CS, login, [Username,<<(Num1 bxor Num2):160>>]) of
						{C1,{ok,_}} ->
							{ok,C1};
						{_,{error,Err}} when Err == closed; Err == econnrefused ->
							{error,closed};
						{_,{exception,Msg}} ->
							{error,Msg};
						{_,Err} ->
							{error,Err}
					end;
				{_,{error,Err}} when Err == closed; Err == econnrefused ->
					{error,closed}
			end;
		{error,econnrefused} ->
			{error,closed};
		{_,{error,Err}} when Err == closed; Err == econnrefused ->
			{error,closed}
	end.

randelem(Args) ->
	case lists:keyfind(crypto,1,application:which_applications()) of
		false ->
			Num = random:uniform(1000000);
		_ ->
			Num = binary:first(crypto:rand_bytes(1))
	end,
	lists:nth((Num rem length(Args)) + 1,Args).

error1(E) ->
	case E of
		{error,X} ->
			error1(X);
		{'InvalidRequestException',?ADBT_ERRORCODE_LOGINFAILED, Msg} ->
			{error,{login_failed,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_MISSINGNODESINSERT, Msg} ->
			{error,{missing_nodes,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_MISSINGGROUPINSERT, Msg} ->
			{error,{missing_group,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_LOCALNODEMISSING, Msg} ->
			{error,{missing_local_node,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_CONSENSUSTIMEOUT, Msg} ->
			{error,{consensus_timeout,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_SQLERROR, Msg} ->
			{error,{sql_error,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_NOTPERMITTED, Msg} ->
			{error,{permission,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_INVALIDTYPE, Msg} ->
			{error,{invalid_type,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_INVALIDACTORNAME, Msg} ->
			{error,{invalid_actor_name,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_EMPTYACTORNAME, Msg} ->
			{error,{empty_actor_name,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_NOTLOGGEDIN, Msg} ->
			{error,{not_logged_in,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_NOCREATE, Msg} ->
			{error,{nocreate,Msg}};
		{'InvalidRequestException',?ADBT_ERRORCODE_ERROR, Msg} ->
			{error,{error,Msg}};
		_ ->
			{error,E}
	end.
