-module(actordb_client).
-include_lib("adbt/include/adbt_types.hrl").
-include_lib("adbt/include/adbt_constants.hrl").
% API
-export([test/0,test/2,test/3, start/2, start/1,
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
actor_columns/2, actor_columns/3,
prot_version/0]).
-behaviour(gen_server).
-behaviour(poolboy_worker).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3]).

% Usage example
test() ->
	test("myuser","mypass").
test(U,Pw) ->
	test(default_pool, U, Pw).
test(Name, U, Pw) ->
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
	start([{Name, PoolInfo, WorkerParams}]).

% Single pool is most likely sufficient for most situations.
% WorkerParams can be a single property list (connect to one host)
% or a list of property lists (worker randomly connects to one of the hosts).
start(PoolParams,WorkerParams) ->
	start([{default_pool,PoolParams,WorkerParams}]).
start([{_Poolname,_PoolParams, _WorkerParams}|_] =  Pools) ->
	% ok = application:set_env(actordb_client, pools,Pools),
	case application:ensure_all_started(?MODULE) of
		{ok,_} ->
			actordb_client_sup:start_children(Pools),
			ok;
		Err ->
			Err
	end.

-record(adbc,{key_type = atom, pool_name = default_pool, query_timeout = infinity}).

% Optional config.
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

% Exec query on config database. Queries will work only when logged in with a root user account
% or if database is uninitalized.
exec_config(Sql) ->
	exec_config(#adbc{},Sql).
exec_config(C, Sql) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_config, [Sql]}, C#adbc.query_timeout)
	end, C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% Get a unique integer id from db.
uniqid() ->
	uniqid(#adbc{}).
uniqid(C) ->
	poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, uniqid, []}, C#adbc.query_timeout)
	end,C#adbc.query_timeout).

% Returns list of actor types 
actor_types() ->
	actor_types(#adbc{}).
actor_types(C) ->
	poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, actor_types, []}, C#adbc.query_timeout)
	end,C#adbc.query_timeout).

% For an actor type, return list of tables in schema
actor_tables(ActorType) ->
	actor_tables(#adbc{},ActorType).
actor_tables(C,ActorType) when is_atom(ActorType) ->
	actor_tables(C,atom_to_binary(ActorType,utf8));
actor_tables(C,ActorType) ->
	poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, actor_tables, [ActorType]}, C#adbc.query_timeout)
	end,C#adbc.query_timeout).

% For actor type and table, which columns it has
actor_columns(ActorType, Table) ->
	actor_columns(#adbc{},ActorType, Table).
actor_columns(C, ActorType, Table) when is_atom(ActorType) ->
	actor_columns(C,atom_to_binary(ActorType,utf8), Table);
actor_columns(C, ActorType, Table) when is_atom(Table) ->
	actor_columns(C,ActorType,atom_to_binary(Table,utf8));
actor_columns(C,ActorType, Table) ->
	poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, actor_columns, [ActorType, Table]}, C#adbc.query_timeout)
	end,C#adbc.query_timeout).

% Get salt for safer login. This way password never gets sent over the wire.
% Then use it like so:
% SHA1( password ) XOR SHA1( SALT <concat> SHA1( SHA1( password ) ) )
salt() ->
	salt(#adbc{}).
salt(C) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, salt, []}, C#adbc.query_timeout)
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% Change schema. Must be logged in as root user.
exec_schema(Sql) ->
	exec_schema(#adbc{},Sql).
exec_schema(C,Sql) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_schema, [Sql]}, C#adbc.query_timeout)
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% Run query on an actor.
% Actor: name of actor (iolist)
% Type: actor type
% Sql: query iolist
% Flags: [create] or []
exec_single(Actor,Type,Sql,Flags) ->
	exec_single(#adbc{},Actor,Type,Sql,Flags).
exec_single(C,Actor,Type,Sql,Flags) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_single, [Actor,tostr(Type),Sql,flags(Flags)]}, C#adbc.query_timeout)
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% Run query on an actor and use parameterized values. This is safer and faster.
% Actor: name of actor (iolist)
% Type: actor type
% Sql: query iolist
% Flags: [create] or []
% BindingVals: List of lists. You can insert many rows using a single call.
% Example: actordb_client:exec_single_param("myactor","type1","insert into tab values (?1,?2,?3);",[create],[[20000000,"bigint!",3]]).
exec_single_param(Actor,Type,Sql,Flags,BindingVals) ->
	exec_single_param(#adbc{},Actor,Type,Sql,Flags,BindingVals).
exec_single_param(C,Actor,Type,Sql,Flags,BindingVals) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, 
			{call, exec_single_param, [Actor,tostr(Type),Sql,flags(Flags),fix_binds(BindingVals)]},C#adbc.query_timeout)
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% Run a query over multiple actors.
% Actors: list of names
% Type: actor type
% Sql: query iolist
% Flags: [create] or []
exec_multi(Actors, Type, Sql,Flags) ->
	exec_multi(#adbc{},Actors,Type,Sql,Flags).
exec_multi(C,[_|_] = Actors, Type, Sql, Flags) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_multi, [Actors,Type,Sql,Flags]},C#adbc.query_timeout)
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% exec_multi_prepare(Actors, Type, Sql,Flags,BindingVals) ->
% 	exec_multi_prepare(default_pool,Actors,Type,Sql,Flags,BindingVals).
% exec_multi_prepare(PoolName,[_|_] = Actors, Type, Sql, Flags,BindingVals) ->
% 	R = poolboy:transaction(PoolName, fun(Worker) ->
% 		gen_server:call(Worker, {call, exec_multi_prepare, [Actors,tostr(Type),Sql,flags(Flags),fix_binds(BindingVals)]})
% 	end),
% 	resp(R).

% Run a type(*) query.
% Type: actor type
% Sql: query iolist
% Flags: [create] or []
exec_all(Type,Sql,Flags) ->
	exec_all(#adbc{},Type,Sql,Flags).
exec_all(C,Type,Sql,Flags) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_all, [Type,Sql,Flags]},C#adbc.query_timeout)
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% exec_all_prepare(Type,Sql,Flags,BindingVals) ->
% 	exec_all_prepare(default_pool,Type,Sql,Flags,BindingVals).
% exec_all_prepare(PoolName,Type,Sql,Flags,BindingVals) ->
% 	R = poolboy:transaction(PoolName, fun(Worker) ->
% 		gen_server:call(Worker, {call, exec_all_prepare, [tostr(Type),Sql,flags(Flags),fix_binds(BindingVals)]})
% 	end),
% 	resp(R).

% Run a query that has everything in it. Must start with "actor ..."
exec(Sql) ->
	exec(#adbc{},Sql).
exec(C, Sql) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_sql, [Sql]},C#adbc.query_timeout)
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

% Run a query that has everything in it and uses parameters. Must start with "actor ..."
exec_param(Sql,BindingVals) ->
	exec_param(#adbc{},Sql,BindingVals).
exec_param(C,Sql,BindingVals) ->
	R = poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, exec_sql_param, [Sql,fix_binds(BindingVals)]},C#adbc.query_timeout)
	end,C#adbc.query_timeout),
	resp(C#adbc.key_type,R).

prot_version() ->
	C = #adbc{},
	poolboy:transaction(C#adbc.pool_name, fun(Worker) ->
		gen_server:call(Worker, {call, protocolVersion, []},C#adbc.query_timeout)
	end,C#adbc.query_timeout).

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
fix_binds1([undefined|T]) ->
	[#'Val'{isnull = true}|fix_binds1(T)];
fix_binds1([null|T]) ->
	[#'Val'{isnull = true}|fix_binds1(T)];
fix_binds1([true|T]) ->
	[#'Val'{bval = true}|fix_binds1(T)];
fix_binds1([false|T]) ->
	[#'Val'{bval = false}|fix_binds1(T)];
fix_binds1([{blob,V}|T]) ->
	[#'Val'{blob = iolist_to_binary(V)}|fix_binds1(T)];
fix_binds1([[_|_] = H|T]) ->
	[#'Val'{text = unicode:characters_to_binary(H)}|fix_binds1(T)];
fix_binds1([[]|T]) ->
	[#'Val'{text = <<>>}|fix_binds1(T)];
fix_binds1([<<_/binary>> = H|T]) ->
	[#'Val'{text = H}|fix_binds1(T)];
fix_binds1([H|T]) when is_atom(H) ->
	[#'Val'{text = atom_to_binary(H,latin1)}|fix_binds1(T)];
fix_binds1([H|T]) when is_float(H) ->
	[#'Val'{real = H}|fix_binds1(T)];
fix_binds1([H|T]) when H >= -32768, H =< 32767 ->
	[#'Val'{smallint = H}|fix_binds1(T)];
fix_binds1([H|T]) when H >= -2147483648, H =< 2147483647 ->
	[#'Val'{integer = H}|fix_binds1(T)];
fix_binds1([H|T]) when is_integer(H) ->
	[#'Val'{bigint = H}|fix_binds1(T)];
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
resp(_,#'Val'{blob = V})  when is_binary(V); is_list(V) ->
	V;
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


-record(dp, {conn, hostinfo = [], otherhosts = [], callqueue = queue:new(), tryconn, rii = 0}).

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
	erlang:send_after(100,self(),check),
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
			{reply, {ok, Reply}, P#dp{conn = C, rii = P#dp.rii+1}};
		{C,{exception,Msg}} ->
			{reply, {ok, Msg}, P#dp{conn = C, rii = P#dp.rii+1}};
		{_,{error,Msg}} when Msg == closed; Msg == econnrefused ->
			(catch thrift_client:close(P#dp.conn)),
			self() ! reconnect,
			error_logger:format("Connection lost to ~p~n",[proplists:get_value(hostname, P#dp.hostinfo)]),
			{reply,error1({error,Msg}),P#dp{conn = {error,closed}, rii = P#dp.rii+1}};
		{_, {error, Msg}} ->
			(catch thrift_client:close(P#dp.conn)),
			self() ! reconnect,
			error_logger:format("reconnecting to db due to error"),
			{reply,error1({error, Msg}), P#dp{conn = {error,Msg}, rii = P#dp.rii+1}};
		{error,E} ->
			(catch thrift_client:close(P#dp.conn)),
			self() ! reconnect,
			error_logger:format("reconnecting to db due to error"),
			{reply, error1({error,E}), P#dp{conn = {error,E}, rii = P#dp.rii+1}};
		{'EXIT',{badarg,_}} ->
			{reply,badarg,P#dp{rii = P#dp.rii+1}}
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

handle_info(check, P) ->
	erlang:send_after(100,self(),check),
	case P#dp.conn of
		{error,_} ->
			ok;
		_ when P#dp.rii == 0 ->
			Me = self(),
			spawn(fun() -> gen_server:call(Me, {call, actor_types,[]}) end);
		_ ->
			ok
	end,
	{noreply, P#dp{rii = 0}};
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
		{error,closed} ->
			{error,connection_failed};
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
