-module(actordb_client_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_children/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_children() ->
	case application:get_env(actordb_client, pools) of
		undefined ->
			PoolSpecs = [];
		{ok, Pools} ->
			PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
				PoolArgs = [{name, {local, Name}},
					{worker_module, actordb_client}] ++ SizeArgs,
				poolboy:child_spec(Name, PoolArgs, WorkerArgs)
			end, Pools)
	end,
	[supervisor:start_child(?MODULE, C) || C <- PoolSpecs].

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	{ok, { {one_for_one, 5, 10}, []} }.

