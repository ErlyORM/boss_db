-module(boss_cache_sup).
-author('emmiller@gmail.com').

-behaviour(supervisor).

-export([start_link/0, start_link/1]).

-export([init/1]).

start_link() ->
    start_link([]).

start_link(StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, StartArgs).

init(StartArgs) ->
    Args = [{name, {local, boss_cache_pool}},
	    {worker_module, boss_cache_controller},
	    {size, 20}, {max_overflow, 40}|StartArgs],
    PoolSpec = 
	{cache_controller, 
	 {poolboy, start_link, [Args]}, 
	 permanent, 2000, worker, [poolboy]},
    {ok, {{one_for_one, 10, 10}, [PoolSpec]}}.
