-module(boss_db_sup).
-author('emmiller@gmail.com').

-behaviour(supervisor).

-export([start_link/0, start_link/1]).

-export([init/1]).

-define(DEFAULT_POOL_SIZE, 5).
-define(DEFAULT_POOL_MAX_OVERFLOW, 10).

start_link() ->
    start_link([]).

start_link(StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, StartArgs).

init(StartArgs0) ->
    StartArgs2 = case lists:keytake(pool_size, 1, StartArgs0) of
        false ->
            [{size, ?DEFAULT_POOL_SIZE} | StartArgs0];
        {value, {pool_size, PoolSize}, StartArgs1} ->
            [{size, PoolSize} | StartArgs1]
    end,
    StartArgsN = case lists:keytake(pool_max_overflow, 1, StartArgs2) of
        false ->
            [{max_overflow, ?DEFAULT_POOL_MAX_OVERFLOW} | StartArgs2];
        {value, {pool_max_overflow, MaxOverflow}, StartArgs3} ->
            [{max_overflow, MaxOverflow} | StartArgs3]
    end,
    Args = [{name, {local, boss_db_pool}},
            {worker_module, boss_db_controller} | StartArgsN],
    PoolSpec = {db_controller, {poolboy, start_link, [Args]},
                permanent, 2000, worker, [poolboy]},
    {ok, {{one_for_one, 10, 10}, [PoolSpec]}}.
