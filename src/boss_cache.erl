-module(boss_cache).
-export([start/0, start/1]).
-export([stop/0]).
-export([get/2, set/4, delete/2]).

-define(POOLNAME, boss_cache_pool).

start() ->
    Adapter = boss_cache_adapter_memcached_bin,
    start([{adapter, Adapter}, {cache_servers, [{"127.0.0.1", 11211, 1}]}]).

start(Options) ->
    Adapter = proplists:get_value(adapter, Options, boss_cache_adapter_memcached_bin),
    Adapter:init(Options),
    boss_cache_sup:start_link(Options).

stop() ->
    ok.

set(Prefix, Key, Val, TTL) ->
    boss_pool:call(?POOLNAME, {set, Prefix, Key, Val, TTL}).

get(Prefix, Key) ->
    boss_pool:call(?POOLNAME, {get, Prefix, Key}).

delete(Prefix, Key) ->
    boss_pool:call(?POOLNAME, {delete, Prefix, Key}).
