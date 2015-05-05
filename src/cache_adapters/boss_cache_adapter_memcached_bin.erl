-module(boss_cache_adapter_memcached_bin).
-behaviour(boss_cache_adapter).

-export([init/1, start/0, start/1, stop/1, terminate/1]).
-export([get/3, set/5, delete/3]).

start() ->
    start([]).

start(Options) ->
    CacheServers = proplists:get_value(cache_servers, Options, [{"localhost", 11211, 1}]),
    ok = erlmc:start(CacheServers),
    ok.

stop(_Conn) ->
    erlmc:quit().

init(Options) ->
    {ok, Options}.

terminate(Conn) ->
    stop(Conn).

get_safe(Conn, Prefix, Key) ->
  case erlmc:get(term_to_key(Prefix, Key)) of
    <<>> -> undefined;
    Bin -> binary_to_term(Bin)
  end.

refresh_memcached_process(Host, Port, PoolSize) ->
  refresh_memcached_process(Host, Port, PoolSize, 1).

refresh_memcached_process(Host, Port, PoolSize, TryCount) ->
  lager:info("Refreshing memcached. host ~p, port ~p, poolsize ~p, try ~p",
             [Host, Port, PoolSize, TryCount]),
  ok = erlmc:refresh_server(Host, Port, PoolSize),
  timer:sleep(1000),
  LiveConnections = length(ets:match(erlmc_connections,
                                     {{Host, Port}, '$1'})),
  case LiveConnections =:= PoolSize of
    true -> lager:info("Refreshing memcached succeeded at try ~p", [TryCount]),
            ok;
    false -> case TryCount of
                 5 -> lager:info("Refreshing memcached failed at try ~p", [TryCount]),
                      error;
                 _ -> refresh_memcached_process(Host, Port, PoolSize, TryCount + 1)
             end
  end.

get(Conn, Prefix, Key) ->
  try erlmc:get(term_to_key(Prefix, Key)) of
      <<>> -> undefined;
      Bin -> binary_to_term(Bin)
  catch exit:erlmc_continuum_empty ->
      [{Host, Port, PoolSize}] =
        proplists:get_value(cache_servers, Conn, [{"localhost", 11211, 1}]),
      case refresh_memcached_process(Host, Port, PoolSize) of
        ok -> get_safe(Conn, Prefix, Key);
        error -> exit(erlmc_continuum_empty)
      end
  end.

set(_Conn, Prefix, Key, Val, TTL) ->
    erlmc:set(term_to_key(Prefix, Key), term_to_binary(Val), TTL).

delete(_Conn, Prefix, Key) ->
    erlmc:delete(term_to_key(Prefix, Key)).

% internal
term_to_key(Prefix, Term) ->
    lists:concat([Prefix, ":", boss_cache:to_hex(erlang:md5(term_to_binary(Term)))]).
