-module(boss_cache_adapter_memcached_bin).
-behaviour(boss_cache_adapter).

-export([init/1, start/0, start/1, stop/1, terminate/1]).
-export([get/3, set/5, delete/3]).

start() ->
    start([]).

start(Options) ->
    CacheServers = proplists:get_value(cache_servers, Options, [{"localhost", 11211, 1}]),
    erlmc:start_link(CacheServers),
    ok.

stop(_Conn) ->
    erlmc:quit(),
    ok.

init(_Options) ->
    {ok, undefined}.

terminate(Conn) ->
    stop(Conn),
    ok.

get(_Conn, Prefix, Key) ->
    case erlmc:get(term_to_key(Prefix, Key)) of
        <<>> ->
            undefined;
        Bin ->
            binary_to_term(Bin)
    end.

set(_Conn, Prefix, Key, Val, TTL) ->
    erlmc:set(term_to_key(Prefix, Key), term_to_binary(Val), TTL, 5000).

delete(_Conn, Prefix, Key) ->
    erlmc:delete(term_to_key(Prefix, Key)).

% internal
term_to_key(Prefix, Term) ->
    lists:concat([Prefix, ":", boss_cache:to_hex(erlang:md5(term_to_binary(Term)))]).
