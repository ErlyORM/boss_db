-module (boss_cache_adapter_redis).
-author('ecestari@gmail.com').
-behaviour(boss_cache_adapter).

-export([init/1, start/0, start/1, stop/1, terminate/1]).
-export([get/3, set/5, delete/3]).

start() ->
    ok.

start(_Options) ->
    ok.

stop(Conn) ->
    redo:shutdown(Conn).

init(Options) ->
    CacheServerOpts = proplists:get_value(cache_servers, Options, []),
    redo:start_link(undefined, CacheServerOpts).

terminate(Conn) ->
    stop(Conn).

get(Conn, Prefix, Key) ->
    case redo:cmd(Conn,["GET", term_to_key(Prefix, Key)]) of
        undefined ->
            undefined;
        Bin -> 
            binary_to_term(Bin)
    end.

set(Conn, Prefix, Key, Val, TTL) ->
    redo:cmd(Conn,["SETEX",term_to_key(Prefix, Key), TTL, term_to_binary(Val)]).

delete(Conn, Prefix, Key) ->
    redo:cmd(Conn, ["DELETE", term_to_key(Prefix, Key)]).

% internal
term_to_key(Prefix, Term) ->
    lists:concat([Prefix, ":", boss_cache:to_hex(erlang:md5(term_to_binary(Term)))]).
