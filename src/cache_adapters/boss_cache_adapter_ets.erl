-module (boss_cache_adapter_ets).
-author('bronzeboyvn@gmail.com').
-behaviour(boss_cache_adapter).

-export([init/1, start/0, start/1, stop/1, terminate/1]).
-export([get/3, set/5, delete/3]).

-spec start() -> 'ok'.
-spec start(_) -> 'ok'.
-spec stop(_) -> 'ok'.
-spec init(_) -> {'ok','undefined'}.
-spec terminate(_) -> 'ok'.
-spec get(_,atom() | string() | number(),_) -> any().
-spec set(_,atom() | string() | number(),_,_,non_neg_integer()) -> 'ok'.
-spec delete(_,atom() | string() | number(),_) -> 'ok'.
-spec term_to_key(atom() | string() | number(),_) -> string().

start() ->
    cache_server:start_link(),
    check_server:start_link(),
    ok.

start(Options) ->
    cache_server:start_link(Options),
    check_server:start_link(Options),
    ok.

stop(_Conn) ->
    cache_server:stop(),
    check_server:stop(),
    ok.

init(_Options) ->
    {ok, undefined}.

terminate(_Conn) ->
    ok.

get(_Conn, Prefix, Key) ->
    Term2Key = term_to_key(Prefix, Key),
    case cache_server:get(Term2Key) of
        <<>> ->
            undefined;
        Bin ->
            binary_to_term(Bin)
    end.

set(_Conn, Prefix, Key, Val, TTL) ->
    Term2Key = term_to_key(Prefix, Key),
    ValBin = term_to_binary(Val),
    cache_server:set(Term2Key, ValBin, TTL).

delete(_Conn, Prefix, Key) ->
    Term2Key = term_to_key(Prefix, Key),
    cache_server:delete(Term2Key).

% internal
term_to_key(Prefix, Term) ->
    lists:concat([Prefix, ":", boss_cache:to_hex(erlang:md5(term_to_binary(Term)))]).
