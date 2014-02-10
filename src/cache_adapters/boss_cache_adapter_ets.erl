-module (boss_cache_adapter_ets).
-author('bronzeboyvn@gmail.com').
-behaviour(boss_cache_adapter).

-export([init/1, start/0, start/1, stop/1, terminate/1]).
-export([get/3, set/5, delete/3]).

-spec start() -> {'ok', pid()}.
-spec start(_) -> {'ok', pid()}.
-spec stop(_) -> 'ok'.
-spec init(_) -> {'ok',pid()} | {'error', string()}.
-spec terminate(pid()) -> 'ok'.
-spec get(pid(),atom() | string() | number(),_) -> 'undefined' | any().
-spec set(pid(),atom() | string() | number(),_,_,non_neg_integer()) -> 'ok'.
-spec delete(pid(),atom() | string() | number(),_) -> 'ok'.
-spec term_to_key(atom() | string() | number(),_) -> string().

start() ->
    {ok, CheckPid} = check_server:start_link(),
    {ok, CachePid} = cache_server:start_link([{checkpid, CheckPid}]),
    {ok, CachePid}.

start(Options) ->
    {ok, CheckPid} = check_server:start_link(Options),
    {ok, CachePid} = cache_server:start_link([{checkpid, CheckPid} | Options]),
    {ok, CachePid}.

stop(Conn) ->
    cache_server:stop(Conn),
    ok.

init(Options) ->
    case proplists:get_value(ets_cachepid, Options, error) of
        error ->
            {error, "boss_cache_adapter_ets hasn't started ets_cache"};
	Conn ->
            {ok, Conn}
    end.

terminate(Conn) ->
    ok.

get(Conn, Prefix, Key) ->
    Term2Key = term_to_key(Prefix, Key),
    case cache_server:get(Conn, Term2Key) of
        <<>> ->
            undefined;
        Bin ->
            binary_to_term(Bin)
    end.

set(Conn, Prefix, Key, Val, TTL) ->
    Term2Key = term_to_key(Prefix, Key),
    ValBin = term_to_binary(Val),
    cache_server:set(Conn, Term2Key, ValBin, TTL),
    ok.

delete(Conn, Prefix, Key) ->
    Term2Key = term_to_key(Prefix, Key),
    cache_server:delete(Conn, Term2Key),
    ok.

% internal
term_to_key(Prefix, Term) ->
    lists:concat([Prefix, ":", boss_cache:to_hex(erlang:md5(term_to_binary(Term)))]).
