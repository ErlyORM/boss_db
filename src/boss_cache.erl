-module(boss_cache).
-export([start/0, start/1]).
-export([stop/0]).
-export([get/2, set/4, delete/2]).
-export([to_hex/1]).
-define(POOLNAME, boss_cache_pool).

start() ->
    Adapter	= boss_cache_adapter_memcached_bin,
    start([{adapter, Adapter}, {cache_servers, [{"127.0.0.1", 11211, 1}]}]).

start(Options) ->
    AdapterName = proplists:get_value(adapter, Options, memcached_bin),
    Adapter	= list_to_atom(lists:concat(["boss_cache_adapter_", AdapterName])),
    Adapter:start(Options),
    boss_cache_sup:start_link(Options).

stop() ->
    ok.

set(Prefix, Key, Val, TTL) ->
    boss_pool:call(?POOLNAME, {set, Prefix, Key, Val, TTL}).

get(Prefix, Key) ->
    boss_pool:call(?POOLNAME, {get, Prefix, Key}).

delete(Prefix, Key) ->
    boss_pool:call(?POOLNAME, {delete, Prefix, Key}).


%% from mochiweb project, mochihex:to_hex/1
%% @spec to_hex(integer | iolist()) -> string()
%% @doc Convert an iolist to a hexadecimal string.
to_hex(0) ->
    "0";
to_hex(I) when is_integer(I), I > 0 ->
    to_hex_int(I, []);
to_hex(B) ->
    to_hex(iolist_to_binary(B), []).

%% @spec hexdigit(integer()) -> char()
%% @doc Convert an integer less than 16 to a hex digit.
hexdigit(C) when C >= 0, C =< 9 ->
    C + $0;
hexdigit(C) when C =< 15 ->
    C + $a - 10.

%% Internal API

to_hex(<<>>, Acc) ->
    lists:reverse(Acc);
to_hex(<<C1:4, C2:4, Rest/binary>>, Acc) ->
    to_hex(Rest, [hexdigit(C2), hexdigit(C1) | Acc]).

to_hex_int(0, Acc) ->
    Acc;
to_hex_int(I, Acc) ->
    to_hex_int(I bsr 4, [hexdigit(I band 15) | Acc]).
