-module(boss_pool).
-export([call/2, call/3]).

call(Pool, Msg) ->
    Worker = poolboy:checkout(Pool),
    Reply = gen_server:call(Worker, Msg),
    poolboy:checkin(Pool, Worker),
    Reply.

call(Pool, Msg, Timeout) ->
    Worker = poolboy:checkout(Pool, true, Timeout),
    Reply = gen_server:call(Worker, Msg, Timeout),
    poolboy:checkin(Pool, Worker),
    Reply.
