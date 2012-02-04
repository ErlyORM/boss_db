-module(boss_db_test).
-export([start/0, start/1, stop/0]).

start() ->
    start([]).

start([]) ->
    application:start(boss_db_test).

stop() ->
    application:stop(boss_db_test).
