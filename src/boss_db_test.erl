-module(boss_db_test).
-export([start/0]).

start() ->
	boss_db_test_app:start(undefined, undefined).
