-module(mysql_driver_test).
-include_lib("eunit/include/eunit.hrl").
-include_lib("pmod_transform/include/pmod.hrl").
-compile(export_all).

-define(test(Desc, F), {setup, fun setup/0, fun cleanup/1, {Desc, F}}).

setup() ->
	{ok, developer} = boss_record_compiler:compile("developer.erl"),

	DBOptions = [
	    {adapter, mysql},
	    {db_host, "127.0.0.1"},
	    {db_port, 3305},
	    {db_username, "root"},
	    {db_password, "131054"},
	    {db_database, "test"},
	    {db_configure, []},
	    {db_ssl, false}, % for now pgsql only

	    {shards, []},
	    {cache_enable, false},
	    {cache_exp_time, 0},

	    {size, 5}, % the size of the connection pool - defaults to 5
	    {max_overflow, 10} % the maximum number of temporary extra workers that can be created past the `size' just above - defaults to 10
	    %% the sum size + max_overflow effectively controls how many concurrent mysql queries can run
	],
	{ok, _} = boss_db:start(DBOptions),
	boss_news:start(),

	ok = boss_db:execute("DROP TABLE IF EXISTS developers"),
	ok = boss_db:execute("create table developers( id bigint auto_increment primary key, name varchar(50), country varchar(50) )"),

  ?debugMsg("top setup").

cleanup(_) ->
    ?debugMsg("top cleanup").

delete_all() ->
	ok = boss_db:execute("delete from developers").

t_test_() ->

	?test("test raw query", [
								?_test(test_raw_sql()), 
								?_test(test_find_model_by_sql()),
								?_test(test_new_model())]).

	%{setup, 
	%	fun top_setup/0,
	%	fun top_cleanup/1,
	%		[{generator, fun test_raw_sql/0},
	%		 {generator, fun test_find_model_by_sql/0}]}.	

test_raw_sql() ->	
	delete_all(),
	ok = boss_db:execute("insert developers (name, country) values ('Pedro', 'Brazil')"),
	{ok,[<<"id">>, <<"name">>,<<"country">>],[[1, <<"Pedro">>,<<"Brazil">>]]}  = boss_db:execute("select id, name, country from developers where name = 'Pedro'"),
	ok.

test_find_model_by_sql() ->
	delete_all(),
	ok = boss_db:execute("insert into developers (name, country) values ('Jonas', 'Brazil')"),
	[{developer,_,"Jonas","Brazil"}] = boss_db:find_by_sql(developer, "select id, name, country from developers where name = 'Jonas'"),
	ok.

test_new_model() ->
	delete_all(),
	Developer = developer:new(id, "Carlos", "Brazil"),
	{ok, NewDeveloper} = Developer:save(),
	{developer, _,"Carlos","Brazil"} = NewDeveloper,
	NewDeveloper = boss_db:find_first(developer, [{id, 'equals', NewDeveloper:id()}]),
	NewDeveloper = boss_db:find_last(developer, [{id, 'equals', NewDeveloper:id()}]),
	1 = boss_db:count(developer),
	ok = boss_db:delete(NewDeveloper:id()),
	0 = boss_db:count(developer),

	{atomic, _} = boss_db:transaction(fun() -> 
		OtherDeveloper = developer:new(id, "Carlos", "Brazil"),
		{ok, _} = OtherDeveloper:save(),
		ok	
	end),

	1 = boss_db:count(developer),

	ok.
