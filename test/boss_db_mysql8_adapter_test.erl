-module(boss_db_mysql8_adapter_test).
-include_lib("eunit/include/eunit.hrl").
-include_lib("pmod_transform/include/pmod.hrl").
-compile(export_all).

-define(test(Desc, F), {setup, fun setup/0, fun cleanup/1, {Desc, F}}).

setup() ->

	MysqlHost = os:getenv("MYSQL_HOST", "localhost"),
	MysqlPort = os:getenv("MYSQL_PORT", "3306"),
	MysqlUser = os:getenv("MYSQL_USER", ""),
	MysqlPassword = os:getenv("MYSQL_PASSWORD", ""),
	MysqlDbName = os:getenv("MYSQL_TEST_DBNAME", "test"),

	?debugFmt("MysqlUser = ~p", [MysqlUser]),

	if 
		length(MysqlUser) =:= 0 ->
			skip_case;
		true ->
			{ok, developer} = boss_record_compiler:compile("developer.erl"),
			DBOptions = [
			    {adapter, mysql},
			    {db_host,  MysqlHost},
			    {db_port, list_to_integer(MysqlPort)},
			    {db_username, MysqlUser},
			    {db_password, MysqlPassword},
			    {db_database, MysqlDbName},
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
			ok = boss_db:execute("create table developers( id bigint auto_increment primary key, name varchar(20), country varchar(10), created_at datetime )") 			
 	end,
 	ok.

cleanup(_) ->
	ok.

delete_all() ->
	ok = boss_db:execute("delete from developers").

t_test_() ->

	?test("test raw query", [
								?_test(test_raw_sql()), 
								?_test(test_find_model_by_sql()),
								?_test(test_new_model()),
								?_test(test_find_model()),
								?_test(test_count_model()),
								?_test(test_delete_model()),
								?_test(test_transaction()),
								?_test(test_transaction_error())
								]).

test_raw_sql() ->		
	delete_all(),
	ok = boss_db:execute("insert developers (name, country) values ('Pedro', 'Brazil')"),
	{ok,[<<"id">>, <<"name">>,<<"country">>, <<"created_at">>],[[1, <<"Pedro">>,<<"Brazil">>, null]]}  = boss_db:execute("select * from developers where name = 'Pedro'"),
	ok.

test_find_model_by_sql() ->	
	delete_all(),
	ok = boss_db:execute("insert into developers (name, country) values ('Jonas', 'Brazil')"),
	[{developer,_,"Jonas","Brazil", _}] = boss_db:find_by_sql(developer, "select * from developers where name = 'Jonas'"),
	ok.

test_new_model() ->	
	delete_all(),
	Now = calendar:local_time(),
	Developer = developer:new(id, "Carlos", "Brazil", Now),
	{ok, NewDeveloper} = Developer:save(),
	{developer, _,"Carlos","Brazil", Now} = NewDeveloper,
	ok.

test_find_model() ->	
	delete_all(),
	Developer = developer:new(id, "Carlos", "Brazil", calendar:local_time()),
	{ok, NewDeveloper} = Developer:save(),
	NewDeveloper = boss_db:find_first(developer, [{id, 'equals', NewDeveloper:id()}]),
	NewDeveloper = boss_db:find_last(developer, [{id, 'equals', NewDeveloper:id()}]),
	ok.

test_count_model() ->	
	delete_all(),
	Developer = developer:new(id, "Carlos", "Brazil", calendar:local_time()),
	{ok, NewDeveloper} = Developer:save(),
	1 = boss_db:count(developer),
	ok.

test_delete_model() ->	
	delete_all(),
	Developer = developer:new(id, "Carlos", "Brazil", calendar:local_time()),
	{ok, NewDeveloper} = Developer:save(),
	ok = boss_db:delete(NewDeveloper:id()),
	0 = boss_db:count(developer),
	ok.

test_transaction() ->		
	delete_all(),
	{atomic, _} = boss_db:transaction(fun() -> 
		Developer = developer:new(id, "Carlos", "Brazil", calendar:local_time()),
		{ok, _} = Developer:save(),
		ok	
	end),

	1 = boss_db:count(developer),

	ok.

test_transaction_error() ->		
	delete_all(),
	{aborted, _} = boss_db:transaction(fun() -> 
		Developer = developer:new(id, "Carlos", "Brazil", calendar:local_time()),
		{ok, _} = Developer:save(),
		OtherDeveloper = developer:new(id, "Mario", "Brazilzilzilzilzilzilzil", calendar:local_time()),
		OtherDeveloper:save() % return error		
	end),

	0 = boss_db:count(developer),

	ok.