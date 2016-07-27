-module(boss_db_adapter_pgsql_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
%% build_insert_sql_test() ->
%%     ?assert(proper:check_spec({boss_db_adapter_pgsql,build_insert_sql, 4},
%% 			     [{to_file, user}])),
%%     ok.

column_type_to_sql_test() ->
    ?assert(proper:check_spec({boss_db_adapter_pgsql,column_type_to_sql, 1},
			     [{to_file, user}])),
    ok.

option_to_sql_test() ->
    ?assert(proper:check_spec({boss_db_adapter_pgsql,option_to_sql, 1},
			     [{to_file, user}])),
    ok.

column_option_to_sql_test() ->
    ?assert(proper:check_spec({boss_db_adapter_pgsql,column_options_to_sql, 1},
			     [{to_file, user}])),
    ok.

id_value_to_string_test() ->
    ?assert(proper:check_spec({boss_db_adapter_pgsql,id_value_to_string, 1},
			     [{to_file, user}])),
    ok.

sort_order_test() ->
    ?assert(proper:check_spec({boss_db_adapter_pgsql,sort_order_sql, 1},
			     [{to_file, user}])),
    ok.

pack_datetime_test() ->
    ?assert(proper:quickcheck(prop_pack_date_tuple(),
                              [{to_file, user}])),

    ?assert(proper:quickcheck(prop_pack_datetime_tuple(),
                              [{to_file, user}])),

    ?assert(proper:check_spec({boss_db_adapter_pgsql, pack_datetime,1},
			      [{to_file, user}])),
    ok.
-type year()   :: 1900..9999.
-type month()  :: 1..12.
-type day()    :: 1..31.
-type hour()   :: 0..24.
-type minute() :: 0..59.
-type second() :: 0..59.

prop_pack_date_tuple() ->
    ?FORALL(Date ,
            {year(), month(), day()},
            ?IMPLIES((calendar:valid_date(Date)),
                     date_format( Date)
                    )).

prop_pack_datetime_tuple() ->
    ?FORALL(DateTime = {Date ,Time},
            {{year(), month(), day()},{hour(), minute(),second()}},
            ?IMPLIES((calendar:valid_date(Date)),
                     date_format( DateTime)
                    )).

equal(A,A) ->
    true;
equal(A,B) ->
    false.
all_true(L) ->
    lists:all(fun(X) ->
                      X
              end, L).

date_format(Date = {Y,M,D}) ->
    Result = boss_db_adapter_pgsql:pack_datetime({Date,{0,0,0}}),
    all_true([equal("TIMESTAMP '", string:sub_string(Result, 1, 11)),
              equal(Y, substr_to_i(Result,12,15)),
              equal(M, substr_to_i(Result,17, 18)),
              equal(D, substr_to_i(Result,20, 21)),
              equal("T00:00:00'", string:sub_string(Result, 22)),
              true]);

date_format(DateTime = {{Y,M,D},{Hour,Min, Sec}}) ->
    Result = boss_db_adapter_pgsql:pack_datetime(DateTime),
    all_true([equal("TIMESTAMP '", string:sub_string(Result, 1, 11)),
              equal(Y, substr_to_i(Result,12,15)),
              equal(M, substr_to_i(Result,17, 18)),
              equal(D, substr_to_i(Result,20, 21)),
              equal(Hour, substr_to_i(Result,23, 25)),
              equal(Min, substr_to_i(Result,26, 28)),
              equal(Sec, substr_to_i(Result,29, 31)),


              true]).




substr_to_i(Result, S, E) ->
    {I,_} = string:to_integer(string:sub_string(Result, S, E)),
    I.

pack_value_test() ->
    ?assert(proper:check_spec({boss_db_adapter_pgsql, pack_value,1},
			      [{to_file, user}])),
	ok.

%% maybe_populate_id_value_test_() ->
%%     {setup,
%%      fun() ->

%% 	     code:load_abs("../priv/gh_repo"),
%% 	     gh_repo
%%      end,
%%      fun(Module) ->
%% 	     code:purge(Module)
%%      end,
%%      ?_test(
%% 	begin
%% 	    ?assert(proper:quickcheck(prop_maybe_populate_id_value(),
%% 	     			      [{to_file, user}])),
%% 	    ok
%% 	end)}.

%% -type keytype() ::uuid|id.
%% prop_maybe_populate_id_value() ->
%%     ?FORALL(Keytype,
%%             keytype(),
%%             begin
%%                 Record = gh_repo:new([]),


%%             end).

%% insert_attributes_test() ->
%%     {setup,
%%      fun() ->

%% 	     code:load_abs("../priv/gh_repo"),
%% 	     gh_repo
%%      end,
%%      fun(Module) ->
%% 	     code:purge(Module)
%%      end,
%%      ?_test(
%% 	begin
%% 	    %% ?assert(proper:quickcheck(prop_normalize_conditions_final(),
%% 	    %% 			      [{to_file, user}])),
%% 	    ok
%% 	end)}.

%% prop_make_insert_attributes() ->
%%     ?FORALL(
%%        {
