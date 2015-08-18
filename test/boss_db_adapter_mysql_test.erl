-module(boss_db_adapter_mysql_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

pack_datetime_test() ->
	%replace_parameters_test(),
    ?assert(proper:quickcheck(prop_pack_date_tuple(),
                              [{to_file, user}])),
    ?assert(proper:quickcheck(prop_pack_datetime_tuple(),
                              [{to_file, user}])),

    ok.
-type year()   :: 1900..9999.
-type month()  :: 1..12.
-type day()    :: 1..31.
-type hour()   :: 0..23.
-type min()    :: 0..59.
-type second() :: 0..59.
prop_pack_date_tuple() ->
    ?FORALL(Date ,
            {year(), month(), day()},
            ?IMPLIES((calendar:valid_date(Date)),
                     date_format( Date)
                    )).

prop_pack_datetime_tuple() ->
    ?FORALL(DateTime = {Date,_Time},
            {{year(), month(), day()},
             {hour(), min(), second()}},
              ?IMPLIES((calendar:valid_date(Date)),
                     datetime_format( DateTime)
                    )).

equal(A,A) ->
    true;
equal(_A,_B) ->
    false.
all_true(L) ->
    lists:all(fun(X) ->
                      X
              end, L).

date_format(Date = {Y,M,D}) ->
    Result = boss_db_adapter_mysql:pack_date(Date),
    all_true([
              equal(Y, substr_to_i(Result,2, 5)),
              equal(M, substr_to_i(Result,7, 9)),
              equal(D, substr_to_i(Result,10, 12)),
              true]).

datetime_format(DateTime = {{Y,M,D},{H,Min, S}}) ->
    Result = boss_db_adapter_mysql:pack_datetime(DateTime),
    all_true([
              equal(Y, substr_to_i(Result,2, 5)),
              equal(M, substr_to_i(Result,7, 9)),
              equal(D, substr_to_i(Result,10, 12)),
              equal(H, substr_to_i(Result,13,15)),
              equal(Min, substr_to_i(Result, 16,18)),
              equal(S, substr_to_i(Result,19,21)),
              true]).

replace_parameters_test() ->
	{inparallel, [
	 ?assertEqual("1 10 99", replace_parameters("$1 $10 $99", lists:seq(1,99))),
	 ?assertEqual("where 'abc'='xyz'", replace_parameters("where $1=$3",["abc","ignore","xyz"])),
	 ?assertEqual("123", replace_parameters("123", [])),
	 ?assertEqual("select * from whatever where x = 'something'",replace_parameters("select * from whatever where x = $1", ["something"]))
	]}.

replace_parameters(Str, Params) ->
	lists:flatten(boss_db_adapter_mysql:replace_parameters(Str, Params)).

substr_to_i(Result, S, E) ->
    {I,_} = string:to_integer(string:sub_string(Result, S, E)),
    I
.
