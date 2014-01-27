-module(boss_db_adapter_mysql_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

pack_datetime_test() ->
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

substr_to_i(Result, S, E) ->
    {I,_} = string:to_integer(string:sub_string(Result, S, E)),
    I
.
