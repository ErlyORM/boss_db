-module(boss_db_module_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").


sort_order_test() ->
    ?assertEqual(descending, boss_db:sort_order([{descending, true}])),
    ?assertEqual(ascending, boss_db:sort_order([{descending, false}])),
    ?assertEqual(ascending, boss_db:sort_order([])).



prop_normalize_conditions_final() ->
    ?FORALL(Acc,
	    list(integer()),
	    begin
		lists:reverse(Acc) =:= boss_db:normalize_conditions([], Acc)
	    end).

prop_normalize_conditions_eq_operator() ->
    ?FORALL({Key, Operator, Value},
	    {atom(), boss_db:eq_operatator(),list(range(65,90))},
	    begin
		[{Key, NewOp, Value}] =  boss_db:normalize_conditions([{Key, Operator, Value}], []),
		case Operator of
		    'eq' ->
			NewOp =:= 'equals';
		    'ne' ->
			NewOp =:= 'not_equals'
		end
	    end).
prop_normalize_conditions_any_operator() ->
    ?FORALL({Key, Operator, Value},
	    {atom(),atom(),list(range(65,90))},
	    begin
		[{Key, Operator, Value}] =:=  boss_db:normalize_conditions([{Key, Operator, Value}], [])
	    end).

prop_normalize_conditions_any_operator_list() ->
    ?FORALL({Key, Operator, Value},
	    {atom(),atom(),list(range(65,90))},
	    begin
		[{Key, Operator, Value}] =:=  boss_db:normalize_conditions([Key, Operator, Value], [])
	    end).

prop_normalize_conditions_any_operator_options() ->
    ?FORALL({Key, Operator, Value, Options},
	    {atom(),atom(),list(range(65,90)), list({atom(), binary()})},
	    begin
		[{Key, Operator, Value, Options}] =:=  boss_db:normalize_conditions([{Key, Operator, Value, Options}], [])
	    end).

prop_normalize_conditions_default_operator() ->
    ?FORALL({Key, Value},
	    {atom(),list(range(65,90))},
	    begin
		[{Key, equals, Value}] =:=  boss_db:normalize_conditions([{Key, Value}], [])
	    end).



normalize_conditions_test() ->
    ?assert(proper:quickcheck(prop_normalize_conditions_final(),
			      [{to_file, user}])),

    ?assert(proper:quickcheck(prop_normalize_conditions_eq_operator(),
			      [{to_file, user}])),

    ?assert(proper:quickcheck(prop_normalize_conditions_any_operator(),
			      [{to_file, user}])),

    ?assert(proper:quickcheck(prop_normalize_conditions_any_operator_list(),
			      [{to_file, user}])),

    ?assert(proper:quickcheck(prop_normalize_conditions_any_operator_options(),
			      [{to_file, user}])),

    ?assert(proper:quickcheck(prop_normalize_conditions_default_operator(),
			      [{to_file, user}])),
    ok.


