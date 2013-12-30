-module(boss_db_adapter_mongodb_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(T_MODULE, boss_db_adapter_mongodb).

hex0_test() ->
    ?assert(proper:check_spec({?T_MODULE,hex0, 1},
			      [{to_file, user}])),
    ok.

dec0_test() ->
    ?assert(proper:check_spec({?T_MODULE,dec0, 1},
			      [{to_file, user}])),
    ok.

prolist_test() ->
    ?assert(proper:check_spec({?T_MODULE,proplist_to_tuple, 1},
			      [{to_file, user}])),
    ?assert(proper:quickcheck(prop_tuple_proplist_conversion(),
			      [{to_file, user}])),
    ok.

-type proplist() :: [{any(), any()}].
prop_tuple_proplist_conversion() ->
    ?FORALL(Proplist,
	    proplist(), 
	    begin
		Tuple = ?T_MODULE:proplist_to_tuple(Proplist),
		NProplist = ?T_MODULE:tuple_to_proplist(Tuple),
		NProplist =:= Proplist
	    end).

pack_sort_order_test() ->
    ?assert(proper:check_spec({?T_MODULE,pack_sort_order, 1},
			      [{to_file, user}])),
    ok.
    
boss_to_mongo_op_test() ->
    ?assert(proper:check_spec({?T_MODULE,boss_to_mongo_op, 1},
			      [{to_file, user}])),
    ok.
