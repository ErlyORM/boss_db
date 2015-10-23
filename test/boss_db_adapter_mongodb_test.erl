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

%% build_conditions2_test() ->
%%     ?assert(proper:check_spec({?T_MODULE, build_conditions2, 3},
%% 			      [{to_file, user}])),
%%     ok.

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



attr_value_test() ->
    ?assert(proper:check_spec({?T_MODULE,attr_value, 2},
			      [{to_file, user}])),
    ok.


prop_pack_value_binary() ->
    ?FORALL(Input,
	    binary(),
	    begin
		Result = ?T_MODULE:pack_value(Input),
		is_binary(Result)
	    end).

prop_pack_value_integer_list() ->
    ?FORALL(Integers,
	    list(integer()),
	    begin
		Result = ?T_MODULE:pack_value({integers, Integers}),
		Result =:= Integers
	    end).

pack_value_test() ->
    ?assert(proper:quickcheck(prop_pack_value_integer_list(),
			      [{to_file, user}])),

    ?assert(proper:quickcheck(prop_pack_value_binary(),
    			      [{to_file, user}])),

    ok.

%% unpack_id_test() ->
%%     ?assert(proper:check_spec({boss_db_adapter_mongodb,unpack_id, 2},
%% 			      [{to_file, user}])),
%%     ok.
%% multiple_where_clauses_string_test() ->
%%     ?assert(proper:quickcheck(prop_multiple_where_cluases_string(),
%% 			      [{to_file,user}])),
%%     ok.

%% prop_multiple_where_cluases_string() ->
%%     ?FORALL( ValueList,
%% 	     ?SIZED(Size, words:word(Size)),
%% 	     begin
%% 		 Key = "Test",
%% 		 Result = ?T_MODULE:multiple_where_clauses_string("~p=~p",
%% 								  Key, ValueList,"and"),
%% 		 ?debugVal(Result),
%% 		 is_list(Result)
%% 	    end).
