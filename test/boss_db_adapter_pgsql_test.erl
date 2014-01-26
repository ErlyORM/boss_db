-module(boss_db_adapter_pgsql_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

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
    ?assert(proper:check_spec({boss_db_adapter_pgsql, pack_datetime,1},
			      [{to_file, user}])),
    ok.

    
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
