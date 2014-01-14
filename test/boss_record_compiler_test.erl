-module(boss_record_compiler_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

association_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler, association_forms, 2},
                              [{to_file, user}])),
    ok.


%% belongs_to_list_forms_test() ->
%%     ?assert(proper:check_spec({boss_record_compiler, belongs_to_list_forms, 1},
%%                               [{to_file, user}])),
%%     ok.

belongs_to_list_make_list_test() ->
    ?assert(proper:check_spec({boss_record_compiler, belongs_to_list_make_list, 1},
                              [{to_file, user}])),
    ok.

attribute_names_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler, attribute_names_forms, 2},
                              [{to_file, user}])),
    ok.


has_one_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler, has_one_forms, 3},
                              [{to_file, user}])),
    ok.


has_many_forms_test_test() ->
    ?assert(proper:check_spec({boss_record_compiler, has_many_forms, 4},
                              [{to_file, user}])),
    ok.


first_or_undefined_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler, first_or_undefined_forms, 1},
                              [{to_file, user}])),
    ok.



has_many_application_forms_test() ->
 ?assert(proper:check_spec({boss_record_compiler, has_many_application_forms, 6},
                              [{to_file, user}])),
    ok.


has_many_query_forms_test() ->
 ?assert(proper:check_spec({boss_record_compiler, has_many_query_forms, 1},
                              [{to_file, user}])),
    ok.


has_many_query_forms_with_conditions_test() ->
 ?assert(proper:check_spec({boss_record_compiler, has_many_query_forms_with_conditions, 1},
                              [{to_file, user}])),
    ok.
        


counter_belongs_to_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler, belongs_to_forms, 3},
                              [{to_file, user}])),
    ok.


counter_getter_forms_test() ->
    ?assertEqual([], boss_record_compiler:counter_getter_forms([])),
    ?assert(proper:check_spec({boss_record_compiler, counter_getter_forms, 1},
                              [{to_file, user}])),
    ok.


counter_reset_forms_test() ->
    ?assertEqual([], boss_record_compiler:counter_reset_forms([])),
    ?assert(proper:check_spec({boss_record_compiler, counter_reset_forms, 1},
                              [{to_file, user}])),
    ok.

counter_incr_forms_test() ->
    ?assertEqual([], boss_record_compiler:counter_incr_forms([])),
    ?assert(proper:check_spec({boss_record_compiler, counter_incr_forms, 1},
                              [{to_file, user}])),
    ok.

counter_name_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler, counter_name_forms, 1},
                              [{to_file, user}])),
    ok.
        
  
    
paramater_to_colname_test() ->
    ?assertEqual("test",     boss_record_compiler:parameter_to_colname('test')),
    ?assertEqual("test_one",boss_record_compiler:parameter_to_colname('TestOne')),
    ?assert(proper:check_spec({boss_record_compiler, parameter_to_colname, 1},
                              [{to_file, user}])),
    ok.
        
  
