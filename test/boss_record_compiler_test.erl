-module(boss_record_compiler_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").


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
        
  
