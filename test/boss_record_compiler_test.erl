-module(boss_record_compiler_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-type error(T)     :: {ok, T} | {error, string()}.
-type syntaxTree() :: erl_syntax:syntaxTree().
-type name()       :: atom()|[byte(),...].
-type fctn_n()     :: {atom(), non_neg_integer()}.
-type fctn()       :: {function, atom(), atom(), non_neg_integer(), _}.
-type pair()       :: {atom(),atom()}.
-type assoc()      :: {has,        {atom(), integer()}}          |
                      {has,        {atom(), integer(), [any()]}} |
                      {belongs_to, atom()}.



make_counters_test() ->
    ?assert(proper:check_spec({boss_record_compiler,make_counters, 1},
                              [{to_file, user}])),
    ok.


prop_duplicated_forms() ->
    ?FORALL({Parameters},
            {
             list(atom())
             },
            begin
                {TokenInfo, Attributes,Counters} = {[], [], [c]},
                ModuleName = 'test',
                case boss_record_compiler:make_generated_forms(ModuleName,Parameters, TokenInfo, Attributes,Counters) of
                    {ok, GF} ->
                        not(boss_record_compiler:has_duplicates(Parameters));
                    {error, _} ->
                        DupFields = Parameters -- sets:to_list(sets:from_list(Parameters)),
                        boss_record_compiler:has_duplicates(Parameters)
                end
            end).


make_generated_forms_test() ->
            ?assert(proper:check_spec({boss_record_compiler,make_generated_forms, 5},
                                      [{to_file, user}])),
            ?assert(proper:quickcheck(prop_duplicated_forms(),
                                      [{to_file, user}])),

            ok.

has_duplicates_test() ->
    ?assert( boss_record_compiler:has_duplicates([1,1])),
    ?assertNot(boss_record_compiler:has_duplicates([1,2])),
    ?assert(proper:check_spec({boss_record_compiler,has_duplicates, 1},
                                      [{to_file, user}])),
    ok.


list_functions_test() ->
            ?assert(proper:check_spec({boss_record_compiler,list_functions, 1},
                                      [{to_file, user}])),
            ?assert(proper:check_spec({boss_record_compiler,list_functions, 2},
                                      [{to_file, user}])),
            ok.

override_functions_test_() ->
    {timeout,
     300,
     ?_test(
        begin
            ?assert(proper:check_spec({boss_record_compiler,override_functions, 2},
                                      [{to_file, user},
                                       40])),
            ?assert(proper:check_spec({boss_record_compiler,override_functions, 3},
                                      [{to_file, user},
                                       40])),
            ok
        end)}.

export_forms_test() ->
    {timeout,
     300,
     ?_test(
        begin

            ?assert(proper:check_spec({boss_record_compiler,export_forms, 1},
                                      [{to_file, user}])),

            ?assert(proper:check_spec({boss_record_compiler,export_forms, 2},
                                      [{to_file, user},
                                       40])),
            ok
        end)}.




database_column_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,database_columns_forms, 3},
                              [{to_file, user}])),
    ok.


database_table_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,database_table_forms, 2},
                              [{to_file, user}])),
    ok.


attribute_types_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,attribute_types_forms, 2},
                              [{to_file, user}])),
    ok.


validate_types_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,validate_types_forms, 1},
                              [{to_file, user}])),
    ok.

validate_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,validate_forms, 1},
                              [{to_file, user}])),
    ok.

save_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,save_forms, 1},
                              [{to_file, user}])),
    ok.


parameter_getter_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,parameter_getter_forms, 1},
                              [{to_file, user}])),
    ok.


deep_get_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,deep_get_forms, 0},
                              [{to_file, user}])),
    ok.

get_attributes_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,get_attributes_forms, 2},
                              [{to_file, user}])),
    ok.


set_attributes_forms_test() ->
    ?assert(proper:check_spec({boss_record_compiler,set_attributes_forms, 2},
                              [{to_file, user}])),
    ok.


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


