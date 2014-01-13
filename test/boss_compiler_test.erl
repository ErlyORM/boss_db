-module(boss_compiler_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-type special_chars() :: 8800|8804|8805|8712|8713|8715|8716|8764|8769|8839|8841|9745|8869|10178|8745.

transform_char_test() ->
    ?assert(proper:quickcheck(prop_transform_char(),
                              [{to_file,user}])),
    ok.

    
prop_transform_char() ->
    ?FORALL(Char,
            special_chars(),
            begin
                {ok, Cond} = boss_compiler:transform_char(Char),
                is_list(Cond)
            end).


flatten_token_locations_test() ->
    ?assert(proper:check_spec({boss_compiler, flatten_token_locations, 1},
                               [{to_file, user}])),
    ok.

cut_at_location_test() ->
    ?assert(proper:check_spec({boss_compiler, cut_at_location,3},
                              [{to_file,user}])),
    ok.

transform_tokens_test() ->
    
    ?assert(proper:quickcheck(prop_transform_tokens(),
                              [{to_file,user}])),
    ?assert(proper:quickcheck(prop_transform_tokens_null(),
                              [{to_file,user}])),
    ok.
prop_transform_tokens() ->
    ?FORALL(Tokens,
            [atom()],
            begin
                Tokens =:=boss_compiler:transform_tokens(fun(X) when is_list(X)->
                                                                 X
                                                         end, Tokens)
            end).
prop_transform_tokens_null() ->
    ?FORALL(Tokens,
            [atom()],
            begin
                {Tokens,undefined} =:=boss_compiler:transform_tokens(undefined, Tokens)
            end).

make_parse_errors_test() ->
    ?assert(proper:check_spec({boss_compiler, make_parse_errors,1},
                              [{to_file,user}])),
    ok.

parse_has_errors_test() ->
    ?assert(proper:check_spec({boss_compiler, parse_has_errors,3},
                              [{to_file,user}])),
    ok.
