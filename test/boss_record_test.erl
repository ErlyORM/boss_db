-module(boss_record_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

convert_test() ->
    ?assert(proper:quickcheck(boss_record:prop_convert_attributes_l(),
			     [{to_file, user}])),
    ?assert(proper:quickcheck(boss_record:prop_convert_attributes_a(),
			     [{to_file, user}])),
    ?assert(proper:quickcheck(boss_record:prop_convert_attributes_b(),
			     [{to_file, user}])).

set_attribute_test_() ->
    {setup,
     fun() ->

	     code:load_abs("../priv/gh_repo"),
	     gh_repo
     end,
     fun(Module) ->
	     code:purge(Module)
     end,
     ?_test(
	begin
	    ?assert(proper:quickcheck(boss_record:prop_set_attribute(),
				      [{to_file, user}])),
	    ?assert(proper:quickcheck(boss_record:prop_set_attribute_data_missing(),
				      [{to_file, user}])),
	    ok
	end)}.
