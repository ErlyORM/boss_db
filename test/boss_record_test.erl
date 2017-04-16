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
