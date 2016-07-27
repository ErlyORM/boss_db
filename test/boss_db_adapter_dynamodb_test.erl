-module(boss_db_adapter_dynamodb_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(T_MODULE, boss_db_adapter_dynamodb).

convert_type_to_ddb_test() ->
    ?assert(proper:check_spec({?T_MODULE,convert_type_to_ddb, 1},
			      [{to_file, user}])),
    ok.
not_empty_test() ->
    ?assert(proper:check_spec({?T_MODULE,not_empty, 1},
			      [{to_file, user}])),
    ok.
% val_to_binary_test() ->
%
% operator_to_ddb_test() ->
%     ?assert(proper:check_spec({?T_MODULE,operator_to_ddb, 1},
%  			      [{to_file, user}])),
%      ok.
