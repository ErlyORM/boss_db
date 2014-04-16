-module(boss_sql_lib).
-export([keytype/1,
        infer_type_from_id/1,
        convert_id_condition_to_use_table_ids/1,
        is_foreign_key/2,
	convert_possible_foreign_key/5,
	get_retyped_foreign_keys/1
    ]).

-define(DEFAULT_KEYTYPE, serial).

keytype(Module) when is_atom(Module) ->
    proplists:get_value(id, boss_record_lib:attribute_types(Module), ?DEFAULT_KEYTYPE);
keytype(Module) when is_list(Module) ->
    proplists:get_value(id, boss_record_lib:attribute_types(list_to_atom(Module)), ?DEFAULT_KEYTYPE);
keytype(Record) when is_tuple(Record) andalso is_atom(element(1, Record)) ->
    proplists:get_value(id, Record:attribute_types(), ?DEFAULT_KEYTYPE).

infer_type_from_id(Id) when is_list(Id) ->
    [Type, TableId] = re:split(Id, "-", [{return, list}, {parts, 2}]),
    TypeAtom = list_to_atom(Type),
    IdColumn = proplists:get_value(id, boss_record_lib:database_columns(TypeAtom)),
    IdValue = case keytype(Type) of
                uuid -> TableId;
                serial -> list_to_integer(TableId)
            end,
    {TypeAtom, boss_record_lib:database_table(TypeAtom), IdColumn, IdValue}.

convert_id_condition_to_use_table_ids({Key, Op, Value}) when Value =:= undefined andalso
                                                             (Op == equals orelse Op == not_equals)->
    {Key, Op, Value};

convert_id_condition_to_use_table_ids({Key, Op, Value}) when Op =:= 'equals'; Op =:= 'not_equals'; Op =:= 'gt';
                                                             Op =:= 'lt'; Op =:= 'ge'; Op =:= 'le' ->
    {_Type, _TableName, _IdColumn, TableId} = infer_type_from_id(Value),
    {Key, Op, TableId};
convert_id_condition_to_use_table_ids({Key, Op, {Min, Max}}) when Op =:= 'in'; Op =:= 'not_in' ->
    {_Type, _TableName, _IdColumn, TableId1} = infer_type_from_id(Min),
    {_Type, _TableName, _IdColumn, TableId2} = infer_type_from_id(Max),
    {Key, Op, {TableId1, TableId2}};
convert_id_condition_to_use_table_ids({Key, Op, ValueList}) when is_list(ValueList) andalso (Op =:= 'in' orelse Op =:= 'not_in') -> 
    Value2 = lists:map(fun(V) ->
                {_Type, _TableName, _IdColumn, TableId} = infer_type_from_id(V),
                TableId
        end, ValueList),
    {Key, Op, Value2}.

is_foreign_key(Type, Key) when is_atom(Key) ->
	KeyTokens = string:tokens(atom_to_list(Key), "_"),
	LastToken = hd(lists:reverse(KeyTokens)),
	case (length(KeyTokens) > 1 andalso LastToken == "id") of
		true -> 
            Module = list_to_atom(join(lists:reverse(tl(lists:reverse(KeyTokens))), "_")),
            DummyRecord = boss_record_lib:dummy_record(Type),
            lists:member(Module, DummyRecord:belongs_to_names());
		false -> false
	end;
is_foreign_key(_Type, _Key) -> false.

get_retyped_foreign_keys(Type) when is_atom(Type) ->
    % a list of belongs_to-s that use a different module and field names.
    % this mainly to reduce the complexity from O(N*M) to O(N*L)
    % where N is all fields, M is all of the belongs_to-s, and L is only the differing ones.
    lists:foldl(fun ({X, X}, Acc) when is_atom(X) ->
			Acc;
		    ({X, Y}, Acc) when is_atom(X) andalso is_atom(Y) ->
			[{X, Y} | Acc]
		end, [], boss_record_lib:belongs_to_types(Type)).

integer_to_id(Val, KeyString) when is_list(KeyString) ->
    ModelName = string:substr(KeyString, 1, string:len(KeyString) - string:len("_id")),
    ModelName ++ "-" ++ boss_record_lib:convert_value_to_type(Val, string).

convert_possible_foreign_key(AwkwardAssociations, Type, Key, Value, AttrType) ->
    case boss_sql_lib:is_foreign_key(Type, Key) of
	true -> 
	    case [ ModuleName || {FieldName, ModuleName} <- AwkwardAssociations, list_to_atom(atom_to_list(FieldName) ++ "_id") =:= Key] of 
		[] ->
		    integer_to_id(Value, atom_to_list(Key));
		[Module] when is_atom(Module) ->
		    atom_to_list(Module) ++ "-" ++ boss_record_lib:convert_value_to_type(Value, string) 
	    end;
	false -> 
	    boss_record_lib:convert_value_to_type(Value, AttrType)
    end.

join([], _) -> [];
join([List|Lists], Separator) ->
     lists:flatten([List | [[Separator,Next] || Next <- Lists]]).
