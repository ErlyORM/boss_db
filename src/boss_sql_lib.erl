-module(boss_sql_lib).
-export([keytype/1,
        infer_type_from_id/1,
        convert_id_condition_to_use_table_ids/1,
        is_foreign_key/1
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

is_foreign_key(Key) when is_atom(Key) ->
	KeyTokens = string:tokens(atom_to_list(Key), "_"),
	LastToken = hd(lists:reverse(KeyTokens)),
	case (length(KeyTokens) > 1 andalso LastToken == "id") of
		true -> 
			Module = join(lists:reverse(tl(lists:reverse(KeyTokens))), "_"),
    		case code:is_loaded(list_to_atom(Module)) of
        		{file, _Loaded} -> true;
        		false -> false
    		end;
		false -> false
	end;
is_foreign_key(_Key) -> false.

join([], _) -> [];
join([List|Lists], Separator) ->
     lists:flatten([List | [[Separator,Next] || Next <- Lists]]).
