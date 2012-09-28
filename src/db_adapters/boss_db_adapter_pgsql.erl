-module(boss_db_adapter_pgsql).
-behaviour(boss_db_adapter).
-export([init/1, terminate/1, start/1, stop/0, find/2, find/7]).
-export([count/3, counter/2, incr/3, delete/2, save_record/2]).
-export([push/2, pop/2, dump/1, execute/2, execute/3, transaction/2]).

start(_) ->
    ok.

stop() ->
    ok.

init(Options) ->
    DBHost = proplists:get_value(db_host, Options, "localhost"),
    DBPort = proplists:get_value(db_port, Options, 5432),
    DBUsername = proplists:get_value(db_username, Options, "guest"),
    DBPassword = proplists:get_value(db_password, Options, ""),
    DBDatabase = proplists:get_value(db_database, Options, "test"),
    pgsql:connect(DBHost, DBUsername, DBPassword, 
        [{port, DBPort}, {database, DBDatabase}]).

terminate(Conn) ->
    pgsql:close(Conn).

find(Conn, Id) when is_list(Id) ->
    {Type, TableName, TableId} = infer_type_from_id(Id),
    Res = pgsql:equery(Conn, ["SELECT * FROM ", TableName, " WHERE id = $1"], [TableId]),
    case Res of
        {ok, _Columns, []} ->
            undefined;
        {ok, Columns, [Record]} ->
            case boss_record_lib:ensure_loaded(Type) of
                true -> activate_record(Record, Columns, Type);
                false -> {error, {module_not_loaded, Type}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

find(Conn, Type, Conditions, Max, Skip, Sort, SortOrder) when is_atom(Type), is_list(Conditions), 
                                                              is_integer(Max) orelse Max =:= all, is_integer(Skip), 
                                                              is_atom(Sort), is_atom(SortOrder) ->
    case boss_record_lib:ensure_loaded(Type) of
        true ->
            Query = build_select_query(Type, Conditions, Max, Skip, Sort, SortOrder),
            Res = pgsql:equery(Conn, Query, []),
            case Res of
                {ok, Columns, ResultRows} ->
                    FilteredRows = case {Max, Skip} of
                        {all, Skip} when Skip > 0 ->
                            lists:nthtail(Skip, ResultRows);
                        _ ->
                            ResultRows
                    end,
                    lists:map(fun(Row) ->
                                activate_record(Row, Columns, Type)
                        end, FilteredRows);
                {error, Reason} ->
                    {error, Reason}
            end;
        false -> {error, {module_not_loaded, Type}}
    end.

count(Conn, Type, Conditions) ->
    ConditionClause = build_conditions(Type, Conditions),
    TableName = type_to_table_name(Type),
    {ok, _, [{Count}]} = pgsql:equery(Conn, 
        ["SELECT COUNT(*) AS count FROM ", TableName, " WHERE ", ConditionClause]),
    Count.

counter(Conn, Id) when is_list(Id) ->
    Res = pgsql:equery(Conn, "SELECT value FROM counters WHERE name = $1", [Id]),
    case Res of
        {ok, _, [{Value}]} -> Value;
        {error, _Reason} -> 0
    end.

incr(Conn, Id, Count) ->
    Res = pgsql:equery(Conn, "UPDATE counters SET value = value + $1 WHERE name = $2 RETURNING value", 
        [Count, Id]),
    case Res of
        {ok, _, _, [{Value}]} -> Value;
        {error, _Reason} -> 
            Res1 = pgsql:equery(Conn, "INSERT INTO counters (name, value) VALUES ($1, $2) RETURNING value", 
                [Id, Count]),
            case Res1 of
                {ok, _, _, [{Value}]} -> Value;
                {error, Reason} -> {error, Reason}
            end
    end.

delete(Conn, Id) when is_list(Id) ->
    {_, TableName, TableId} = infer_type_from_id(Id),
    Res = pgsql:equery(Conn, ["DELETE FROM ", TableName, " WHERE id = $1"], [TableId]),
    case Res of
        {ok, _Count} -> 
            pgsql:equery(Conn, "DELETE FROM counters WHERE name = $1", [Id]),
            ok;
        {error, Reason} -> {error, Reason}
    end.

save_record(Conn, Record) when is_tuple(Record) ->
    case Record:id() of
        id ->
            Record1 = maybe_populate_id_value(Record),
            Type = element(1, Record1),
            Query = build_insert_query(Record1),
            Res = pgsql:equery(Conn, Query, []),
            case Res of
                {ok, _, _, [{Id}]} ->
                    {ok, Record1:set(id, lists:concat([Type, "-", id_value_to_string(Id)]))};
                {error, Reason} -> {error, Reason}
            end;
        Defined when is_list(Defined) ->
            Query = build_update_query(Record),
            Res = pgsql:equery(Conn, Query, []),
            case Res of
                {ok, _} -> {ok, Record};
                {error, Reason} -> {error, Reason}
            end
    end.


push(Conn, Depth) ->
    case Depth of 0 -> pgsql:squery(Conn, "BEGIN"); _ -> ok end,
    pgsql:squery(Conn, "SAVEPOINT savepoint"++integer_to_list(Depth)).

pop(Conn, Depth) ->
    pgsql:squery(Conn, "ROLLBACK TO SAVEPOINT savepoint"++integer_to_list(Depth - 1)).

dump(_Conn) -> "".

execute(Conn, Commands) ->
    pgsql:squery(Conn, Commands).

execute(Conn, Commands, Params) ->
    pgsql:equery(Conn, Commands, Params).

transaction(Conn, TransactionFun) ->
    case pgsql:with_transaction(Conn, fun(_C) -> TransactionFun() end) of
        {rollback, Reason} -> {aborted, Reason};
        Other -> {atomic, Other}
    end.

% internal

id_value_to_string(Id) when is_atom(Id) -> atom_to_list(Id);
id_value_to_string(Id) when is_integer(Id) -> integer_to_list(Id);
id_value_to_string(Id) when is_binary(Id) -> binary_to_list(Id);
id_value_to_string(Id) -> Id.

infer_type_from_id(Id) when is_list(Id) ->
    [Type, TableId] = re:split(Id, "-", [{return, list}, {parts, 2}]),
    IdValue = case boss_record_lib:keytype(Type) of
                uuid -> TableId;
                serial -> list_to_integer(TableId)
            end,
    {list_to_atom(Type), type_to_table_name(Type), IdValue}.

maybe_populate_id_value(Record) ->
    case boss_record_lib:keytype(Record) of 
        uuid -> Record:set(id, uuid:to_string(uuid:uuid4()));
        _ -> Record
end.

type_to_table_name(Type) when is_atom(Type) ->
    type_to_table_name(atom_to_list(Type));
type_to_table_name(Type) when is_list(Type) ->
    inflector:pluralize(Type).

integer_to_id(Val, KeyString) ->
    ModelName = string:substr(KeyString, 1, string:len(KeyString) - string:len("_id")),
    ModelName ++ "-" ++ id_value_to_string(Val).

activate_record(Record, Metadata, Type) ->
    AttributeTypes = boss_record_lib:attribute_types(Type),
    apply(Type, new, lists:map(fun
                (id) ->
                    Index = keyindex(<<"id">>, 2, Metadata),
                    atom_to_list(Type) ++ "-" ++ id_value_to_string(element(Index, Record));
                (Key) ->
                    KeyString = atom_to_list(Key),
                    Index = keyindex(list_to_binary(KeyString), 2, Metadata),
                    AttrType = proplists:get_value(Key, AttributeTypes),
                    case element(Index, Record) of
                        undefined -> undefined;
                        null -> undefined;
                        Val -> 
                            case lists:suffix("_id", KeyString) of
                                true -> integer_to_id(Val, KeyString);
                                false -> boss_record_lib:convert_value_to_type(Val, AttrType)
                            end
                    end
            end, boss_record_lib:attribute_names(Type))).

keyindex(Key, N, TupleList) ->
    keyindex(Key, N, TupleList, 1).

keyindex(Key, _N, [], _Index) ->
    throw({error, "Expected attribute '" ++ binary_to_list(Key) ++ "' was not found in Postgres query results. Synchronise your model and data'"});
keyindex(Key, N, [Tuple|Rest], Index) ->
    case element(N, Tuple) of
        Key -> Index;
        _ -> keyindex(Key, N, Rest, Index + 1)
    end.

sort_order_sql(descending) ->
    "DESC";
sort_order_sql(ascending) ->
    "ASC".

build_insert_query(Record) ->
    Type = element(1, Record),
    TableName = type_to_table_name(Type),
    {Attributes, Values} = lists:foldl(fun
            ({id, V}, {Attrs, Vals}) when is_integer(V) -> {[atom_to_list(id)|Attrs], [pack_value(V)|Vals]};
            ({id, V}, {Attrs, Vals}) when is_list(V) -> {[atom_to_list(id)|Attrs], [pack_value(V)|Vals]};
            ({id, _}, Acc) -> Acc;
            ({_, undefined}, Acc) -> Acc;
            ({A, V}, {Attrs, Vals}) ->
                AString = atom_to_list(A),
                Value = case lists:suffix("_id", AString) of
                    true ->
                        {_, _, ForeignId} = infer_type_from_id(V),
                        ForeignId;
                    false ->
                        V
                end,
                {[AString|Attrs], [pack_value(Value)|Vals]}
        end, {[], []}, Record:attributes()),
    ["INSERT INTO ", TableName, " (", 
        string:join(Attributes, ", "),
        ") values (",
        string:join(Values, ", "),
        ")",
        " RETURNING id"
    ].

build_update_query(Record) ->
    {_, TableName, Id} = infer_type_from_id(Record:id()),
    Updates = lists:foldl(fun
            ({id, _}, Acc) -> Acc;
            ({A, V}, Acc) -> 
                AString = atom_to_list(A),
                Value = case lists:suffix("_id", AString) of
                    true ->
                        {_, _, ForeignId} = infer_type_from_id(V),
                        ForeignId;
                    false ->
                        V
                end,
                [AString ++ " = " ++ pack_value(Value)|Acc]
        end, [], Record:attributes()),
    ["UPDATE ", TableName, " SET ", string:join(Updates, ", "),
        " WHERE id = ", pack_value(Id)].

build_select_query(Type, Conditions, Max, Skip, Sort, SortOrder) ->
    TableName = type_to_table_name(Type),
    ["SELECT * FROM ", TableName, 
        " WHERE ", build_conditions(Type, Conditions),
        " ORDER BY ", atom_to_list(Sort), " ", sort_order_sql(SortOrder),
        case Max of all -> ""; _ -> [" LIMIT ", integer_to_list(Max),
                    " OFFSET ", integer_to_list(Skip)] end
    ].

join([], _) -> [];
join([List|Lists], Separator) ->
     lists:flatten([List | [[Separator,Next] || Next <- Lists]]).

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

%% take the "type-" part out of id_values of "type-nnn" 
%% it allows to assert([boss_db:find("type-nnn")] == boss_db:find(type, [{id, equals, "type-nnn"}]) 
%% while it forbids boss_db:find(type, [{id, equals, "othertype-nnn"}] where othertype is from the untrusted input
%% without this patch, the second part returns:
%%    {error,{error,error,<<"22P02">>,
%%              <<"invalid input syntax for integer: \"type-1\"">>,
%%              [{position,<<"35">>}]}}
de_type_id(Type, Conditions) ->
    lists:map(fun({id, Operator, Operand}) when is_list(Operand) ->
                    {id, Operator, sanitize_value(Type, Operand)};
               ({id, Operator, Operand}) when is_binary(Operand) ->  %% allow for binary operands, dunno if that occurs.
                    {id, Operator, sanitize_value(Type, binary_to_list(Operand))};
               (Other) -> Other %% anything not an 'id' is passed on as-is to the DB.
            end, Conditions).

%% Make sure the value that goes in the WHERE-clause is the number-part of the Type-Number record identifier.
sanitize_value(Type, Value) ->
    TypeL = atom_to_list(Type),	    %% we must match the expected Type
    case string:tokens(Value, "-") of
      [Value] -> Value;             %% just a string, no record-123 composite, pass it on
      [TypeL, IdValue] -> IdValue;  %% take out the record- part and give the supposed number.
      %% don't let missing input validation go unnoticed, scream loudly:
      _Err -> throw({error, "Id value must be of the same type as the requested record.\nExpected type: \"" ++ TypeL ++ "\". Got value: \"" ++ Value ++ "\"."})
    end.

build_conditions(Type, Conditions) ->
    Conds = de_type_id(Type, Conditions),
    build_conditions1(Conds, [" TRUE"]).

build_conditions1([], Acc) ->
    Acc;

build_conditions1([{Key, 'equals', Value}|Rest], Acc) when Value == undefined ->
    build_conditions1(Rest, add_cond(Acc, Key, "is", pack_value(Value)));
build_conditions1([{Key, 'equals', Value}|Rest], Acc) ->
    case is_foreign_key(Key) of
        true -> 
            {_Type, _TableName, TableId} = infer_type_from_id(Value),
            build_conditions1(Rest, add_cond(Acc, Key, "=", pack_value(TableId)));
        false -> build_conditions1(Rest, add_cond(Acc, Key, "=", pack_value(Value)))
    end;
build_conditions1([{Key, 'not_equals', Value}|Rest], Acc) when Value == undefined ->
    build_conditions1(Rest, add_cond(Acc, Key, "is not", pack_value(Value)));
build_conditions1([{Key, 'not_equals', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "!=", pack_value(Value)));
build_conditions1([{Key, 'in', Value}|Rest], Acc) when is_list(Value) ->
    PackedValues = pack_set(Value),
    build_conditions1(Rest, add_cond(Acc, Key, "IN", PackedValues));
build_conditions1([{Key, 'not_in', Value}|Rest], Acc) when is_list(Value) ->
    PackedValues = pack_set(Value),
    build_conditions1(Rest, add_cond(Acc, Key, "NOT IN", PackedValues));
build_conditions1([{Key, 'in', {Min, Max}}|Rest], Acc) when Max >= Min ->
    PackedValues = pack_range(Min, Max),
    build_conditions1(Rest, add_cond(Acc, Key, "BETWEEN", PackedValues));
build_conditions1([{Key, 'not_in', {Min, Max}}|Rest], Acc) when Max >= Min ->
    PackedValues = pack_range(Min, Max),
    build_conditions1(Rest, add_cond(Acc, Key, "NOT BETWEEN", PackedValues));
build_conditions1([{Key, 'gt', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, ">", pack_value(Value)));
build_conditions1([{Key, 'lt', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "<", pack_value(Value)));
build_conditions1([{Key, 'ge', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, ">=", pack_value(Value)));
build_conditions1([{Key, 'le', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "<=", pack_value(Value)));
build_conditions1([{Key, 'matches', "*"++Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "~*", pack_value(Value)));
build_conditions1([{Key, 'not_matches', "*"++Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "!~*", pack_value(Value)));
build_conditions1([{Key, 'matches', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "~", pack_value(Value)));
build_conditions1([{Key, 'not_matches', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "!~", pack_value(Value)));
build_conditions1([{Key, 'contains', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, pack_tsvector(Key), "@@", pack_tsquery([Value], "&")));
build_conditions1([{Key, 'not_contains', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, pack_tsvector(Key), "@@", pack_tsquery_not([Value], "&")));
build_conditions1([{Key, 'contains_all', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_tsvector(Key), "@@", pack_tsquery(Values, "&")));
build_conditions1([{Key, 'not_contains_all', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_tsvector(Key), "@@", pack_tsquery_not(Values, "&")));
build_conditions1([{Key, 'contains_any', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_tsvector(Key), "@@", pack_tsquery(Values, "|")));
build_conditions1([{Key, 'contains_none', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_tsvector(Key), "@@", pack_tsquery_not(Values, "|"))).

add_cond(Acc, Key, Op, PackedVal) ->
    [lists:concat([Key, " ", Op, " ", PackedVal, " AND "])|Acc].

pack_tsvector(Key) ->
    atom_to_list(Key) ++ "::tsvector".

pack_tsquery(Values, Op) ->
    "'" ++ string:join(lists:map(fun escape_sql/1, Values), " "++Op++" ") ++ "'::tsquery".

pack_tsquery_not(Values, Op) ->
    "'!(" ++ string:join(lists:map(fun escape_sql/1, Values), " "++Op++" ") ++ ")'::tsquery".

pack_set(Values) ->
    "(" ++ string:join(lists:map(fun pack_value/1, Values), ", ") ++ ")".

pack_range(Min, Max) ->
    pack_value(Min) ++ " AND " ++ pack_value(Max).

escape_sql(Value) ->
    escape_sql1(Value, []).

escape_sql1([], Acc) ->
    lists:reverse(Acc);
escape_sql1([$'|Rest], Acc) ->
    escape_sql1(Rest, [$', $'|Acc]);
escape_sql1([C|Rest], Acc) ->
    escape_sql1(Rest, [C|Acc]).

pack_datetime({Date, {Y, M, S}}) when is_float(S) ->
    pack_datetime({Date, {Y, M, erlang:round(S)}});
pack_datetime(DateTime) ->
    "TIMESTAMP '" ++ erlydtl_filters:date(DateTime, "c") ++ "'".

pack_now(Now) -> pack_datetime(calendar:now_to_datetime(Now)).

pack_value(undefined) ->
    "null";
pack_value(V) when is_binary(V) ->
    pack_value(binary_to_list(V));
pack_value(V) when is_list(V) ->
    "'" ++ escape_sql(V) ++ "'";
pack_value({MegaSec, Sec, MicroSec}) when is_integer(MegaSec) andalso is_integer(Sec) andalso is_integer(MicroSec) ->
    pack_now({MegaSec, Sec, MicroSec});
pack_value({{_, _, _}, {_, _, _}} = Val) ->
    pack_datetime(Val);
pack_value(Val) when is_integer(Val) ->
    integer_to_list(Val);
pack_value(Val) when is_float(Val) ->
    float_to_list(Val);
pack_value(true) ->
    "TRUE";
pack_value(false) ->
    "FALSE".
