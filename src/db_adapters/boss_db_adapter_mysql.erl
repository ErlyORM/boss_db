-module(boss_db_adapter_mysql).
-behaviour(boss_db_adapter).
-export([init/1, terminate/1, start/1, stop/0, find/2, find/7, find_by_sql/4]).
-export([count/3, counter/2, incr/3, delete/2, save_record/2]).
-export([push/2, pop/2, dump/1, execute/2, execute/3, transaction/2]).
-export([get_migrations_table/1, migration_done/3]).
-compile(export_all).
start(_) ->
    ok.

stop() ->
    ok.

init(Options) ->
    DBHost       = proplists:get_value(db_host,     Options, "localhost"),
    DBPort       = proplists:get_value(db_port,     Options, 3306),
    DBUsername   = proplists:get_value(db_username, Options, "guest"),
    DBPassword   = proplists:get_value(db_password, Options, ""),
    DBDatabase   = proplists:get_value(db_database, Options, "test"),
    DBIdentifier = proplists:get_value(db_shard_id, Options, boss_pool),
    Encoding     = utf8,
    mysql_conn:start_link(DBHost, DBPort, DBUsername, DBPassword, DBDatabase, 
        fun(_, _, _, _) -> ok end, Encoding, DBIdentifier).

terminate(Pid) -> 
    exit(Pid, normal).

find_by_sql(Pid, Type, Sql, Parameters) when is_atom(Type), is_list(Sql), is_list(Parameters) ->
	case boss_record_lib:ensure_loaded(Type) of
		true ->
			Res = fetch(Pid, Sql, Parameters),
			case Res of
				{data, MysqlRes} ->
					Rows = mysql:get_result_rows(MysqlRes),
					Columns = mysql:get_result_field_info(MysqlRes),
					lists:map(fun(Row) ->
						activate_record(Row, Columns, Type)
					end, Rows);
				{error, MysqlRes} ->
					{error, mysql:get_result_reason(MysqlRes)}
			end;
		false -> 
			{error, {module_not_loaded, Type}}
	end.


find(Pid, Id) when is_list(Id) ->
    {Type, TableName, IdColumn, TableId} = boss_sql_lib:infer_type_from_id(Id),
    Res = fetch(Pid, ["SELECT * FROM ", TableName, " WHERE ", IdColumn, " = ", pack_value(TableId)]),
    case Res of
        {data, MysqlRes} ->
            case mysql:get_result_rows(MysqlRes) of
                [] -> undefined;
                [Row] ->
                    Columns = mysql:get_result_field_info(MysqlRes),
                    case boss_record_lib:ensure_loaded(Type) of
                        true  -> activate_record(Row, Columns, Type);
                        false -> {error, {module_not_loaded, Type}}
                    end
            end;
        {error, MysqlRes} ->
            {error, mysql:get_result_reason(MysqlRes)}
    end.

find(Pid, Type, Conditions, Max, Skip, Sort, SortOrder) when is_atom(Type), is_list(Conditions), 
                                                              is_integer(Max) orelse Max =:= all, is_integer(Skip), 
                                                              is_atom(Sort), is_atom(SortOrder) ->
    case boss_record_lib:ensure_loaded(Type) of
        true ->
            Query = build_select_query(Type, Conditions, Max, Skip, Sort, SortOrder),
            Res = fetch(Pid, Query),

            case Res of
                {data, MysqlRes} ->
                    Columns = mysql:get_result_field_info(MysqlRes),
                    ResultRows = mysql:get_result_rows(MysqlRes),
                    FilteredRows = case {Max, Skip} of
                        {all, Skip} when Skip > 0 ->
                            lists:nthtail(Skip, ResultRows);
                        _ ->
                            ResultRows
                    end,
                    lists:map(fun(Row) ->
                                activate_record(Row, Columns, Type)
                        end, FilteredRows);
                {error, MysqlRes} ->
                    {error, mysql:get_result_reason(MysqlRes)}
            end;
        false -> {error, {module_not_loaded, Type}}
    end.

count(Pid, Type, Conditions) ->
    ConditionClause = build_conditions(Type, Conditions),
    TableName = boss_record_lib:database_table(Type),
    Res = fetch(Pid, ["SELECT COUNT(*) AS count FROM ", TableName, " WHERE ", ConditionClause]),
    case Res of
        {data, MysqlRes} ->
            [[Count]] = mysql:get_result_rows(MysqlRes),
            Count;
        {error, MysqlRes} ->
            {error, mysql:get_result_reason(MysqlRes)}
    end.
    
table_exists(Pid, Type) ->
    TableName = boss_record_lib:database_table(Type),
    Res = fetch(Pid, ["SELECT 1 FROM ", TableName," LIMIT 1"]),
    case Res of
        {updated, _} ->
            ok;
        {error, MysqlRes} -> {error, mysql:get_result_reason(MysqlRes)}
    end.

counter(Pid, Id) when is_list(Id) ->
    Res = fetch(Pid, ["SELECT value FROM counters WHERE name = ", pack_value(Id)]),
    case Res of
        {data, MysqlRes} ->
            [[Value]] = mysql:get_result_rows(MysqlRes),
            Value;
        {error, _Reason} -> 0
    end.

incr(Pid, Id, Count) ->
    Res = fetch(Pid, ["UPDATE counters SET value = value + ", pack_value(Count), 
            " WHERE name = ", pack_value(Id)]),
    case Res of
        {updated, _} ->
            counter(Pid, Id); % race condition
        {error, _Reason} -> 
            Res1 = fetch(Pid, ["INSERT INTO counters (name, value) VALUES (",
                    pack_value(Id), ", ", pack_value(Count), ")"]),
            case Res1 of
                {updated, _} -> counter(Pid, Id); % race condition
                {error, MysqlRes} -> {error, mysql:get_result_reason(MysqlRes)}
            end
    end.

delete(Pid, Id) when is_list(Id) ->
    {_, TableName, IdColumn, TableId} = boss_sql_lib:infer_type_from_id(Id),
    Res = fetch(Pid, ["DELETE FROM ", TableName, " WHERE ", IdColumn, " = ", 
            pack_value(TableId)]),
    case Res of
        {updated, _} ->
            fetch(Pid, ["DELETE FROM counters WHERE name = ", 
                    pack_value(Id)]),
            ok;
        {error, MysqlRes} -> {error, mysql:get_result_reason(MysqlRes)}
    end.

save_record(Pid, Record) when is_tuple(Record) ->
    case Record:id() of
        id ->
            Type = element(1, Record),
            Query = build_insert_query(Record),
            Res = fetch(Pid, Query),
            case Res of
                {updated, _} ->
                    Res1 = fetch(Pid, "SELECT last_insert_id()"),
                    case Res1 of
                        {data, MysqlRes} ->
                            [[Id]] = mysql:get_result_rows(MysqlRes),
                            {ok, Record:set(id, lists:concat([Type, "-", integer_to_list(Id)]))};
                        {error, MysqlRes} ->
                            {error, mysql:get_result_reason(MysqlRes)}
                    end;
                {error, MysqlRes} -> {error, mysql:get_result_reason(MysqlRes)}
            end;
        Identifier when is_integer(Identifier) ->
            Type = element(1, Record),
            Query = build_insert_query(Record),
            Res = fetch(Pid, Query),
            case Res of
                {updated, _} ->
                    {ok, Record:set(id, lists:concat([Type, "-", integer_to_list(Identifier)]))};
                {error, MysqlRes} -> {error, mysql:get_result_reason(MysqlRes)}
            end;			
        Defined when is_list(Defined) ->
            Query = build_update_query(Record),
            Res = fetch(Pid, Query),
            case Res of
                {updated, _} -> {ok, Record};
                {error, MysqlRes} -> {error, mysql:get_result_reason(MysqlRes)}
            end
    end.

-ifdef(boss_test).
%% This is to dodge problems with MySQL's fulltext indexes inside transactions
push(_,_) -> ok.
pop(_,_) -> ok.
-else.
push(Pid, Depth) ->
    case Depth of 0 -> fetch(Pid, "BEGIN"); _ -> ok end,
    fetch(Pid, ["SAVEPOINT `savepoint", integer_to_list(Depth),"`"]).

pop(Pid, Depth) ->
    fetch(Pid, ["ROLLBACK TO SAVEPOINT `savepoint", integer_to_list(Depth-1),"`"]),
    fetch(Pid, ["RELEASE SAVEPOINT `savepoint", integer_to_list(Depth-1), "`"]).
-endif.

dump(_Conn) -> "".

execute(Pid, Commands) ->
    fetch(Pid, Commands).

execute(Pid, Commands, Params) ->
	fetch(Pid, Commands, Params).

transaction(Pid, TransactionFun) when is_function(TransactionFun) ->
    do_transaction(Pid, TransactionFun).
    
do_transaction(Pid, TransactionFun) when is_function(TransactionFun) ->
    case do_begin(Pid, self()) of
        {error, _} = Err ->	
            {aborted, Err};
        {updated,{mysql_result,[],[],0,0,[]}} ->
            case catch TransactionFun() of
                error = Err ->  
                    do_rollback(Pid, self()),
                    {aborted, Err};
                {error, _} = Err -> 
                    do_rollback(Pid, self()),
                    {aborted, Err};
                {'EXIT', _} = Err -> 
                    do_rollback(Pid, self()),
                    {aborted, Err};
                Res ->
                    case do_commit(Pid, self()) of
                        {error, _} = Err ->
                            do_rollback(Pid, self()),
                            {aborted, Err};
                        _ ->
                            {atomic, Res}
                    end
            end
    end.

do_begin(Pid,_)->
    fetch(Pid, ["BEGIN"]).	

do_commit(Pid,_)->
    fetch(Pid, ["COMMIT"]).

do_rollback(Pid,_)->
    fetch(Pid, ["ROLLBACK"]).

get_migrations_table(Pid) ->
    fetch(Pid, "SELECT * FROM schema_migrations").

migration_done(Pid, Tag, up) ->
    fetch(Pid, ["INSERT INTO schema_migrations (version, migrated_at) values (",
                 atom_to_list(Tag), ", NOW())"]);
migration_done(Pid, Tag, down) ->
    fetch(Pid, ["DELETE FROM schema_migrations WHERE version = ", atom_to_list(Tag)]).

% internal

%% integer_to_id(Val, KeyString) ->
%%     ModelName = string:substr(KeyString, 1, string:len(KeyString) - string:len("_id")),
%%     ModelName ++ "-" ++ integer_to_list(Val).

activate_record(Record, Metadata, Type) ->
    AttributeTypes = boss_record_lib:attribute_types(Type),
    AttributeColumns = boss_record_lib:database_columns(Type),

    RetypedForeignKeys = boss_sql_lib:get_retyped_foreign_keys(Type),

    apply(Type, new, lists:map(fun
                (id) ->
                    DBColumn = proplists:get_value('id', AttributeColumns),
                    Index = keyindex(list_to_binary(DBColumn), 2, Metadata),
                    atom_to_list(Type) ++ "-" ++ integer_to_list(lists:nth(Index, Record));
                (Key) ->
                    DBColumn = proplists:get_value(Key, AttributeColumns),
                    Index = keyindex(list_to_binary(DBColumn), 2, Metadata),
                    AttrType = proplists:get_value(Key, AttributeTypes),
                    case lists:nth(Index, Record) of
                        undefined -> undefined;
                        {datetime, DateTime} -> boss_record_lib:convert_value_to_type(DateTime, AttrType);
                        Val -> 
                            boss_sql_lib:convert_possible_foreign_key(RetypedForeignKeys, Type, Key, Val, AttrType)
                    end
            end, boss_record_lib:attribute_names(Type))).

keyindex(Key, N, TupleList) ->
    keyindex(Key, N, TupleList, 1).

keyindex(_Key, _N, [], _Index) ->
    undefined;
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
    TableName = boss_record_lib:database_table(Type),
    AttributeColumns = Record:database_columns(),
    {Attributes, Values} = lists:foldl(fun
            ({_, undefined}, Acc) -> Acc;
            ({'id', 'id'}, Acc) -> Acc;
            ({'id', V}, {Attrs, Vals}) when is_integer(V) -> 
                 {[atom_to_list(id)|Attrs], [pack_value(V)|Vals]};
            ({'id', V}, {Attrs, Vals}) -> 
                DBColumn = proplists:get_value('id', AttributeColumns),
                {_, _, _, TableId} = boss_sql_lib:infer_type_from_id(V),
                {[DBColumn|Attrs], [pack_value(TableId)|Vals]};
            ({A, V}, {Attrs, Vals}) ->
                DBColumn = proplists:get_value(A, AttributeColumns),
                Value    = case boss_sql_lib:is_foreign_key(Type, A) of
                    true ->
                        {_, _, _, ForeignId} = boss_sql_lib:infer_type_from_id(V),
                        ForeignId;
                    false ->
                        V
                end,
                {[DBColumn|Attrs], [pack_value(Value)|Vals]}
        end, {[], []}, Record:attributes()),
    ["INSERT INTO ", TableName, " (", 
        string:join(escape_attr(Attributes), ", "),
        ") values (",
        string:join(Values, ", "),
        ")"
    ].
escape_attr(Attrs) ->
    [["`", Attr, "`"] || Attr <- Attrs].

build_update_query(Record) ->
    {Type, TableName, IdColumn, TableId} = boss_sql_lib:infer_type_from_id(Record:id()),
    AttributeColumns = Record:database_columns(),
    Updates = lists:foldl(fun
            ({id, _}, Acc) -> Acc;
            ({A, V}, Acc) -> 
                DBColumn = proplists:get_value(A, AttributeColumns),
                Value = case {boss_sql_lib:is_foreign_key(Type, A), V =/= undefined} of
                    {true, true} ->
                        {_, _, _, ForeignId} = boss_sql_lib:infer_type_from_id(V),
                        ForeignId;
                    {_, false} ->
                        null;
                    _ ->
                        V
                end,
                ["`"++DBColumn ++ "` = " ++ pack_value(Value)|Acc]
        end, [], Record:attributes()),
    ["UPDATE ", TableName, " SET ", string:join(Updates, ", "),
        " WHERE ", IdColumn, " = ", pack_value(TableId)].

build_select_query(Type, Conditions, Max, Skip, Sort, SortOrder) ->	
    TableName = boss_record_lib:database_table(Type),
    ["SELECT * FROM ", TableName, 
        " WHERE ", build_conditions(Type, Conditions),
        " ORDER BY ", atom_to_list(Sort), " ", sort_order_sql(SortOrder),
        case Max of all -> ""; _ -> [" LIMIT ", integer_to_list(Max),
                    " OFFSET ", integer_to_list(Skip)] end
    ].

build_conditions(Type, Conditions) ->
    AttributeColumns = boss_record_lib:database_columns(Type),
    Conditions2 = lists:map(fun
            ({'id' = Key, Op, Value}) ->
                Key2 = proplists:get_value(Key, AttributeColumns, Key),
                boss_sql_lib:convert_id_condition_to_use_table_ids({Key2, Op, Value});
            ({Key, Op, Value}) ->
                Key2 = proplists:get_value(Key, AttributeColumns, Key),
                case boss_sql_lib:is_foreign_key(Type, Key) of
                    true -> boss_sql_lib:convert_id_condition_to_use_table_ids({Key2, Op, Value});
                    false -> {Key2, Op, Value}
                end
        end, Conditions),
    build_conditions1(Conditions2, [" TRUE"]).

build_conditions1([], Acc) ->
    Acc;
build_conditions1([{Key, 'equals', Value}|Rest], Acc) when Value == undefined ; Value == null->
    build_conditions1(Rest, add_cond(Acc, Key, "is", pack_value(Value)));
build_conditions1([{Key, 'equals', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "=", pack_value(Value)));
build_conditions1([{Key, 'not_equals', Value}|Rest], Acc) when Value == undefined ; Value == null ->
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
    build_conditions1(Rest, add_cond(Acc, Key, "REGEXP", pack_value(Value)));
build_conditions1([{Key, 'not_matches', "*"++Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "NOT REGEXP", pack_value(Value)));
build_conditions1([{Key, 'matches', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "REGEXP BINARY", pack_value(Value)));
build_conditions1([{Key, 'not_matches', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, Key, "NOT REGEXP BINARY", pack_value(Value)));
build_conditions1([{Key, 'contains', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, pack_match(Key), "AGAINST", pack_boolean_query([Value], "")));
build_conditions1([{Key, 'not_contains', Value}|Rest], Acc) ->
    build_conditions1(Rest, add_cond(Acc, pack_match_not(Key), "AGAINST", pack_boolean_query([Value], "")));
build_conditions1([{Key, 'contains_all', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_match(Key), "AGAINST", pack_boolean_query(Values, "+")));
build_conditions1([{Key, 'not_contains_all', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_match_not(Key), "AGAINST", pack_boolean_query(Values, "+")));
build_conditions1([{Key, 'contains_any', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_match(Key), "AGAINST", pack_boolean_query(Values, "")));
build_conditions1([{Key, 'contains_none', Values}|Rest], Acc) when is_list(Values) ->
    build_conditions1(Rest, add_cond(Acc, pack_match_not(Key), "AGAINST", pack_boolean_query(Values, ""))).

add_cond(Acc, Key, Op, PackedVal) ->
    [lists:concat([Key, " ", Op, " ", PackedVal, " AND "])|Acc].

pack_match(Key) ->
    lists:concat(["MATCH(", Key, ")"]).

pack_match_not(Key) ->
    lists:concat(["NOT MATCH(", Key, ")"]).

pack_boolean_query(Values, Op) ->
    "('" ++ string:join(lists:map(fun(Val) -> Op ++ escape_sql(Val) end, Values), " ") ++ "' IN BOOLEAN MODE)".
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

pack_datetime(DateTime) ->
    dh_date:format("'Y-m-d H:i:s'",DateTime ).

pack_date(Date) ->
    dh_date:format("'Y-m-d\TH:i:s'",{Date, {0,0,0}}).


%pack_now(Now) -> pack_datetime(calendar:now_to_datetime(Now)).

pack_value(null) ->
	"null";
pack_value(undefined) ->
	"null";
pack_value(V) when is_binary(V) ->
    pack_value(binary_to_list(V));
pack_value(V) when is_list(V) ->
    mysql:encode(V);
pack_value({_, _, _} = Val) ->
	pack_date(Val);    
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

fetch(Pid, Query) ->
    lager:info("Query ~s", [Query]),
    Res = mysql_conn:fetch(Pid, [Query], self()),
	case Res of
		{error, MysqlRes} ->
			lager:error("SQL Error: ~p",[mysql:get_result_reason(MysqlRes)]);
		_ -> ok
	end,
	Res.

fetch(Pid, Query, Parameters) ->
	Sql = replace_parameters(lists:flatten(Query), Parameters),
	fetch(Pid, Sql).

replace_parameters([$$, X, Y | Rest], Parameters) when X >= $1, X =< $9, Y >= $0, Y =< $9 ->
	Position = (X-$0)*10 + (Y-$0),
	[lookup_single_parameter(Position, Parameters) | replace_parameters(Rest, Parameters)];
replace_parameters([$$, X | Rest], Parameters) when X >= $1, X =< $9 ->
	Position = X-$0,
	[lookup_single_parameter(Position, Parameters) | replace_parameters(Rest, Parameters)];
replace_parameters([X | Rest], Parameters) ->
	[X | replace_parameters(Rest, Parameters)];
replace_parameters([], _) ->
	[].

lookup_single_parameter(Position, Parameters) ->
	try lists:nth(Position, Parameters) of
		V -> pack_value(V)
	catch
		Error -> throw(io_lib:format("Error (~p) getting parameter $~w. Provided Params: ~p", [Error, Position, Parameters]))
	end.
