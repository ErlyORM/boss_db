%% @doc Chicago Boss database abstraction

-module(boss_db).

-export([start/1, stop/0]).

-export([
        migrate/1,
        migrate/2,
        find/1, 
        find/2, 
        find/3, 
        find/4, 
        find_by_sql/3,
        find_by_sql/2,
        find_first/1,
        find_first/2,
        find_first/3,
        find_first/4,
        find_last/1,
        find_last/2,
        find_last/3,
        find_last/4,
        count/1,
        count/2,
        count/3,
        counter/1, 
        counter/2, 
        incr/1, 
        incr/2, 
        incr/3, 
        delete/1, 
        delete/2, 
        push/0,
        push/1,
        pop/0,
        pop/1,
        create_table/2,
        create_table/3,
        table_exists/1,
        table_exists/2,
        depth/0,
        depth/1,
        dump/0,
        dump/1,
        execute/1,
        execute/2,
        execute/3,
        transaction/1,
        transaction/2,
        mock_transaction/1,
        save_record/1, 
        save_record/2, 
        validate_record/1,
        validate_record/2,
        validate_record_types/1,
        type/1,
        type/2,
        data_type/2,
        sort_order/1]).

-ifdef(TEST).
-compile(export_all).
-endif.

-type sort_order()      :: ascending|descending.
-type eq_operatator()   :: 'eq'|'ne'.
-type normal_operator() ::'equals'|'not_equals'.
-export_type([sort_order/0, eq_operatator/0, normal_operator/0]).

-define(DEFAULT_TIMEOUT, (30 * 1000)).
-define(POOLNAME, boss_db_pool).

start(Options) ->
    AdapterName = proplists:get_value(adapter, Options, mock),
    Adapter     = list_to_atom(lists:concat(["boss_db_adapter_", AdapterName])),
    lager:info("Start Database Adapter ~p options ~p", [Adapter, Options]),
    Adapter:start(Options),
    lists:foldr(fun(ShardOptions, Acc) ->
                case proplists:get_value(db_shard_models, ShardOptions, []) of
                    [] -> Acc;
                    _ ->
                        ShardAdapter = case proplists:get_value(db_adapter, ShardOptions) of
                            undefined -> Adapter;
                            ShortName -> list_to_atom(lists:concat(["boss_db_adapter_", ShortName]))
                        end,
                        ShardAdapter:start(ShardOptions ++ Options),
                        Acc
                end
        end, [], proplists:get_value(shards, Options, [])),
    boss_db_sup:start_link(Options).

stop() ->
    ok.

db_call(Msg) ->
    db_call(Msg, ?DEFAULT_TIMEOUT).

db_call(Msg, Timeout) when is_integer(Timeout), Timeout > 0 ->
    case erlang:get(boss_db_transaction_info) of
        undefined ->
            boss_pool:call(?POOLNAME, Msg, ?DEFAULT_TIMEOUT);
        State ->
            {reply, Reply, NewState} =
                boss_db_controller:handle_call(Msg, undefined, State),
            erlang:put(boss_db_transaction_info, NewState),
            Reply
    end.

%% @doc Apply migrations from list [{Tag, Fun}]
%% currently runs all migrations 'up'
migrate(Migrations) when is_list(Migrations) ->
    %% 1. Do we have a migrations table?  If not, create it.
    create_migration_table_if_needed(),
    %% 2. Get all the current migrations from it.
    DoneMigrations    = db_call({get_migrations_table}),
    DoneMigrationTags = [binary_to_atom(Tag, 'utf8') ||
                            {_Id, Tag, _MigratedAt} <- DoneMigrations],
    %% 3. Run the ones that are not in this list.
    transaction(fun() ->
                    [migrate({Tag, Fun}, up) ||
                        {Tag, Fun} <- Migrations,
                        not lists:member(Tag, DoneMigrationTags)]
                end).

create_migration_table_if_needed() ->
    %% 1. Do we have a migrations table?  If not, create it.
    case table_exists(schema_migrations) of
        false ->
            ok = create_table(schema_migrations, [{id, auto_increment, []},
                                                  {version, string, [not_null]},
                                                  {migrated_at, datetime, []}]);
        _ ->
            noop
    end.

%% @doc Run database migration {Tag, Fun} in Direction
migrate({Tag, Fun}, Direction) ->
    lager:info("Running migration: ~p ~p~n", [Tag, Direction]),
    Fun(Direction),
    db_call({migration_done, Tag, Direction}).

%% @spec find(Id::string()) -> Value | {error, Reason}
%% @doc Find a BossRecord with the specified `Id' (e.g. "employee-42") or a value described
%% by a dot-separated path (e.g. "employee-42.manager.name").
find(Key) ->
    find(Key, ?DEFAULT_TIMEOUT).

find("", Timeout) when is_integer(Timeout) -> undefined;
find(Key, Timeout) when is_list(Key), is_integer(Timeout) ->
    [IdToken|Rest] = string:tokens(Key, "."),
    case db_call({find, IdToken}, Timeout) of
        undefined -> undefined;
        {error, Reason} -> {error, Reason};
        BossRecord -> BossRecord:get(string:join(Rest, "."))
    end;
find(_, Timeout) when is_integer(Timeout) ->
    {error, invalid_id};

%% @spec find(Type::atom(), Conditions) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns all BossRecords of type
%% `Type' matching all of the given `Conditions'
find(Type, Conditions) when is_list(Conditions) ->
    find(Type, Conditions, [], ?DEFAULT_TIMEOUT).

%% @spec find(Type::atom(), Conditions, Options::proplist()) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns BossRecords of type
%% `Type' matching all of the given `Conditions'. Options may include
%% `limit' (maximum number of records to return), `offset' (number of records
%% to skip), `order_by' (attribute to sort on), `descending' (whether to
%% sort the values from highest to lowest), and `include' (list of belongs_to
%% associations to pre-cache)
find(Type, Conditions, Options) ->
    find(Type, Conditions, Options, ?DEFAULT_TIMEOUT).

find(Type, Conditions, Options, Timeout) ->
    Max = proplists:get_value(limit, Options, all),
    Skip = proplists:get_value(offset, Options, 0),
    Sort = proplists:get_value(order_by, Options, id),
    SortOrder = case proplists:get_value(descending, Options) of
        true -> descending;
        _ -> ascending
    end,
    Include = proplists:get_value(include, Options, []),
    db_call({find, Type, normalize_conditions(Conditions),
             Max, Skip, Sort, SortOrder, Include}, Timeout).

-spec(find_by_sql(Type::atom(), Sql::string()) -> [BossRecord::tuple()]).
find_by_sql(Type, Sql) when is_list(Sql) ->
    find_by_sql(Type, Sql, []).

-spec(find_by_sql(Type::atom(), Sql::string(), Parmeters::list()) -> [BossRecord::tuple()]).
find_by_sql(Type, Sql, Parameters) when is_list(Sql), is_list(Parameters) ->
    db_call({find_by_sql, Type, Sql, Parameters}, ?DEFAULT_TIMEOUT).

-spec(sort_order(jsx:json_term()) -> sort_order()).
sort_order(Options) ->
    case proplists:get_value(descending, Options) of
        true -> descending;
        _ -> ascending
    end.

%% @spec find_first( Type::atom() ) -> Record | undefined
%% @doc Query for the first BossRecord of type `Type'.
find_first(Type) ->
    return_one(find(Type, [], [{limit, 1}])).

%% @spec find_first( Type::atom(), Conditions ) -> Record | undefined
%% @doc Query for the first BossRecord of type `Type' matching all of the given `Conditions'
find_first(Type, Conditions) ->
    find_first(Type, Conditions, ?DEFAULT_TIMEOUT).

%% @spec find_first( Type::atom(), Conditions, Sort::atom() ) -> Record | undefined
%% @doc Query for the first BossRecord of type `Type' matching all of the given `Conditions',
%% sorted on the attribute `Sort'.
find_first(Type, Conditions, Timeout) when is_integer(Timeout) ->
    return_one(find(Type, Conditions, [{limit, 1}], Timeout));

find_first(Type, Conditions, Sort) when is_atom(Sort) ->
    find_first(Type, Conditions, Sort, ?DEFAULT_TIMEOUT).

find_first(Type, Conditions, Sort, Timeout) ->
    return_one(find(Type, Conditions, [{limit, 1}, {order_by, Sort}], Timeout)).

%% @spec find_last( Type::atom() ) -> Record | undefined
%% @doc Query for the last BossRecord of type `Type'.
find_last(Type) ->
    return_one(find(Type, [], [{limit, 1}, descending])).

%% @spec find_last( Type::atom(), Conditions ) -> Record | undefined
%% @doc Query for the last BossRecord of type `Type' matching all of the given `Conditions'
find_last(Type, Conditions) ->
    find_last(Type, Conditions, ?DEFAULT_TIMEOUT).

%% @spec find_last( Type::atom(), Conditions, Sort ) -> Record | undefined
%% @doc Query for the last BossRecord of type `Type' matching all of the given `Conditions'
find_last(Type, Conditions, Timeout) when is_integer(Timeout) ->
    return_one(find(Type, Conditions, [{limit, 1}, descending], Timeout));

find_last(Type, Conditions, Sort) when is_atom(Sort) ->
    find_last(Type, Conditions, Sort, ?DEFAULT_TIMEOUT).

find_last(Type, Conditions, Sort, Timeout) ->
    return_one(find(Type, Conditions,
                    [{limit, 1}, {order_by, Sort}, descending], Timeout)).

%% @spec count( Type::atom() ) -> integer()
%% @doc Count the number of BossRecords of type `Type' in the database.
count(Type) ->
    count(Type, [], ?DEFAULT_TIMEOUT).

%% @spec count( Type::atom(), Conditions ) -> integer()
%% @doc Count the number of BossRecords of type `Type' in the database matching
%% all of the given `Conditions'.
count(Type, Timeout) when is_integer(Timeout) ->
    count(Type, [], Timeout);

count(Type, Conditions) when is_list(Conditions) ->
    count(Type, Conditions, ?DEFAULT_TIMEOUT).

count(Type, Conditions, Timeout) ->
    db_call({count, Type, normalize_conditions(Conditions)}, Timeout).

%% @spec counter( Id::string() ) -> integer()
%% @doc Treat the record associated with `Id' as a counter and return its value.
%% Returns 0 if the record does not exist, so to reset a counter just use
%% "delete".
counter(Key) ->
    counter(Key, ?DEFAULT_TIMEOUT).

counter(Key, Timeout) ->
    db_call({counter, Key}, Timeout).

%% @spec incr( Id::string() ) -> integer()
%% @doc Treat the record associated with `Id' as a counter and atomically increment its value by 1.
incr(Key) ->
    incr(Key, 1, ?DEFAULT_TIMEOUT).

%% @spec incr( Id::string(), Increment::integer() ) -> integer()
%% @doc Treat the record associated with `Id' as a counter and atomically increment its value by `Increment'.
incr(Key, Count) ->
    incr(Key, Count, ?DEFAULT_TIMEOUT).

incr(Key, Count, Timeout) ->
    db_call({incr, Key, Count}, Timeout).

%% @spec delete( Id::string() ) -> ok | {error, Reason}
%% @doc Delete the BossRecord with the given `Id'.
delete(Key) ->
    delete(Key, ?DEFAULT_TIMEOUT).

delete(Key, Timeout) when is_integer(Timeout) ->
    case boss_db:find(Key, Timeout) of
        undefined ->
            {error, not_found};
        AboutToDelete ->
            case boss_record_lib:run_before_delete_hooks(AboutToDelete) of
                ok ->
                    Result = db_call({delete, Key}, Timeout),
                    case Result of
                        ok ->
                            boss_news:deleted(Key, AboutToDelete:attributes()),
                            ok;
                        _ ->
                            Result
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.


push() ->
    db_call(push).

push(Timeout) ->
    db_call(push, Timeout).

pop() ->
    db_call(pop).

pop(Timeout) ->
    db_call(pop, Timeout).

depth() ->
    db_call(depth).

depth(Timeout) ->
    db_call(depth, Timeout).

dump() ->
    db_call(dump).

dump(Timeout) ->
    db_call(dump, Timeout).

%% @spec create_table ( TableName::string(), TableDefinition ) -> ok | {error, Reason}
%% @doc Create a table based on TableDefinition
create_table(TableName, TableDefinition) ->
    create_table(TableName, TableDefinition, ?DEFAULT_TIMEOUT).

create_table(TableName, TableDefinition, Timeout) ->
    db_call({create_table, TableName, TableDefinition}, Timeout).

table_exists(TableName) ->
    table_exists(TableName, ?DEFAULT_TIMEOUT).

table_exists(TableName, Timeout) ->
    db_call({table_exists, TableName}, Timeout).

%% @spec execute( Commands::iolist() ) -> RetVal
%% @doc Execute raw database commands on SQL databases
execute(Commands) ->
    execute(Commands, ?DEFAULT_TIMEOUT).

%% @spec execute( Commands::iolist(), Params::list() ) -> RetVal
%% @doc Execute database commands with interpolated parameters on SQL databases
execute(Commands, Timeout) when is_integer(Timeout) ->
    db_call({execute, Commands}, Timeout);

execute(Commands, Params) when is_list(Params) ->
    execute(Commands, Params, ?DEFAULT_TIMEOUT).

execute(Commands, Params, Timeout) ->
    db_call({execute, Commands, Params}, Timeout).

%% @spec transaction( TransactionFun::function() ) -> {atomic, Result} | {aborted, Reason}
%% @doc Execute a fun inside a transaction.
transaction(TransactionFun) ->
    transaction(TransactionFun, ?DEFAULT_TIMEOUT).

transaction(TransactionFun, Timeout) ->
    Worker = poolboy:checkout(?POOLNAME, true, Timeout),
    State = gen_server:call(Worker, state, Timeout),
    put(boss_db_transaction_info, State),
    {reply, Reply, State} =
        boss_db_controller:handle_call({transaction, TransactionFun},
                                       undefined, State),
    put(boss_db_transaction_info, undefined),
    poolboy:checkin(?POOLNAME, Worker),
    Reply.

mock_transaction(TransactionFun) ->
    Worker = poolboy:checkout(?POOLNAME, true, ?DEFAULT_TIMEOUT),
    State = gen_server:call(Worker, state, ?DEFAULT_TIMEOUT),
    put(boss_db_transaction_info, State),
    TransactionFun(),
    put(boss_db_transaction_info, undefined),
    poolboy:checkin(?POOLNAME, Worker).

%% @spec save_record( BossRecord ) -> {ok, SavedBossRecord} | {error, [ErrorMessages]}
%% @doc Save (that is, create or update) the given BossRecord in the database.
%% Performs validation first; see `validate_record/1'.
save_record(Record) ->
    save_record(Record, ?DEFAULT_TIMEOUT).

save_record(Record, Timeout) ->
    case validate_record(Record) of
        ok ->
            RecordId = Record:id(),
            {IsNew, OldRecord} = if
                RecordId =:= 'id' ->
                    {true, Record};
                true ->
                    case find(RecordId, Timeout) of
                        {error, _Reason} -> {true, Record};
                        undefined -> {true, Record};
                        FoundOldRecord -> {false, FoundOldRecord}
                    end
            end,
            % Action dependent valitation
            case validate_record(Record, IsNew) of
                ok ->
                    HookResult = case boss_record_lib:run_before_hooks(Record, IsNew) of
                                     ok -> {ok, Record};
                                     {ok, Record1} -> {ok, Record1};
                                     {error, Reason} -> {error, Reason}
                                 end,
                    case HookResult of
                        {ok, PossiblyModifiedRecord} ->
                            case db_call({save_record, PossiblyModifiedRecord}, Timeout) of
                                {ok, SavedRecord} ->
                                    boss_record_lib:run_after_hooks(OldRecord, SavedRecord, IsNew),
                                    {ok, SavedRecord};
                                Err -> Err
                            end;
                        Err -> Err
                    end;
                Err -> Err
            end;
        Err -> Err
    end.

%% @spec validate_record( BossRecord ) -> ok | {error, [ErrorMessages]}
%% @doc Validate the given BossRecord without saving it in the database.
%% `ErrorMessages' are generated from the list of tests returned by the BossRecord's
%% `validation_tests/0' function (if defined). The returned list should consist of
%% `{TestFunction, ErrorMessage}' tuples, where `TestFunction' is a fun of arity 0
%% that returns `true' if the record is valid or `false' if it is invalid.
%% `ErrorMessage' should be a (constant) string which will be included in `ErrorMessages'
%% if the `TestFunction' returns `false' on this particular BossRecord.
validate_record(Record) ->
    Type = element(1, Record),
    Errors1 = case validate_record_types(Record) of
        ok -> [];
        {error, Errors} -> Errors
    end,
    Errors2 = case Errors1 of
        [] ->
            case erlang:function_exported(Type, validation_tests, 1) of
                true -> [String || {TestFun, String} <- Record:validation_tests(), not TestFun()];
                false -> []
            end;
        _ -> Errors1
    end,
    case length(Errors2) of
        0 -> ok;
        _ -> {error, Errors2}
    end.

%% @spec validate_record( BossRecord, IsNew ) -> ok | {error, [ErrorMessages]}
%% @doc Validate the given BossRecord without saving it in the database.
%% `ErrorMessages' are generated from the list of tests returned by the BossRecord's
%% `validation_tests/1' function (if defined), where parameter is atom() `on_create | on_update'.
%% The returned list should consist of `{TestFunction, ErrorMessage}' tuples,
%% where `TestFunction' is a fun of arity 0
%% that returns `true' if the record is valid or `false' if it is invalid.
%% `ErrorMessage' should be a (constant) string which will be included in `ErrorMessages'
%% if the `TestFunction' returns `false' on this particular BossRecord.
validate_record(Record, IsNew) ->
    Type = element(1, Record),
    Action = case IsNew of
                   true -> on_create;
                   false -> on_update
               end,
    Errors = case erlang:function_exported(Type, validation_tests, 2) of
                 % makes Action optional
                 true -> [String || {TestFun, String} <- try Record:validation_tests(Action)
                                                         catch error:function_clause -> []
                                                         end,
                                    not TestFun()];
                 false -> []
             end,
    case length(Errors) of
        0 -> ok;
        _ -> {error, Errors}
    end.

%% @spec validate_record_types( BossRecord ) -> ok | {error, [ErrorMessages]}
%% @doc Validate the parameter types of the given BossRecord without saving it
%% to the database.
validate_record_types(Record) ->
    Errors = lists:foldl(fun
            ({Attr, Type}, Acc) ->
                case Attr of
                  id -> Acc;
                  _  ->
                    Data = Record:Attr(),
                    GreatSuccess = case {Data, Type} of
                        {undefined, _} ->
                            true;
                        {Data, string} when is_list(Data) ->
                            true;
                        {Data, binary} when is_binary(Data) ->
                            true;
                        {Data, uuid} when is_list(Data) ->
                            true;
                        {{{D1, D2, D3}, {T1, T2, T3}}, datetime} when is_integer(D1), is_integer(D2), is_integer(D3),
                                                                      is_integer(T1), is_integer(T2), is_integer(T3) ->
                            true;
                        {{D1, D2, D3}, date} when is_integer(D1), is_integer(D2), is_integer(D3) ->
                            true;
                        {Data, integer} when is_integer(Data) ->
                            true;
                        {Data, float} when is_float(Data) ->
                            true;
                        {Data, boolean} when is_boolean(Data) ->
                            true;
                        {{N1, N2, N3}, timestamp} when is_integer(N1), is_integer(N2), is_integer(N3) ->
                            true;
                        {Data, atom} when is_atom(Data) ->
                            true;
                        {_Data, Type} ->
                            false
                    end,
                    if
                        GreatSuccess ->
                            Acc;
                        true ->
                            [lists:concat(["Invalid data type for ", Attr])|Acc]
                    end
                  end
        end, [], Record:attribute_types()),
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end.

%% @spec type( Id::string() ) -> Type::atom()
%% @doc Returns the type of the BossRecord with `Id', or `undefined' if the record does not exist.
type(Key) ->

    type(Key, ?DEFAULT_TIMEOUT).

type(Key, Timeout) ->
    case find(Key, Timeout) of
        undefined -> undefined;
        Record -> element(1, Record)

    end.


data_type(_, _Val) when is_float(_Val) ->
    "float";
data_type(_, _Val) when is_binary(_Val) ->
    "binary";
data_type(_, _Val) when is_integer(_Val) ->
    "integer";
data_type(_, _Val) when is_tuple(_Val) ->
    "datetime";
data_type(_, _Val) when is_boolean(_Val) ->
    "boolean";
data_type(_, null) ->
    "null";
data_type(_, undefined) ->
    "null";
data_type('id', _) ->
    "id";
data_type(Key, Val) when is_list(Val) ->
    case lists:suffix("_id", atom_to_list(Key)) of
        true -> "foreign_id";
        false -> "string"
    end.

normalize_conditions(Conditions) ->
    normalize_conditions(Conditions, []).

normalize_conditions([], Acc) ->
    lists:reverse(Acc);
normalize_conditions([Key, Operator, Value|Rest], Acc) when is_atom(Key), is_atom(Operator) ->
    normalize_conditions(Rest, [{Key, Operator, Value}|Acc]);

normalize_conditions([{Key, Value}|Rest], Acc) when is_atom(Key) ->
    normalize_conditions(Rest, [{Key, 'equals', Value}|Acc]);

normalize_conditions([{Key, 'eq', Value}|Rest], Acc) when is_atom(Key) ->
    normalize_conditions(Rest, [{Key, 'equals', Value}|Acc]);

normalize_conditions([{Key, 'ne', Value}|Rest], Acc) when is_atom(Key) ->
    normalize_conditions(Rest, [{Key, 'not_equals', Value}|Acc]);

normalize_conditions([{Key, Operator, Value}|Rest], Acc) when is_atom(Key), is_atom(Operator) ->
    normalize_conditions(Rest, [{Key, Operator, Value}|Acc]);

normalize_conditions([{Key, Operator, Value, Options}|Rest], Acc) when is_atom(Key), is_atom(Operator), is_list(Options) ->
    normalize_conditions(Rest, [{Key, Operator, Value, Options}|Acc]).

return_one([]) -> undefined;
return_one([Result]) ->Result.
