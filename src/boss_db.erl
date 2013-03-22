%% @doc Chicago Boss database abstraction

-module(boss_db).

-export([start/1, stop/0]).

-export([
        find/1,
        find/2,
        find/3,
        find_first/2,
        find_first/3,
        find_last/2,
        find_last/3,
        count/1,
        count/2,
        counter/1,
        incr/1,
        incr/2,
        delete/1,
        save_record/1,
        push/0,
        pop/0,
        depth/0,
        dump/0,
        execute/1,
        execute/2,
        transaction/1,
        validate_record/1,
        validate_record_constraints/2,
        validate_record_types/1,
        type/1,
        data_type/2]).

-define(DEFAULT_TIMEOUT, (30 * 1000)).
-define(POOLNAME, boss_db_pool).

start(Options) ->
    AdapterName = proplists:get_value(adapter, Options, mock),
    Adapter = list_to_atom(lists:concat(["boss_db_adapter_", AdapterName])),
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
    case erlang:get(boss_db_transaction_info) of
        undefined ->
            boss_pool:call(?POOLNAME, Msg, ?DEFAULT_TIMEOUT);
        State ->
            {reply, Reply, _} = boss_db_controller:handle_call(Msg, self(), State),
            Reply
    end.

%% @spec find(Id::string()) -> Value | {error, Reason}
%% @doc Find a BossRecord with the specified `Id' (e.g. "employee-42") or a value described
%% by a dot-separated path (e.g. "employee-42.manager.name").
find("") -> undefined;
find(Key) when is_list(Key) ->
    [IdToken|Rest] = string:tokens(Key, "."),
    case db_call({find, IdToken}) of
        undefined -> undefined;
        {error, Reason} -> {error, Reason};
        BossRecord -> BossRecord:get(string:join(Rest, "."))
    end;
find(_) ->
    {error, invalid_id}.

%% @spec find(Type::atom(), Conditions) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns all BossRecords of type
%% `Type' matching all of the given `Conditions'
find(Type, Conditions) ->
    find(Type, Conditions, []).

%% @spec find(Type::atom(), Conditions, Options::proplist()) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns BossRecords of type
%% `Type' matching all of the given `Conditions'. Options may include
%% `limit' (maximum number of records to return), `offset' (number of records
%% to skip), `order_by' (attribute to sort on), `descending' (whether to
%% sort the values from highest to lowest), and `include' (list of belongs_to
%% associations to pre-cache)
find(Type, Conditions, Options) ->
    Max = proplists:get_value(limit, Options, all),
    Skip = proplists:get_value(offset, Options, 0),
    Sort = proplists:get_value(order_by, Options, id),
    SortOrder = case proplists:get_value(descending, Options) of
        true -> descending;
        _ -> ascending
    end,
    Include = proplists:get_value(include, Options, []),
    db_call({find, Type, normalize_conditions(Conditions), Max, Skip, Sort, SortOrder, Include}).

%% @spec find_first( Type::atom(), Conditions ) -> Record | undefined
%% @doc Query for the first BossRecord of type `Type' matching all of the given `Conditions'
find_first(Type, Conditions) ->
    return_one(find(Type, Conditions, [{limit, 1}])).

%% @spec find_first( Type::atom(), Conditions, Sort::atom() ) -> Record | undefined
%% @doc Query for the first BossRecord of type `Type' matching all of the given `Conditions',
%% sorted on the attribute `Sort'.
find_first(Type, Conditions, Sort) ->
    return_one(find(Type, Conditions, [{limit, 1}, {order_by, Sort}])).

%% @spec find_last( Type::atom(), Conditions ) -> Record | undefined
%% @doc Query for the last BossRecord of type `Type' matching all of the given `Conditions'
find_last(Type, Conditions) ->
    return_one(find(Type, Conditions, [{limit, 1}, descending])).

%% @spec find_last( Type::atom(), Conditions, Sort ) -> Record | undefined
%% @doc Query for the last BossRecord of type `Type' matching all of the given `Conditions'
find_last(Type, Conditions, Sort) ->
    return_one(find(Type, Conditions, [{limit, 1}, {order_by, Sort}, descending])).

%% @spec count( Type::atom() ) -> integer()
%% @doc Count the number of BossRecords of type `Type' in the database.
count(Type) ->
    count(Type, []).

%% @spec count( Type::atom(), Conditions ) -> integer()
%% @doc Count the number of BossRecords of type `Type' in the database matching
%% all of the given `Conditions'.
count(Type, Conditions) ->
    db_call({count, Type, normalize_conditions(Conditions)}).

%% @spec counter( Id::string() ) -> integer()
%% @doc Treat the record associated with `Id' as a counter and return its value.
%% Returns 0 if the record does not exist, so to reset a counter just use
%% "delete".
counter(Key) ->
    db_call({counter, Key}).

%% @spec incr( Id::string() ) -> integer()
%% @doc Treat the record associated with `Id' as a counter and atomically increment its value by 1.
incr(Key) ->
    incr(Key, 1).

%% @spec incr( Id::string(), Increment::integer() ) -> integer()
%% @doc Treat the record associated with `Id' as a counter and atomically increment its value by `Increment'.
incr(Key, Count) ->
    db_call({incr, Key, Count}).

%% @spec delete( Id::string() ) -> ok | {error, Reason}
%% @doc Delete the BossRecord with the given `Id'.
delete(Key) ->
    case boss_db:find(Key) of
        undefined ->
            {error, not_found};
        AboutToDelete ->
            case boss_record_lib:run_before_delete_hooks(AboutToDelete) of
                ok ->
                    Result = db_call({delete, Key}),
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

pop() ->
    db_call(pop).

depth() ->
    db_call(depth).

dump() ->
    db_call(dump).

%% @spec execute( Commands::iolist() ) -> RetVal
%% @doc Execute raw database commands on SQL databases
execute(Commands) ->
    db_call({execute, Commands}).

%% @spec execute( Commands::iolist(), Params::list() ) -> RetVal
%% @doc Execute database commands with interpolated parameters on SQL databases
execute(Commands, Params) ->
    db_call({execute, Commands, Params}).

%% @spec transaction( TransactionFun::function() ) -> {atomic, Result} | {aborted, Reason}
%% @doc Execute a fun inside a transaction.
transaction(TransactionFun) ->
    Worker = poolboy:checkout(?POOLNAME),
    State = gen_server:call(Worker, state, ?DEFAULT_TIMEOUT),
    put(boss_db_transaction_info, State),
    {reply, Reply, _} = boss_db_controller:handle_call({transaction, TransactionFun}, self(), State),
    put(boss_db_transaction_info, undefined),
    poolboy:checkin(?POOLNAME, Worker),
    Reply.

%% @spec save_record( BossRecord ) -> {ok, SavedBossRecord} | {error, [ErrorMessages]}
%% @doc Save (that is, create or update) the given BossRecord in the database.
%% Performs validation first; see `validate_record/1'.
save_record(Record) ->
    case validate_record(Record) of
        ok ->
            RecordId = Record:id(),
            {IsNew, OldRecord} = if
                RecordId =:= 'id' ->
                    {true, Record};
                true ->
                    case find(RecordId) of
                        {error, _Reason} -> {true, Record};
                        undefined -> {true, Record};
                        FoundOldRecord -> {false, FoundOldRecord}
                    end
            end,
            case validate_record_constraints(Record, IsNew) of
                ok ->
                    HookResult = case boss_record_lib:run_before_hooks(Record, IsNew) of
                                     ok -> {ok, Record};
                                     {ok, Record1} -> {ok, Record1};
                                     {error, Reason} -> {error, Reason}
                                 end,
                    case HookResult of
                        {ok, PossiblyModifiedRecord} ->
                            case db_call({save_record, PossiblyModifiedRecord}) of
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

%% @spec validate_record_constraints( BossRecord ) -> ok | {error, [ErrorMessages]}
%% @doc Validate the given BossRecord without saving it in the database.
%% `ErrorMessages' are generated from the list of constraints returned by the BossRecord's
%% `constraints_create/0' or `constraints_update/0' function (if defined).
%% The returned list should consist of `{ConstraintFunction, ErrorMessage}' tuples,
%% where `ConstraintFunction' is a fun of arity 0
%% that returns `true' if the record is valid or `false' if it is invalid.
%% `ErrorMessage' should be a (constant) string which will be included in `ErrorMessages'
%% if the `ConstraintFunction' returns `false' on this particular BossRecord.
validate_record_constraints(Record, IsNew) ->
    Type = element(1, Record),
    Function = case IsNew of
                   true -> constraints_create;
                   false -> constraints_update
               end,
    Errors = case erlang:function_exported(Type, Function, 1) of
                 true -> [String || {ConstaintFun, String} <- Record:Function(), not ConstaintFun()];
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
    case find(Key) of
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
    normalize_conditions(Rest, [{Key, Operator, Value}|Acc]).

return_one(Result) ->
    case Result of
        [] -> undefined;
        [Record] -> Record
    end.
