%% @doc Chicago Boss database abstraction

-module(boss_db).

-export([start/1, stop/0]).

-export([
        find/1, 
        find/2, 
        find/3, 
        find/4, 
        find/5, 
        find/6,
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
        transaction/1,
        validate_record/1,
        validate_record_types/1,
        type/1,
        data_type/2]).

-define(DEFAULT_TIMEOUT, (30 * 1000)).
-define(POOLNAME, boss_db_pool).

start(Options) ->
    AdapterName = proplists:get_value(adapter, Options, mock),
    Adapter = list_to_atom(lists:concat(["boss_db_adapter_", AdapterName])),
    Adapter:init(Options),
    lists:foldr(fun(ShardOptions, Acc) ->
                case proplists:get_value(db_shard_models, ShardOptions, []) of
                    [] -> Acc;
                    _ ->
                        ShardAdapter = case proplists:get_value(db_adapter, ShardOptions) of
                            undefined -> Adapter;
                            ShortName -> list_to_atom(lists:concat(["boss_db_adapter_", ShortName]))
                        end,
                        ShardAdapter:init(ShardOptions ++ Options),
                        Acc
                end
        end, [], proplists:get_value(shards, Options, [])),
    boss_db_sup:start_link(Options).

stop() ->
    ok.

db_call(Msg) ->
    case get(boss_db_transaction_info) of
        undefined ->
            boss_pool:call(?POOLNAME, Msg, ?DEFAULT_TIMEOUT);
        State ->
            {reply, Reply, _} = boss_db_controller:handle_call(Msg, self(), State),
            Reply
    end.

%% @spec find(Id::string()) -> BossRecord | {error, Reason}
%% @doc Find a BossRecord with the specified `Id'.
find("") -> undefined;
find(Key) when is_list(Key) ->
    db_call({find, Key});
find(_) ->
    {error, invalid_id}.

%% @spec find(Type::atom(), Conditions) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns all BossRecords of type
%% `Type' matching all of the given `Conditions'
find(Type, Conditions) ->
    find(Type, Conditions, all).

%% @spec find(Type::atom(), Conditions, Max::integer() | all ) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns up to `Max' number of BossRecords of type
%% `Type' matching all of the given `Conditions'
find(Type, Conditions, Max) ->
    find(Type, Conditions, Max, 0).

%% @spec find( Type::atom(), Conditions, Max::integer() | all, Skip::integer() ) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns up to `Max' number of BossRecords of type
%% `Type' matching all of the given `Conditions', skipping the first `Skip' results.
find(Type, Conditions, Max, Skip) ->
    find(Type, Conditions, Max, Skip, id).

%% @spec find( Type::atom(), Conditions, Max::integer() | all, Skip::integer(), Sort::atom() ) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns up to `Max' number of BossRecords of type
%% `Type' matching all of the given `Conditions', skipping the
%% first `Skip' results, sorted on the attribute `Sort'.
find(Type, Conditions, Max, Skip, Sort) ->
    find(Type, Conditions, Max, Skip, Sort, str_ascending).

%% @spec find( Type::atom(), Conditions, Max::integer() | all, Skip::integer(), Sort::atom(), SortOrder ) -> [ BossRecord ]
%%       SortOrder = num_ascending | num_descending | str_ascending | str_descending
%% @doc Query for BossRecords. Returns up to `Max' number of BossRecords of type
%% Type matching all of the given `Conditions', skipping the
%% first `Skip' results, sorted on the attribute `Sort'. `SortOrder' specifies whether
%% to treat values as strings or as numbers, and whether to sort ascending or
%% descending. (`SortOrder' = `num_ascending', `num_descending', `str_ascending', or
%% `str_descending')
%%
%% Note that Time attributes are stored internally as numbers, so you should
%% sort them numerically.

find(Type, Conditions, Max, Skip, Sort, SortOrder) ->
    db_call({find, Type, normalize_conditions(Conditions), Max, Skip, Sort, SortOrder}).

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
    AboutToDelete = boss_db:find(Key),
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

%% @spec validate_record_types( BossRecord ) -> ok | {error, [ErrorMessages]}
%% @doc Validate the parameter types of the given BossRecord without saving it
%% to the database.
validate_record_types(Record) ->
    Errors = lists:foldl(fun
            ({Attr, Type}, Acc) ->
                Data = Record:Attr(),
                GreatSuccess = case {Data, Type} of
                    {Data, string} when is_list(Data) ->
                        true;
                    {Data, binary} when is_binary(Data) ->
                        true;
                    {{{D1, D2, D3}, {T1, T2, T3}}, datetime} when is_integer(D1), is_integer(D2), is_integer(D3), 
                                                                  is_integer(T1), is_integer(T2), is_integer(T3) ->
                        true;
                    {Data, integer} when is_integer(Data) ->
                        true;
                    {Data, float} when is_float(Data) ->
                        true;
                    {Data, number} when is_number(Data) ->
                        true;
                    {{N1, N2, N3}, now} when is_integer(N1), is_integer(N2), is_integer(N3) ->
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
normalize_conditions([{Key, Operator, Value}|Rest], Acc) when is_atom(Key), is_atom(Operator) ->
    normalize_conditions(Rest, [{Key, Operator, Value}|Acc]).
