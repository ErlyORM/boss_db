-module(boss_record_lib).
-export([run_before_hooks/3,
        run_after_hooks/3,
        run_before_delete_hooks/1,
        is_boss_record/2,
        dummy_record/1,
        attribute_names/1,
        attribute_types/1,
        database_columns/1,
        database_table/1,
	belongs_to_types/1,
        convert_value_to_type/2,
        ensure_loaded/1
    ]).
-ifdef(TEST).
-compile(export_all).
-endif.
-define(THOUSAND, 1000).
-define(MILLION, ?THOUSAND*?THOUSAND).

-spec run_before_hooks(tuple(),tuple(),boolean()) -> any().
-spec run_after_hooks(_,tuple(),boolean()) -> any().
-spec run_before_delete_hooks(tuple()) -> any().
-spec run_hooks(tuple(),atom() | tuple(),'after_create' | 'before_create' | 'before_delete') -> any().
-spec run_hooks(tuple(),tuple(),atom() | tuple(),'after_update' | 'before_update') -> any().
-spec is_boss_record(_,_) -> boolean().
-spec dummy_record(atom() | tuple()) -> any().
-spec attribute_names(atom() | tuple()) -> any().
-spec attribute_types(atom() | tuple()) -> any().
-spec database_columns(atom() | tuple()) -> any().
-spec database_table(atom() | tuple()) -> any().
-spec belongs_to_types(atom()) -> any().
-spec ensure_loaded(atom()) -> boolean().
-type target_types() :: 'binary' | 'boolean' | 'date' | 'datetime' | 'float' | 'integer' | 'string' | 'timestamp' | 'undefined'.
-spec convert_value_to_type(_,target_types()) -> any().

run_before_hooks(_OldRecord, Record, true) ->
    run_hooks(Record, element(1, Record), before_create);
run_before_hooks(OldRecord, Record, false) ->
    run_hooks(OldRecord, Record, element(1, Record), before_update).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
run_after_hooks(_UnsavedRecord, SavedRecord, true) ->
    boss_news:created(SavedRecord:id(), SavedRecord:attributes()),
    run_hooks(SavedRecord, element(1, SavedRecord), after_create);
run_after_hooks(UnsavedRecord, SavedRecord, false) ->
    boss_news:updated(SavedRecord:id(), UnsavedRecord:attributes(), SavedRecord:attributes()),
    run_hooks(UnsavedRecord, SavedRecord, element(1, SavedRecord), after_update).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
run_before_delete_hooks(Record) ->
    run_hooks(Record, element(1, Record), before_delete).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
run_hooks(Record, Type, Function) ->
    case erlang:function_exported(Type, Function, 1) of
        true  -> Record:Function();
        false -> ok
    end.

run_hooks(OldRecord, Record, Type, Function) ->
    case erlang:function_exported(Type, Function, 2) of
        true  ->
            Record:Function(OldRecord);
        false ->
            %% As for backward compatibilities check if a old *_update/1 function exists
            run_hooks(Record, Type, Function)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
is_boss_record(Record, ModelList) when is_tuple(Record) andalso is_atom(element(1, Record)) ->
    Type = element(1, Record),
    lists:member(atom_to_list(Type), ModelList)            andalso
        erlang:function_exported(Type, attribute_names, 1) andalso
        erlang:function_exported(Type, new, tuple_size(Record) - 1);
is_boss_record(_, _) ->
    false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dummy_record(Module) ->
    NumArgs = proplists:get_value('new', Module:module_info(exports)),
    apply(Module, 'new', ['id'] ++ lists:duplicate(NumArgs-1, undefined)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
attribute_names(Module) ->
    DummyRecord = dummy_record(Module),
    DummyRecord:attribute_names().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
attribute_types(Module) ->
    DummyRecord = dummy_record(Module),
    DummyRecord:attribute_types().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
database_columns(Module) ->
    DummyRecord = dummy_record(Module),
    DummyRecord:database_columns().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
database_table(Module) ->
    DummyRecord = dummy_record(Module),
    DummyRecord:database_table().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
belongs_to_types(Module) when is_atom(Module) ->
    (dummy_record(Module)):belongs_to_types().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
ensure_loaded(Module) ->
    case code:ensure_loaded(Module) of
        {module, Module} ->
            Exports = Module:module_info(exports),
            proplists:get_value(attribute_names, Exports) =:= 1;
        _ -> false
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
convert_value_to_type(Val, undefined) ->
    Val;
convert_value_to_type(null, _) ->
    null;
convert_value_to_type(Val, integer) when is_integer(Val) ->
    Val;
convert_value_to_type(Val, integer) when is_list(Val) ->
    list_to_integer(Val);
convert_value_to_type(Val, integer) when is_binary(Val) ->
    list_to_integer(binary_to_list(Val));
convert_value_to_type(Val, float) when is_binary(Val) ->
    binary_to_float(Val);
convert_value_to_type(Val, float) when is_float(Val) ->
    Val;
convert_value_to_type(Val, float) when is_integer(Val) ->
    1.0 * Val;
convert_value_to_type(Val, string) when is_integer(Val) ->
    integer_to_list(Val);
convert_value_to_type(Val, string) when is_binary(Val) ->
    binary_to_list(Val);
convert_value_to_type(Val, string) when is_list(Val) ->
    Val;
convert_value_to_type(Val, binary) when is_integer(Val) ->
    list_to_binary(integer_to_list(Val));
convert_value_to_type(Val, binary) when is_list(Val) ->
    list_to_binary(Val);
convert_value_to_type(Val, binary) when is_binary(Val) ->
    Val;
convert_value_to_type({{D1, D2, D3}, {T1, T2, T3}}, Type) when is_integer(D1), is_integer(D2), is_integer(D3),
                                                               is_integer(T1), is_integer(T2), is_float(T3) ->
    convert_value_to_type({{D1, D2, D3}, {T1, T2, round(T3)}}, Type);
convert_value_to_type({{D1, D2, D3}, {T1, T2, T3}} = Val, integer) when is_integer(D1), is_integer(D2), is_integer(D3),
                                                                        is_integer(T1), is_integer(T2), is_integer(T3) ->
    calendar:datetime_to_gregorian_seconds(Val);
convert_value_to_type({{D1, D2, D3}, {T1, T2, T3}} = Val, timestamp) when is_integer(D1), is_integer(D2), is_integer(D3),
                                                                          is_integer(T1), is_integer(T2), is_integer(T3) ->
    Secs = calendar:datetime_to_gregorian_seconds(Val) - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
    {Secs rem ?MILLION, Secs div ?MILLION, 0};
convert_value_to_type({{D1, D2, D3}, {T1, T2, T3}} = Val, datetime) when is_integer(D1), is_integer(D2), is_integer(D3),
                                                                         is_integer(T1), is_integer(T2), is_integer(T3) ->
    Val;
convert_value_to_type({D1, D2, D3} = Val, date) when is_integer(D1), is_integer(D2), is_integer(D3) ->
    Val;
convert_value_to_type({date, {D1, D2, D3} = Val}, date) when is_integer(D1), is_integer(D2), is_integer(D3) ->
    Val;
convert_value_to_type(<<"1">>,     boolean) -> true;
convert_value_to_type(<<"0">>,     boolean) -> false;
convert_value_to_type(<<"true">>,  boolean) -> true;
convert_value_to_type(<<"false">>, boolean) -> false;
convert_value_to_type("1",         boolean) -> true;
convert_value_to_type("0",         boolean) -> false;
convert_value_to_type("true",      boolean) -> true;
convert_value_to_type("false",     boolean) -> false;
convert_value_to_type(1,           boolean) -> true;
convert_value_to_type(0,           boolean) -> false;
convert_value_to_type(true,        boolean) -> true;
convert_value_to_type(false,       boolean) -> false.
