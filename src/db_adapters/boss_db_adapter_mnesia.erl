-module(boss_db_adapter_mnesia).
-behaviour(boss_db_adapter).
-export([init/1, terminate/1, start/1, find/2, find/7]).
-export([count/3, counter/2, incr/3, delete/2, save_record/2]).
-export([transaction/2]).
-export([table_exists/2, get_migrations_table/1, migration_done/3]).

%-define(TRILLION, (1000 * 1000 * 1000 * 1000)).

start(_) ->
    application:start(mnesia).

% -----

init(_Options) ->
    {ok, undefined}.

% -----
terminate(_) ->
    application:stop(mnesia).

% -----
find(_, Id) when is_list(Id) ->
    Type = infer_type_from_id(Id),
    Fun = fun () -> mnesia:read(Type,Id) end,
    case mnesia:transaction(Fun)  of
        {atomic,[]} ->
            undefined;
        {atomic,[Record]} ->   % I dont like this - we should really be checking that we only got 1 record
            case boss_record_lib:ensure_loaded(Type) of
                true ->
                    Record;
                false ->
                    {error, {module_not_loaded, Type}}
            end;
        {aborted, Reason} ->
            {error, Reason}
    end.

% -----
find(_, Type, Conditions, Max, Skip, Sort, SortOrder) when is_atom(Type), is_list(Conditions),
                                                        is_integer(Max) orelse Max =:= all,
                                                        is_integer(Skip), is_atom(Sort), is_atom(SortOrder) ->
% Mnesia allows a pattern to be provided against which it will check records.
% This allows 'eq' conditions to be handled by Mnesia itself. The list of remaining
% conditions form a 'filter' against which each record returned by Mnesia is tested here.
% So...the first job here is to split the Conditions into a Pattern and a 'Filter'.
    case boss_record_lib:ensure_loaded(Type) of
        true ->
            {Pattern, Filter} = build_query(Type, Conditions, Max, Skip, Sort, SortOrder),
            RawList = mnesia:dirty_match_object(list_to_tuple([Type | Pattern])),
            FilteredList = apply_filters(RawList, Filter),
            SortedList = apply_sort(FilteredList, Sort, SortOrder),
            SkippedList = apply_skip(SortedList, Skip),
            MaxList = apply_max(SkippedList, Max),
            MaxList;
        false ->
            []
    end.

apply_filters(List, Filters) ->
    apply_filters(List, Filters, []).

apply_filters([],_Filters,Acc) ->
    Acc;
apply_filters([First|Rest],Filters,Acc) ->
    case filter_rec(First,Filters) of
        keep ->
            apply_filters(Rest,Filters,[First|Acc]);
        drop ->
            apply_filters(Rest,Filters,Acc)
    end.

filter_rec(_Rec, []) ->
    keep;
filter_rec(Rec, [First|Rest]) ->
    case test_rec(Rec, First) of
        true ->
            filter_rec(Rec,Rest);
        false ->
            drop
    end.

apply_sort([], _Key, _Order) ->
    [];
apply_sort(List, primary, Order) ->
    apply_sort(List, id, Order);
apply_sort(List, Key, ascending) ->
    Fun = fun (A, B) -> apply(A,Key,[]) =< apply(B,Key,[]) end,
    lists:sort(Fun, List);
apply_sort(List, Key, descending) ->
    Fun = fun (A, B) -> apply(A,Key,[]) >= apply(B,Key,[]) end,
    lists:sort(Fun, List).



apply_skip(List, 0) ->
    List;
apply_skip(List, Skip) when Skip >= length(List) ->
    [];
apply_skip(List, Skip) ->
    lists:nthtail(Skip, List).

apply_max(List, all) ->
    List;
apply_max(List, Max) when is_integer(Max) ->
    lists:sublist(List, Max).

test_rec(Rec,{Key, 'not_equals', Value}) ->
    apply(Rec,Key,[]) /= Value;
test_rec(Rec,{Key, 'in', Value}) when is_list(Value) ->
    lists:member(apply(Rec,Key,[]), Value) ;
test_rec(Rec,{Key, 'not_in', Value}) when is_list(Value) ->
    not lists:member(apply(Rec,Key,[]), Value) ;
test_rec(Rec,{Key, 'in', {Min, Max}}) when Max >= Min ->
    Fld = apply(Rec,Key,[]),
    (Fld >= Min) and (Fld =< Max);
test_rec(Rec,{Key, 'not_in', {Min, Max}}) when Max >= Min ->
    Fld = apply(Rec,Key,[]),
    (Fld < Min) or (Fld > Max);
test_rec(Rec,{Key, 'gt', Value}) ->
    apply(Rec,Key,[]) > Value;
test_rec(Rec,{Key, 'lt', Value}) ->
    apply(Rec,Key,[]) < Value;
test_rec(Rec,{Key, 'ge', Value}) ->
    apply(Rec,Key,[]) >= Value;
test_rec(Rec,{Key, 'le', Value}) ->
    apply(Rec,Key,[]) =< Value;
test_rec(Rec,{Key, 'matches', "*"++Value}) ->
    {ok, MP} = re:compile(Value, [caseless]),
    case re:run(apply(Rec,Key,[]), MP) of
        {match,_} -> true;
        _ -> false
    end;
test_rec(Rec,{Key, 'matches', Value}) ->
    {ok, MP} = re:compile(Value),
    case re:run(apply(Rec,Key,[]), MP) of
        {match,_} -> true;
        _ -> false
    end;
test_rec(Rec,{Key, 'not_matches', Value}) ->
    not test_rec(Rec,{Key, 'matches', Value});
test_rec(Rec,{Key, 'contains', Value}) ->
    lists:member(Value,apply(Rec,Key,[]));
test_rec(Rec,{Key, 'not_contains', Value}) ->
    not lists:member(Value,apply(Rec,Key,[]));
test_rec(Rec,{Key, 'contains_all', Values}) when is_list(Values) ->
    lists:all(fun (Ele) -> lists:member(Ele, apply(Rec,Key,[])) end, Values);
test_rec(Rec,{Key, 'not_contains_all', Values}) when is_list(Values) ->
    lists:any(fun (Ele) -> not lists:member(Ele, apply(Rec,Key,[])) end, Values);
test_rec(Rec,{Key, 'contains_any', Values}) when is_list(Values) ->
    lists:any(fun (Ele) -> lists:member(Ele, apply(Rec,Key,[])) end, Values);
test_rec(Rec,{Key, 'contains_none', Values}) when is_list(Values) ->
    lists:any(fun (Ele) -> not lists:member(Ele, apply(Rec,Key,[])) end, Values).

% -----
count(Conn, Type, Conditions) ->
    length(find(Conn, Type, Conditions, all, 0, id, ascending)).

% -----
counter(Conn, Id) when is_list(Id) ->
    counter(Conn, list_to_binary(Id));
counter(_, Id) when is_binary(Id) ->
    mnesia:dirty_update_counter('_ids_', Id, 0).

% -----
incr(Conn, Id, Count) when is_list(Id) ->
    incr(Conn, list_to_binary(Id), Count);
incr(_, Id, Count) ->
    mnesia:dirty_update_counter('_ids_', Id, Count).

% -----
delete(Conn, Id) when is_binary(Id) ->

    delete(Conn, binary_to_list(Id));
delete(_, Id) when is_list(Id) ->
    Type = infer_type_from_id(Id),
    Fun = fun () -> mnesia:delete({Type,Id}) end,
    case mnesia:transaction(Fun)  of
        {atomic,ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.

% -----
save_record(_, Record) when is_tuple(Record) ->
    Type = element(1, Record),
    Id = case Record:id() of
        id ->
            atom_to_list(Type) ++ "-" ++ integer_to_list(gen_uid(Type));
        Defined ->
            Defined
    end,
    RecordWithId = Record:set(id, Id),

    Fun = fun() -> mnesia:write(Type, RecordWithId, write) end,

    case mnesia:transaction(Fun) of
	    {atomic, ok} ->
	        {ok, RecordWithId};
	    {aborted, Reason} ->
	        {error, Reason}
    end.

transaction(_, TransactionFun) when is_function(TransactionFun) ->
    mnesia:transaction(TransactionFun).

% This is needed to support boss_db:migrate
table_exists(_, TableName) when is_atom(TableName) ->
    lists:member(TableName, mnesia:table_info(schema, tables)).

get_migrations_table(_) ->
    mnesia:dirty_match_object({schema_migrations, '_', '_', '_'}).

migration_done(_, Tag, up) ->
    Id = "schema_migrations-" ++ integer_to_list(gen_uid(schema_migrations)),
    RecordWithId = {schema_migrations, Id, atom_to_list(Tag), os:timestamp()},

    Fun = fun() -> mnesia:write(schema_migrations, RecordWithId, write) end,

    case mnesia:transaction(Fun) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end;
migration_done(_, Tag, down) ->
    case mnesia:dirty_match_object({schema_migrations, '_', atom_to_list(Tag), '_'}) of
        [] ->
            ok;
        [Migration] ->
            Id = element(2, Migration),
            Fun = fun () -> mnesia:delete({schema_migrations,Id}) end,

            case mnesia:transaction(Fun)  of
                {atomic,ok} ->
                    ok;
                {aborted, Reason} ->
                    {error, Reason}
            end
    end.

% -----

gen_uid(Tab) ->
    mnesia:dirty_update_counter('_ids_', Tab, 1).

%-----
infer_type_from_id(Id) when is_list(Id) ->
    list_to_atom(hd(string:tokens(Id, "-"))).

%-----
build_query(Type, Conditions, _Max, _Skip, _Sort, _SortOrder) -> % a Query is a {Pattern, Filter} combo
    Fldnames = mnesia:table_info(Type, attributes),
    BlankPattern = [ {Fld, '_'} || Fld <- Fldnames],
    {Pattern, Filter} = build_conditions(BlankPattern, [], Conditions),
    {[proplists:get_value(Fldname, Pattern) || Fldname <- Fldnames], Filter}.

build_conditions(Pattern, Filter, Conditions) ->
    build_conditions1(Conditions, Pattern, Filter).

build_conditions1([], Pattern, Filter) ->
    {Pattern, Filter};
build_conditions1([{Key, 'equals', Value}|Rest], Pattern, Filter) ->
    build_conditions1([{Key, 'eq', Value}|Rest], Pattern, Filter);
build_conditions1([{Key, 'eq', Value}|Rest], Pattern, Filter) ->
    build_conditions1(Rest, lists:keystore(Key, 1, Pattern, {Key, Value}), Filter);
build_conditions1([First|Rest], Pattern, Filter) ->
    build_conditions1(Rest, Pattern, [First|Filter]).
