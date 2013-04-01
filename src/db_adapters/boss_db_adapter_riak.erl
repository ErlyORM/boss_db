-module(boss_db_adapter_riak).
-behaviour(boss_db_adapter).
-export([init/1, terminate/1, start/1, stop/0, find/2, find/7]).
-export([count/3, counter/2, incr/2, incr/3, delete/2, save_record/2]).
-export([push/2, pop/2]).

-define(LOG(Name, Value), io:format("DEBUG: ~s: ~p~n", [Name, Value])).

-define(HUGE_INT, 1000 * 1000 * 1000 * 1000).

start(_Options) ->
    % TODO: crypto is needed for unique_id_62/0. Remove it when
    %       unique_id_62/0 is not needed.
    crypto:start().

stop() ->
    ok.

init(Options) ->
    Host = proplists:get_value(db_host, Options, "localhost"),
    Port = proplists:get_value(db_port, Options, 8087),
    riakc_pb_socket:start_link(Host, Port).

terminate(Conn) ->
    riakc_pb_socket:stop(Conn).

find(Conn, Id) ->
    {Type, Bucket, Key} = infer_type_from_id(Id),
    case riakc_pb_socket:get(Conn, Bucket, Key) of
        {ok, Res} ->
            Value = riakc_obj:get_value(Res),
            Data = binary_to_term(Value),
            ConvertedData = decode_data_from_riak(Data),
            AttributeTypes = boss_record_lib:attribute_types(Type),
            Record = apply(Type, new, lists:map(fun (AttrName) ->
                            Val = proplists:get_value(AttrName, ConvertedData),
                            AttrType = proplists:get_value(AttrName, AttributeTypes),
                            boss_record_lib:convert_value_to_type(Val, AttrType)
                    end, boss_record_lib:attribute_names(Type))),
            Record:set(id, Id);
        {error, Reason} ->
            {error, Reason}
    end.

find_acc(_, _, [], Acc) ->
    lists:reverse(Acc);
find_acc(Conn, Prefix, [Id | Rest], Acc) ->
    case find(Conn, Prefix ++ binary_to_list(Id)) of
        {error, _Reason} ->
            find_acc(Conn, Prefix, Rest, Acc);

        Value ->
            find_acc(Conn, Prefix, Rest, [Value | Acc])
    end.

% this is a stub just to make the tests runable
find(Conn, Type, Conditions, Max, Skip, Sort, SortOrder) ->
    Bucket = type_to_bucket_name(Type),
    {ok, Keys} = case Conditions of
        [] ->
            riakc_pb_socket:list_keys(Conn, Bucket);
        _ ->
            {ok, {search_results, KeysExt, _, _}} = riakc_pb_socket:search(
                Conn, Bucket, build_search_query(Conditions)),
            ConvertedKeysExt = decode_search_from_riak(KeysExt),
            {ok, lists:map(fun ({_,X})->
                             Id = proplists:get_value(id, X),
                             list_to_binary(Id)
                           end, ConvertedKeysExt)}
    end,
    Records = find_acc(Conn, atom_to_list(Type) ++ "-", Keys, []),
    Sorted = if
        is_atom(Sort) ->
            lists:sort(fun (A, B) ->
                        case SortOrder of
                            ascending -> A:Sort() =< B:Sort();
                            descending -> A:Sort() > B:Sort()
                        end
                end,
                Records);
        true -> Records
    end,
    case Max of
        all -> lists:nthtail(Skip, Sorted);
        Max when Skip < length(Sorted) ->
            lists:sublist(Sorted, Skip + 1, Max);
        _ ->
            []
    end.

% this is a stub just to make the tests runable
count(Conn, Type, Conditions) ->
    length(find(Conn, Type, Conditions, all, 0, 0, 0)).

counter(_Conn, _Id) ->
    {error, notimplemented}.

incr(Conn, Id) ->
    incr(Conn, Id, 1).
incr(_Conn, _Id, _Count) ->
    {error, notimplemented}.


delete(Conn, Id) ->
    {_Type, Bucket, Key} = infer_type_from_id(Id),
    ok = riakc_pb_socket:delete(Conn, Bucket, Key).

save_record(Conn, Record) ->
    Type = element(1, Record),
    Bucket = list_to_binary(type_to_bucket_name(Type)),
    PropList = [{K, V} || {K, V} <- Record:attributes(), K =/= id],
    ConvertedPropList = encode_data_for_riak(PropList),
    Key = case Record:id() of
        id ->
            % TODO: The next release of Riak will support server-side ID
            %       generating. Get rid of unique_id_62/0.
            unique_id_62();
        DefinedId when is_list(DefinedId) ->
            [_ | Tail] = string:tokens(DefinedId, "-"),
            string:join(Tail, "-")
    end,
    BinKey = list_to_binary(Key),
    ok = case riakc_pb_socket:get(Conn, Bucket, BinKey) of
        {ok, O} ->
            O2 = riakc_obj:update_value(O, ConvertedPropList),
            riakc_pb_socket:put(Conn, O2);
        {error, _} ->
            O = riakc_obj:new(Bucket, BinKey, ConvertedPropList, <<"application/x-erlang-term">>),
            riakc_pb_socket:put(Conn, O)
    end,
    {ok, Record:set(id, lists:concat([Type, "-", Key]))}.

% These 2 functions are not part of the behaviour but are required for
% tests to pass
push(_Conn, _Depth) -> ok.

pop(_Conn, _Depth) -> ok.

% Internal functions

infer_type_from_id(Id) when is_list(Id) ->
    [Type | Tail] = string:tokens(Id, "-"),
    BossId = string:join(Tail, "-"),
    {list_to_atom(Type), type_to_bucket(Type), list_to_binary(BossId)}.

% Find bucket name from Boss type
type_to_bucket(Type) ->
    list_to_binary(type_to_bucket_name(Type)).

type_to_bucket_name(Type) when is_atom(Type) ->
    type_to_bucket_name(atom_to_list(Type));
type_to_bucket_name(Type) when is_list(Type) ->
    inflector:pluralize(Type).

% Unique key generator (copy&pasted from riak_core_util.erl)
% see https://github.com/basho/riak_core/blob/master/src/riak_core_util.erl#L131
% for details.
% TODO: Get rid of this code when server-side ID generating is available
%       in Riak.

%% @spec integer_to_list0(Integer :: integer(), Base :: integer()) ->
%% string()
%% @doc Convert an integer to its string representation in the given
%% base. Bases 2-62 are supported.
integer_to_list0(I, 10) ->
    erlang:integer_to_list(I);
integer_to_list0(I, Base)
  when is_integer(I), is_integer(Base),Base >= 2, Base =< 1+$Z-$A+10+1+$z-$a ->
    if I < 0 ->
            [$-|integer_to_list0(-I, Base, [])];
       true ->
            integer_to_list0(I, Base, [])
    end;
integer_to_list0(I, Base) ->
    erlang:error(badarg, [I, Base]).

%% @spec integer_to_list0(integer(), integer(), string()) -> string()
integer_to_list0(I0, Base, R0) ->
    D = I0 rem Base,
    I1 = I0 div Base,
    R1 = if D >= 36 ->
                [D-36+$a|R0];
            D >= 10 ->
                [D-10+$A|R0];
            true ->
                [D+$0|R0]
         end,
    if I1 =:= 0 ->
            R1;
       true ->
            integer_to_list0(I1, Base, R1)
    end.

%% @spec unique_id_62() -> string()
%% @doc Create a random identifying integer, returning its string
%% representation in base 62.
unique_id_62() ->
    Rand = crypto:sha(term_to_binary({make_ref(), now()})),
    <<I:160/integer>> = Rand,
    integer_to_list0(I, 62).

build_search_query(Conditions) ->
    Terms = build_search_query(Conditions, []),
    string:join(Terms, " AND ").

build_search_query([], Acc) ->
    lists:reverse(Acc);
build_search_query([{Key, 'equals', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", quote_value(Value)])|Acc]);
build_search_query([{Key, 'not_equals', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", Key, ":", quote_value(Value)])|Acc]);
build_search_query([{Key, 'in', Value}|Rest], Acc) when is_list(Value) ->
    build_search_query(Rest, [lists:concat(["(", string:join(lists:map(fun(Val) ->
                                    lists:concat([Key, ":", quote_value(Val)])
                            end, Value), " OR "), ")"])|Acc]);
build_search_query([{Key, 'not_in', Value}|Rest], Acc) when is_list(Value) ->
    build_search_query(Rest, [lists:concat(["(", string:join(lists:map(fun(Val) ->
                                    lists:concat(["NOT ", Key, ":", quote_value(Val)])
                            end, Value), " AND "), ")"])|Acc]);
build_search_query([{Key, 'in', {Min, Max}}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "[", Min, " TO ", Max, "]"])|Acc]);
build_search_query([{Key, 'not_in', {Min, Max}}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", Key, ":", "[", Min, " TO ", Max, "]"])|Acc]);
build_search_query([{Key, 'gt', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "{", Value, " TO ", ?HUGE_INT, "}"])|Acc]);
build_search_query([{Key, 'lt', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "{", -?HUGE_INT, " TO ", Value, "}"])|Acc]);
build_search_query([{Key, 'ge', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "[", Value, " TO ", ?HUGE_INT, "]"])|Acc]);
build_search_query([{Key, 'le', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "[", -?HUGE_INT, " TO ", Value, "]"])|Acc]);
build_search_query([{Key, 'matches', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", Value])|Acc]);
build_search_query([{Key, 'not_matches', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", Key, ":", Value])|Acc]);
build_search_query([{Key, 'contains', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", escape_value(Value)])|Acc]);
build_search_query([{Key, 'not_contains', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", Key, ":", escape_value(Value)])|Acc]);
build_search_query([{Key, 'contains_all', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["(", string:join(lists:map(fun(Val) ->
                                lists:concat([Key, ":", escape_value(Val)])
                        end, Value), " AND "), ")"])|Acc]);
build_search_query([{Key, 'not_contains_all', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", "(", string:join(lists:map(fun(Val) ->
                                lists:concat([Key, ":", escape_value(Val)])
                        end, Value), " AND "), ")"])|Acc]);
build_search_query([{Key, 'contains_any', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["(", string:join(lists:map(fun(Val) ->
                                lists:concat([Key, ":", escape_value(Val)])
                        end, Value), " OR "), ")"])|Acc]);
build_search_query([{Key, 'contains_none', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", "(", string:join(lists:map(fun(Val) ->
                                lists:concat([Key, ":", escape_value(Val)])
                        end, Value), " OR "), ")"])|Acc]).

quote_value(Value) ->
    quote_value(Value, []).

quote_value([], Acc) ->
    [$"|lists:reverse([$"|Acc])];
quote_value([$"|T], Acc) ->
    quote_value(T, lists:reverse([$\\, $"], Acc));
quote_value([H|T], Acc) ->
    quote_value(T, [H|Acc]).

escape_value(Value) ->
    escape_value(Value, []).

escape_value([], Acc) ->
    lists:reverse(Acc);
escape_value([H|T], Acc) when H=:=$+; H=:=$-; H=:=$&; H=:=$|; H=:=$!; H=:=$(; H=:=$);
                              H=:=$[; H=:=$]; H=:=${; H=:=$}; H=:=$^; H=:=$"; H=:=$~;
                              H=:=$*; H=:=$?; H=:=$:; H=:=$\\ ->
    escape_value(T, lists:reverse([$\\, H], Acc));
escape_value([H|T], Acc) ->
    escape_value(T, [H|Acc]).

encode_data_for_riak(Data) ->
    lists:map(fun({K, V}) ->
        BinaryKey = list_to_binary(atom_to_list(K)),
        ConvertedValue = try
                           list_to_binary(V)
                         catch
                           error:_ -> V
                         end,
        {BinaryKey, ConvertedValue}
    end, Data).

decode_data_from_riak(Data) ->
    lists:map(fun({K, V}) ->
        BinaryKey = binary_to_atom(K, utf8),
        ConvertedValue = try
                           binary_to_list(V)
                         catch
                           error:_ -> V
                         end,
        {BinaryKey, ConvertedValue}
    end, Data).

decode_search_from_riak(Data) ->
    lists:map(fun({K, V}) ->
        {K, decode_data_from_riak(V)}
    end, Data).
