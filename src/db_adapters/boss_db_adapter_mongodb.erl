-module(boss_db_adapter_mongodb).
-behaviour(boss_db_adapter).
%-include_lib("mongodb/include/mongo_protocol.hrl").
-export([start/1, stop/0, init/1, terminate/1, find/2, find/7]).
-export([count/3, counter/2, incr/2, incr/3, delete/2, save_record/2]).
-export([execute/2, transaction/2]).
-export([push/2, pop/2, dump/1]).
-export([table_exists/2, get_migrations_table/1, migration_done/3]).

-define(LOG(Name, Value), lager:debug("DEBUG: ~s: ~p~n", [Name, Value])).
-type maybe(X)   :: X|undefined.
-type error_m(X) :: X|{error, any()}.

% Number of seconds between beginning of gregorian calendar and 1970
-define(GREGORIAN_SECONDS_1970, 62167219200).
-compile(export_all).
-ifdef(TEST).
-compile(export_all).
-endif.
% JavaScript expression formats to query MongoDB
-define(CONTAINS_FORMAT, "this.~s.indexOf('~s') != -1").
-define(NOT_CONTAINS_FORMAT, "this.~s.indexOf('~s') == -1").
-type db_op()			:: 'not_equals'|'gt'|'ge'|'lt'|'le'|'in'|'not_in'.
-type mongo_op()		:: '$ne'|'$gt'|'$gte'|'$lt'|'$lte'|'$in'|'$nin'.
-type read_mode()               :: 'master'|'slave_ok'.
-type proplist(Key,Value)	:: [{Key, Value}].
-type proplist()		:: proplist(any(), any()).

-spec boss_to_mongo_op(db_op()) -> mongo_op().
-spec pack_sort_order('ascending' | 'descending') -> -1 | 1.
-spec tuple_to_proplist(tuple()) -> [{_,_}].% Tuple size must be even
-spec proplist_to_tuple([{any(),any()}]) -> tuple().
-spec dec2hex(binary()) -> bitstring().
-spec dec2hex(bitstring(),binary()) -> bitstring().
-spec hex2dec(binary() | [byte(),...]) -> bitstring().
-spec hex2dec(bitstring(),binary()) -> bitstring().
-spec dec0(byte()) -> integer().
-spec hex0(byte()) -> 1..1114111.


start(_Options) ->
    application:start(mongodb).

stop() ->
    ok.

init(Options) ->
    Database		= proplists:get_value(db_database, Options, "test"),
    WriteMode		= proplists:get_value(db_write_mode, Options, safe),
    ReadMode		= proplists:get_value(db_read_mode, Options, master),

    ReadConnection	= make_read_connection(Options, ReadMode),
    WriteConnection	= make_write_connection(Options, ReadConnection),
						% We pass around arguments required by mongo:do/5
    case {proplists:get_value(db_username, Options),proplists:get_value(db_password, Options)} of
	{undefined,undefined}  ->
	    {ok, {readwrite,
		  {WriteMode, ReadMode, ReadConnection, list_to_atom(Database)},
		  {WriteMode, ReadMode, WriteConnection, list_to_atom(Database)}}
	    };
	{User, Pass} ->
	    {ok, {readwrite,
		  {WriteMode, ReadMode, ReadConnection,  list_to_atom(Database),list_to_binary(User),list_to_binary(Pass)},
		  {WriteMode, ReadMode, WriteConnection, list_to_atom(Database),list_to_binary(User),list_to_binary(Pass)}}
	    }
    end.

make_write_connection(Options, ReadConnection) ->
    case proplists:get_value(db_write_host, Options) of
	undefined ->
	    ReadConnection;
	WHost ->
	    WPort       = proplists:get_value(db_write_host_port, Options, 27017),
	    {ok, WConn} = mongo:connect({WHost, WPort}),
	    WConn
    end.

-spec(make_write_connection(proplist(),read_mode()) -> error_m(mongo:connection())).
make_read_connection(Options, ReadMode) ->
    case proplists:get_value(db_replication_set, Options) of
	undefined ->
	    Host = proplists:get_value(db_host, Options, "localhost"),
	    Port = proplists:get_value(db_port, Options, 27017),
	    {ok, Conn} = mongo:connect({Host, Port}),
	    Conn;
	ReplSet ->
	    RSConn        = mongo:rs_connect(ReplSet),
	    case read_connect1(ReadMode, RSConn) of
		{ok, RSConn1} ->
		    RSConn1;
		Error = {error,_} ->
		    Error
	    end
    end.
-spec(read_connect1(read_mode(), mongo:rs_connection()) ->
	     error_m( mongo:connection())).

read_connect1(master, RSConn) ->
     mongo_replset:primary(RSConn);
read_connect1(slave_ok, RSConn) ->
     mongo_replset:secondary_ok(RSConn).

terminate({_, _, Connection, _}) ->
    case element(1, Connection) of
        connection    -> mongo:disconnect(Connection);
        rs_connection -> mongo:rs_disconnect(Connection)
    end;

terminate({_, _, Connection, _,_,_}) ->
    case element(1, Connection) of
        connection -> mongo:disconnect(Connection);
        rs_connection -> mongo:rs_disconnect(Connection)
    end.


execute({WriteMode, ReadMode, Connection, Database}, Fun) ->
    mongo:do(WriteMode, ReadMode, Connection, Database, Fun) ;
execute({WriteMode, ReadMode, Connection, Database, User, Password}, Fun) ->
    mongo:do(WriteMode, ReadMode, Connection, Database,
	     fun() ->
		     case mongo:auth(User,Password) of
			 true ->
			     Fun();
			 _ ->
			     _ = lager:error("Mongo DB Login Error check username and password ~p:~p", [User,Password]),
			     {error,bad_login}
		     end
	     end).

% Transactions are not currently supported, but we'll treat them as if they are.
% Use at your own risk!
transaction(_Conn, TransactionFun) ->
    {atomic, TransactionFun()}.

find(Conn, Id) when is_list(Id) ->
    {Type, Collection, MongoId} = infer_type_from_id(Id),

    Res = execute(Conn, fun() ->
				mongo:find_one(Collection, {'_id', MongoId})
			end),
    case Res of
        {ok, {}}			-> undefined;
        {ok, {Doc}}			-> mongo_tuple_to_record(Type, Doc);
        {failure, Reason}		-> {error, Reason}

    end.

find(Conn, Type, Conditions, Max, Skip, Sort, SortOrder) when is_atom(Type),
							      is_list(Conditions),
                                                              is_integer(Max) orelse Max =:= all,
							      is_integer(Skip),
                                                              is_atom(Sort),
							      is_atom(SortOrder) ->
    case boss_record_lib:ensure_loaded(Type) of
        true ->
            Collection = type_to_collection(Type),
            Res = execute_find(Conn, Conditions, Max, Skip, Sort, SortOrder,
			       Collection),
            case Res of
                {ok, Curs} ->
                    lists:map(fun(Row) ->
				      mongo_tuple_to_record(Type, Row)
			      end, mongo:rest(Curs));
                {failure, Reason} -> {error, Reason}

            end;
        false -> {error, {module_not_loaded, Type}}
    end.

execute_find(Conn, Conditions, Max, Skip, Sort, SortOrder,
	     Collection) ->
    execute(Conn, fun() ->
			  Selector = build_conditions(Conditions, {Sort, pack_sort_order(SortOrder)}),
			  case Max of
			      all -> mongo:find(Collection, Selector, [], Skip);
			      _ -> mongo:find(Collection, Selector, [], Skip, Max)
                          end
                  end).


count(Conn, Type, Conditions) ->
    Collection = type_to_collection(Type),
    {ok, Count} = execute(Conn, fun() ->
					C = build_conditions(Conditions),
						%                ?LOG("Conditions", C),
					mongo:count(Collection, C)
				end),
    Count.

counter(Conn, Id) when is_list(Id) ->
    Res = execute(Conn, fun() ->
                mongo:find_one(boss_counters, {'name', list_to_binary(Id)})
        end),
    case Res of
        {ok, {Doc}} ->
            PropList = tuple_to_proplist(Doc),
            proplists:get_value(value, PropList);
        {failure, Reason} -> {error, Reason}

    end.

incr(Conn, Id) ->
    incr(Conn, Id, 1).

incr(Conn, Id, Count) ->
    Res = execute(Conn, fun() ->
                 mongo:repsert(boss_counters,
                         {'name', list_to_binary(Id)},
                         {'$inc', {value, Count}}
                         )
        end),
    case Res of
        {ok, ok}			-> counter(Conn, Id);
        {failure, Reason}		-> {error, Reason}

    end.

delete(Conn, Id) when is_list(Id) ->
    {_Type, Collection, MongoId} = infer_type_from_id(Id),

    Res = execute(Conn, fun() ->
                mongo:delete(Collection, {'_id', MongoId})
        end),
    resolve(Res).


save_record(Conn, Record) when is_tuple(Record) ->
    Type	= element(1, Record),
    Collection	= type_to_collection(Type),
    Res		= case Record:id() of
		      id ->
			  execute_save_record(Conn, Record, Collection);
		      DefinedId when is_list(DefinedId) ->
			  execute_save_record(Conn, Record, Collection, DefinedId)
		  end,
    case Res of
        {ok, ok}			-> {ok, Record};
        {ok, Id}			-> {ok, Record:set(id, unpack_id(Type, Id))};
        {failure, Reason}		-> {error, Reason}

    end.

execute_save_record(Conn, Record, Collection, DefinedId) ->
    PackedId = pack_id(DefinedId),
    PropList = lists:map(fun
			     ({id,_}) -> {'_id', PackedId};
			     ({K,V}) ->
				 PackedVal = pack_value(K, V),
				 {K, PackedVal}
                         end, Record:attributes()),
    Doc = proplist_to_tuple(PropList),
    execute(Conn, fun() ->
			  mongo:repsert(Collection, {'_id', PackedId}, Doc)
                  end).

pack_value(K, V) ->
    case is_id_attr(K) of
	true  -> pack_id(V);
	false -> pack_value(V)
    end.

execute_save_record(Conn, Record, Collection) ->
    PropList = lists:foldr(fun
			       ({id,_}, Acc) -> Acc;
			       ({K,V}, Acc) ->
				   PackedVal = pack_value(K, V),
				   [{K, PackedVal}|Acc]
                           end, [], Record:attributes()),
    Doc = proplist_to_tuple(PropList),
    execute(Conn, fun() ->
			  mongo:insert(Collection, Doc)
                  end).

% These 3 functions are not part of the behaviour but are required for
% tests to pass
push(_Conn, _Depth) -> ok.
pop(_Conn,  _Depth) -> ok.
dump(_Conn) -> ok.

% This is needed to support boss_db:migrate
table_exists(_Conn, _TableName) -> ok.

resolve(_Res ={ok, _}) ->
    ok;
resolve(_Res = {failure, Reason}) ->
    {error, Reason};
resolve(_Res = {connection_failure, Reason}) ->
    _ = lager:error("connection failure ~p", [Reason]),
    {error, Reason}.



get_migrations_table(Conn) ->
    Res = execute(Conn, fun() ->
				mongo:find(schema_migrations, {})
			end),
    resolve(Res).

make_curser(Curs) ->
    lists:map(fun(Row) ->
		      MongoDoc = tuple_to_proplist(Row),
		      {attr_value('id', MongoDoc),
		       attr_value(version, MongoDoc),
		       attr_value(migrated_at, MongoDoc)}
              end, mongo:rest(Curs)).

migration_done(Conn, Tag, up) ->
    Res = execute(Conn, fun() ->
                Doc = {version, pack_value(atom_to_list(Tag)), migrated_at, pack_value(os:timestamp())},
                mongo:insert(schema_migrations, Doc)
        end),
    resolve(Res);

migration_done(Conn, Tag, down) ->
    Res = execute(Conn, fun() ->
                mongo:delete(schema_migrations, {version, pack_value(atom_to_list(Tag))})
        end),
    resolve(Res).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%
%% Query generation
%%

build_conditions(Conditions) ->
    proplist_to_tuple(build_conditions1(Conditions, [])).

build_conditions(Conditions, OrderBy) ->
    {'query', build_conditions(Conditions), orderby, OrderBy}.

build_conditions1([], Acc) ->
    Acc;

build_conditions1([{Key, 'matches', Value, Options}|Rest], Acc) ->
    MongoOptions = mongo_regex_options_for_re_module_options(Options),
    build_conditions1(Rest, [{Key, {regex, list_to_binary(Value), list_to_binary(MongoOptions)}}|Acc]);

build_conditions1([{Key, 'not_matches', Value, Options}|Rest], Acc) ->
    MongoOptions = mongo_regex_options_for_re_module_options(Options),
    build_conditions1(Rest, [{Key, {'$not', {regex, list_to_binary(Value), list_to_binary(MongoOptions)}}}|Acc]);

build_conditions1([{id, Operator, Value}|Rest], Acc) ->
    build_conditions1([{'_id', Operator, Value}|Rest], Acc);

build_conditions1([{Key, Operator, Value}|Rest], Acc) ->
    Condition = build_conditions2(Key, Operator, Value),
    %    ?LOG("Condition", Condition),
    build_conditions1(Rest, lists:append(Condition, Acc)).


-spec(build_conditions2(string(),boss_db_adapter_types:model_operator(),string()) ->
	     [
	      {atom(), binary()}         |
	      {string(), {regex, binary(),binary()}} |
	      {string(), {'$not', {regex, binary(), binary()}}}
	      ]).
build_conditions2(Key, Operator, Value) ->
    case {Operator, Value} of
	{'not_matches', "*" ++ Value1} ->
	    [{Key, {'$not', {regex, list_to_binary(Value1), <<"i">>}}}];
	{'not_matches', Value} ->
	    [{Key, {'$not', {regex, list_to_binary(Value), <<"">>}}}];
	{'matches', "*" ++ Value1} ->
	    [{Key, {regex, list_to_binary(Value1), <<"i">>}}];
	{'matches', Value} ->
	    [{Key, {regex, list_to_binary(Value), <<"">>}}];
	{'contains', Value} ->
	    WhereClause = where_clause(
			    ?CONTAINS_FORMAT, [Key, Value]),
	    [{'$where', WhereClause}];
	{'not_contains', Value} ->
	    WhereClause = where_clause(
			    ?NOT_CONTAINS_FORMAT, [Key, Value]),
	    [{'$where', WhereClause}];
	{'contains_all', ValueList} ->
	    WhereClause = multiple_where_clauses(
			    ?CONTAINS_FORMAT, Key, ValueList, "&&"),
	    [{'$where', WhereClause}];
	{'not_contains_all', ValueList} ->
	    WhereClause = "!(" ++ multiple_where_clauses_string(
				    ?CONTAINS_FORMAT, Key, ValueList, "&&") ++ ")",
	    [{'$where', erlang:iolist_to_binary(WhereClause)}];
	{'contains_any', ValueList} ->
	    WhereClause = multiple_where_clauses(
			    ?CONTAINS_FORMAT, Key, ValueList, "||"),
	    [{'$where', WhereClause}];
	{'contains_none', ValueList} ->
	    WhereClause = multiple_where_clauses(
			    ?NOT_CONTAINS_FORMAT, Key, ValueList, "&&"),
	    [{'$where', WhereClause}];
	{'equals', Value} when is_list(Value) ->
	    case is_id_attr(Key) of
		true ->
		    [{Key, pack_id(Value)}];
		false ->
		    [{Key, list_to_binary(Value)}]
	    end;
	{'not_equals', Value} when is_list(Value) ->
	    [{Key, {'$ne', list_to_binary(Value)}}];
	{'equals', {{_,_,_},{_,_,_}} = Value} ->
	    [{Key, datetime_to_now(Value)}];
	{'equals', Value} ->
	    [{Key, Value}];
	{Operator, {{_,_,_},{_,_,_}} = Value} ->
	    [{Key, {boss_to_mongo_op(Operator), datetime_to_now(Value)}}];
	{'in', [H|T]} ->
	    [{Key, {'$in', lists:map(list_pack_function(Key), [H|T])}}];
	{'in', {Min, Max}} ->
	    [{Key, {'$gte', Min}}, {Key, {'$lte', Max}}];
	{'not_in', [H|T]} ->
	    [{Key, {'$nin', lists:map(list_pack_function(Key), [H|T])}}];
	{'not_in', {Min, Max}} ->
	    [{'$or', [{Key, {'$lt', Min}}, {Key, {'$gt', Max}}]}];
	{Operator, Value} ->
	    [{Key, {boss_to_mongo_op(Operator), Value}}]
    end.

list_pack_function(Key) ->
    case is_id_attr(Key) of
        true ->
            fun(Id) -> pack_id(Id) end;
        false ->
            fun(Value) when is_list(Value) ->
                    list_to_binary(Value);
               (Value) ->
                    Value
            end
    end.


where_clause(Format, Params) ->
    erlang:iolist_to_binary(
                io_lib:format(Format, Params)).

multiple_where_clauses_string(Format, Key, ValueList, Operator) ->
    ClauseList = lists:map(fun(Value) ->
				   lists:flatten(io_lib:format(Format, [Key, Value]))
			   end, ValueList),
    string:join(ClauseList, " " ++ Operator ++ " ").

multiple_where_clauses(Format, Key, ValueList, Operator) ->
    erlang:iolist_to_binary(multiple_where_clauses_string(Format, Key,
            ValueList, Operator)).


%%
%% Boss models introspection
%%

infer_type_from_id(Id) when is_list(Id) ->
    [Type, _BossId] = string:tokens(Id, "-"),
    {list_to_atom(Type), type_to_collection(Type), pack_id(Id)}.

is_id_attr(AttrName) ->
    lists:suffix("_id", atom_to_list(AttrName)).


%%
%% Conversion between Chicago Boss en MongoDB
%%

% Find MongoDB collection from Boss type
type_to_collection(Type) ->
    list_to_atom(type_to_collection_name(Type)).

type_to_collection_name(Type) when is_atom(Type) ->
    type_to_collection_name(atom_to_list(Type));
type_to_collection_name(Type) when is_list(Type) ->
    inflector:pluralize(Type).

% Convert a tuple return by the MongoDB driver to a Boss record
mongo_tuple_to_record(Type, Row) ->
    MongoDoc		= tuple_to_proplist(Row),
    AttributeTypes	= boss_record_lib:attribute_types(Type),
    AttributeNames	= boss_record_lib:attribute_names(Type),
    Args                = mongo_make_args(Type, MongoDoc, AttributeTypes,
					  AttributeNames),
    apply(Type, new, Args).


mongo_make_args(Type, MongoDoc, AttributeTypes, AttributeNames) ->
    lists:map(fun
		  (id) ->
		      MongoValue = attr_value(id, MongoDoc),
		      unpack_id(Type, MongoValue);
		  (AttrName) ->
		      MongoValue = attr_value(AttrName, MongoDoc),
		      ValueType = proplists:get_value(AttrName, AttributeTypes),
		      unpack_value(AttrName, MongoValue, ValueType)
	      end,
              AttributeNames).

mongo_regex_options_for_re_module_options(Options) ->
    mongo_regex_options_for_re_module_options(Options, []).

mongo_regex_options_for_re_module_options([], Acc) ->
    lists:reverse(Acc);
mongo_regex_options_for_re_module_options([caseless|Rest], Acc) ->
    mongo_regex_options_for_re_module_options(Rest, [$i|Acc]);
mongo_regex_options_for_re_module_options([dotall|Rest], Acc) ->
    mongo_regex_options_for_re_module_options(Rest, [$s|Acc]);
mongo_regex_options_for_re_module_options([extended|Rest], Acc) ->
    mongo_regex_options_for_re_module_options(Rest, [$x|Acc]);
mongo_regex_options_for_re_module_options([multiline|Rest], Acc) ->
    mongo_regex_options_for_re_module_options(Rest, [$m|Acc]).

% Boss and MongoDB have a different conventions to id attributes (id vs. '_id').
-spec attr_value(atom()|string(), proplist(atom()|string(),string())) -> maybe(string()).

attr_value(id, MongoDoc) ->
    proplists:get_value('_id', MongoDoc);
attr_value(AttrName, MongoDoc) ->
    proplists:get_value(AttrName, MongoDoc).

% Id conversions
pack_id(BossId) ->
    try
        [_, MongoId] = string:tokens(BossId, "-"),
        {hex2dec(MongoId)}
    catch
        Error:Reason ->
            error_logger:warning_msg("Error parsing Boss record id: ~p:~p~n",
                [Error, Reason]),
            []
    end.

-spec unpack_id(atom() | maybe_improper_list(),'undefined' | tuple()) -> 'undefined' | string().
unpack_id(_Type, undefined) ->
    undefined;
unpack_id(Type, MongoId) ->
    lists:concat([Type, "-", binary_to_list(dec2hex(element(1, MongoId)))]).


% Value conversions

pack_value({{_, _, _}, {_, _, _}} = Val) ->
    datetime_to_now(Val);
pack_value([]) -> <<"">>;
pack_value(V) when is_binary(V) -> pack_value(binary_to_list(V));
pack_value([H|T]) when is_integer(H) -> list_to_binary([H|T]);
pack_value({integers, List}) -> List;
pack_value(V) -> V.

unpack_value(_AttrName, [H|T], _ValueType) when is_integer(H) ->
    {integers, [H|T]};
unpack_value(_AttrName, {_, _, _} = Value, datetime) ->
    calendar:now_to_datetime(Value);
unpack_value(AttrName, Value, ValueType) ->
    case is_id_attr(AttrName) and (Value =/= "") of
        true ->
            IdType = id_type_from_foreign_key(AttrName),
            unpack_id(IdType, Value);
        false ->
            boss_record_lib:convert_value_to_type(Value, ValueType)
    end.

id_type_from_foreign_key(ForeignKey) ->
    Tokens = string:tokens(atom_to_list(ForeignKey), "_"),
    NameTokens = lists:filter(fun(Token) -> Token =/= "id" end,
        Tokens),
    string:join(NameTokens, "_").


% Operators

boss_to_mongo_op('not_equals')	-> '$ne';
boss_to_mongo_op('gt')		-> '$gt';
boss_to_mongo_op('ge')		-> '$gte';
boss_to_mongo_op('lt')		-> '$lt';
boss_to_mongo_op('le')		-> '$lte';
boss_to_mongo_op('in')		-> '$in';
boss_to_mongo_op('not_in')	-> '$nin'.


% Sort clauses

pack_sort_order(ascending)	-> 1;
pack_sort_order(descending)	-> -1.


%%
%% Generic data structure conversions
%%

% The mongodb driver uses "associative tuples" which look like:
%      {key1, Value1, key2, Value2}
tuple_to_proplist(Tuple) ->
    List = tuple_to_list(Tuple),
    Ret = lists:reverse(list_to_proplist(List, [])),
    Ret.

proplist_to_tuple(PropList) ->
    ListOfLists = lists:reverse([[K,V]||{K,V} <- PropList]),
    list_to_tuple(lists:foldl(
            fun([K, V], Acc) ->
                    [K,V|Acc]
            end, [], ListOfLists)).

list_to_proplist([], Acc) -> Acc;
list_to_proplist([K,V|T], Acc) ->
    list_to_proplist(T, [{K, V}|Acc]).

datetime_to_now(DateTime) ->
    GSeconds = calendar:datetime_to_gregorian_seconds(DateTime),
    ESeconds = GSeconds - ?GREGORIAN_SECONDS_1970,
    {ESeconds div 1000000, ESeconds rem 1000000, 0}.


%%
%% Decimal to hexadecimal conversion
%%
%% Functions below copied from emongo <https://github.com/boorad/emongo>
%%
%% Copyright (c) 2009 Jacob Vorreuter <jacob.vorreuter@gmail.com>
%% Jacob Perkins <japerk@gmail.com>
%% Belyaev Dmitry <rumata-estor@nm.ru>
%% François de Metz <fdemetz@af83.com>
%%

dec2hex(Dec) ->
    dec2hex(<<>>, Dec).

dec2hex(N, <<I:8,Rem/binary>>) ->
    dec2hex(<<N/binary, (hex0((I band 16#f0) bsr 4)):8, (hex0((I band 16#0f))):8>>, Rem);
dec2hex(N,<<>>) ->
    N.

hex2dec(Hex) when is_list(Hex) ->
    hex2dec(list_to_binary(Hex));

hex2dec(Hex) ->
    hex2dec(<<>>, Hex).

hex2dec(N,<<A:8,B:8,Rem/binary>>) ->
    hex2dec(<<N/binary, ((dec0(A) bsl 4) + dec0(B)):8>>, Rem);
hex2dec(N,<<>>) ->
    N.

dec0($a) -> 10;
dec0($b) -> 11;
dec0($c) -> 12;
dec0($d) -> 13;
dec0($e) -> 14;
dec0($f) -> 15;
dec0(X) -> X - $0.

hex0(10) -> $a;
hex0(11) -> $b;
hex0(12) -> $c;
hex0(13) -> $d;
hex0(14) -> $e;
hex0(15) -> $f;
hex0(I) ->  $0 + I.
