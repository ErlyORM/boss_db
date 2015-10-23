-module(boss_db_adapter_dynamodb).
-behaviour(boss_db_adapter).
-export([start/1, stop/0, init/1, terminate/1, find/2, find/7]).
-export([count/3, counter/2, incr/2, incr/3, delete/2, save_record/2]).
-export([push/2, pop/2]).

-ifdef(TEST).
-compile(export_all).
-endif.

-define(LOG(Name, Value), lager:debug("DEBUG: ~s: ~p~n", [Name, Value])).

% Number of seconds between beginning of gregorian calendar and 1970
-define(GREGORIAN_SECONDS_1970, 62167219200).

start(_Options) ->
    application:start(ddb).

stop() ->
    ok.

init(Options) ->
    AccessKey 	= proplists:get_value(db_username, Options, os:getenv("AWS_ACCESS_KEY_ID")),
    SecretKey	= proplists:get_value(db_password, Options, os:getenv("AWS_SECRET_ACCESS_KEY")),
    Endpoint    = proplists:get_value(db_host, Options, "dynamodb.us-east-1.amazonaws.com"),

    %% startup dependencies.  some of these may have already been started, but that's ok.
    inets:start(),
    ssl:start(),
    %%lager:start(),
    application:start(ibrowse),


    %% init initial credentials.  note that these will be refeshed automatically as needed
    ddb_iam:credentials(AccessKey, SecretKey),
    {'ok', Key, Secret, Token} = ddb_iam:token(129600),
    %% 129600 is the lifetime duration for the token
    ddb:credentials(Key, Secret, Token, Endpoint),

    init_tables(Options),
    {ok, undefined}.

terminate(_Conn) ->
    ok.

find(_Conn, Id) when is_list(Id) ->
	[Type | _BossId] = string:tokens(Id, "-"),
	case ddb:get(list_to_binary(Type), ddb:key_value(list_to_binary(Id), 'string')) of
		{ok, Rec} ->
			create_from_ddbitem(Type, proplists:get_value(<<"Item">>, Rec));
		{error, Error} ->
			{error, Error}
	end.

find(_Conn, Type, Conditions, Max, Skip, Sort, SortOrder)
  when is_atom(Type), is_list(Conditions),
       is_integer(Max) orelse Max =:= all, is_integer(Skip),
       is_atom(Sort), is_atom(SortOrder) ->

	case boss_record_lib:ensure_loaded(Type) of
		true ->
			DDB_cond	= convert_conditions(Conditions),
			Items		= do_scan_loop(atom_to_binary(Type, latin1), DDB_cond, 'none'),
			Records		= create_from_ddbitem_list(Type, Items),

			case SortOrder of
				ascending -> Sorted = lists:sort(fun(A,B) -> A:Sort() =< B:Sort() end, Records);
				descending -> Sorted = lists:sort(fun(A,B) -> A:Sort() > B:Sort() end, Records);
				_ -> Sorted = Records
			end,

			Sorted;
		false ->
			{error, {module_not_loaded, Type}}
	end.


count(_Conn, _Type, _Conditions) ->
    1.

counter(_Conn, Id) when is_list(Id) ->
    1.

incr(Conn, Id) ->
    incr(Conn, Id, 1).

incr(Conn, Id, _Count) ->
    counter(Conn, Id).

delete(_Conn, Id) when is_list(Id) ->
    ok.


save_record(_Conn, Record) when is_tuple(Record) ->
	case Record:id() of
		id ->
			create_new(Record);
		_Existing ->
			update_existing(Record)
	end.


% These 2 functions are not part of the behaviour but are required for
% tests to pass
push(_Conn, _Depth) -> ok.

pop(_Conn, _Depth) -> ok.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%
%% setup the DynamoDB tables as needed
%%
-spec(init_tables([{_,_}]) ->
	     ok).
init_tables(Options) ->
    {ok, Tables}	= ddb:tables(),
    Models		= lists:concat( [ boss_files:model_list(ModelName) || {ModelName, _, _} <- application:which_applications() ] ),
    BinModels		= [ erlang:list_to_binary(X) || X <- Models],
    Create		= BinModels -- Tables,
    [init_table(Model, Options) || Model <- Create],
    ok.


init_table(Model, Options) when is_binary(Model) ->
	%% it looks like we don't need to initialize the table with the field names for now
    ModelAtom		= erlang:binary_to_atom(Model, 'utf8'),
    Dummy		= boss_record_lib:dummy_record(ModelAtom),
    ModelAttributes	= ModelAtom:module_info(attributes),

    GlobalReadCap	= proplists:get_value(db_read_capacity,		 Options, 2),
    GlobalWriteCap	= proplists:get_value(db_write_capacity,	 Options, 1),

    LocalReadCaps	= proplists:get_value(db_model_read_capacity,	 Options, []),
    LocalWriteCaps	= proplists:get_value(db_model_write_capacity,	 Options, []),

    ReadCap		= proplists:get_value(ModelAtom, LocalReadCaps,  GlobalReadCap),
    WriteCap		= proplists:get_value(ModelAtom, LocalWriteCaps, GlobalWriteCap),

    [PrimaryKey]	= proplists:get_value(primary_key, ModelAttributes, [id]),
    PrimaryKeyType	= proplists:get_value(PrimaryKey, Dummy:attribute_types(), 'string'),

    KeyType		= proplists:get_value(range_key, ModelAtom:module_info(attributes)),


    Keys		= lookup_key_type(Dummy, PrimaryKey, PrimaryKeyType, KeyType),


    ddb:create_table(Model, Keys, ReadCap, WriteCap),
    init_meta_entry(Model).


lookup_key_type(_Dummy, PrimaryKey, PrimaryKeyType, undefined) ->
    ddb:key_type(atom_to_binary(PrimaryKey, latin1), PrimaryKeyType);
lookup_key_type(Dummy, PrimaryKey, PrimaryKeyType, [RangeKey]) ->
    RangeKeyType = convert_type_to_ddb(
		     proplists:get_value(RangeKey, Dummy:attribute_types())),
    ddb:key_type(atom_to_binary(PrimaryKey, latin1),
		 PrimaryKeyType,
		 atom_to_binary(RangeKey, latin1),
		 RangeKeyType).


%% Help function
-spec(convert_type_to_ddb('string'|'float'|'integer'|'datetime'|'timestamp'|'undefined') ->
	     'string'|'number').
convert_type_to_ddb(float) ->
    'number';
convert_type_to_ddb(integer) ->
    'number';
convert_type_to_ddb(datetime) ->
    'number';
convert_type_to_ddb(timestamp) ->
    'number';
convert_type_to_ddb(_) ->
    'string'.

init_meta_entry(Model) when is_binary(Model)->
    ModelAtom	= binary_to_atom(Model, latin1),
    Attributes	= ModelAtom:module_info(attributes),
    case proplists:get_value(primary_key, Attributes) of
        undefined ->
            ddb:put(Model, [{<<"id">>, <<"__metadata">>, 'string'},
                            {<<"__counter">>, <<"0">>, 'number'}]);
        [Field] ->
            FieldBin = atom_to_binary(Field, latin1),
            ddb:put(Model, [{FieldBin, <<"__metadata">>, 'string'},
                            {<<"__counter">>, <<"0">>, 'number'}])
    end.

%%
%% Helper functions for creating records

create_new(Record) ->
	Model = element(1, Record),
	SavedRec = Record:set(id, get_new_unique_id(Model)),
	Attrs = SavedRec:attributes(),
	%% WEIRD: dynamodb does not allow empty strings as values, don't store them.
	Fields = [ {atom_to_binary(K,latin1), val_to_binary(V), type_of(V)} || {K, V} <- Attrs, not_empty(V) ],
	ddb:put(atom_to_binary(Model, latin1), Fields),
    {ok, SavedRec}.

update_existing(Record) ->
	Model  = element(1, Record),
	Keys   = ddb:key_value(list_to_binary(Record:id()), 'string'),
	Attrs  = Record:attributes(),
	Fields = [ {atom_to_binary(K, latin1), val_to_binary(V), type_of(V), 'put'} ||
		     {K, V} <- Attrs, not_empty(V), K /= id],
	_Res    = ddb:update(atom_to_binary(Model, latin1), Keys, Fields),
	{ok, Record}.

-spec(not_empty(_) ->boolean()).
not_empty([]) ->
	false;
not_empty({}) ->
	false;
not_empty(_Val) ->
	true.

%val_to_binary({{Y,M, D}, {H, M, S}} = Datetime) when is_integer(Y), is_integer(M), is_integer(D), is_integer(H), is_integer(M), is_integer(S) ->
%	StringDate = httpd_util:rfc1123_date(Datetime),
%	list_to_binary(StringDate);
-type printable() :: boolean()|atom()|number()|binary().
-spec(val_to_binary(printable()|[printable()]) -> binary()).
val_to_binary(Val) when is_boolean(Val) ->
    case Val of
        true ->
            <<"__true">>;
        false ->
            <<"__false">>
    end;
val_to_binary(Val) when is_atom(Val) ->
	atom_to_binary(Val, latin1);
val_to_binary(Val) when is_list(Val) ->
	case io_lib:printable_list(Val) of
		true ->
			list_to_binary(Val);
		false ->
			_List = [ val_to_binary(X) || X <- Val]
	end;
val_to_binary(Val) when is_number(Val) ->
	list_to_binary(io_lib:format("~w", [Val]));
val_to_binary(Val) when is_binary(Val) ->
	Val;
val_to_binary(Val) ->
	%% for everything else let Erlang convert it
	list_to_binary(lists:flatten(io_lib:format("~p", [Val]))).

type_of(Val) when is_atom(Val) ->
	'string';
type_of(Val) when is_list(Val) ->
	case io_lib:printable_list(Val) of
		true ->
			'string';
		false ->
			[ type_of(hd(Val)) ]
	end;
type_of(Val) when is_number(Val) ->
	'number';
type_of(Val) when is_binary(Val) ->
	'binary'.

create_from_ddbitem(_Model, undefined) ->
	{error, not_found};
create_from_ddbitem(Model, Item) when is_list(Model) ->
	create_from_ddbitem(list_to_atom(Model), Item);
create_from_ddbitem(Model, Item) when is_atom(Model) ->
	Attrs = [ {binary_to_atom(K, latin1), convert_val(V, T)} || {K, [{T, V}]} <- Item],
	boss_record:new(Model, Attrs).

create_from_ddbitem_list(_Model, []) ->
	[];
create_from_ddbitem_list(Model, [Item | Tail]) ->
	%% skip any metadata entries
    PrimaryKey =
        case proplists:get_value(primary_key, Model:module_info(attributes)) of
            undefined ->
                <<"id">>;
            [Key] ->
                atom_to_binary(Key, latin1)
        end,

	case proplists:get_value(PrimaryKey, Item) of
		[{<<"S">>, <<"__metadata">>}] ->
			create_from_ddbitem_list(Model, Tail);
		_X ->
			[create_from_ddbitem(Model, Item) | create_from_ddbitem_list(Model, Tail) ]
	end.

convert_val(Val, <<"SS">>) when is_list(Val) ->
	[ convert_val(V, <<"S">>) || V <- Val ];
convert_val(Val, <<"NS">>) when is_list(Val) ->
	[ convert_val(V, <<"N">>) || V <- Val ];
convert_val(Val, _) when Val == <<"__true">> ->
    true;
convert_val(Val, _) when Val == <<"__false">> ->
    false;
convert_val(Val, <<"SS">>) ->
	List = binary_to_list(Val),
	list_to_term(List);
convert_val(Val, <<"NS">>) ->
	List = binary_to_list(Val),
	list_to_term(List);
convert_val(Val, <<"S">>) ->
	List = binary_to_list(Val),
	%% if it looks like it might be a stored erlang term as a string, then try to evaluate it to a term
	case string:left(List, 1) of
		"[" -> list_to_term(List);
		"{"-> list_to_term(List);
		_ -> List
	end;
convert_val(Val, <<"N">>) ->
	String = binary_to_list(Val),
	list_to_term(String).

list_to_term(String) ->
    {ok, T, _} = erl_scan:string(String++"."),
    case erl_parse:parse_term(T) of
        {ok, Term} ->
            Term;
        {error, _Error} ->
            String
    end.

get_new_unique_id(Model) when is_atom(Model) ->
	get_new_unique_id(erlang:atom_to_binary(Model, latin1));
get_new_unique_id(Model) when is_binary(Model) ->
	{ok, Ret} = ddb:get(Model, ddb:key_value(<<"__metadata">>, 'string')),
	case proplists:get_value(<<"Item">>, Ret) of
		undefined ->
			init_meta_entry(Model),
			get_new_unique_id(Model);
		Attrs ->
			[{<<"N">>, CounterBin}] = proplists:get_value(<<"__counter">>, Attrs),
			Counter = list_to_term(binary_to_list(CounterBin)),
			Id = Counter + 1,
			ddb:update(Model, ddb:key_value(<<"__metadata">>, 'string'), [{<<"__counter">>, list_to_binary(io_lib:format("~p", [Id])), 'number', 'put'}]),
			lists:flatten(io_lib:format("~p-~p", [binary_to_atom(Model, latin1), Id]))
	end.

%%
%% Query generation
%%

do_scan_loop(Model, Cond, StartKey) ->
	{ok, Results} 	= ddb:scan(Model, Cond, StartKey),
	Items 			= proplists:get_value(<<"Items">>, Results),
	case proplists:get_value(<<"LastEvaluatedKey">>, Results) of
		undefined ->
			Items;
		LastKey ->
			Items ++ do_scan_loop(Model, Cond, LastKey)
	end.

convert_conditions([], Acc) ->
	Acc;
convert_conditions([H | T], Acc) ->
	Update = lists:append(condition(H), Acc),
	convert_conditions(T, Update).

convert_conditions([]) ->
	"";
convert_conditions(Conditions) ->
	CondList = convert_conditions(Conditions, []),
	[{<<"ScanFilter">>, CondList}].

%%unfortuanately the 'IN' operator has a different syntax (sigh), treat special
condition({Key, 'in', Value}) ->
	AVL = [ [{ddb_type_of(V), val_to_binary(V)}] || V <- Value],
	[{atom_to_binary(Key, latin1), [{<<"AttributeValueList">>, AVL}, {<<"ComparisonOperator">>, <<"IN">>}]}];
condition({Key, Operator, Value}) ->
	[{atom_to_binary(Key, latin1), [{<<"AttributeValueList">>, [[{ddb_type_of(Value), val_to_binary(Value)}]]}, {<<"ComparisonOperator">>, operator_to_ddb(Operator)}]}].

ddb_type_of(Value) when is_list(Value) ->
	%% handle the case when the list is really a list of numbers or a list of strings
	case io_lib:printable_list(Value) of
		true ->
			<<"S">>;
		false ->
			H = hd(Value),
			case is_number(H) of
				true ->
					<<"NS">>;
				false ->
					<<"SS">>
			end
	end;
ddb_type_of(Value) when is_number(Value) ->
	<<"N">>;
ddb_type_of(_Value) ->
	<<"S">>.

-spec(operator_to_ddb(boss_db_adapter_types:model_operator()) ->
	     binary()).
operator_to_ddb('equals') ->
	<<"EQ">>;
operator_to_ddb('not_equals') ->
	<<"NE">>;
operator_to_ddb('contains') ->
	<<"CONTAINS">>;
operator_to_ddb('contains_all') ->
	exit(not_yet_implemented),
	<<"CONTAINS">>;
operator_to_ddb('not_contains_all') ->
	exit(not_yet_implemented),
	<<"NOT_CONTAINS">>;
operator_to_ddb('contains_any') ->
	exit(not_yet_implemented),
	<<"CONTAINS">>;
operator_to_ddb('contains_none') ->
	exit(not_yet_implemented),
	<<"NOT_CONTAINS">>;
operator_to_ddb('gt') ->
	<<"GT">>;
operator_to_ddb('ge') ->
	<<"GE">>;
operator_to_ddb('lt') ->
	<<"LT">>;
operator_to_ddb('le') ->
	<<"LE">>;
operator_to_ddb('in') ->
	<<"IN">>;
operator_to_ddb('not_in') ->
	<<"NOT_IN">>;
operator_to_ddb('matches') ->
	exit(not_yet_implemented),
	<<"EQ">>;
operator_to_ddb('not_matches') ->
	exit(not_yet_implemented),
	<<"NE">>.
