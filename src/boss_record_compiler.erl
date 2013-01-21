-module(boss_record_compiler).
-author('emmiller@gmail.com').
-define(DATABASE_MODULE, boss_db).
-define(PREFIX, "BOSSRECORDINTERNAL").

-export([compile/1, compile/2, edoc_module/1, edoc_module/2, process_tokens/1, trick_out_forms/2]).

%% @spec compile( File::string() ) -> {ok, Module} | {error, Reason}
%% @equiv compile(File, [])
compile(File) ->
    compile(File, []).

compile(File, Options) ->
    boss_compiler:compile(File, 
        [{pre_revert_transform, fun ?MODULE:trick_out_forms/2},
            {token_transform, fun ?MODULE:process_tokens/1}|Options]).

%% @spec edoc_module( File::string() ) -> {Module::atom(), EDoc}
%% @equiv edoc_module(File, [])
edoc_module(File) ->
    edoc_module(File, []).

%% @spec edoc_module( File::string(), Options ) -> {Module::atom(), EDoc}
%% @doc Return an `edoc_module()' for the given Erlang source file when
%% compiled as a BossRecord.
edoc_module(File, Options) ->
    {ok, Forms, TokenInfo} = boss_compiler:parse(File, fun ?MODULE:process_tokens/1, []),
    edoc_extract:source(trick_out_forms(Forms, TokenInfo), edoc:read_comments(File), 
        File, edoc_lib:get_doc_env([]), Options).

process_tokens(Tokens) ->
    process_tokens(Tokens, [], []).

process_tokens([{']',_},{')',_},{dot,_}|_]=Tokens, TokenAcc, Acc) ->
    {lists:reverse(TokenAcc, Tokens), Acc};
process_tokens([{'-',N}=T1,{atom,N,module}=T2,{'(',_}=T3,{atom,_,_ModuleName}=T4,{',',_}=T5,
        {'[',_}=T6,{var,_,'Id'}=T7|Rest], TokenAcc, []) ->
    process_tokens(Rest, lists:reverse([T1, T2, T3, T4, T5, T6, T7], TokenAcc), []);
process_tokens([{'-',_N}=T1,{atom,_,module}=T2,{'(',_}=T3,{atom,_,_ModuleName}=T4,{',',_}=T5,
                {'[',_}=T6,{var,_,'Id'}=T7,{'::',_},{atom,_,VarType},{'(',_},{')',_}|Rest], TokenAcc, []) ->    
    process_tokens(Rest, lists:reverse([T1, T2, T3, T4, T5, T6, T7], TokenAcc), [{'Id', VarType}]);
process_tokens([{',',_}=T1,{var,_,VarName}=T2,{'::',_},{atom,_,VarType},{'(',_},{')',_}|Rest], TokenAcc, Acc) ->
    process_tokens(Rest, lists:reverse([T1, T2], TokenAcc), [{VarName, VarType}|Acc]);
process_tokens([H|T], TokenAcc, Acc) ->
    process_tokens(T, [H|TokenAcc], Acc).

trick_out_forms(Forms, TokenInfo) ->
    trick_out_forms(Forms, [], TokenInfo).

trick_out_forms([
        {attribute, _Pos, module, {ModuleName, Parameters}} = H
        | Forms], LeadingForms, TokenInfo) ->
    trick_out_forms(lists:reverse([H|LeadingForms]), Forms, ModuleName, Parameters, TokenInfo);
trick_out_forms([H|T], LeadingForms, TokenInfo) ->
    trick_out_forms(T, [H|LeadingForms], TokenInfo).

trick_out_forms(LeadingForms, Forms, ModuleName, Parameters, TokenInfo) ->
    Attributes = proplists:get_value(attributes, erl_syntax_lib:analyze_forms(LeadingForms ++ Forms), []),
    [{eof, _Line}|ReversedOtherForms] = lists:reverse(Forms),
    UserForms = lists:reverse(ReversedOtherForms),
    Counters = lists:foldl(
        fun
            ({counter, Counter}, Acc) -> [Counter|Acc];
            (_, Acc) -> Acc
        end, [], Attributes),

    GeneratedForms = 
        attribute_names_forms(ModuleName, Parameters) ++
        attribute_types_forms(ModuleName, TokenInfo) ++
        database_columns_forms(ModuleName, Parameters, Attributes) ++
        database_table_forms(ModuleName, Attributes) ++
        validate_types_forms(ModuleName) ++
        validate_forms(ModuleName) ++
        save_forms(ModuleName) ++
        set_attributes_forms(ModuleName, Parameters) ++
        get_attributes_forms(ModuleName, Parameters) ++
        counter_getter_forms(Counters) ++
        counter_reset_forms(Counters) ++
        counter_incr_forms(Counters) ++
        association_forms(ModuleName, Attributes) ++
        parameter_getter_forms(Parameters),

    UserFunctionList = list_functions(UserForms),
    GeneratedFunctionList = list_functions(GeneratedForms),

    GeneratedExportForms = export_forms(GeneratedFunctionList),

    LeadingForms ++ GeneratedExportForms ++ UserForms ++ 
        override_functions(GeneratedForms, UserFunctionList).

list_functions(Forms) ->
    list_functions(Forms, []).

list_functions([], DefinedFunctions) ->
    lists:reverse(DefinedFunctions);
list_functions([{'function', _, Name, Arity, _}|Rest], DefinedFunctions) ->
    list_functions(Rest, [{Name, Arity}|DefinedFunctions]);
list_functions([{tree, 'function', _, {'function', {tree, 'atom', _, Name}, 
                [{tree, 'clause', _, {'clause', Args, _, _}}|_]}}|Rest], 
    DefinedFunctions) ->
    Arity = length(Args), 
    list_functions(Rest, [{Name, Arity}|DefinedFunctions]);
list_functions([_H|T], DefinedFunctions) ->
    list_functions(T, DefinedFunctions).

override_functions(Forms, DefinedFunctions) ->
    override_functions(Forms, [], DefinedFunctions).

override_functions([{'function', _, Name, Arity, _} = Function|Rest], Acc, DefinedFunctions) ->
    case lists:member({Name, Arity}, DefinedFunctions) of
        true -> override_functions(Rest, Acc, DefinedFunctions);
        false -> override_functions(Rest, [Function|Acc], [{Name, Arity}|DefinedFunctions])
    end;
override_functions([{tree, 'function', _, {'function', {tree, 'atom', _, Name}, 
                [{tree, 'clause', _, {'clause', Args, _, _}}|_]
            }} = Function|Rest], 
    Acc, DefinedFunctions) ->
    Arity = length(Args),
    case lists:member({Name, Arity}, DefinedFunctions) of
        true -> override_functions(Rest, Acc, DefinedFunctions);
        false -> override_functions(Rest, [Function|Acc], [{Name, Arity}|DefinedFunctions])
    end;
override_functions([H|T], Acc, DefinedFunctions) ->
    override_functions(T, [H|Acc], DefinedFunctions);
override_functions([], Acc, _) ->
    lists:reverse(Acc).

export_forms(FunctionList) ->
    export_forms(FunctionList, []).

export_forms([], Acc) ->
    lists:reverse(Acc);
export_forms([{Name, Arity}|Rest], Acc) ->
    export_forms(Rest, [erl_syntax:attribute(erl_syntax:atom(export), [erl_syntax:list([erl_syntax:arity_qualifier(erl_syntax:atom(Name), erl_syntax:integer(Arity))])])|Acc]).

database_columns_forms(ModuleName, Parameters, Attributes) ->
    DefinedColumns = proplists:get_value(Attributes, columns, []),
    Function = erl_syntax:function(
        erl_syntax:atom(database_columns),
        [erl_syntax:clause([], none, [erl_syntax:list(lists:map( fun(P) -> 
                                    LC = parameter_to_colname(P),
                                    AC = list_to_atom(LC),
                                    Column = proplists:get_value(AC, DefinedColumns, LC),
                                    erl_syntax:tuple([erl_syntax:atom(AC), erl_syntax:string(Column)])
                            end, Parameters))])]),

    [erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec database_columns() -> [{atom(), string()}]",
                        lists:concat(["% @doc A proplist of the database field names of each `", ModuleName, "' parameter."])])],
            Function)].

database_table_forms(ModuleName, Attributes) ->
    DefinedTableName = proplists:get_value(Attributes, table, inflector:pluralize(atom_to_list(ModuleName))),
    Function = erl_syntax:function(
        erl_syntax:atom(database_table),
        [erl_syntax:clause([], none, [erl_syntax:string(DefinedTableName)])]),
    [erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec database_table() -> string()",
                        lists:concat(["% @doc The name of the database table used to store `", ModuleName, "' records (if any)."])])],
            Function)].

attribute_types_forms(ModuleName, TypeInfo) ->
    [erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec attribute_types() -> [{atom(), atom()}]",
                        lists:concat(["% @doc A proplist of the types of each `", ModuleName, "' parameter, if specified."])])],
            erl_syntax:function(
                erl_syntax:atom(attribute_types),
                [erl_syntax:clause([], none, [erl_syntax:list(lists:map(
                                    fun({P, T}) -> erl_syntax:tuple([erl_syntax:atom(parameter_to_colname(P)), erl_syntax:atom(T)]) end,
                                    TypeInfo))])]))].

validate_types_forms(ModuleName) ->
    [erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec validate_types() -> ok | {error, [ErrorMessages]}",
                        lists:concat(["% @doc Validates the parameter types of `", ModuleName, "' without saving to the database."])
                    ])], 
            erl_syntax:function(
                erl_syntax:atom(validate_types),
                [erl_syntax:clause([], none,
                        [erl_syntax:application(
                                erl_syntax:atom(?DATABASE_MODULE),
                                erl_syntax:atom(validate_record_types),
                                [erl_syntax:variable("THIS")]
                            )])]))].

validate_forms(ModuleName) ->
    [erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec validate() -> ok | {error, [ErrorMessages]}",
                        lists:concat(["% @doc Validates this `", ModuleName, "' without saving to the database."]),
                        "% Errors are generated from this model's `validation_tests/0' function (if defined), ",
                        "% which should return a list of `{TestFunction, ErrorMessage}' tuples. For each test, ",
                        "% `TestFunction' should be a fun of arity 0 that returns `true' if the record is valid ",
                        "% or `false' if it is invalid. `ErrorMessage' should be a (constant) string that will be ",
                        "% included in `ErrorMessages' if the associated `TestFunction' returns `false' on this ",
                        lists:concat(["% particular `", ModuleName, "'."])
                    ])], 
            erl_syntax:function(
                erl_syntax:atom(validate),
                [erl_syntax:clause([], none,
                        [erl_syntax:application(
                                erl_syntax:atom(?DATABASE_MODULE),
                                erl_syntax:atom(validate_record),
                                [erl_syntax:variable("THIS")]
                            )])]))].

save_forms(ModuleName) ->
    [erl_syntax:add_precomments([erl_syntax:comment(
                    [lists:concat(["% @spec save() -> {ok, Saved", inflector:camelize(atom_to_list(ModuleName)), "} | {error, [ErrorMessages]}"]),
                        lists:concat(["% @doc Saves this `", ModuleName, "' record to the database. The returned record"]),
                        "% will have an auto-generated ID if the record's ID was set to 'id'.",
                        "% Performs validation first, returning `ErrorMessages' if validation fails.  See `validate/0'."])],
            erl_syntax:function(
                erl_syntax:atom(save),
                [erl_syntax:clause([], none,
                        [erl_syntax:application(
                                erl_syntax:atom(?DATABASE_MODULE),
                                erl_syntax:atom(save_record),
                                [erl_syntax:variable("THIS")]
                            )])]))].  
parameter_getter_forms(Parameters) ->
    lists:map(fun(P) -> 
                erl_syntax:add_precomments([erl_syntax:comment(
                        [lists:concat(["% @spec ", parameter_to_colname(P), "() -> ", P]),
                            lists:concat(["% @doc Returns the value of `", P, "'"])])],
                    erl_syntax:function(
                        erl_syntax:atom(parameter_to_colname(P)),
                        [erl_syntax:clause([], none, [erl_syntax:variable(P)])]))
        end, Parameters).

get_attributes_forms(ModuleName, Parameters) ->
    [erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec attributes() -> [{Attribute::atom(), Value::string() | undefined}]",
                        lists:concat(["% @doc A proplist of the `", ModuleName, "' parameters and their values."])])],
            erl_syntax:function(
                erl_syntax:atom(attributes),
                [erl_syntax:clause([], none,
                        [erl_syntax:list(lists:map(fun(P) ->
                                            erl_syntax:tuple([
                                                    erl_syntax:atom(parameter_to_colname(P)),
                                                    erl_syntax:variable(P)])
                                    end, Parameters))])]))].

set_attributes_forms(ModuleName, Parameters) ->
    [erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec set([{Attribute::atom(), Value}]) -> "++inflector:camelize(atom_to_list(ModuleName)),
                        "% @doc Set multiple record attributes at once. Does not save the record."])],
            erl_syntax:function(
                erl_syntax:atom(set),
                [erl_syntax:clause([erl_syntax:variable("AttributeProplist")], none,
                        [erl_syntax:application(
                                erl_syntax:atom(ModuleName),
                                erl_syntax:atom(new),
                                lists:map(fun(P) ->
                                            erl_syntax:application(
                                                erl_syntax:atom(proplists),
                                                erl_syntax:atom(get_value),
                                                [erl_syntax:atom(parameter_to_colname(P)),
                                                    erl_syntax:variable("AttributeProplist"),
                                                    erl_syntax:variable(P)])
                                    end, Parameters))])])),
    erl_syntax:add_precomments([erl_syntax:comment(
                ["% @spec set(Attribute::atom(), NewValue::any()) -> "++inflector:camelize(atom_to_list(ModuleName)),
                        "% @doc Set the value of a particular attribute. Does not save the record."])],
            erl_syntax:function(
                erl_syntax:atom(set),
                [erl_syntax:clause([erl_syntax:variable(?PREFIX++"Attribute"), erl_syntax:variable(?PREFIX++"NewValue")], none,
                        [
                            erl_syntax:application(
                                erl_syntax:atom(set),
                                [erl_syntax:list([erl_syntax:tuple([erl_syntax:variable(?PREFIX++"Attribute"), erl_syntax:variable(?PREFIX++"NewValue")])])])
                        ])]))
    ].

association_forms(ModuleName, Attributes) ->
    {Forms, BelongsToList} = lists:foldl(
        fun
            ({has, {HasOne, 1}}, {Acc, BT}) ->
                {has_one_forms(HasOne, ModuleName, []) ++ Acc, BT};
            ({has, {HasOne, 1, Opts}}, {Acc, BT}) ->
                {has_one_forms(HasOne, ModuleName, Opts) ++ Acc, BT};
            ({has, {HasMany, Limit}}, {Acc, BT}) ->
                {has_many_forms(HasMany, ModuleName, Limit, []) ++ Acc, BT};
            ({has, {HasMany, Limit, Opts}}, {Acc, BT}) ->
                {has_many_forms(HasMany, ModuleName, Limit, Opts) ++ Acc, BT};
            ({belongs_to, BelongsTo}, {Acc, BT}) ->
                {[belongs_to_forms(BelongsTo, BelongsTo, ModuleName)|Acc], [BelongsTo|BT]};
            ({OtherAttr, BelongsTo}, {Acc, BT}) ->
                case atom_to_list(OtherAttr) of
                    "belongs_to_"++Type ->
                        {[belongs_to_forms(Type, BelongsTo, ModuleName)|Acc], [BelongsTo|BT]};
                    _ ->
                        {Acc, BT}
                end;
            (_, Acc) ->
                Acc
        end, {[], []}, Attributes),
    Forms ++ belongs_to_list_forms(BelongsToList).


belongs_to_list_forms(BelongsToList) ->
    [ erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec belongs_to_names() -> [{atom()}]",
                        lists:concat(["% @doc Retrieve a list of the names of `belongs_to' associations."])])],
            erl_syntax:function(
                erl_syntax:atom(belongs_to_names),
                [erl_syntax:clause([], none, [erl_syntax:list(lists:map(
                                    fun(P) -> erl_syntax:atom(P) end, BelongsToList))])])),
    
    erl_syntax:add_precomments([erl_syntax:comment(
                ["% @spec belongs_to() -> [{atom(), BossRecord}]",
                    lists:concat(["% @doc Retrieve all of the `belongs_to' associations at once."])])],
        erl_syntax:function(
            erl_syntax:atom(belongs_to),
            [erl_syntax:clause([], none, [erl_syntax:list(lists:map(
                                fun(P) -> erl_syntax:tuple([
                                                erl_syntax:atom(P),
                                                erl_syntax:application(none, erl_syntax:atom(P), [])]) end, BelongsToList))])]))
            ].

attribute_names_forms(ModuleName, Parameters) ->
    [ erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec attribute_names() -> [atom()]",
                        lists:concat(["% @doc A list of the lower-case `", ModuleName, "' parameters."])])],
            erl_syntax:function(
                erl_syntax:atom(attribute_names),
                [erl_syntax:clause([], none, [erl_syntax:list(lists:map(
                                    fun(P) -> erl_syntax:atom(parameter_to_colname(P)) end,
                                    Parameters))])]))].

has_one_forms(HasOne, ModuleName, Opts) ->
    Type = proplists:get_value(module, Opts, HasOne),
    ForeignKey = proplists:get_value(foreign_key, Opts, atom_to_list(ModuleName) ++ "_id"),
    [erl_syntax:add_precomments([erl_syntax:comment(
                    [lists:concat(["% @spec ", HasOne, "() -> ", Type, " | undefined"]),
                        lists:concat(["% @doc Retrieves the `", Type, "' with `", ForeignKey, "' ",
                                "set to the `Id' of this `", ModuleName, "'"])])],
            erl_syntax:function(erl_syntax:atom(HasOne),
                [erl_syntax:clause([], none, [
                            first_or_undefined_forms(
                                has_many_application_forms(Type, ForeignKey, 1, id, false)
                            )
                        ])]))
    ].

has_many_forms(HasMany, ModuleName, many, Opts) ->
    has_many_forms(HasMany, ModuleName, all, Opts);
has_many_forms(HasMany, ModuleName, Limit, Opts) -> 
    Sort = proplists:get_value(order_by, Opts, 'id'),
    IsDescending = proplists:get_value(descending, Opts, false),
    Singular = inflector:singularize(atom_to_list(HasMany)),
    Type = proplists:get_value(module, Opts, Singular),
    ForeignKey = proplists:get_value(foreign_key, Opts, atom_to_list(ModuleName) ++ "_id"),
    [erl_syntax:add_precomments([erl_syntax:comment(
                    [
                        lists:concat(["% @spec ", HasMany, "() -> [ ", Type, " ]"]),
                        lists:concat(["% @doc Retrieves `", Type, "' records with `", ForeignKey, "' ",
                                "set to the `Id' of this `", ModuleName, "'"])])],
            erl_syntax:function(erl_syntax:atom(HasMany),
                [erl_syntax:clause([], none, [
                            has_many_application_forms(Type, ForeignKey, Limit, Sort, IsDescending)
                        ])])),
        erl_syntax:add_precomments([erl_syntax:comment(
                    [
                        lists:concat(["% @spec first_", Singular, "() -> ", Type, " | undefined"]),
                        lists:concat(["% @doc Retrieves the first `", Type, 
                                "' that would be returned by `", HasMany, "()'"])])],
            erl_syntax:function(erl_syntax:atom("first_"++Singular),
                [erl_syntax:clause([], none, [
                            first_or_undefined_forms(
                                has_many_application_forms(Type, ForeignKey, 1, Sort, IsDescending)
                            )
                        ])])),
        erl_syntax:add_precomments([erl_syntax:comment(
                    [
                        lists:concat(["% @spec last_", Singular, "() -> ", Type, " | undefined"]),
                        lists:concat(["% @doc Retrieves the last `", Type,
                                "' that would be returned by `", HasMany, "()'"])])],
            erl_syntax:function(erl_syntax:atom("last_"++Singular),
                [erl_syntax:clause([], none, [
                            first_or_undefined_forms(
                                    has_many_application_forms(Type, ForeignKey, 1, Sort, not IsDescending)
                                )
                        ])]))
    ].

first_or_undefined_forms(Forms) ->
    erl_syntax:case_expr(Forms,
        [erl_syntax:clause([erl_syntax:list([erl_syntax:variable(?PREFIX++"Record")])], none,
                [erl_syntax:variable(?PREFIX++"Record")]),
            erl_syntax:clause([erl_syntax:underscore()], none, [erl_syntax:atom(undefined)])]).

has_many_application_forms(Type, ForeignKey, Limit, Sort, IsDescending) ->
    erl_syntax:application(
        erl_syntax:atom(?DATABASE_MODULE), 
        erl_syntax:atom(find),
        [erl_syntax:atom(Type),
            erl_syntax:list([
                    erl_syntax:tuple([
                            erl_syntax:atom(ForeignKey),
                            erl_syntax:variable("Id")])
                ]),
            erl_syntax:list([
                    erl_syntax:tuple([
                            erl_syntax:atom(limit),
                            erl_syntax:integer(Limit)]),
                    erl_syntax:tuple([
                            erl_syntax:atom(order_by),
                            erl_syntax:atom(Sort)]),
                    erl_syntax:tuple([
                            erl_syntax:atom(descending),
                            erl_syntax:atom(IsDescending)])
                ])
        ]).

belongs_to_forms(Type, BelongsTo, ModuleName) ->
    erl_syntax:add_precomments([erl_syntax:comment(
                [lists:concat(["% @spec ", BelongsTo, "() -> ", 
                            inflector:camelize(atom_to_list(BelongsTo))]),
                    lists:concat(["% @doc Retrieves the ", Type, 
                            " with `Id' equal to the `", 
                            inflector:camelize(atom_to_list(BelongsTo)), "Id'",
                           " of this ", ModuleName])])],
        erl_syntax:function(erl_syntax:atom(BelongsTo),
            [erl_syntax:clause([], none, [
                        erl_syntax:application(
                            erl_syntax:atom(?DATABASE_MODULE),
                            erl_syntax:atom(find),
                            [erl_syntax:variable(inflector:camelize(atom_to_list(BelongsTo)) ++ "Id")]
                        )])])).

counter_getter_forms(Counters) ->
    lists:map(
        fun(Counter) ->
                erl_syntax:add_precomments([erl_syntax:comment(
                            [
                                lists:concat(["% @spec ", Counter, "() -> integer()"]),
                                lists:concat(["% @doc Retrieve the value of the `", Counter, "' counter"])])],
                    erl_syntax:function(erl_syntax:atom(Counter),
                        [erl_syntax:clause([], none, [
                                    erl_syntax:application(
                                        erl_syntax:atom(?DATABASE_MODULE),
                                        erl_syntax:atom(counter),
                                        [erl_syntax:infix_expr(
                                                erl_syntax:variable("Id"),
                                                erl_syntax:operator("++"),
                                                erl_syntax:string("-counter-" ++ atom_to_list(Counter))
                                            )])])])) end, Counters).

counter_reset_forms([]) ->
    [];
counter_reset_forms(Counters) ->
    [erl_syntax:add_precomments([erl_syntax:comment(
                ["% @spec reset( Counter::atom() ) -> ok | {error, Reason}",
                    "% @doc Reset a counter to zero"])],
        erl_syntax:function(erl_syntax:atom(reset),
            lists:map(
                fun(Counter) ->
                        erl_syntax:clause([erl_syntax:atom(Counter)], none, [
                                erl_syntax:application(
                                    erl_syntax:atom(?DATABASE_MODULE),
                                    erl_syntax:atom(delete),
                                    [counter_name_forms(Counter)])])
                end, Counters)))].

counter_incr_forms([]) ->
    [];
counter_incr_forms(Counters) ->
    [ erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec incr( Counter::atom() ) -> integer()",
                        "@doc Atomically increment a counter by 1."])],
            erl_syntax:function(erl_syntax:atom(incr),
                lists:map(
                    fun(Counter) ->
                            erl_syntax:clause([erl_syntax:atom(Counter)], none, [
                                    erl_syntax:application(
                                        erl_syntax:atom(?DATABASE_MODULE),
                                        erl_syntax:atom(incr),
                                        [counter_name_forms(Counter)])]) 
                    end, Counters))),
        erl_syntax:add_precomments([erl_syntax:comment(
                    ["% @spec incr( Counter::atom(), Increment::integer() ) ->"++
                        " integer()",
                        "% @doc Atomically increment a counter by the specified increment"])],
            erl_syntax:function(erl_syntax:atom(incr),
                lists:map(
                    fun(Counter) ->
                            erl_syntax:clause([erl_syntax:atom(Counter),
                                    erl_syntax:variable("Amount")], none, [
                                    erl_syntax:application(
                                        erl_syntax:atom(?DATABASE_MODULE),
                                        erl_syntax:atom(incr),
                                        [counter_name_forms(Counter),
                                            erl_syntax:variable("Amount")])])
                    end, Counters)))].

counter_name_forms(CounterVariable) ->
    erl_syntax:infix_expr(
        erl_syntax:infix_expr(
            erl_syntax:variable("Id"),
            erl_syntax:operator("++"),
            erl_syntax:string("-counter-")),
        erl_syntax:operator("++"),
        erl_syntax:application(
            erl_syntax:atom('erlang'),
            erl_syntax:atom('atom_to_list'),
            [erl_syntax:atom(CounterVariable)])).

parameter_to_colname(Parameter) when is_atom(Parameter) ->
    string:to_lower(inflector:underscore(atom_to_list(Parameter))).
