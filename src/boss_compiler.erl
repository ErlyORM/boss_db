-module(boss_compiler).
-export([compile/1, compile/2, parse/3]).

-ifdef(TEST).
-compile(export_all).
-endif.
-type syntaxTree()   :: erl_syntax:syntaxTree().
-type opt(X)         :: X|undefined.
-type error(X)       :: {ok, X}|{error, string()}.
-type error(X,Y)     :: {ok, X,Y}|{error, _}.
-type special_char() :: 8800|8804|8805|8712|8713|8715|8716|8764|8769|8839|8841|9745|8869|10178|8745.
-type token()        :: erl_scan:token().
-type form()         :: atom().
-type position()     :: {non_neg_integer(), non_neg_integer()}.

-type otp_version() :: 14|15|16|17|18|19|20.
-spec(make_forms_by_version([syntaxTree()], otp_version()) ->syntaxTree()).
-spec compile(binary() | [atom() | [any()] | char()]) -> {'error',atom() | {_,[any(),...]}}.
-spec compile(binary() | [atom() | [any()] | char()],[any()]) -> {'error',atom() | {_,[any(),...]}}.
-spec compile_forms(_,binary() | [atom() | [any()] | char()],atom() | [any()]) -> any().

-spec parse(binary() | [atom() | [any()] | char()],_,_) ->
                   {'error',atom() | {'undefined' | [any()],[any(),...]}} |
                   {'error',[{_,_}]} |
                   {'ok',[any()],_}.

-spec parse_text('undefined' | [atom() | [any()] | char()],binary(),_,_) -> {'error',{'undefined' | [any()],[any(),...]}} |
                                                                            {'error',[{_,_}]} |
                                                                            {'ok',[any()],_}.

%% @spec compile( File::string() ) -> {ok, Module} | {error, Reason}
compile(File) ->
    compile(File, []).

compile(File, Options) ->
    _ = logger:notice("Compile file ~p with options ~p ", [File, Options]),
    IncludeDirs    = ["include"] ++ proplists:get_value(include_dirs,    Options, []) ++
                     proplists:get_all_values(i, compiler_options(Options)),
    TokenTransform = proplists:get_value(token_transform, Options),
    case parse(File, TokenTransform, IncludeDirs) of
        {ok, Forms, TokenInfo} ->
            handle_parse_success(File, Options, Forms, TokenInfo);
        Error = {error, _} ->
            Error
    end.

handle_parse_success(File, Options, Forms, TokenInfo) ->
    Version         = otp_version(),
    CompilerOptions = compiler_options(Options),

    Forms1          = make_new_forms(Options, Forms, TokenInfo),
    {Forms2, BossDBParseTransforms} = make_forms_by_version(Forms1,
                                                            Version),

    ParseTransforms = make_parse_transforms(Options, BossDBParseTransforms),
    RevertedForms   = make_reverted_forms(CompilerOptions, Forms2,
                                          ParseTransforms),

    compile_forms1(File, Options, CompilerOptions, RevertedForms).

compiler_options(Options) ->
    proplists:get_value(compiler_options, Options,
                        [verbose, return_errors]).

compile_forms1(File, Options, CompilerOptions, RevertedForms) ->
    case compile_forms(RevertedForms, File, CompilerOptions) of
        {ok, Module, Bin} ->
            ok = write_beam(Options, Module, Bin),
            {ok, Module};
        Error ->
            Error
    end.

write_beam(Options, Module, Bin) ->
    case proplists:get_value(out_dir, Options) of
        undefined -> ok;
        OutDir ->
            BeamFile = filename:join([OutDir, lists:concat([Module, ".beam"])]),
            filelib:ensure_dir(BeamFile),
            file:write_file(BeamFile, Bin)
    end.

make_parse_transforms(Options, BossDBParseTransforms) ->
    BossDBParseTransforms ++
proplists:get_value(parse_transforms, Options, []).

otp_version() ->
    OTPVersion = erlang:system_info(otp_release),
    case OTPVersion of
        [$R, V1, V2 | _] -> %% Erlang versions prior to 17 were numbered like R15B01. This extracts the "15"
            Version = list_to_integer([V1, V2]),
            Version;
        [V1, V2 | _] -> %% Erlang versions after 17 will be numbered as "17" or "17.0" This just strips any potential number after the first two.
            list_to_integer([V1,V2])
    end.

make_forms_by_version(NewForms, Version) when Version >= 16->
                                                % OTP Version starting with R16A needs boss_db_pmod_pt
                                                % boss_db_pmod_pt needs the form to end with {eof, 0} tagged tupple
    NewForms1 = NewForms ++ [{eof,0}],
                                                % boss_db_pmod_pt needs the form to be in "new" format
    {erl_syntax:revert_forms(erl_syntax:revert(NewForms1)), [boss_db_pmod_pt, boss_db_pt]};
make_forms_by_version(NewForms, _Version) ->
    {erl_syntax:revert(NewForms), [boss_db_pt]}.


make_reverted_forms(CompilerOptions, NewNewForms, ParseTransforms) ->
    lists:foldl(fun(Mod, Acc) ->
                        Mod:parse_transform(Acc, CompilerOptions)
                end, NewNewForms, ParseTransforms).

make_new_forms(Options, Forms, TokenInfo) ->
    Transform = proplists:get_value(pre_revert_transform, Options),
    transform_action(Forms, TokenInfo, Transform).

-spec(transform_action([form()],
                       [token()],
                       undefined|
                       fun(([form()]) -> _)|
                          fun(([form()], [token()]) ->_)) -> _).
transform_action(Forms, _, undefined) ->
    Forms;
transform_action(Forms, _TokenInfo, TransformFun) when is_function(TransformFun, 1) ->
    TransformFun(Forms);
transform_action(Forms,  TokenInfo, TransformFun) when is_function(TransformFun, 2) ->
    TransformFun(Forms, TokenInfo).


compile_forms(Forms, File, Options) ->
    case compile:forms(Forms, Options) of
        {ok, Module1, Bin} ->
            code:purge(Module1),
            load_binary(File, Module1, Bin);
        OtherError ->
            OtherError
    end.

load_binary(File, Module1, Bin) ->
    case code:load_binary(Module1, File, Bin) of
        {module, _} -> {ok, Module1, Bin};
        _           -> {error, lists:concat(["code reload failed: ", Module1])}
    end.

parse(File, TokenTransform, IncludeDirs) when is_list(File) ->
    case file:read_file(File) of
        {ok, FileContents} ->
            parse_text(File, FileContents, TokenTransform, IncludeDirs);
        Error ->
            Error
    end;

parse(File, TokenTransform, IncludeDirs) when is_binary(File) ->
    parse_text(undefined, File, TokenTransform, IncludeDirs).

parse_text(FileName, FileContents, TokenTransform, IncludeDirs) ->
    case scan_transform(FileContents) of
        {ok, Tokens} ->
            {NewTokens, TokenInfo} = transform_tokens(TokenTransform, Tokens),
            case aleppo:process_tokens(NewTokens, [{file, FileName}, {include, IncludeDirs}]) of
                {ok, ProcessedTokens} ->
                    handle_tokens(FileName, TokenInfo, ProcessedTokens);
                {error, ErrorInfo} ->
                    {error, {FileName, [ErrorInfo]}}
            end;
        {error, ErrorInfo} ->
            {error, {FileName, [ErrorInfo]}}
    end.

handle_tokens(FileName, TokenInfo, ProcessedTokens) ->
    % We have to flatten the token locations because the Erlang parser
    % has a bug that chokes on {Line, Col} locations in typed record
    % definitions
    TokensWithOnlyLineNumbers = flatten_token_locations(ProcessedTokens),

    Version = otp_version(),
    {Forms, Errors}           = parse_tokens(TokensWithOnlyLineNumbers, FileName, Version),
    parse_has_errors(TokenInfo, Forms, Errors).


-spec(parse_has_errors(token(), any(), [{string(), string()}]) ->
             error(any(),any())).
parse_has_errors(TokenInfo, Forms, []) ->
    {ok, Forms, TokenInfo};
parse_has_errors(_TokenInfo, _Forms, Errors) ->
     {error, make_parse_errors(Errors)}.


-spec(make_parse_errors([{string(), string()}]) -> [{string() , [string()]}]).
make_parse_errors(Errors) ->
    lists:map(fun(File) ->
                      {File, proplists:get_all_values(File, Errors)}
              end, proplists:get_keys(Errors)).

-spec(transform_tokens(opt(fun(([token()])-> _)), [token()]) -> _).
transform_tokens(undefined, Tokens) ->
    {Tokens, undefined};
transform_tokens(TransformFun,Tokens) when is_function(TransformFun) ->
    TransformFun(Tokens).


-spec parse_tokens([any()],'undefined' | [atom() | [any()] | char()], otp_version()) -> {[any()],[{_,_}]}.
parse_tokens(Tokens, FileName, Version) ->
    parse_tokens(Tokens, [], [], [], FileName, Version).

-spec parse_tokens([any()],[any()],[any()],[{_,{_,_,_}}],_, otp_version()) -> {[any()],[{_,_}]}.
parse_tokens([], _, FormAcc, ErrorAcc, _, _Version) ->
    {lists:reverse(FormAcc), lists:reverse(ErrorAcc)};
parse_tokens([{dot, _}=Token|Rest], TokenAcc, FormAcc, ErrorAcc, FileName, Version) ->
    case erl_parse:parse_form(lists:reverse([Token|TokenAcc])) of
        {ok, {attribute, _, file, {NewFileName, _Line}} = AbsForm} ->
            parse_tokens(Rest, [], [AbsForm|FormAcc], ErrorAcc, NewFileName, Version);
        {ok, {attribute, La, record, {Record, Fields}} = AbsForm} when Version < 19 ->
            case epp:normalize_typed_record_fields(Fields) of
                {typed, NewFields} ->
                    parse_tokens(Rest, [], lists:reverse([
                                {attribute, La, record, {Record, NewFields}},
                                {attribute, La, type, {{record, Record}, Fields, []}}],
                            FormAcc), ErrorAcc, FileName, Version);
                not_typed ->
                    parse_tokens(Rest, [], [AbsForm|FormAcc], ErrorAcc, FileName, Version)
            end;
        {ok, AbsForm} ->
            parse_tokens(Rest, [], [AbsForm|FormAcc], ErrorAcc, FileName, Version);
        {error, ErrorInfo} ->
            parse_tokens(Rest, [], FormAcc, [{FileName, ErrorInfo}|ErrorAcc], FileName, Version)
    end;
parse_tokens([{eof, Location}], TokenAcc, FormAcc, ErrorAcc, FileName, Version) ->
    parse_tokens([], TokenAcc, [{eof, Location}|FormAcc], ErrorAcc, FileName, Version);
parse_tokens([Token|Rest], TokenAcc, FormAcc, ErrorAcc, FileName, Version) ->
    parse_tokens(Rest, [Token|TokenAcc], FormAcc, ErrorAcc, FileName, Version).

-spec scan_transform(binary()) -> syntaxTree().
scan_transform(FileContents) ->
    scan_transform(FileContents, {1, 1}).

-spec scan_transform('eof' | binary() | string(),integer() | {integer(),pos_integer()}) -> {'error',{integer() | {_,_},atom() | tuple(),_}} | {'ok',[{_,_} | {_,_,_},...]}.
scan_transform([], StartLocation) ->
    {ok, [{eof, StartLocation}]};
scan_transform(FileContents, StartLocation) when is_binary(FileContents) ->
    scan_transform(unicode:characters_to_list(FileContents), StartLocation);
scan_transform(FileContents, StartLocation) ->
    ScanResults = erl_scan:tokens([], FileContents, StartLocation),
    case ScanResults of
        {done, Return, Rest} ->
            case Return of
                {ok, Tokens, EndLocation} ->
                    case scan_transform(Rest, EndLocation) of
                        {ok, NewTokens} ->
                            {ok, Tokens ++ NewTokens};
                        Err -> Err
                    end;
                {eof, EndLocation} ->
                    {ok, [{eof, EndLocation}]};
                {error, ErrorInfo, _EndLocation} ->
                    case ErrorInfo of
                        {ErrorLocation, erl_scan, {illegal,character}} ->
                            {Truncated, IllegalChar, Rest1} = cut_at_location(ErrorLocation, FileContents, StartLocation),
                            scan_transform_illegal_char(StartLocation,
                                                        ErrorInfo, Truncated,
                                                        IllegalChar, Rest1);
                        ErrorInfo ->
                            {error, ErrorInfo}
                    end
            end;
         {more, Continuation1} ->
            {done, Return, eof} = erl_scan:tokens(Continuation1, eof, eof),
           scan_transform_result(Return)
    end.


-spec(scan_transform_result({ok, [token()], pos_integer()}|
                            {eof, pos_integer()} |
                            {error, string(), pos_integer()}) ->
             error([any()])).
scan_transform_result(Return) ->
    case Return of
        {ok, Tokens, _EndLocation} ->
            {ok, Tokens};
        {eof, EndLocation} ->
            {ok, [{eof, EndLocation}]};
        {error, ErrorInfo, _EndLocation} ->
            {error, ErrorInfo}
    end.

-spec(scan_transform_illegal_char(pos_integer(),any(), string(), special_char(), string()) -> error(_)).
scan_transform_illegal_char(StartLocation, ErrorInfo, Truncated,
                            IllegalChar, Rest1) ->
    case transform_char(IllegalChar) of
        {ok, String} ->
            Transformed = Truncated ++ String ++ Rest1,
            scan_transform(Transformed, StartLocation);
        error ->
            {error, ErrorInfo}
    end.

-spec transform_char(special_char() |char()) -> 'error' | {'ok',[any()]}.
transform_char(8800) -> % ≠
    {ok, ",'not_equals',"};
transform_char(8804) -> % ≤
    {ok, ",'le',"};
transform_char(8805) -> % ≥
    {ok, ",'ge',"};
transform_char(8712) -> % ∈
    {ok, ",'in',"};
transform_char(8713) -> % ∉
    {ok, ",'not_in',"};
transform_char(8715) -> % ∋
    {ok, ",'contains',"};
transform_char(8716) -> % ∌
    {ok, ",'not_contains',"};
transform_char(8764) -> % ∼
    {ok, ",'matches',"};
transform_char(8769) -> % ≁
    {ok, ",'not_matches',"};
transform_char(8839) -> % ⊇
    {ok, ",'contains_all',"};
transform_char(8841) -> % ⊉
    {ok, ",'not_contains_all',"};
transform_char(8745) -> % ∩
    {ok, ",'contains_any',"};
transform_char(8869) -> % ⊥
    {ok, ",'contains_none',"};
transform_char(10178) -> % ⊥ look-alike
    {ok, ",'contains_none',"};
transform_char(Char) when Char > 127 ->
    Bytes = binary_to_list(unicode:characters_to_binary([Char], unicode, utf8)),
    {ok, lists:flatten(lists:map(fun(Byte) ->
                        io_lib:format("\\x{~.16B}", [Byte])
                end, Bytes))};
transform_char(_) ->
    error.

-spec cut_at_location(position(), nonempty_string(),{integer(),pos_integer()}) -> {string(),char(),string()}.
cut_at_location({CutLine, CutCol}, FileContents, {StartLine, StartCol}) ->
    cut_at_location1({CutLine, CutCol}, FileContents, {StartLine, StartCol}, []).

-spec cut_at_location1(position(),string(),{integer(),pos_integer()},string()) -> {string(),char(),string()}.
cut_at_location1(_, [], _, Acc) ->
    {lists:reverse(Acc), 0, ""};
cut_at_location1({Line, Col}, [C|Rest], {Line, Col}, Acc) ->
    {lists:reverse(Acc), C, Rest};
cut_at_location1({Line, Col}, [C|Rest], {ThisLine, _}, Acc) when C =:= $\n ->
    cut_at_location1({Line, Col}, Rest, {ThisLine + 1, 1}, [C|Acc]);
cut_at_location1({Line, Col}, [C|Rest], {ThisLine, ThisCol}, Acc) ->
    cut_at_location1({Line, Col}, Rest, {ThisLine, ThisCol + 1}, [C|Acc]).

-spec flatten_token_locations([any()]) -> [any()].
flatten_token_locations(Tokens) ->
    flatten_token_locations1(Tokens, []).

-spec flatten_token_locations1([token()],[token()]) -> [token()].
flatten_token_locations1([], Acc) ->
    lists:reverse(Acc);
flatten_token_locations1([{Type, {Line, _Col}}|Rest], Acc) ->
    flatten_token_locations1(Rest, [{Type, Line}|Acc]);
flatten_token_locations1([{Type, {Line, _Col}, Extra}|Rest], Acc) ->
    flatten_token_locations1(Rest, [{Type, Line, Extra}|Acc]);
flatten_token_locations1([Other|Rest], Acc) ->
    flatten_token_locations1(Rest, [Other|Acc]).
