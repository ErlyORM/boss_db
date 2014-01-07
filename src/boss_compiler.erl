-module(boss_compiler).
-export([compile/1, compile/2, parse/3]).
-compile(export_all).
-ifdef(TEST).
-compile(export_all).
-endif.
-type token() ::erl_scan:token().
-type position() ::{non_neg_integer(), non_neg_integer()}.
-spec compile(binary() | [atom() | [any()] | char()]) -> any().
-spec compile(binary() | [atom() | [any()] | char()],[any()]) -> any().
-spec compile_forms(_,binary() | [atom() | [any()] | char()],atom() | [any()]) -> any().
-spec parse(binary() | [atom() | [any()] | char()],_,_) -> {'error',atom() | {'undefined' | [any()],[any(),...]}} | {'error',[{_,_}],[]} | {'ok',[any()],_}.
-spec parse_text('undefined' | [atom() | [any()] | char()],binary(),_,_) -> {'error',{'undefined' | [any()],[any(),...]}} | {'error',[{_,_}],[]} | {'ok',[any()],_}.
-spec parse_tokens([any()],'undefined' | [atom() | [any()] | char()]) -> {[any()],[{_,_}]}.
-spec parse_tokens([any()],[any()],[any()],[{_,{_,_,_}}],_) -> {[any()],[{_,_}]}.
-spec scan_transform(binary()) -> {'error',{integer() | {_,_},atom() | tuple(),_}} | {'ok',[{_,_} | {_,_,_},...]}.
-spec scan_transform('eof' | binary() | string(),integer() | {integer(),pos_integer()}) -> {'error',{integer() | {_,_},atom() | tuple(),_}} | {'ok',[{_,_} | {_,_,_},...]}.
-spec transform_char(char()) -> 'error' | {'ok',[any()]}.
-spec cut_at_location(position(), nonempty_string(),{integer(),pos_integer()}) -> {string(),char(),string()}.
-spec cut_at_location1(position(),string(),{integer(),pos_integer()},string()) -> {string(),char(),string()}.
-spec flatten_token_locations([token()]) -> [token()].
-spec flatten_token_locations1([token()],[token()]) -> [token()].

%% @spec compile( File::string() ) -> {ok, Module} | {error, Reason}
compile(File) ->
    compile(File, []).

compile(File, Options) ->
    IncludeDirs = proplists:get_value(include_dirs, Options, []),
    TokenTransform = proplists:get_value(token_transform, Options),
    case parse(File, TokenTransform, IncludeDirs) of
        {ok, Forms, TokenInfo} ->
            CompilerOptions = proplists:get_value(compiler_options, Options, 
                [verbose, return_errors]),
            NewForms = case proplists:get_value(pre_revert_transform, Options) of
                undefined ->
                    Forms;
                TransformFun when is_function(TransformFun) ->
                    case erlang:fun_info(TransformFun, arity) of
                        {arity, 1} ->
                            TransformFun(Forms);
                        {arity, 2} ->
                            TransformFun(Forms, TokenInfo)
                    end
            end,
            OTPVersion = erlang:system_info(otp_release),
            {Version, _Rest} = string:to_integer(string:sub_string(OTPVersion, 2, 3)),
            {NewNewForms, BossDBParseTransforms} = case Version of
                Version when Version >= 16 ->
                    % OTP Version starting with R16A needs boss_db_pmod_pt
                    % boss_db_pmod_pt needs the form to end with {eof, 0} tagged tupple
                    NewForms1 = NewForms ++ [{eof,0}],
                    % boss_db_pmod_pt needs the form to be in "new" format
                    {erl_syntax:revert_forms(erl_syntax:revert(NewForms1)), [boss_db_pmod_pt, boss_db_pt]};
                _ ->
                    {erl_syntax:revert(NewForms), [boss_db_pt]}
            end,

            ParseTransforms = BossDBParseTransforms ++ proplists:get_value(parse_transforms, Options, []),
            RevertedForms = lists:foldl(fun(Mod, Acc) ->
                    Mod:parse_transform(Acc, CompilerOptions)
                end, NewNewForms, ParseTransforms),

            case compile_forms(RevertedForms, File, CompilerOptions) of
                {ok, Module, Bin} ->
                    ok = case proplists:get_value(out_dir, Options) of
                        undefined -> ok;
                        OutDir ->
                            BeamFile = filename:join([OutDir, lists:concat([Module, ".beam"])]),
                            file:write_file(BeamFile, Bin)
                    end,
                    {ok, Module};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

compile_forms(Forms, File, Options) ->
    case compile:forms(Forms, Options) of
        {ok, Module1, Bin} ->
            code:purge(Module1),
            case code:load_binary(Module1, File, Bin) of
                {module, _} -> {ok, Module1, Bin};
                _ -> {error, lists:concat(["code reload failed: ", Module1])}
            end;
        OtherError ->
            OtherError
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
            {NewTokens, TokenInfo} = case TokenTransform of
                undefined ->
                    {Tokens, undefined};
                TransformFun when is_function(TransformFun) ->
                    TransformFun(Tokens)
            end,
            case aleppo:process_tokens(NewTokens, [{file, FileName}, {include, IncludeDirs}]) of
                {ok, ProcessedTokens} ->
                    % We have to flatten the token locations because the Erlang parser
                    % has a bug that chokes on {Line, Col} locations in typed record
                    % definitions
                    TokensWithOnlyLineNumbers = flatten_token_locations(ProcessedTokens), 
                    {Forms, Errors} = parse_tokens(TokensWithOnlyLineNumbers, FileName),
                    case length(Errors) of
                        0 ->
                            {ok, Forms, TokenInfo};
                        _ ->
                            Errors1 = lists:map(fun(File) ->
                                        {File, proplists:get_all_values(File, Errors)}
                                end, proplists:get_keys(Errors)),
                            {error, Errors1, []}
                    end;
                {error, ErrorInfo} ->
                    {error, {FileName, [ErrorInfo]}}
            end;
        {error, ErrorInfo} ->
            {error, {FileName, [ErrorInfo]}}
    end.

parse_tokens(Tokens, FileName) ->
    parse_tokens(Tokens, [], [], [], FileName).

parse_tokens([], _, FormAcc, ErrorAcc, _) ->
    {lists:reverse(FormAcc), lists:reverse(ErrorAcc)};
parse_tokens([{dot, _}=Token|Rest], TokenAcc, FormAcc, ErrorAcc, FileName) ->
    case erl_parse:parse_form(lists:reverse([Token|TokenAcc])) of
        {ok, {attribute, _, file, {NewFileName, _Line}} = AbsForm} ->
            parse_tokens(Rest, [], [AbsForm|FormAcc], ErrorAcc, NewFileName);
        {ok, {attribute, La, record, {Record, Fields}} = AbsForm} ->
            case epp:normalize_typed_record_fields(Fields) of
                {typed, NewFields} ->
                    parse_tokens(Rest, [], lists:reverse([
                                {attribute, La, record, {Record, NewFields}},
                                {attribute, La, type, {{record, Record}, Fields, []}}],
                            FormAcc), ErrorAcc, FileName);
                not_typed ->
                    parse_tokens(Rest, [], [AbsForm|FormAcc], ErrorAcc, FileName)
            end;
        {ok, AbsForm} ->
            parse_tokens(Rest, [], [AbsForm|FormAcc], ErrorAcc, FileName);
        {error, ErrorInfo} ->
            parse_tokens(Rest, [], FormAcc, [{FileName, ErrorInfo}|ErrorAcc], FileName)
    end;
parse_tokens([{eof, Location}], TokenAcc, FormAcc, ErrorAcc, FileName) ->
    parse_tokens([], TokenAcc, [{eof, Location}|FormAcc], ErrorAcc, FileName);
parse_tokens([Token|Rest], TokenAcc, FormAcc, ErrorAcc, FileName) ->
    parse_tokens(Rest, [Token|TokenAcc], FormAcc, ErrorAcc, FileName).

scan_transform(FileContents) ->
    scan_transform(FileContents, {1, 1}). 

scan_transform([], StartLocation) ->
    {ok, [{eof, StartLocation}]};
scan_transform(FileContents, StartLocation) when is_binary(FileContents) ->
    scan_transform(unicode:characters_to_list(FileContents), StartLocation);
scan_transform(FileContents, StartLocation) ->
    case erl_scan:tokens([], FileContents, StartLocation) of
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
                            case transform_char(IllegalChar) of
                                {ok, String} ->
                                    Transformed = Truncated ++ String ++ Rest1,
                                    scan_transform(Transformed, StartLocation);
                                error ->
                                    {error, ErrorInfo}
                            end;
                        ErrorInfo ->
                            {error, ErrorInfo}
                    end
 
            end;
         {more, Continuation1} ->
            {done, Return, eof} = erl_scan:tokens(Continuation1, eof, eof),
            case Return of
                {ok, Tokens, _EndLocation} ->
                    {ok, Tokens};
                {eof, EndLocation} ->
                    {ok, [{eof, EndLocation}]};
                {error, ErrorInfo, _EndLocation} ->
                    {error, ErrorInfo}
            end
    end.

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

cut_at_location({CutLine, CutCol}, FileContents, {StartLine, StartCol}) ->
    cut_at_location1({CutLine, CutCol}, FileContents, {StartLine, StartCol}, []).

cut_at_location1(_, [], _, Acc) ->
    {lists:reverse(Acc), 0, ""};
cut_at_location1({Line, Col}, [C|Rest], {Line, Col}, Acc) ->
    {lists:reverse(Acc), C, Rest};
cut_at_location1({Line, Col}, [C|Rest], {ThisLine, _}, Acc) when C =:= $\n ->
    cut_at_location1({Line, Col}, Rest, {ThisLine + 1, 1}, [C|Acc]);
cut_at_location1({Line, Col}, [C|Rest], {ThisLine, ThisCol}, Acc) ->
    cut_at_location1({Line, Col}, Rest, {ThisLine, ThisCol + 1}, [C|Acc]).

flatten_token_locations(Tokens) ->
    flatten_token_locations1(Tokens, []).

flatten_token_locations1([], Acc) ->
    lists:reverse(Acc);
flatten_token_locations1([{Type, {Line, _Col}}|Rest], Acc) ->
    flatten_token_locations1(Rest, [{Type, Line}|Acc]);
flatten_token_locations1([{Type, {Line, _Col}, Extra}|Rest], Acc) ->
    flatten_token_locations1(Rest, [{Type, Line, Extra}|Acc]);
flatten_token_locations1([Other|Rest], Acc) ->
    flatten_token_locations1(Rest, [Other|Acc]).
