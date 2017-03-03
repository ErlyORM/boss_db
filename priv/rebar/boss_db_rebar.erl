%%% @author Roman Tsisyk <roman@tsisyk.com>
%%% @doc
%%% Rebar plugin to compile boss_db models
%%%
%%% Configuration options should be placed in rebar.config under
%%% 'boss_db_opts'.
%%%
%%% Available options include:
%%%
%%%  model_dir: where to find templates to compile
%%%            "src/model" by default
%%%
%%%  out_dir: where to put compiled template beam files
%%%           "ebin" by default
%%%
%%%  source_ext: the file extension the template sources have
%%%              ".erl" by default
%%%
%%%  recursive: boolean that determines if model_dir need to be
%%%             scanned recursively for matching template file names
%%%             (default: false).
%%%
%%%  compiler_options: options to determine the behavior of the model
%%%                    compiler, see the Erlang docs (compile:file/2)
%%%                    for valid options
%%%
%%% The default settings are the equivalent of:
%%% {boss_db_opts, [
%%%    {model_root, "src/model"},
%%%    {out_root, "ebin"},
%%%    {source_ext, ".erl"},
%%%    {recursive, false},
%%%    {compiler_options, [verbose, return_errors]}
%%% ]}.
%%% @end

-module(boss_db_rebar).

-export([pre_compile/2]).
-export([pre_eunit/2]).

%% @doc A pre-compile hook to compile boss_db models
pre_compile(RebarConf, _AppFile) ->
    BossDbOpts = boss_db_opts(RebarConf),
    pre_compile_helper(RebarConf, BossDbOpts, option(out_dir, BossDbOpts)).

%% @doc A pre-eunit hook to compile boss_db models before eunit.
%% It also copies model's .erl files to the .eunit directory if
%% coverage is needed.
pre_eunit(RebarConf, _AppFile) ->
    BossDbOpts = boss_db_opts(RebarConf),
    pre_compile_helper(RebarConf, BossDbOpts, ".eunit"),
    case coverage_modifications_needed(RebarConf, BossDbOpts) of
        true ->
            ModelDir = option(model_dir, BossDbOpts),
            eunit_coverage_helper(ModelDir);
        false ->
            ok
    end.

%% --------------------------------------------------------------------
%% Internal functions
%% --------------------------------------------------------------------

%% @doc Gets the boss_db options
boss_db_opts(RebarConf) ->
    rebar_config:get(RebarConf, boss_db_opts, []).

%% @doc Gets the value for coverage_enabled from the rebar config
coverage_enabled(RebarConf) ->
    rebar_config:get(RebarConf, cover_enabled, false).

%% @doc A pre-compile hook to compile boss_db models
pre_compile_helper(RebarConf, BossDbOpts, TargetDir) ->
    SourceDir = option(model_dir, BossDbOpts),
    SourceExt = option(source_ext, BossDbOpts),
    TargetExt = ".beam",
    rebar_base_compiler:run(RebarConf, [],
        SourceDir,
        SourceExt,
        TargetDir,
        TargetExt,
        fun(S, T, _C) ->
            compile_model(S, T, BossDbOpts, RebarConf)
        end,
        [{check_last_mod, true},
        {recursive, option(recursive, BossDbOpts)}]).

option(Opt, BossDbOpts) ->
    proplists:get_value(Opt, BossDbOpts, option_default(Opt)).

option_default(model_dir) -> "src/model";
option_default(out_dir)  -> "ebin";
option_default(source_ext) -> ".erl";
option_default(recursive) -> false;
option_default(compiler_options) -> [verbose, return_errors].

compiler_options(ErlOpts, BossDbOpts) ->
    set_debug_info_option(proplists:get_value(debug_info, ErlOpts), option(compiler_options, BossDbOpts)).

set_debug_info_option(true, BossCompilerOptions) ->
    [debug_info | BossCompilerOptions];
set_debug_info_option(undefined, BossCompilerOptions) ->
    BossCompilerOptions.

compile_model(Source, Target, BossDbOpts, RebarConfig) ->
    ErlOpts = rebar_config:get(RebarConfig, erl_opts, []),
    RecordCompilerOpts = [{out_dir, filename:dirname(Target)}, {compiler_options, compiler_options(ErlOpts, BossDbOpts)}],
    case boss_record_compiler:compile(Source, RecordCompilerOpts) of
        {ok, _Mod} ->
            ok;
        {ok, _Mod, Ws} ->
            rebar_base_compiler:ok_tuple(RebarConfig, Source, Ws);
        {error, Es, Ws} ->
            rebar_base_compiler:error_tuple(RebarConfig, Source,
                Es, Ws, RecordCompilerOpts)
    end.

%% @private
%% @doc copies `.erl' files into the `.eunit' directory
eunit_coverage_helper(ModelDir) ->
    {ok, Filenames} = file:list_dir(ModelDir),
    lists:foreach(
        fun(Filename) ->
            copy_model_to_eunit_dir(Filename, ModelDir)
        end,
        Filenames
    ).

%% @private
copy_model_to_eunit_dir(Filename, ModelDir) ->
    %% We only want to copy erl files. If it's something else,
    %% don't touch it.
    case is_erl_file(Filename) of
        true ->
            Source = filename:join(ModelDir, Filename),
            Destination = filename:join(".eunit", Filename),
            file:copy(Source, Destination);
        false ->
            ok
    end.

%% @private
%% @doc simple helper to determine whether this file is an
%% erlang file
is_erl_file(Filename) ->
    filename:extension(Filename) =:= ".erl".

%% @private
%% @doc Modifications are only needed if both coverage is enabled
%% and the models are not in the src directory.
coverage_modifications_needed(RebarConf, BossDbOpts) ->
    coverage_enabled(RebarConf) and model_dir_is_not_src(BossDbOpts).

%% @private
%% @doc We only want to copy the models if they are stored outside of
%% the src directory.
model_dir_is_not_src(BossDbOpts) ->
    ModelsDir = option(model_dir, BossDbOpts),
    [FirstDir | _] = filename:split(ModelsDir),
    FirstDir =/= "src".
