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

%% @doc A pre-compile hook to compile boss_db models
pre_compile(Config, _AppFile) ->
    Opts = rebar_config:get(Config, boss_db_opts, []),
    SourceDir = option(model_dir, Opts),
    SourceExt = option(source_ext, Opts),
    TargetDir = option(out_dir, Opts),
    TargetExt = ".beam",
    rebar_base_compiler:run(Config, [],
        SourceDir,
        SourceExt,
        TargetDir,
        TargetExt,
        fun(S, T, _C) ->
            compile_model(S, T, Opts, Config)
        end,
        [{check_last_mod, true},
        {recursive, option(recursive, Opts)}]).

%% --------------------------------------------------------------------
%% Internal functions
%% --------------------------------------------------------------------

option(Opt, Opts) ->
    proplists:get_value(Opt, Opts, option_default(Opt)).

option_default(model_dir) -> "src/model";
option_default(out_dir)  -> "ebin";
option_default(source_ext) -> ".erl";
option_default(recursive) -> false;
option_default(compiler_options) -> [verbose, return_errors].

compiler_options(ErlOpts, Opts) ->
    set_debug_info_option(proplists:get_value(debug_info, ErlOpts), option(compiler_options, Opts)).

set_debug_info_option(true, BossCompilerOptions) ->
    [debug_info | BossCompilerOptions];
set_debug_info_option(undefined, BossCompilerOptions) ->
    BossCompilerOptions.

compile_model(Source, _Target, Opts, RebarConfig) ->
    ErlOpts = rebar_config:get(RebarConfig, erl_opts, []),
    RecordCompilerOpts = [{out_dir, option(out_dir, Opts)}, {compiler_options, compiler_options(ErlOpts, Opts)}],
    boss_record_compiler:compile(Source, RecordCompilerOpts),
    ok.
