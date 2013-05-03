-module(boss_db_app).
-author('mjtruog [at] gmail (dot) com').

-behaviour(application).

%% application callbacks
-export([start/2, stop/1]).

%%%------------------------------------------------------------------------
%%% Callback functions from application
%%%------------------------------------------------------------------------

%%-------------------------------------------------------------------------
%% @doc
%% ===Start the BossDB application.===
%% @end
%%-------------------------------------------------------------------------

-spec start(StartType :: normal | {takeover, node()} | {failover, node()},
            StartArgs :: any()) ->
    {ok, Pid :: pid()} |
    {ok, Pid :: pid(), State :: any()} |
    {error, Reason :: any()}.

start(_, _) ->
    boss_db_app_sup:start_link(application:get_all_env()).

%%-------------------------------------------------------------------------
%% @doc
%% ===Stop the BossDB application.===
%% @end
%%-------------------------------------------------------------------------

-spec stop(State :: any()) ->
    'ok'.

stop(_) ->
    ok.

