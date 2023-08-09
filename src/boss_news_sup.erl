-module(boss_news_sup).
-behaviour(supervisor).

-export([
         start_link/0,
         start_link/1,
         init/1,
         is_started/0
        ]).

start_link() ->
    start_link([]).

start_link(StartArgs) ->
    supervisor:start_link({global, ?MODULE}, ?MODULE, StartArgs).

is_started() ->
    global:whereis_name({global, ?MODULE}) /= {error, no_proc}.

init(StartArgs) ->
    {ok, {{one_for_one, 10, 10}, [
                {news_controller, {boss_news_controller, start_link, [StartArgs]},
                    permanent,
                    2000,
                    worker,
                    [boss_news_controller]}
                ]}}.
