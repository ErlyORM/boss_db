-module(boss_db_app_sup).
-author('mjtruog [at] gmail (dot) com').

-behaviour(supervisor).

-export([start_link/1,
         init/1]).

start_link(Env) when is_list(Env) ->
    supervisor:start_link(?MODULE, [Env]).

init([Env]) ->
    DBOptions = proplists:get_value(db_options, Env),
    BossCache = proplists:get_value(boss_cache, Env),
    BossNews = proplists:get_value(boss_news, Env),

    Children0 = [child_specification(boss_db, DBOptions, BossNews /= false)],
    Children1 = if
        is_list(BossCache) ->
            Children0 ++ [child_specification(boss_cache, BossCache)];
        BossCache =:= false ->
            Children0
    end,
    Children = if
        BossNews =:= true ->
            Children1 ++ [child_specification(boss_news)];
        BossNews =:= false ->
            Children1
    end,
    MaxRestarts = 5,
    MaxTime = 60, % seconds (1 minute)
    {ok,
     {{one_for_one, MaxRestarts, MaxTime}, Children}}.

child_specification(boss_db, DBOptions0, BossNews) ->
    true = is_boolean(BossNews),
    DBOptions = lists:keystore(boss_news, 1, DBOptions0,
                               {boss_news, BossNews}),
    {boss_db_sup,
     {boss_db_sup, start_link, [DBOptions]},
     permanent, infinity, supervisor, [boss_db_sup]}.

child_specification(boss_cache, BossCache) ->
    {boss_cache_sup,
     {boss_cache_sup, start_link, [BossCache]},
     permanent, infinity, supervisor, [boss_cache_sup]}.

child_specification(boss_news) ->
    {boss_news_sup,
     {boss_news_sup, start_link, []},
     permanent, infinity, supervisor, [boss_news_sup]}.
