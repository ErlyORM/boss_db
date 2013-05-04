-module(boss_db_app_sup).
-author('mjtruog [at] gmail (dot) com').

-behaviour(supervisor).

-export([start_link/1,
         init/1]).

start_link(Env) when is_list(Env) ->
    supervisor:start_link(?MODULE, [Env]).

init([Env]) ->
    DBOptions = proplists:get_value(db_options, Env),
    CacheOptions = proplists:get_value(cache_options, Env, []),
    BossCache = case proplists:get_value(cache_enable, Env) of
        undefined ->
            proplists:get_value(cache_enable, DBOptions);
        Value ->
            Value
    end,
    BossNews = proplists:get_value(news_enable, Env),

    Children0 = [child_specification(boss_db, DBOptions, BossCache, BossNews)],
    Children1 = if
        BossCache =:= true ->
            Children0 ++ [child_specification(boss_cache, CacheOptions)];
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

child_specification(boss_db, DBOptions0, BossCache, BossNews) ->
    true = is_boolean(BossNews),
    DBOptions1 = lists:keystore(news_enable, 1, DBOptions0,
                                {news_enable, BossNews}),
    true = is_boolean(BossCache),
    DBOptions = lists:keystore(cache_enable, 1, DBOptions1,
                               {cache_enable, BossCache}),
    {boss_db_sup,
     {boss_db_sup, start_link, [DBOptions]},
     permanent, infinity, supervisor, [boss_db_sup]}.

child_specification(boss_cache, CacheOptions) ->
    {boss_cache_sup,
     {boss_cache_sup, start_link, [CacheOptions]},
     permanent, infinity, supervisor, [boss_cache_sup]}.

child_specification(boss_news) ->
    {boss_news_sup,
     {boss_news_sup, start_link, []},
     permanent, infinity, supervisor, [boss_news_sup]}.
