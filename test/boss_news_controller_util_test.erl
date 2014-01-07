-module(boss_news_controller_util_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-include("../src/boss_news.hrl").

'prop_process dict single entry'() ->
    ?FORALL({WatchId, TopicString, Dict0},
            {string(), string(), [{string(), [string()]}]},
            begin
                Dict1 = dict:from_list(Dict0),
                Dict2 = dict:store(TopicString, [WatchId], Dict1),
                RDict = boss_news_controller_util:process_dict(WatchId, Dict2,TopicString),
                false =:= dict:is_key(TopicString, RDict)
            end).

all([true]) ->true;
all([false|_Rest] ) ->
    false;
all([true|Rest]) ->
    all(Rest).

keylen(Dict, Key) ->
    case  dict:find(Key, Dict) of
        {ok, List} -> length(List);
        _ -> 0
    end.
all_test() ->
    ?assert(all([true])),
    ?assert(all([true, true, true])),
    ?assertNot(all([true,false])),
    ?assertNot(all([true,false, true])),
    ?assertNot(all([true,true, false, true])),
    ?assertNot(all([true,true,false, false, true,false])),
    ?assert(proper:quickcheck(prop_all(),
                             [{to_file,user}])),
    ok.

prop_all() ->
    ?FORALL(CondList, 
            [boolean()],
            begin
                IsAllTrue = lists:all(fun(X) -> X end, CondList),
                IsAllTrue =:= all(CondList)
            end).


'prop_process dict list entry'() ->
    ?FORALL({WatchList, TopicString, Dict0},
            {[nonempty_string()|[nonempty_string()]], nonempty_string(), [{nonempty_string(), [nonempty_string()]}]},
            begin
                Dict1   = dict:from_list(Dict0),
                WatchP  = random:uniform(length(WatchList)),
                WatchId = lists:nth(WatchP, WatchList),

                Dict2   = dict:store(TopicString, WatchList, Dict1),
              
                RDict   = boss_news_controller_util:process_dict(WatchId, Dict2,TopicString),
                all([ keylen(RDict, TopicString) +1 =:= keylen(Dict2, TopicString),
                      true])
            end).

process_dict_test() ->
    ?assert(proper:quickcheck('prop_process dict single entry'(),
                              [{to_file,user},
                               {spec_timeout, infinity}])),
    ?assert(proper:quickcheck('prop_process dict list entry'(),
                              [{to_file,user},
                               {spec_timeout, infinity}])),
    ok.

                            

prop_prune_itterator() ->
    ?FORALL({WatchId,WatchList}, 
            {nonempty_string(),#watch{}},
            begin
                Dict      = dict:from_list([{WatchId, WatchList}]),
                InitState = #state{watch_dict = Dict},
                
                #state{watch_dict = NewDict} = boss_news_controller_util:prune_itterator(WatchId,
                                                                                         InitState),
              
                not(dict:is_key(WatchId, NewDict))
            end).

prune_itterator_test() ->
    ?assert(proper:quickcheck(prop_prune_itterator(),
                              [{to_file,user}])),
    ok.
