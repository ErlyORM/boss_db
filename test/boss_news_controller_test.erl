-module(boss_news_controller_test).
-include("std.hrl").

-record(state, {
        watch_dict		= dict:new()       ::dict(),
        ttl_tree		= gb_trees:empty() ::gb_tree(),

        set_watchers		= dict:new()       ::dict(), 
        id_watchers		= dict:new()       ::dict(),

        set_attr_watchers	= dict:new()       ::dict(),
        id_attr_watchers	= dict:new()       ::dict(),
        watch_counter		= 0                ::integer()}).

prop_make_id_watchers() ->
    ?FORALL({AttrWatchers0, Module, Value},
            {dict(), binary(), binary()},
            ?IMPLIES((dict:size(AttrWatchers0) < 30),
                     begin
                         AttrWatchers1 = dict:store(Module, Value,AttrWatchers0),
                         State = #state{id_attr_watchers=AttrWatchers1},
                         R = boss_news_controller:make_id_watchers(Module,State
                                                                   ),
                         R =:= Value
                     end)).

prop_make_id_watchers_null() ->
    ?FORALL({AttrWatchers0, Module},
            {dict(), binary()},
            ?IMPLIES((not (dict:is_key(Module, AttrWatchers0))),
                     begin
                         State = #state{id_attr_watchers=AttrWatchers0},
                         [] =:= boss_news_controller:make_id_watchers(Module,State )
                     end)).

prop_make_wildcard_watchers() ->
    ?FORALL({AttrWatchers0, Module, Value},
            {dict(), binary(), binary()},
            ?IMPLIES((dict:size(AttrWatchers0) < 30),
                     begin
                         AttrWatchers1 = dict:store(Module, Value,AttrWatchers0),
                         State = #state{set_attr_watchers=AttrWatchers1},
                         R = boss_news_controller:make_wildcard_watchers(State, Module),
                         R =:= Value
                     end)).

prop_make_wildcard_watchers_null() ->
    ?FORALL({AttrWatchers0, Module},
            {dict(), binary()},
            ?IMPLIES(not(dict:is_key(Module, AttrWatchers0)),
                     begin
                         []=:= boss_news_controller:make_wildcard_watchers(#state{set_attr_watchers=AttrWatchers0}, Module)
                     end)).


    
prop_test_ () ->
     gen([
          fun prop_make_id_watchers/0,
          fun prop_make_id_watchers_null/0,
          fun prop_make_wildcard_watchers/0,
          fun prop_make_wildcard_watchers_null/0,
          {init, 1},
          {future_time, 1}
         ],
         boss_news_controller,25, 8).


execute_function_test() ->
    Event	= event,
    EventInfo	= event_info,
    UserInfo	= user_info,
    {ok, 2}	= boss_news_controller_util:execute_fun(fun lambda/2,
						   Event, EventInfo, UserInfo),
    {ok, 3}	= boss_news_controller_util:execute_fun(fun lambda/3,
						   Event, EventInfo, UserInfo).

lambda(_Event, _EventInfo) ->
    {ok,2}.
    
lambda(_Event, _EventInfo,_UserInfo) ->
    {ok,3}.
    

    

