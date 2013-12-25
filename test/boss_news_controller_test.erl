-module(boss_news_controller_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

lambda(_Event, _EventInfo) ->
    {ok,2}.
    
lambda(_Event, _EventInfo,_UserInfo) ->
    {ok,3}.
    


execute_function_test() ->
    Event	= event,
    EventInfo	= event_info,
    UserInfo	= user_info,
    {ok, 2}	= boss_news_controller_util:execute_fun(fun lambda/2,
						   Event, EventInfo, UserInfo),
    {ok, 3}	= boss_news_controller_util:execute_fun(fun lambda/3,
						   Event, EventInfo, UserInfo).
    

    
