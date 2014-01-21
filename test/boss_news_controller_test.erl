-module(boss_news_controller_test).
-include_lib("proper/include/proper.hrl").
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
    

    
init_test() ->
    ?assert(proper:check_spec({boss_news_controller,init, 1},
			     [{to_file, user}])),
    ok.

