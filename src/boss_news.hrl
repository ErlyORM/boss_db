-record(state, {
        watch_dict		= dict:new() ::dict(),
        ttl_tree		= gb_trees:empty() ::gb_tree(),

        set_watchers		= dict:new()  ::dict(), 
        id_watchers		= dict:new()  ::dict(),

        set_attr_watchers	= dict:new()  ::dict(),
        id_attr_watchers	= dict:new()  ::dict(),
        watch_counter		= 0           ::integer()}).

-record(watch, {
        watch_list		= [],
        callback   ::news_callback() ,
        user_info  ::user_info(),
        exp_time   ::calendar:datetime1970(),
        ttl        :: 0..120}).
-type news_callback()	:: fun((event(),event_info()) ->any()) | fun((event(),event_info(), user_info())-> any()).
-type event()		:: any().
-type event_info()	:: any().
-type user_info()	:: any().
-type watch_id()        :: string().
