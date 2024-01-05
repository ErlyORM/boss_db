-module(boss_news_controller).

-behaviour(gen_server).
-ifdef(TEST).
-compile(export_all).
-endif.
-record(state, {
        watch_dict              = dict:new()       ::dict:dict(),
        ttl_tree                = gb_trees:empty() ::gb_trees:tree(),

        set_watchers            = dict:new()       ::dict:dict(),
        id_watchers             = dict:new()       ::dict:dict(),

        set_attr_watchers       = dict:new()       ::dict:dict(),
        id_attr_watchers        = dict:new()       ::dict:dict(),
        watch_counter           = 0                ::integer()}).

-type news_callback()   :: fun((event(),event_info()) ->any()) | fun((event(),event_info(), user_info())-> any()).
-type event()           :: any().
-type event_info()      :: any().
-type user_info()       :: any().

-record(watch, {
        watch_list              = [],
        callback   ::news_callback() ,
        user_info  ::user_info(),
        exp_time,
        ttl        ::non_neg_integer()}).


-spec start_link()          -> 'ignore' | {'error',_} | {'ok',pid()}.
-spec start_link(_)         -> 'ignore' | {'error',_} | {'ok',pid()}.
-spec init([])               -> {'ok',  #state{}}.
-spec handle_call('dump' | 'reset' | {'cancel_watch',_} | {'extend_watch',_} | {'created',binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | []),_} | {'deleted',binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | []),_} | {'updated',binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | []),_,_} | {'watch',binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | []),_,_,number()} | {'set_watch',_,binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | []),_,_,number()},_,_) ->
                         {'reply',_,#state{ttl_tree::gb_trees:tree()}}.
-spec handle_cast(_,_)      -> {'noreply',_}.
-spec terminate(_,_)        -> 'ok'.
-spec code_change(_,_,_)    -> {'ok',_}.
-spec handle_info(_,_)      -> {'noreply',_}.
-spec future_time(non_neg_integer()) -> non_neg_integer().
-spec activate_record(binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | []),_) -> any().

-export([start_link/0, start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([activate_record/2]).

-export([future_time/1]).
-spec make_wildcard_watchers(
        #state{
               watch_dict::dict:dict(_,_),
               ttl_tree::gb_trees:tree(_,_),
               set_watchers::dict:dict(_,_),
               id_watchers::dict:dict(_,_),
               set_attr_watchers::dict:dict(_,_),
               id_attr_watchers::dict:dict(_,_),
               watch_counter::integer()
              },
        binary() | maybe_improper_list(any(),binary() | [])
                            ) -> any().


start_link() ->
    start_link([]).

start_link(Args) ->
    gen_server:start_link({global, boss_news}, ?MODULE, Args, []).

init(_Options) ->
    {ok, #state{}}.

handle_call(reset, _From, _State) ->
    {reply, ok, #state{}};

handle_call(dump, _From, State0) ->
    State = boss_news_controller_util:prune_expired_entries(State0),
    {reply, State, State};

handle_call({watch, TopicString, CallBack, UserInfo, TTL}, From, State0 = #state{watch_counter= WatchId}) ->
    {reply, RetVal, State} = handle_call({set_watch, WatchId, TopicString, CallBack, UserInfo, TTL}, From, State0),
    case RetVal of
        ok ->
            {reply, {ok, WatchId}, State#state{ watch_counter = WatchId + 1 }};
        Other ->
            {reply, Other, State0}
    end;
handle_call({set_watch, WatchId, TopicString, CallBack, UserInfo, TTL}, From, State0) ->
    {reply, _, State} = handle_call({cancel_watch, WatchId}, From, State0),
    ExpTime = future_time(TTL),
    {RetVal, NewState, WatchList} = lists:foldr(fun
            (SingleTopic, {ok, StateAcc, WatchListAcc}) ->
                case re:split(SingleTopic, "\\.", [{return, list}]) of
                    [Id, Attr] ->
                        [Module, IdNum] = re:split(Id, "-", [{return, list}, {parts, 2}]),
                        {NewState1, WatchInfo} = case IdNum of
                            "*" ->
                                SetAttrWatchers = case dict:find(Module, StateAcc#state.set_attr_watchers) of
                                    {ok, Val} -> Val;
                                    _ -> []
                                end,
                                {StateAcc#state{
                                        set_attr_watchers = dict:store(Module, [WatchId|SetAttrWatchers], StateAcc#state.set_attr_watchers)
                                    }, {set_attr, Module, Attr}};
                            _ ->
                                IdAttrWatchers = case dict:find(Id, StateAcc#state.id_attr_watchers) of
                                    {ok, Val} -> Val;
                                    _ -> []
                                end,
                                {StateAcc#state{
                                        id_attr_watchers = dict:store(Id, [WatchId|IdAttrWatchers], StateAcc#state.id_attr_watchers)
                                    }, {id_attr, Id, Attr}}
                        end,
                        {ok, NewState1, [WatchInfo|WatchListAcc]};
                    _ ->
                        case re:split(SingleTopic, "-", [{return, list}, {parts, 2}]) of
                            [_Module, _IdNum] ->
                                IdWatchers = case dict:find(SingleTopic, State#state.id_watchers) of
                                    {ok, Val} -> Val;
                                    _ -> []
                                end,
                                {ok, StateAcc#state{
                                        id_watchers = dict:store(SingleTopic, [WatchId|IdWatchers], StateAcc#state.id_watchers)
                                    }, [{id, SingleTopic}|WatchListAcc]};
                            [_PluralModel] ->
                                SetWatchers = case dict:find(SingleTopic, StateAcc#state.set_watchers) of
                                    {ok, Val} -> Val;
                                    _ -> []
                                end,
                                {ok, StateAcc#state{
                                        set_watchers = dict:store(SingleTopic, [WatchId|SetWatchers], StateAcc#state.set_watchers)
                                    }, [{set, SingleTopic}|WatchListAcc]}
                        end
                end;
            (_, Error) ->
                Error
        end, {ok, State, []}, re:split(TopicString, ", +", [{return, list}, {parts, 2}])),
    case RetVal of
        ok -> {reply, RetVal, NewState#state{
                    watch_dict = dict:store(WatchId,
                        #watch{
                            watch_list = WatchList,
                            callback = CallBack,
                            user_info = UserInfo,
                            exp_time = ExpTime,
                            ttl = TTL}, NewState#state.watch_dict),
                    ttl_tree = tiny_pq:insert_value(ExpTime, WatchId, NewState#state.ttl_tree)
                }};
        Error -> {reply, Error, State}
    end;
handle_call({cancel_watch, WatchId}, _From, State) ->
    {RetVal, NewState} = case dict:find(WatchId, State#state.watch_dict) of
        {ok, #watch{ exp_time = ExpTime }} ->
            NewTree = tiny_pq:move_value(ExpTime, 0, WatchId, State#state.ttl_tree),
            {ok, State#state{ ttl_tree = NewTree }};
        _ ->
            {{error, not_found}, State}
    end,
    {reply, RetVal, boss_news_controller_util:prune_expired_entries(NewState)};
handle_call({extend_watch, WatchId}, _From, State0) ->
    State = boss_news_controller_util:prune_expired_entries(State0),
    WatchF = dict:find(WatchId, State#state.watch_dict),
    {RetVal, NewState} = case WatchF of
        {ok, #watch{ exp_time = ExpTime, ttl = TTL } = Watch} ->
            NewExpTime = future_time(TTL),
            NewTree    = tiny_pq:move_value(ExpTime, NewExpTime, WatchId, State#state.ttl_tree),
            {ok, State#state{
                   ttl_tree   = NewTree,
                   watch_dict = dict:store(WatchId,
                                            Watch#watch{ exp_time = NewExpTime },
                                            State#state.watch_dict) }};
        _ ->
            {{error, not_found}, State}
    end,
    {reply, RetVal, NewState};
handle_call({created, Id, Attrs}, _From, State0) ->
    State                = boss_news_controller_util:prune_expired_entries(State0),
    [Module | _IdNum]   = re:split(Id, "-", [{return, list}, {parts, 2}]),
    PluralModel         = inflector:pluralize(Module),
    Watchers            = dict:find(PluralModel, State#state.set_watchers),
    {RetVal, State1}    = boss_news_controller_util:created_news_process(Id, Attrs, State,
                                                                         PluralModel, Watchers),
    {reply, RetVal, State1};
handle_call({deleted, Id, OldAttrs}, _From, State0) ->
    State                = boss_news_controller_util:prune_expired_entries(State0),
    [Module | _IdNum]   = re:split(Id, "-", [{return, list}, {parts, 2}]),
    PluralModel         = inflector:pluralize(Module),
    Watchers            = dict:find(PluralModel, State#state.set_watchers),
    {RetVal, State1}    = boss_news_controller_util:delete_news_watchers(Id, OldAttrs, State,
                                                                         PluralModel, Watchers),
    {reply, RetVal, State1};

handle_call({updated, Id, OldAttrs, NewAttrs}, _From, State0) ->
    State                = boss_news_controller_util:prune_expired_entries(State0),
    [Module | _IdNum]   = re:split(Id, "-", [{return, list}, {parts, 2}]),

    IdWatchers          = make_id_watchers(Id, State),
    WildcardWatchers    = make_wildcard_watchers(State, Module),

    AllWatchers         = IdWatchers ++ WildcardWatchers,

    OldRecord           = activate_record(Id, OldAttrs),
    OldAttributes       = OldRecord:attributes(),

    NewRecord           = activate_record(Id, NewAttrs),
    NewAttributes       = NewRecord:attributes(),

    NewState            = boss_news_controller_util:news_update_controller_inner_1(Id, State,
                                                                                   Module,
                                                                                   AllWatchers,
                                                                                   NewRecord,
                                                                                   OldAttributes,
                                                                                   NewAttributes),
    {reply, ok, NewState}.

make_wildcard_watchers(State, Module) ->
    case dict:find(Module, State#state.set_attr_watchers) of
        {ok, Val1} -> Val1;
        _ -> []
    end.

make_id_watchers(Id, State) ->
    case dict:find(Id, State#state.id_attr_watchers) of
        {ok, Val} -> Val;
        _ -> []
    end.

handle_cast(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.


future_time(TTL) ->
    {MegaSecs, Secs, _} = os:timestamp(),
    MegaSecs * 1000 * 1000 + Secs + TTL.

activate_record(Id, Attrs) ->
    [Module | _IdNum]   = re:split(Id, "-", [{return, list}, {parts, 2}]),
    Type                = list_to_existing_atom(Module),
    DummyRecord         = boss_record_lib:dummy_record(Type),
    apply(Type, new, lists:map(fun
                (id) -> Id;
                (Key) -> proplists:get_value(Key, Attrs)
            end, DummyRecord:attribute_names())).
