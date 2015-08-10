-module(boss_news_controller_util).

-ifdef(TEST).
-compile(export_all).
-endif.

-export([execute_callback/5]).

-export([process_dict/3]).

-export([created_news_process/5]).

-export([delete_news_watchers/5]).

-export([news_update_controller_inner_1/7]).

-export([process_news_state/3]).

-export([prune_expired_entries/1]).
-include("boss_news.hrl").
-spec execute_callback(news_callback(),event(),event_info(),user_info(),_) -> pid().
-spec execute_fun(news_callback(),event(),event_info(),user_info()) -> any().


execute_callback(Fun, Event, EventInfo, UserInfo, WatchId) when is_function(Fun) ->
    erlang:spawn(fun() ->
			 Result = execute_fun(Fun, Event, EventInfo, UserInfo),
			 case Result of
			     {ok, cancel_watch} -> 
				 boss_news:cancel_watch(WatchId);
			     {ok, extend_watch} -> 
				 boss_news:extend_watch(WatchId);
			     _ ->
                        ok
			 end
		 end).


execute_fun(Fun, Event, EventInfo, UserInfo) ->
    case proplists:get_value(arity, erlang:fun_info(Fun)) of
	2 ->
	    Fun(Event, EventInfo);
	3 ->
	    Fun(Event, EventInfo, UserInfo)
    end.

%% this one first
-spec(process_dict( watch_id(), dict:dict(), string()) -> dict:dict()).
process_dict(WatchId, Dict, TopicString) ->
    case dict:fetch(TopicString, Dict) of
	[WatchId] ->
	    dict:erase(TopicString, Dict);
	Watchers ->
	    dict:store(TopicString, lists:delete(WatchId, Watchers), Dict)
    end.

-spec created_news_process(_,_,_,_,_) -> {'ok',_}.
-spec created_news_process_inner_1(_,_,[any()],_) -> any().
-spec created_news_process_inner_2(_,_,_,_,[any()],_,_) -> any().


created_news_process(Id, Attrs, State, PluralModel, _Watchers = {ok, SetWatchers} ) ->
    Record   = boss_news_controller:activate_record(Id, Attrs),
    NewState = created_news_process_inner_1(State, PluralModel, SetWatchers,
				            Record),
    {ok, NewState};
created_news_process(_Id, _Attrs, State, _PluralModel, _Watchers ) ->
    {ok, State}.

created_news_process_inner_1(State, PluralModel, SetWatchers, Record) ->
    lists:foldr(fun(WatchId, Acc0) ->
			#watch{watch_list = WatchList,
			       callback   = CallBack,
			       user_info  = UserInfo} = dict:fetch(WatchId, State#state.watch_dict),
			created_news_process_inner_2(PluralModel, Record,
					             WatchId, Acc0, WatchList,
					             CallBack, UserInfo)
                end, State, SetWatchers).


created_news_process_inner_2(PluralModel, Record, WatchId, Acc0,
			     WatchList, CallBack, UserInfo) ->
    lists:foldr(fun({set, TopicString}, Acc1) when TopicString =:= PluralModel ->
			execute_callback(CallBack, created, Record, UserInfo, WatchId),
			Acc1;
		   (_, Acc1) ->
                        Acc1
                end, Acc0, WatchList).

-spec delete_news_watchers(_,_,_,_,_) -> {'ok',_}.
-spec delete_news_watchers_inner(_,_,_,[any()],_) -> any().
-spec delete_news_watchers_inner_1(_,_,_,_,_,[any()],_,_) -> any().



delete_news_watchers(Id, OldAttrs, State, PluralModel, _Watchers = {ok, SetWatchers}) ->
    Record   = boss_news_controller:activate_record(Id, OldAttrs),
    NewState = delete_news_watchers_inner(Id, State, PluralModel,
				          SetWatchers, Record),
    {ok, NewState};
delete_news_watchers(_Id, _OldAttrs, State, _PluralModel, _Watchers ) ->
    {ok, State}.


delete_news_watchers_inner(Id, State, PluralModel, SetWatchers,
			   Record) ->
    lists:foldr(fun(WatchId, Acc0) ->
			#watch{watch_list = WatchList,
			       callback       = CallBack,
			       user_info      = UserInfo} = dict:fetch(WatchId, State#state.watch_dict),
			delete_news_watchers_inner_1(Id, PluralModel, Record,
					             WatchId, Acc0, WatchList,
					             CallBack, UserInfo)
                end, State, SetWatchers).


delete_news_watchers_inner_1(Id, PluralModel, Record, WatchId, Acc0,
			     WatchList, CallBack, UserInfo) ->
    lists:foldr(fun
		    ({set, TopicString}, Acc1) when TopicString =:= PluralModel ->
			execute_callback(CallBack, deleted, Record, UserInfo, WatchId),
			Acc1;
		    ({id, TopicString}, Acc1) when TopicString =:= Id ->
			execute_callback(CallBack, deleted, Record, UserInfo, WatchId),
			Acc1;
                    (_, Acc1) ->
                        Acc1
                end, Acc0, WatchList).
-spec news_update_controller_inner_1(_,_,_,_,_,[any()],_) -> any().
-spec news_update_controller_inner_2(_,_,_,[any()],_,_,_,_,_,_) -> any().
-spec news_update_controller_inner_3(_,_,_,_,_,_,_,_,_,[any()],_,_) -> any().



news_update_controller_inner_1(Id, State, Module, AllWatchers,
                               NewRecord, OldAttributes, NewAttributes) ->
    lists:foldr(fun
		    ({Key, OldVal}, Acc0) ->
			KeyString = atom_to_list(Key),
			case proplists:get_value(Key, NewAttributes, OldVal) of
			    OldVal -> Acc0;
			    NewVal ->
				news_update_controller_inner_2(Id, State, Module,
							       AllWatchers, NewRecord,
							       Key, OldVal, Acc0,
							       KeyString, NewVal)
			end
                end, State, OldAttributes).



news_update_controller_inner_2(Id, State, Module, AllWatchers,
                               NewRecord, Key, OldVal, Acc0, KeyString,
                               NewVal) ->
    lists:foldr(fun(WatchId, Acc1) ->
                #watch{watch_list = WatchList,
                   callback = CallBack,
                   user_info = UserInfo} = dict:fetch(WatchId, State#state.watch_dict),
                news_update_controller_inner_3(Id, Module,
                                               NewRecord,
                                               Key, OldVal,
                                               KeyString,
                                               NewVal,
                                               WatchId,
                                               Acc1,
                                               WatchList,
                                               CallBack,
                                               UserInfo)
                end, Acc0, AllWatchers).

news_update_controller_inner_3(Id, Module, NewRecord, Key, OldVal,
                               KeyString, NewVal, WatchId, Acc1, WatchList,
                               CallBack, UserInfo) ->
    lists:foldr(fun
		    ({id_attr, ThisId, Attr}, Acc2) when ThisId =:= Id, Attr =:= KeyString ->
                execute_callback(CallBack, updated, {NewRecord, Key, OldVal, NewVal}, UserInfo, WatchId),
                Acc2;
            ({id_attr, ThisId, "*"}, Acc2) when ThisId =:= Id ->
                                execute_callback(CallBack, updated, {NewRecord, Key, OldVal, NewVal}, UserInfo, WatchId),
                                Acc2;
                    ({set_attr, ThisModule, Attr}, Acc2) when ThisModule =:= Module, Attr =:= KeyString ->
                        execute_callback(CallBack, updated, {NewRecord, Key, OldVal, NewVal}, UserInfo, WatchId),
                        Acc2;
                    ({set_attr, ThisModule, "*"}, Acc2) when ThisModule =:= Module ->
                        execute_callback(CallBack, updated, {NewRecord, Key, OldVal, NewVal}, UserInfo, WatchId),
                        Acc2;
                    (_, Acc2) -> Acc2
                end, Acc1, WatchList).

process_news_state( WatchId, StateAcc, WatchList) ->
    lists:foldr(fun
		    ({id, TopicString}, Acc) ->
			NewDict = process_dict(WatchId, StateAcc#state.id_watchers, TopicString),
			Acc#state{id_watchers = NewDict};
		    ({set, TopicString}, Acc) ->
			NewDict = process_dict(WatchId, StateAcc#state.set_watchers, TopicString),
			Acc#state{set_watchers = NewDict};
		    ({id_attr, Id, _Attr}, Acc) ->
			NewDict = process_dict(WatchId, StateAcc#state.id_attr_watchers, Id),
			Acc#state{id_attr_watchers = NewDict};
		    ({set_attr, Module, _Attr}, Acc) ->
			NewDict = process_dict(WatchId, StateAcc#state.set_attr_watchers, Module),
			Acc#state{set_attr_watchers = NewDict};
		    (_, Acc) ->
			Acc
                end, StateAcc, WatchList).



%src/boss_news_controller_util.erl:237:
% Record construction #state{watch_dict::dict(),ttl_tree::{number(),'nil' | {_,_,'nil' | {_,_,_,_},_}},set_watchers::dict(),id_watchers::dict(),set_attr_watchers::dict(),id_attr_watchers::dict(),watch_counter::integer()}%
%violates the declared type of field ttl_tree::gb_tree()
-spec prune_expired_entries(#state{ttl_tree::gb_trees:tree()}) -> #state{ttl_tree::gb_trees:tree()}.
prune_expired_entries(#state{ ttl_tree = Tree } = State) ->
    Now                 = boss_news_controller:future_time(0),
    {NewState, NewTree} = tiny_pq:prune_collect_old(fun prune_itterator/2, State, Tree, Now),
    NewState#state{ ttl_tree = NewTree }.


-spec(prune_itterator(watch_id(), #state{}) -> #state{}).
prune_itterator(WatchId, StateAcc) ->
    #watch{ watch_list = WatchList } = dict:fetch(WatchId, StateAcc#state.watch_dict),
    NewState = process_news_state(WatchId, StateAcc, WatchList),
    NewState#state{ watch_dict = dict:erase(WatchId, StateAcc#state.watch_dict) }.
