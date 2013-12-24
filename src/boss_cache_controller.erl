-module(boss_cache_controller).

-behaviour(gen_server).

-export([start_link/0, start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
        adapter,
        connection
    }).

start_link() ->
    start_link([]).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Options) ->
    AdapterName = proplists:get_value(adapter, Options, memcached_bin),
    Adapter	= list_to_atom(lists:concat(["boss_cache_adapter_", AdapterName])),
    {ok, Conn}	= Adapter:init(Options),
    {ok, #state{ adapter = Adapter, connection = Conn }}.

handle_call({get, Prefix, Key}, 
	    _From, State = #state{adapter=Adapter, connection = Conn}) ->
    {reply, Adapter:get(Conn, Prefix, Key), State};
handle_call({set, Prefix, Key, Value, TTL},
	    _From, State = #state{adapter=Adapter, connection = Conn}) ->
    {reply, Adapter:set(Conn, Prefix, Key, Value, TTL), State};
handle_call({delete, Prefix, Key}, 
	    _From, State = #state{adapter=Adapter, connection = Conn}) ->
    {reply, Adapter:delete(Conn, Prefix, Key), State}.

handle_cast(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State = #state{adapter=Adapter, connection = Conn}) ->
    Adapter:terminate(Conn).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.
