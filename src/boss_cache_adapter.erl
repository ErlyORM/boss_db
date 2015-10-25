-module(boss_cache_adapter).

-callback start() -> pid() | 'ok'.
-callback start(_) -> pid() | 'ok'.
-callback stop(atom() | pid() | {atom(),_} | {'via',_,_}) -> 'ok'.
-callback init(_) -> {'ok', pid() | undefined}.
-callback terminate(atom() | pid() | {atom(),_} | {'via',_,_}) -> 'ok'.
-callback get(_,atom() | string() | number(),_) -> any().
-callback set(_,atom() | string() | number(),_,_,non_neg_integer()) -> 'ok'.
-callback delete(atom() | pid() | {atom(),_} | {'via',_,_},atom() | string() | number(),_) -> 'ok'.
