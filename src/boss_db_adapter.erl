-module(boss_db_adapter).

%% TODO: exact types
-callback start(_) -> ok.
-callback stop() -> ok.
-callback init(_) -> any().
-callback terminate(_) -> any().
-callback find(_, _) -> any().
-callback find(_, _, _, _, _, _, _) -> any().
-callback count(_, _, _) -> any().
-callback delete(_, _) -> any().
-callback counter(_, _) -> any().
-callback incr(_, _, _) -> any().
-callback save_record(_, _) -> any().
