-module(boss_db_adapter_types).
-type model_operator() :: 'not_matches'|matches|contains|not_contains|contains_all|not_contains_all|
			    contains_any| contains_none|in|not_in.
-export_type([model_operator/0]).
