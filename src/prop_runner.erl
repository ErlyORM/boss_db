-module(prop_runner).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").


-compile(export_all).


gen(Tests, TModule) ->
    gen(Tests, TModule, 100, 12).
gen(Tests, TModule,Count,PCount) ->
    {inparallel, PCount,
     begin
         [case Test of
              {Funct, Arity} ->
                  ?_assert(proper:check_spec({TModule, Funct, Arity},
                                            [{to_file, user}, Count]));

              F when is_function(F,0) ->
                  ?_assert(proper:quickcheck(F(),
                                             [{to_file, user},Count]))
          end||Test <-Tests]
     end}.
