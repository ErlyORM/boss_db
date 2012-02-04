BossDB: A sharded, caching, evented ORM for Erlang
==================================================

Complete API references
-----------------------

Querying: http://www.chicagoboss.org/api-db.html
Records: http://www.chicagoboss.org/api-record.html
BossNews: http://chicagoboss.org/api-news.html

Usage
-----

    boss_db:start(DBOptions),
    boss_cache:start(CacheOptions), % If you want cacheing with Memcached
    boss_news:start() % If you want events

    DBOptions = [
        {adapter, mock | tyrant | riak | mysql | pgsql | mnesia | mongodb},
        {db_host, HostName::string()},
        {db_port, PortNumber::integer()},
        {db_username, UserName::string()},
        {db_password, Password::string()},
        {shards, [
            {db_shard_models, [ModelName::atom()]},
            {db_shard_id, ShardId::atom()},
            {db_host, _}, {db_port, _}, ...
        ]},
        {cache_enable, true | false},
        {cache_exp_time, TTLSeconds::integer()}
    ]

    CacheOptions = [
        {adapter, memcached_bin}, % More in the future
        {cache_servers, [{HostName::string(), Port::integer(), Weight::integer()}]}
    ]

Introduction
------------

BossDB is a compiler chain and run-time library for accessing a database via
Erlang parameterized modules. It solves the age-old problem of retrieving
named fields without resorting to verbosities like proplists:get_value/2 or
dict:find/2. For example, if you want to look up a puppy by ID and print its
name, you would write:

    Puppy = boss_db:find("puppy-1"),
    io:format("Puppy's name: ~p~n", [Puppy:name()]).

Functions for accessing field names are generated automatically. All you need
to do is create a model file and compile it with boss_record_compiler. Example:

The model file, call it puppy.erl:

    -module(puppy, [Id, Name, BreedId]).

Then compile it like:

    {ok, puppy} = boss_record_compiler:compile("puppy.erl")

...and you're ready to go.

BossDB supports database associations. Suppose you want to model the dog breed
(golden retriever, poodle, etc). You would create a model file with a special
"-has" attribute, like:

    -module(breed, [Id, Name]).
    -has({puppies, many}).

Then back in puppy.erl you'd add a "-belongs_to" attribute:

    -module(puppy, [Id, Name, BreedId]).
    -belongs_to(breed).

Once you've compile breed.erl with boss_record_compiler, you can print a puppy's
associated breed like:

    Breed = Puppy:breed(),
    io:format("Puppy's breed: ~p~n", [Breed:name()]).

Similarly, you could iterate over all the puppies of a particular breed:

    Breed = boss_db:find("breed-47"),
    lists:map(fun(Puppy) -> 
            io:format("Puppy: ~p~n", [Puppy:name()]) 
        end, Breed:puppies())

You can achieve the same thing with a database query, for example:

    Puppies = boss_db:find(puppy, [{breed_id, 'equals', "breed-47"}])

This is somewhat verbose. If you compile the source file with boss_compiler,
you'll be able to write the more simple expression:

    Puppies = boss_db:find(puppy, [breed_id = "breed-47"])

BossDB supports many query operators; see the API references at the top.

To create and save a new record, you would write:

    Breed = breed:new(id, "Golden Retriever"),
    {ok, SavedBreed} = Breed:save()

You can provide validation logic by adding a validation_tests/0 function
to your model file, e.g.

    -module(breed, [Id, Name]).
    -has({puppies, many}).
    -export([validation_tests/0]).

    validation_tests() ->
        [{fun() -> length(Name) > 0 end,
            "Name must not be empty!"}].

If validation fails, the save/0 function will return a list of error messages
instead of the saved record.


Events
------

BossDB provides two kinds of model events: synchronous save hooks, and
asynchronous notifications via BossNews. Save hooks are simple; just
define one or more of these functions in your model file:

    before_create/0 -> ok | {ok, ModifiedRecord} | {error, Reason}
    before_update/0 -> ok | {ok, ModifiedRecord} | {error, Reason}
    after_create/0 
    after_update/0
    before_delete/0 -> ok | {error, Reason}

BossNews is more complicated but also more powerful. It is a notification
system that executes asynchronously, so the code that calls "save" does
not have to wait for callbacks to complete. The central concept in BossNews
is a "watch", which is an event observer. You can create and destroy watches
programmatically:

    {ok, WatchId} = boss_news:watch(TopicString, CallBack),
    boss_news:cancel_watch(WatchId)

You can watch individual records or collections of records for changes, which
is handy for providing real-time notifications or alerts. For details see
the documentation at http://www.chicagoboss.org/api-news.html
