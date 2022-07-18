-include_lib("pmod_transform/include/pmod.hrl").
-module(developer, [Id, Name, Country, CreatedAt]).
-compile(export_all).

attribute_types() -> 
	[{name, string}, {country, string}, {created_at, datetime}]. 
