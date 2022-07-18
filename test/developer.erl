-include_lib("pmod_transform/include/pmod.hrl").
-module(developer, [Id, Name, Country]).

attribute_types() -> 
	[{name, string}, {country, string}]. 