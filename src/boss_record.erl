-module(boss_record).
-export([new/2, new_from_json/2]).
-include_lib("proper/include/proper.hrl").


new(Model, Attributes) ->
    DummyRecord = boss_record_lib:dummy_record(Model),
    DummyRecord:set(Attributes).

-spec(new_from_json(module(), jsx:json_term()) -> tuple()).
new_from_json(Model, JSON) ->
    DummyRecord = boss_record_lib:dummy_record(Model),
    Attributes  = DummyRecord:attribute_names(),
    Set         = set_attribute(_,[{id,id}] ++ JSON, _),
    lists:foldl(Set, DummyRecord, Attributes).

set_attribute(FieldName, JSON, Model) ->
    BName = atom_to_binary(FieldName,'utf8'),
    Model:set(FieldName, proplists:get_value(BName, JSON, null)).
    
    
-spec(convert_attributes({string()|binary()|atom(), any()}) ->
     {atom(), any()}).
convert_attributes({Name, Value}) when is_list(Name) ->
    {list_to_existing_atom(Name), Value};
convert_attributes({Name, Value}) when is_binary(Name) ->
    {binary_to_existing_atom(Name, 'utf8'), Value};
convert_attributes({Name, Value}) ->
    {Name, Value}.

%-ifdef(TEST).
-type test_fields() :: id|name|full_name|private|html_url|description|fork|url|forks|forks_url|
		       keys_url|collaborators_url|teams_url|hooks_url|issue_events_url|events_url|
		       assignees_url|branches_url|tags_url|blobs_url|git_tags_url|git_refs_url|
		       trees_url|status_url|languages_url|star_gazers_url|commits_url|
		       git_commits_url|comments_url|issue_comment_url|contents_url|compare_url|
		       merge_url|archives_url|downloads_url|issues_url|pulls_url|milestores_url|
		       notifications_url|labels_url|releases_url|created_at|updated_at|pushed_at|
		       git_url|ssh_url|clone_url|home_page|size|stargazers_count|watchers_count|
		       language|has_issues|has_downloads|has_wiki|forks_count|mirror_url|
		       open_issues_count|forks|open_issues|watchers|default_branch|master_branch.
prop_set_attribute() ->
    DummyRecord = boss_record_lib:dummy_record(gh_repo),
    ?FORALL({Field, Value},
	    {test_fields(), union(binary(),integer(),boolean(), null)},
	    begin
		JSON		= [{atom_to_binary(Field, 'utf8'), Value}],
		NewModel	= set_attribute(Field, JSON, DummyRecord),
		Value		=:= NewModel:Field()
	    end).
		   
prop_set_attribute_data_missing() ->
    DummyRecord = boss_record_lib:dummy_record(gh_repo),
    ?FORALL({Field, Value},
	    {test_fields(), union(binary(),integer(),boolean(), null)},
	    begin
		JSON		= [{<<"BOGUS">>, Value}],
		NewModel	= set_attribute(Field, JSON, DummyRecord),
		null    	=:= NewModel:Field()
	    end).
	

prop_convert_attributes_l() ->
    ?FORALL({Name,Value}, 
	    { list(range(65,90)), binary()},
	    begin
	        RName           = list_to_atom(Name),
		{AName, ValueP} = convert_attributes({Name, Value}),
		all_true([is_atom(AName), ValueP =:=Value,RName =:= AName]) 
	    end).

prop_convert_attributes_a() ->
    ?FORALL({Name,Value}, 
	    { atom(), binary()},
	    begin

		{AName, ValueP} = convert_attributes({Name, Value}),
		all_true([is_atom(AName), ValueP =:=Value,Name =:= AName]) 
	    end).

prop_convert_attributes_b() ->
    ?FORALL({Name,Value}, 
	    { list(range(65,90)), binary()},
	    begin
		B		= list_to_binary(Name),
		RName		= binary_to_atom(B, 'utf8'),
		{AName, ValueP} = convert_attributes({B, Value}),
		all_true([is_atom(AName), ValueP =:=Value,RName =:= AName]) 
	    end).


-spec(all_true([boolean()]) -> boolean()).
all_true(L) ->
    lists:all(fun(X) ->
		      X 
	      end, L).
%-endif.
