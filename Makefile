ERL=erl
REBAR=./rebar3
GIT = git
REBAR_VER = 3.2.0
DB_CONFIG_DIR=priv/test_db_config

.PHONY: deps get-deps test

all: compile

compile:
	@$(REBAR) compile

boss_db:
	@$(REBAR) compile skip_deps=true

rebar_src:
	@rm -rf $(PWD)/rebar_src
	@$(GIT) clone https://github.com/erlang/rebar3.git rebar_src
	@$(GIT) -C rebar_src checkout tags/$(REBAR_VER)
	@cd $(PWD)/rebar_src/; ./bootstrap
	@cp $(PWD)/rebar_src/rebar3 $(PWD)
	@rm -rf $(PWD)/rebar_src

get-deps:
	@$(REBAR) upgrade

deps:
	@$(REBAR) compile

dialyze:
	@$(REBAR) dialyzer

clean:
	@$(REBAR) clean
	rm -fv erl_crash.dump

test:
	@$(REBAR) skip_deps=true eunit

compile_db_test:
	@$(REBAR) clean skip_deps=true
	@$(REBAR) --config "rebar.test.config" compile skip_deps=true

test_db_mock: compile_db_test
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/mock -noshell

test_db_mysql: compile_db_test
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/mysql -noshell

test_db_pgsql: compile_db_test
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/pgsql -noshell

test_db_mongodb: compile_db_test
	echo "db.boss_db_test_models.remove();"|mongo boss_test
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/mongodb -noshell

test_db_riak: compile_db_test
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/riak -noshell
