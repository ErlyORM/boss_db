
ERL=erl
REBAR=./rebar
DB_CONFIG_DIR=priv/test_db_config

.PHONY: deps get-deps test

all:
	@$(REBAR) get-deps
	@$(REBAR) compile

boss_db:
	@$(REBAR) compile skip_deps=true

clean:
	@$(REBAR) clean

get-deps:
	@$(REBAR) get-deps

deps:
	@$(REBAR) compile

test:
	@$(REBAR) skip_deps=true eunit

compile_db_test:
	@$(REBAR) clean skip_deps=true
	@$(REBAR) --config "rebar.test.config" compile skip_deps=true

test_db_mock: compile_db_test
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/mock -noshell

test_db_mysql: compile_db_test
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/mysql -noshell

test_db_pgsql:
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/pgsql -noshell

test_db_mongodb:
	echo "db.boss_db_test_models.remove();"|mongo boss_test
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/mongodb -noshell

test_db_riak:
	$(ERL) -pa ebin -pa deps/*/ebin -run boss_db_test start -config $(DB_CONFIG_DIR)/riak -noshell
