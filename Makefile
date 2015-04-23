.PHONY: all compile deps clean test devrel rel

# https://registry.hub.docker.com/u/tray/dynamodb-local/

REBAR_CONFIG = rebar.config
APP_NAME = ddb

all: clean deps test

deps: get-deps update-deps
	@./rebar -C $(REBAR_CONFIG) compile

update-deps:
	@./rebar -C $(REBAR_CONFIG) update-deps

get-deps:
	@./rebar -C $(REBAR_CONFIG) get-deps

compile:
	@./rebar -C $(REBAR_CONFIG) compile skip_deps=true
	@./rebar -C $(REBAR_CONFIG) xref skip_deps=true


dynamodb_local_start:
	docker run -d -i -t --name dynamodb-local -p 8000:8000 tray/dynamodb-local -inMemory -port 8000

dynamodb_local_stop:
	docker stop dynamodb-local
	docker rm dynamodb-local

rel: compile
	mkdir -p dev
	mkdir -p deps
	(cd rel && rm -rf ../dev/$(APP_NAME) && ../rebar generate && ../rebar generate target_dir=../dev/$(APP_NAME))

test: compile
	rm -rf .eunit
	@./rebar -C $(REBAR_CONFIG) eunit skip_deps=true

clean:
	@./rebar -C $(REBAR_CONFIG) clean skip_deps=true

distclean: clean
	@./rebar -C $(REBAR_CONFIG) clean
	@./rebar -C $(REBAR_CONFIG) delete-deps
	rm -rf dev

dialyze-init:
	dialyzer --build_plt --apps erts kernel stdlib mnesia crypto public_key snmp reltool
	dialyzer --add_to_plt --plt ~/.dialyzer_plt --output_plt $(APP_NAME).plt -c .
	dialyzer -c ebin -Wunmatched_returns -Werror_handling -Wrace_conditions -Wunderspecs

dialyze: compile
	dialyzer --check_plt --plt $(APP_NAME).plt -c .
	dialyzer -c ebin
