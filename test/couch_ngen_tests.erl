-module(couch_ngen_tests).


-include_lib("eunit/include/eunit.hrl").


couch_ngen_test_()->
    test_engine_util:create_tests(couch_ngen).
