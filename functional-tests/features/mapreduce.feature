Feature: MapReduce
    In order to execute MapReduce algorithm
    As developers
    We'll implement an algorithm for each data backend and parallelization driver

    Scenario: Riak and IPython
        Given I use the middleware "mapreduce+ipython:///riak/ipython?store_uri=kvstore%2Briak%3A%2F%2Flocalhost%3A8087%2Fdefault%2Friak_ipython%3Fprotocol%3Dpbc"
        When I process the values "test_riak_ipython" in "features/map.json"
        Then I get the value in "features/reduced.json"

    Scenario: Riak and multiprocessing
        Given I use the middleware "mapreduce+multiprocessing:///riak/proc?store_uri=kvstore%2Briak%3A%2F%2Flocalhost%3A8087%2Fdefault%2Friak_proc%3Fprotocol%3Dpbc"
        When I process the values "test_riak_process" in "features/map.json"
        Then I get the value in "features/reduced.json"
