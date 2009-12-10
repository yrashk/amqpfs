-module(simple_amqpfs_provider). 
-behaviour(amqpfs_provider).

-export([amqp_credentials/0, init/1, list_dir/2]).

amqp_credentials() ->
    []. % default

init(State) ->
    amqpfs_provider:announce(directory, "/simple", [], State),
    amqpfs_provider:announce(directory, "/simple_on_demand", State),
    amqpfs_provider:announce(file, "/just_a_file", State),
    amqpfs_provider:announce(file, "/simple/file1", State),
    amqpfs_provider:announce(file, "/simple/file2", State),
    State.


list_dir("/simple_on_demand", State) ->
    [{"bogus",{file, undefined}}].
