-module(simple_amqpfs_provider). 
-behaviour(amqpfs_provider).

-export([amqp_credentials/0, init/1, list_dir/2, open/2, read/4, getattr/2]).

-include_lib("amqpfs/include/amqpfs.hrl").

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
    [{"bogus",{file, on_demand}}].

open("/simple_on_demand/bogus", State) ->
    ok.


-define(BOGUS_CONTENT, "This is a bogus file. Hello!" ++ [10]).

read("/simple_on_demand/bogus", Size, Offset, State) ->
    list_to_binary(string:substr(?BOGUS_CONTENT, Offset + 1, Size)).

getattr("/simple_on_demand/bogus",State) ->
    #stat{ st_mode = ?S_IFREG bor 8#0444, 
           st_size = length(?BOGUS_CONTENT) };

getattr(Rest,State) ->
    #stat{ st_mode = ?S_IFREG bor 8#0444, 
           st_size = 0 }.
