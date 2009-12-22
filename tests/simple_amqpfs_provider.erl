-module(simple_amqpfs_provider). 

-export([init/1, list_dir/2, create/4, create_dir/4, remove/2, rename/3, open/3, resize/3, append/3, write/4, object/2, atime/2, mtime/2, ttl/2]).

-include_lib("amqpfs/include/amqpfs.hrl").

init(State) ->
    amqpfs_provider:announce(directory, "/simple", State),
    amqpfs_provider:announce(directory, "/simple_on_demand", State),
    State.


create(Path, Name, _Mode, _State) ->
    io:format("Trying to create ~s/~s~n",[Path,Name]),
    enotsup.

create_dir(Path, Name, _Mode, _State) ->
    io:format("Trying to create directory ~s/~s~n",[Path,Name]),
    enotsup.

remove("/simple/" ++ File, _State) ->
    io:format("Trying to remove /simple/~s~n",[File]),
    ok.

rename(Path, NewPath, _State) ->
    io:format("Trying to rename ~s to ~s~n",[Path, NewPath]),
    enotsup.

list_dir("/simple", _State) ->
    [{"file1", {file, on_demand}}, {"file2", {file, on_demand}}];

list_dir("/simple_on_demand", State) ->
    {value, {_, _, Node }} = lists:keysearch(<<"node">>,1,State#amqpfs_provider_state.request_headers),
    [{"bogus",{file, on_demand}}, {binary_to_list(Node), {file, on_demand}}].

open("/simple_on_demand/bogus", _Fi, _State) ->
    ok.

resize("/simple_on_demand/bogus", NewSize, _State) ->
    io:format("Resizing to ~p~n",[NewSize]),
    NewSize.


append("/simple_on_demand/bogus", Data, _State) ->
    io:format("Appending with ~p~n",[binary_to_list(Data)]),
    size(Data).
                
write("/simple_on_demand/bogus", Data, Offset, _State) ->
    io:format("Writing ~p at ~p~n",[binary_to_list(Data), Offset]),
    size(Data).
                                           
    
-define(BOGUS_CONTENT, "This is a bogus file. Hello!" ++ [10]).

object("/simple_on_demand/bogus", _State) ->
    ?BOGUS_CONTENT.

atime("/simple_on_demand/bogus", _State) ->
    {{2009,12,19},{18,45,35}}.

mtime("/simple_on_demand/bogus", _State) ->
    {{2009,12,19},{18,45,37}}.

ttl("/simple_on_demand/bogus"=Path, #amqpfs_provider_state{ request_command = {read, Path, _, _} }) ->
    3000000.
