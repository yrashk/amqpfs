-module(amqpfs_util).
-export([path_to_matching_routing_key/1,path_to_routing_key/1]).

path_to_matching_routing_key(Path) ->
    list_to_binary(string:join(lists:map(fun base64:encode_to_string/1, string:tokens(Path,"/")),".") ++ [".#"]).

path_to_routing_key(Path) ->
    list_to_binary(string:join(lists:map(fun base64:encode_to_string/1, string:tokens(Path,"/")),".")).
