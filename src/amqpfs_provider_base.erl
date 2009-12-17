-module(amqpfs_provider_base). 

-export([amqp_credentials/0, init/1, list_dir/2, open/2, read/4, getattr/2,
         object/2,
         allow_request/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

amqp_credentials() ->
    []. % default

init(State) ->
    State.


list_dir(_, _State) ->
    [].
    
open(_, _State) ->
    ok.

read(Path, Size, Offset, State) ->
    Result =
    case amqpfs_provider:call_module(object, [Path, State], State) of
        Datum when is_list(Datum) ->
            list_to_binary(Datum);
        Datum when is_binary(Datum) ->
            Datum
    end,
    {_, Result1} = split_binary(Result, Offset),
    {Result2, _} = split_binary(Result1, Size),
    Result2.

object(_Path, _State) ->
    <<>>.

getattr(Path,State) ->
    Size = 
    case amqpfs_provider:call_module(object, [Path, State], State) of
        Datum when is_list(Datum) ->
            length(Datum);
        Datum when is_binary(Datum) ->
            size(Datum)
    end,
    #stat{ st_size = Size }.

allow_request(_State) ->
    true.
