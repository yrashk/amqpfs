-module(amqpfs_provider_base).

-export([amqp_credentials/0, init/1, list_dir/2, open/2, read/4, getattr/2, setattr/3,
         object/2, size/2,
         write/4,
         handle_info/2,
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
    ProperOffset = erlang:min(Offset, size(Result)),
    {_, Result1} = split_binary(Result, ProperOffset),
    ProperSize = erlang:min(Size, size(Result1)),
    {Result2, _} = split_binary(Result1, ProperSize),
    Result2.

write(_Path, _Data, _Offset, _State) ->
    eio.

object(_Path, _State) ->
    <<>>.

size(Path, State) ->
    case amqpfs_provider:call_module(object, [Path, State], State) of
        Datum when is_list(Datum) ->
            length(Datum);
        Datum when is_binary(Datum) ->
            size(Datum)
    end.

setattr(_Path, Attr, _State) ->
    Attr.

getattr(Path,State) ->
    Size = amqpfs_provider:call_module(size, [Path, State], State),
    #stat{ st_size = Size }.

handle_info(_Msg, _State) ->
    ignore.

allow_request(_State) ->
    true.
