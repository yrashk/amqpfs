-module(amqpfs_provider_base).

-export([amqp_credentials/0, init/1, 
         list_dir/2, 
         open/3, release/3,
         read/4, getattr/2, setattr/5,
         object/2, size/2, resize/3,
         write/4,
         handle_info/2,
         ttl/2,
         allow_request/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

amqp_credentials() ->
    []. % default

init(State) ->
    State.


list_dir(_, _State) ->
    [].

open(_, _Fi, _State) ->
    ok.

release(_, _Fi, _State) ->
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

resize(_Path, NewSize, _State) ->
    NewSize.

setattr(Path, Stat, Attr, ToSet, State) ->
    NewMode = 
        if ToSet band ?FUSE_SET_ATTR_MODE > 0 -> Attr#stat.st_mode;
           true -> Stat#stat.st_mode
        end,
    
    NewUid = 
        if ToSet band ?FUSE_SET_ATTR_UID > 0 -> Attr#stat.st_uid;
           true -> Stat#stat.st_uid
        end,
    
    NewGid = 
        if ToSet band ?FUSE_SET_ATTR_GID > 0 -> Attr#stat.st_gid;
           true -> Stat#stat.st_gid
        end,
    
    NewSize = 
        if ToSet band ?FUSE_SET_ATTR_SIZE > 0 -> 
                amqpfs_provider:call_module(resize, [Path, Attr#stat.st_size, State], State),
                Attr#stat.st_size;
           true -> 
                Stat#stat.st_size
        end,
    
    NewATime = 
        if ToSet band ?FUSE_SET_ATTR_ATIME > 0 -> Attr#stat.st_atime;
           true -> Stat#stat.st_atime
        end,
    
    NewMTime = 
        if ToSet band ?FUSE_SET_ATTR_MTIME > 0 -> Attr#stat.st_mtime;
           true -> Stat#stat.st_mtime
        end,
    
    Stat#stat{ st_mode = NewMode,
               st_uid = NewUid,
               st_gid = NewGid,
               st_size = NewSize,
               st_atime = NewATime,
               st_mtime = NewMTime }.


getattr(Path,State) ->
    Size = amqpfs_provider:call_module(size, [Path, State], State),
    #stat{ st_size = Size }.

handle_info(_Msg, _State) ->
    ignore.

ttl(_, _) ->
    0.

allow_request(_State) ->
    true.
