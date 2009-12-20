-module(amqpfs_provider_base).

-export([amqp_credentials/0, init/1, 
         list_dir/2, 
         open/3, release/3,
         read/4, getattr/2, setattr/5,
         object/2, size/2, resize/3,
         atime/2, mtime/2,
         append/3,
         write/4, output/4,
         readable/3, writable/3, executable/3,
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

append(_Path, _Data, _State) ->
    eio. % we do not know what to do with appending by default

write(_Path, _Data, _Offset, _State) ->
    eio. % we do not know what to do with it

output(Path, Data, Offset, State) ->
    Size = amqpfs_provider:call_module(size, [Path, State], State),
    if Size == Offset ->
            amqpfs_provider:call_module(append, [Path, Data, State], State);
       true -> 
            amqpfs_provider:call_module(write, [Path, Data, Offset, State], State)
    end.

object(_Path, _State) ->
    <<>>.

size(Path, State) ->
    case amqpfs_provider:call_module(object, [Path, State], State) of
        Datum when is_list(Datum) ->
            length(Datum);
        Datum when is_binary(Datum) ->
            size(Datum)
    end.

atime(_Path, _State) ->
    {{1969,12,31},{00,00,00}}.

mtime(_Path, _State) ->
    {{1969,12,31},{00,00,00}}.

readable(_Path, _Group, _State) ->
    true.

writable(_Path, _Group, _State) ->
    true.

executable(_Path, _Group, _State) ->
    false.

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


mode(true, N) ->
    N;
mode(false, _) ->
    0.

getattr(Path,State) ->
    Size = amqpfs_provider:call_module(size, [Path, State], State),
    ATime = amqpfs_util:datetime_to_unixtime(amqpfs_provider:call_module(atime, [Path, State], State)),
    MTime = amqpfs_util:datetime_to_unixtime(amqpfs_provider:call_module(mtime, [Path, State], State)),
    Mode =
        (mode(readable(Path, owner, State), ?S_IRUSR) bor
         mode(writable(Path, owner, State), ?S_IWUSR) bor
         mode(executable(Path, owner, State), ?S_IXUSR)) bor
        (mode(readable(Path, group, State), ?S_IRGRP) bor
         mode(writable(Path, group, State), ?S_IWGRP) bor
         mode(executable(Path, group, State), ?S_IXGRP)) bor
        (mode(readable(Path, other, State), ?S_IROTH) bor
         mode(writable(Path, other, State), ?S_IWOTH) bor
         mode(executable(Path, other, State), ?S_IXOTH)),
    #stat{ st_atime = ATime,
           st_mtime = MTime,
           st_size = Size,
           st_mode = Mode
         }.

handle_info(_Msg, _State) ->
    ignore.

ttl(_, _) ->
    0.

allow_request(_State) ->
    true.
