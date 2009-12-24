-module(amqpfs_provider_base).

-export([amqp_credentials/0, init/1, 
         list_dir/2, 
         create/4, create_dir/4,
         rmdir/2, remove/2,
         rename/3, link/3, symlink/3, readlink/2,
         open/3, release/3,
         read/4, getattr/2, setattr/5,
         object/2, size/2, resize/3,
         atime/2, mtime/2,
         append/3,
         write/4, output/4, flush/3,
         readable/3, writable/3, executable/3,
         access/3,
         uid/2, gid/2,
         mode/2,
         get_lock/4, set_lock/5,
         handle_info/2,
         ttl/2,
         allow_request/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

amqp_credentials() ->
    []. % default

init(State) ->
    State.


list_dir(_Path, _State) ->
    [].

create(_Path,_Name,_Mode,_State) ->
    enotsup.

create_dir(_Path,_Name,_Mode,_State) ->
    enotsup.

rmdir(_Path,_State) ->
    enotsup.

remove(_Path,_State) ->    
    enotsup.

rename(_Path,_NewPath,_State) ->
    enotsup.

link(_Path, _NewPath, _State) ->
    enotsup.

symlink(_Path, _Contents, _State) ->
    ok.

readlink(_Path, _State) ->
    einval.

open(_Path, _Fi, _State) ->
    ok.

release(_Path, _Fi, _State) ->
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

flush(_Path, _Fi, _State) ->
    ok.

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
    erlang:localtime().

mtime(_Path, _State) ->
    erlang:localtime().

readable(_Path, _Group, _State) ->
    true.

writable(_Path, _Group, _State) ->
    true.

executable(_Path, _Group, _State) ->
    false.

access(Path, Mask, State) ->
    TestFunctor = fun (Permission) ->
                          Uid = hdr(<<"uid">>, State),
                          Gid = hdr(<<"gid">>, State),
                          PUid = uid(Path, State),
                          PGid = gid(Path, State),
                          if 
                              Uid =:= PUid ->
                                  fun () -> amqpfs_provider:call_module(Permission, [Path, owner, State], State) end;
                              Gid =:= PGid ->
                                  fun () -> amqpfs_provider:call_module(Permission, [Path, group, State], State) end;
                              true ->
                                  fun () -> amqpfs_provider:call_module(Permission, [Path, other, State], State) end
                          end
                  end,
    case 
    lists:all(fun (X) ->
                      X
              end, 
              [case Mode band Mask of
                   0 ->
                       true;
                   _ -> Test()
               end
               || {Mode, Test} <- [{?R_OK, TestFunctor(readable)},
                                   {?W_OK, TestFunctor(writable)},
                                   {?X_OK, TestFunctor(executable)}]]) of
        true ->
            ok;
        false ->
            eacces
    end.

uid(_Path, State) ->
    hdr(<<"uid">>, State).

gid(_Path, State) ->
    hdr(<<"gid">>, State).


get_lock(_Path, _Fi, _Lock, _State) ->
    #flock{}.

set_lock(_Path,_Fi,_Lock,_Sleep,_State)->
    enotsup.

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


mode(_Path, _State) ->
    0.

getattr(Path,State) ->
    Size = amqpfs_provider:call_module(size, [Path, State], State),
    ATime = amqpfs_util:datetime_to_unixtime(amqpfs_provider:call_module(atime, [Path, State], State)),
    MTime = amqpfs_util:datetime_to_unixtime(amqpfs_provider:call_module(mtime, [Path, State], State)),
    Mode =
        (b_mode(readable(Path, owner, State), ?S_IRUSR) bor
         b_mode(writable(Path, owner, State), ?S_IWUSR) bor
         b_mode(executable(Path, owner, State), ?S_IXUSR)) bor
        (b_mode(readable(Path, group, State), ?S_IRGRP) bor
         b_mode(writable(Path, group, State), ?S_IWGRP) bor
         b_mode(executable(Path, group, State), ?S_IXGRP)) bor
        (b_mode(readable(Path, other, State), ?S_IROTH) bor
         b_mode(writable(Path, other, State), ?S_IWOTH) bor
         b_mode(executable(Path, other, State), ?S_IXOTH)),
    UID = amqpfs_provider:call_module(uid, [Path, State], State),
    GID = amqpfs_provider:call_module(gid, [Path, State], State),
    #stat{ st_atime = ATime,
           st_mtime = MTime,
           st_size = Size,
           st_mode = Mode bor amqpfs_provider:call_module(mode, [Path, State], State),
           st_uid = UID,
           st_gid = GID
         }.

handle_info(_Msg, _State) ->
    ignore.

ttl(_Path, _State) ->
    0.

allow_request(_State) ->
    true.

%

b_mode(true, N) ->
    N;
b_mode(false, _) ->
    0.

hdr(Name, #amqpfs_provider_state{request_headers = Headers} = _State) ->
    {value, {Name, _, Val}} = lists:keysearch(Name, 1, Headers),
    Val.
