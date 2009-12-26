-module(ramfs_amqpfs_provider).

-export([
         init/1,
         list_dir/2,
         create/4, create_dir/4,
         object/2,
         append/3, write/4,
         resize/3,
         rename/3,
         rmdir/2, remove/2,
         atime/2, mtime/2,
         set_atime/3, set_mtime/3,
         statfs/2
         ]).

-record(ramfs,
        {
          files,
          objects,
          attrs
        }).

-include_lib("amqpfs/include/amqpfs.hrl").

init(#amqpfs_provider_state{ args = Args } = State) ->
    RamFS = #ramfs {
      files = ets:new(ramfs_files, [public, bag]),
      objects = ets:new(ramfs_objects, [public, set]),
      attrs = ets:new(ramfs_attrs, [public, set])
     },
    amqpfs_provider:announce(directory, proplists:get_value(dir, Args, "/ramfs"), State),
    State#amqpfs_provider_state{ extra = RamFS }.

list_dir(Path, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files } = RamFS,
    lists:map(fun ([Name, Type]) -> {Name, Type} end, ets:match(Files,{'$1',Path,'$2'})).

create(Path, Name, _Mode, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files, attrs = Attrs } = RamFS,
    case ets:match(Files, {Name, Path, '_'}) of
        [] ->
            Now = calendar:local_time(),
            ets:insert(Files, {Name, Path, {file, on_demand}}),
            ets:insert(Attrs, {Path ++ [Name], Now, Now}), % atime, mtime
            ok;
        _ ->
            eexist
    end.

create_dir(Path, Name, _Mode, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files, attrs = Attrs } = RamFS,
    case ets:match(Files, {Name, Path, '_','_'}) of
        [] ->
            Now = calendar:local_time(),
            ets:insert(Files, {Name, Path, {directory, on_demand}}),
            ets:insert(Attrs, {Path ++ [Name], Now, Now}), % atime, mtime
            ok;
        _ ->
            eexist
    end.

object(Path, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ objects = Objects } = RamFS,
    case ets:lookup(Objects, Path) of
        [{Path, Object}] ->
            Object;
        [] ->
            ""
    end.

append(Path, Data, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ objects = Objects } = RamFS,
    case ets:lookup(Objects, Path) of
        [{Path, Object}] ->
            ets:insert(Objects, {Path, erlang:list_to_binary([Object, Data])}),
            size(Data);
        [] ->
            ets:insert(Objects, {Path, Data}),
            size(Data)
    end.

write(Path, Data, Offset, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ objects = Objects } = RamFS,
    case ets:lookup(Objects, Path) of
        [{Path, Object}] ->
            NewOffset = erlang:min(Offset, size(Object)),
            {ObjectBeforeOffset, ObjectOnAndAfterOffset} = erlang:split_binary(Object, NewOffset),
            Sz = erlang:min(size(ObjectOnAndAfterOffset), size(Data)),
            {_, RestOfTheObject} = erlang:split_binary(ObjectOnAndAfterOffset, Sz),
            ets:insert(Objects, {Path, erlang:list_to_binary([ObjectBeforeOffset, Data, RestOfTheObject])}),
            size(Data);
        [] ->
            ets:insert(Objects, {Path, Data}),
            size(Data)
    end.

resize(Path, NewSize, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ objects = Objects } = RamFS,
    case ets:lookup(Objects, Path) of
        [{Path, Object}] ->
            {NewObject, _} = erlang:split_binary(Object, NewSize),
            ets:insert(Objects, {Path, NewObject}),
            NewSize;
        [] ->
            BitSize = NewSize*8,
            ets:insert(Objects, {Path, <<0:BitSize>>}),
            NewSize
    end.

rename(Path, NewPath, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ objects = Objects, files = Files, attrs = Attrs } = RamFS,
    Name = hd(lists:reverse(Path)),
    NewName = filename:basename(NewPath),
    Base = lists:reverse(tl(lists:reverse(Path))),
    NewBase = string:tokens(filename:dirname(NewPath),"/"),
    [{Name, Base, Type}] = ets:match_object(Files, {Name, Base, '_'}),
    Object = 
    case ets:lookup(Objects, Path) of
        [{Path, Obj}] ->
            Obj;
        [] ->
            <<>>
    end,
    [{Path, ATime, MTime}] = ets:lookup(Attrs, Path),
    ets:insert(Objects, {string:tokens(NewPath,"/"), Object}),
    ets:insert(Attrs, {string:tokens(NewPath,"/"), ATime, MTime}),
    ets:insert(Files, {NewName, NewBase, Type}),
    ets:match_delete(Files,  {Name, Base, '_'}),
    ets:delete(Objects, Path),
    ets:delete(Attrs, Path),
    ok.
    

rmdir(Path, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files, attrs = Attrs } = RamFS,
    Name = hd(lists:reverse(Path)),
    Base = lists:reverse(tl(lists:reverse(Path))),
    ets:match_delete(Files, {Name, Base, '_'}),
    ets:delete(Attrs, Path),
    ok.

remove(Path, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files, objects = Objects, attrs = Attrs } = RamFS,
    Name = hd(lists:reverse(Path)),
    Base = lists:reverse(tl(lists:reverse(Path))),
    ets:match_delete(Files, {Name, Base, '_'}),
    ets:delete(Objects, Path),
    ets:delete(Attrs, Path),
    ok.
    
atime(Path, #amqpfs_provider_state{ extra = RamFS }=State) ->
    #ramfs{ attrs = Attrs } = RamFS,
    case ets:lookup(Attrs, Path) of
        [{Path, ATime, _}] -> 
            ATime;
        _ ->
            Now = calendar:local_time(),
            set_atime(Path, Now, State),
            Now
    end.

mtime(Path, #amqpfs_provider_state{ extra = RamFS }=State) ->
    #ramfs{ attrs = Attrs } = RamFS,
    case ets:lookup(Attrs, Path) of
        [{Path, _, MTime}] -> 
            MTime;
        _ ->
            Now = calendar:local_time(),
            set_mtime(Path, Now, State),
            Now
    end.
    
   
set_atime(Path, Datetime, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ attrs = Attrs } = RamFS,
    ets:update_element(Attrs, Path, {2, Datetime}).


set_mtime(Path, Datetime, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ attrs = Attrs } = RamFS,
    ets:update_element(Attrs, Path, {3, Datetime}).

statfs(_,_) ->
    #statvfs { 
        f_bsize = 1024*1024,
        f_namemax = 4096
       }.
