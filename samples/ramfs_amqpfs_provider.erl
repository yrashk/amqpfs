-module(ramfs_amqpfs_provider).

-export([
         init/1,
         list_dir/2,
         create/4, create_dir/4,
         object/2,
         append/3, write/4,
         resize/3,
         rmdir/2, remove/2
         ]).

-record(ramfs,
        {
          files,
          objects
        }).

-include_lib("amqpfs/include/amqpfs.hrl").

init(#amqpfs_provider_state{ args = Args } = State) ->
    RamFS = #ramfs {
      files = ets:new(ramfs_files, [public, bag]),
      objects = ets:new(ramfs_objects, [public, set])
     },
    amqpfs_provider:announce(directory, proplists:get_value(dir, Args, "/ramfs"), State),
    State#amqpfs_provider_state{ extra = RamFS }.

list_dir(Path, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files } = RamFS,
    lists:map(fun ([Name, Type]) -> {Name, Type} end, ets:match(Files,{'$1',Path,'$2','_'})).

create(Path, Name, _Mode, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files } = RamFS,
    case ets:match(Files, {Name, Path, '_','_'}) of
        [] ->
            ets:insert(Files, {Name, Path, {file, on_demand}, undefined}),
            ok;
        _ ->
            eexist
    end.

create_dir(Path, Name, _Mode, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files } = RamFS,
    case ets:match(Files, {Name, Path, '_','_'}) of
        [] ->
            ets:insert(Files, {Name, Path, {directory, on_demand}, undefined}),
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
            {_, RestOfTheObject} = erlang:split_binary(ObjectOnAndAfterOffset, size(Data)),
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

rmdir(Path, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files } = RamFS,
    Name = hd(lists:reverse(Path)),
    Base = lists:reverse(tl(lists:reverse(Path))),
    ets:match_delete(Files, {Name, Base, '_', '_'}),
    ets:match_delete(Files, {'_', Path, '_', '_'}),
    ok.

remove(Path, #amqpfs_provider_state{ extra = RamFS }) ->
    #ramfs{ files = Files, objects = Objects } = RamFS,
    Name = hd(lists:reverse(Path)),
    Base = lists:reverse(tl(lists:reverse(Path))),
    ets:match_delete(Files, {Name, Base, '_', '_'}),
    ets:delete(Objects, Path),
    ok.
    
