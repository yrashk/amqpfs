-module (amqpfs_server).
-export ([start_link/0, start_link/2, start_link/3]).
%-behaviour (fuserl).
-export ([ code_change/3,
           handle_info/2,
           init/1,
           set_response_policies/2,
           path_to_announced/2,
           terminate/2,
           getattr/4,
           lookup/5,
           open/5,
           opendir/5,
           read/7,
           readdir/7,
           readlink/4,
           write/7,
           mknod/7,
           create/7,
           access/5,
           flush/5,
           forget/5,
           fsync/6,
           fsyncdir/6,
           getxattr/6,
           listxattr/5,
           release/5,
           releasedir/5,

           getlk/6,
           setlk/7,
           link/6,
           mkdir/6,
%%            removexattr/5,
           rename/7,
           rmdir/5,
           setattr/7,
%%            setxattr/7,
%%            statfs/4,
%%            symlink/6,
           unlink/5

          ]).

-include_lib("amqpfs/include/amqpfs.hrl").

-define(ON_DEMAND_TIMEOUT, 60000).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link() ->
    start_link(false, proplists:get_value(mount_point, application:get_all_env(amqpfs),"/amqpfs"), proplists:get_value(mount_options, application:get_all_env(amqpfs), "")).

start_link (LinkedIn, Dir) ->
    start_link (LinkedIn, Dir, "").

start_link (LinkedIn, Dir, MountOpts) ->
    fuserlsrv:start_link({local, amqpfs}, ?MODULE, LinkedIn, MountOpts, Dir, [], []).

%-=====================================================================-
%-                           fuserl callbacks                          -
%-=====================================================================-

init ([]) ->
    {ok, AmqpConn} = erabbitmq_connections:start(),
    {ok, AmqpChannel} = erabbitmq_channels:open(AmqpConn),
    amqpfs_util:setup(AmqpChannel),
    Queue = amqpfs_util:announce_queue_name(),
    ResponseQueue = amqpfs_util:response_queue_name(),
    #'queue.declare_ok'{} = amqp_channel:call(AmqpChannel, #'queue.declare'{
                                                                            queue = Queue,
                                                                            passive = false, durable = true,
                                                                            exclusive = false, auto_delete = false,
                                                                            nowait = false, arguments = []}),
    #'queue.declare_ok'{} = amqp_channel:call(AmqpChannel, #'queue.declare'{
                                                                            queue = ResponseQueue,
                                                                            passive = false, durable = false,
                                                                            exclusive = false, auto_delete = false,
                                                                            nowait = false, arguments = []}),
    #'queue.bind_ok'{} = amqp_channel:call(AmqpChannel, #'queue.bind'{
                                                                      queue = Queue, exchange = <<"amqpfs.announce">>,
                                                                      routing_key = <<"">>,
                                                                      nowait = false, arguments = []}),
    #'queue.bind_ok'{} = amqp_channel:call(AmqpChannel, #'queue.bind'{
                                                                      queue = ResponseQueue, exchange = <<"amqpfs.response">>,
                                                                      routing_key = <<"">>,
                                                                      nowait = false, arguments = []}),
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:subscribe(AmqpChannel, #'basic.consume'{
                                                                                                           queue = Queue,
                                                                                                           consumer_tag = <<"">>,
                                                                                                           no_local = false,
                                                                                                           no_ack = true,
                                                                                                           exclusive = false,
                                                                                                           nowait = false}, self()),
    receive
          #'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
    end,
    #'basic.consume_ok'{consumer_tag = ResponseConsumerTag} = amqp_channel:subscribe(AmqpChannel, #'basic.consume'{
                                                                                                                   queue = ResponseQueue,
                                                                                                                   consumer_tag = <<"">>,
                                                                                                                   no_local = false,
                                                                                                                   no_ack = true,
                                                                                                                   exclusive = false,
                                                                                                                   nowait = false}, self()),
    receive
          #'basic.consume_ok'{consumer_tag = ResponseConsumerTag} -> ok
    end,

    State = #amqpfs{ inodes = ets:new(inodes, [public, ordered_set]),
                     names = ets:new(names, [public, set]),
                     announcements = ets:new(announcements, [public, duplicate_bag]),
                     response_routes = ets:new(response_routes, [public, set]),
                     response_cache = ets:new(response_cache, [public, set]),
                     response_policies = ets:new(response_policies, [public, set]),
                     response_buffers = ets:new(response_buffers, [public, duplicate_bag]),
                     amqp_conn = AmqpConn,
                     amqp_channel = AmqpChannel,
                     amqp_consumer_tag = ConsumerTag,
                     amqp_response_consumer_tag = ResponseConsumerTag
                  },
    { ok, State }.

code_change (_OldVsn, State, _Extra) -> { ok, State }.

handle_info({#'basic.deliver'{consumer_tag=ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, 
                              exchange = <<"amqpfs.response">>, routing_key=_RoutingKey}, Content}, 
            #amqpfs{response_routes = Tab, amqp_response_consumer_tag = ConsumerTag, response_buffers = ResponseBuffers }=State) ->
    #amqp_msg{payload = Payload } = Content,
    #'P_basic'{content_type = ContentType, headers = Headers, reply_to = Route} = Content#amqp_msg.props,
    TTL = 
        case lists:keysearch(<<"ttl">>, 1, Headers) of
            {value, {<<"ttl">>, _, Val}} ->
                Val;
            _ ->
                0
        end,
    Response = amqpfs_util:decode_payload(ContentType, Payload),
    ets:insert(ResponseBuffers, {Route, Response, TTL}),
    case ets:lookup(Tab, Route) of 
        [{Route, Pid, Path, Command}] ->
            {{CollectM, CollectF, CollectA}, {ReduceM, ReduceF, ReduceA}} = get_response_policy(Path, Command, State),
            case apply(CollectM, CollectF, CollectA ++ [Route, Response, State]) of
                last_response ->
                    Responses = ets:lookup(ResponseBuffers, Route),
                    ets:delete(ResponseBuffers, Route),
                    unregister_response_route(Route, State), % it is pretty safe to assume that there are no more messages to deliver
                    {ResponseToSend, TTLToSend} = apply(ReduceM, ReduceF, ReduceA ++ [lists:map(fun ({_, ResponseA, TTLA}) -> {ResponseA, TTLA} end, Responses)]),
                    Pid ! {response, ResponseToSend, TTLToSend};
                _ ->
                    continue
            end;
        _ ->
            ets:delete(ResponseBuffers, Route),
            discard
    end,
    {noreply, State};
    
handle_info({#'basic.deliver'{consumer_tag=ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered,
                              exchange = <<"amqpfs.announce">>, routing_key=_RoutingKey}, Content}, 
            #amqpfs{amqp_consumer_tag = ConsumerTag}=State) ->
    #amqp_msg{payload = Payload } = Content,
    #'P_basic'{content_type = ContentType, headers = _Headers} = Content#amqp_msg.props,
    Command = amqpfs_util:decode_payload(ContentType, Payload),
    {noreply, handle_command(Command, State)};

handle_info({set_response_policies, Path, Policies}, State) ->
    set_response_policies(Path, Policies, State),
    {noreply, State};
    

handle_info (_Msg, State) -> { noreply, State }.

terminate (_Reason, _State) -> ok.

handle_command({announce, directory, {Path, Contents}}, #amqpfs{ announcements = Announcements} = State) ->
    {_, State1} = make_inode(Path, {directory, Contents}, State),
    ets:insert(Announcements, {Path, {directory, Contents}}),
    set_new_response_policies(Path, amqpfs_response_policies:new(), State),
    State1;

handle_command({cancel, directory, _Path}, State) ->
    State.


set_new_response_policies(Path, Policies, #amqpfs{ response_policies = ResponsePolicies } = _State) ->
    ets:insert_new(ResponsePolicies, {Path, Policies}).

set_response_policies(Path, Policies, #amqpfs{ response_policies = ResponsePolicies } = _State) ->
    ets:insert(ResponsePolicies, {Path, Policies}).

set_response_policies(Path, Policies) ->
    amqpfs ! {set_response_policies, Path, Policies},
    ok.

get_response_policy(Path, Command, #amqpfs{ response_policies = ResponsePolicies } = State) ->
    case ets:lookup(ResponsePolicies, Path) of
        [] ->
            get_response_policy(filename:dirname(Path), Command, State);
        [{Path, Policies}] ->
            CommandName = element(1,Command),
            {value, {CommandName, Collect, Reduce}} = lists:keysearch(CommandName, 1, Policies),
            {normalize_collect_function(Collect), normalize_reduce_function(Reduce)};
        _ ->
            never_happens
    end.

normalize_collect_function(Fun) when is_atom(Fun) ->
    {amqpfs_response_collect, Fun, []};
normalize_collect_function({Fun, Args}) when is_atom(Fun) andalso is_list(Args) ->
    {amqpfs_response_collect, Fun, Args};
normalize_collect_function({Module, Fun}) when is_atom(Module) andalso is_atom(Fun) ->
    {Module, Fun, []};
normalize_collect_function({Module, Fun, Args}) when is_atom(Module) andalso is_atom(Fun) andalso is_list(Args) ->
    {Module, Fun, Args}.

normalize_reduce_function(Fun) when is_atom(Fun) ->
    {amqpfs_response_reduce, Fun, []};
normalize_reduce_function({Fun, Args}) when is_atom(Fun) andalso is_list(Args) ->
    {amqpfs_reduece_policy, Fun, Args};
normalize_reduce_function({Module, Fun}) when is_atom(Module) andalso is_atom(Fun) ->
    {Module, Fun, []};
normalize_reduce_function({Module, Fun, Args}) when is_atom(Module) andalso is_atom(Fun) andalso is_list(Args) ->
    {Module, Fun, Args}.


getattr(Ctx, Ino, Cont, State) ->
    spawn_link(fun () -> getattr_async(Ctx,
                                       Ino,
                                       Cont,
                                       State)
               end),
    { noreply, State }.

getattr_async(Ctx, Ino, Cont, State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, Ino) of
        [{Ino, Path}] ->
            case ets:lookup(State#amqpfs.names, Path) of
                [{Path, { Ino, _ } }] ->
                    case remote_getattr(Path, Ctx, State) of
                        #stat{}=Stat -> #fuse_reply_attr{ attr = Stat, attr_timeout_ms = 1000 };
                        Err -> #fuse_reply_err { err = Err}
                    end;
                _ ->
                    #fuse_reply_err{ err = enoent }
            end;

        _ ->
            #fuse_reply_err{ err = enoent }
    end,
    fuserlsrv:reply (Cont, Result).



lookup(Ctx, ParentIno, BinPath, Cont, State) ->
    spawn_link(fun () -> lookup_async(Ctx,
                                      ParentIno,
                                      BinPath,
                                      Cont,
                                      State)
               end),
    { noreply, State }.

lookup_async(Ctx, ParentIno, BinPath, Cont, State) ->
    case ets:lookup(State#amqpfs.inodes, ParentIno) of
        [{ParentIno,Path}] ->
            Result =
            case ets:lookup(State#amqpfs.names, Path) of
                [{Path, { ParentIno, {directory, on_demand}}}] ->
                    Response = remote_list_dir(Path, Ctx, State),
                    List = lists:map(fun ({P,E}) -> 
                                             {_Ino, _} = make_inode(amqpfs_util:concat_path([Path, P]), E, State),
                                             P
                                     end, Response),
                    lookup_impl(BinPath, Path, List, Ctx, State);
                _ ->
                    #fuse_reply_err{ err = enoent }
            end,
            fuserlsrv:reply (Cont, Result);
        _ ->
            fuserlsrv:reply (Cont, #fuse_reply_err{ err = enoent })
    end.

lookup_impl(BinPath, Path, List, Ctx, State) ->
    case lists:any(fun (P) -> P == BinPath end,  lists:map(fun erlang:list_to_binary/1, List)) of
        true -> % there is something
            Path2 = amqpfs_util:concat_path([Path,binary_to_list(BinPath)]),
            case ets:lookup(State#amqpfs.names, Path2) of
                [{Path2, {Ino, _}}] ->
                    Stat = remote_getattr(Path2, Ctx, State),
                    #fuse_reply_entry{ 
                                       fuse_entry_param = #fuse_entry_param{ ino = Ino,
                                                                             generation = 1,  % (?)
                                                                             attr_timeout_ms = 1000,
                                                                             entry_timeout_ms = 1000,
                                                                             attr = Stat } };
                _ ->
                    #fuse_reply_err{ err = enoent }
            end;
        false ->
            #fuse_reply_err{ err = enoent }
    end.


open(Ctx, Ino, Fi, Cont, State) ->
    spawn_link(fun () -> open_async(Ctx, Ino, Fi, Cont, State) end),
    { noreply, State }.

opendir(Ctx, Ino, Fi, Cont, State) ->
    spawn_link (fun () -> open_async(Ctx, Ino, Fi, Cont, State) end),
    { noreply, State }.

open_async(Ctx, Ino, Fi, Cont, State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, Ino) of
        [{Ino,Path}] ->
            Response = remote(Path, {open, Path, Fi}, Ctx, State),
            case Response of
                ok -> #fuse_reply_open{fuse_file_info = Fi};
                enoent -> #fuse_reply_err { err = enoent};
                _ -> #fuse_reply_err { err = einval}
            end;
        _ ->
            #fuse_reply_err{ err = enoent }
    end,
    fuserlsrv:reply(Cont, Result).

read(Ctx, Ino, Size, Offset, Fi, Cont, State) ->
    spawn_link(fun () -> read_async(Ctx,
                                    Ino,
                                    Size,
                                    Offset,
                                    Fi,
                                    Cont,
                                    State)
               end),
    { noreply, State }.

read_async(Ctx, Ino, Size, Offset, _Fi, Cont, State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, Ino) of
        [{Ino,Path}] ->
            Response = remote(Path, {read, Path, Size, Offset}, Ctx, State),
            case Response of
                Buf when is_binary(Buf), size(Buf) =< Size -> #fuse_reply_buf{ size = size(Buf), buf = Buf };
                eio -> #fuse_reply_err { err = eio};
                _ -> #fuse_reply_err { err = einval}
            end;
        _ ->
            #fuse_reply_err{ err = enoent }
    end,
    fuserlsrv:reply(Cont, Result).

readdir(Ctx, Ino, Size, Offset, Fi, Cont, State) ->
    spawn_link(fun () -> readdir_async (Ctx,
                                        Ino,
                                        Size,
                                        Offset,
                                        Fi,
                                        Cont,
                                        State)
               end),
    { noreply, State }.

readdir_async(Ctx, Ino, Size, Offset, _Fi, Cont, #amqpfs{}=State) ->
    {Contents, _} =
        case ets:lookup(State#amqpfs.inodes, Ino) of
            [{Ino,Path}] ->
                case ets:lookup(State#amqpfs.names, Path) of
                    [{Path, { Ino, {directory, on_demand}}}] ->
                        Response = remote_list_dir(Path, Ctx, State),
                        lists:foldl(fun ({P, E}, {L, Acc}) -> 
                                            Path2 = amqpfs_util:concat_path([Path, P]),
                                            make_inode(Path2, E, State),
                                            case ets:lookup(State#amqpfs.names, Path2) of                      
                                                [{Path2, {_ChildIno, _}}] ->
                                                    Stat = remote_getattr(Path2, Ctx, State),
                                                    {L ++ [#direntry{ name = P, offset = Acc, stat = Stat }], Acc + 1};
                                                _ ->
                                                    {L, Acc}
                                            end
                                    end, {[],3},  Response);
                    _ ->
                        {[], 3}
                end
                % according to FuseInvariants wiki page on FUSE, readdir() is only called with an existing directory name, so there is no other clause in this case
        end,
    DirEntryList = 
        take_while 
          (fun (E, { Total, Max }) -> 
                   Cur = fuserlsrv:dirent_size (E),
                   if 
                       Total + Cur =< Max ->
                           { continue, { Total + Cur, Max } };
                       true ->
                           stop
                   end
           end,
           { 0, Size },
           lists:nthtail 
           (Offset,
            [ #direntry{ name = ".", offset = 1, stat = remote_getattr(Path, Ctx, State) },
              #direntry{ name = "..", offset = 2, stat = remote_getattr(filename:dirname(Path), Ctx, State) }
             ] ++ Contents)),
    fuserlsrv:reply (Cont, #fuse_reply_direntrylist{ direntrylist = DirEntryList }).

readlink (_, _, _, State) ->
    { #fuse_reply_err{ err = einval }, State }.

write(Ctx, Ino, Data, Offset, Fi, Cont, State) ->
    spawn_link(fun () -> write_async(Ctx,
                                     Ino,
                                     Data,
                                     Offset,
                                     Fi,
                                     Cont,
                                     State)
               end),
    { noreply, State }.

write_async(Ctx, Ino, Data, Offset, _Fi, Cont, State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, Ino) of
        [{Ino,Path}] ->
            Response = remote(Path, {write, Path, Data, Offset}, Ctx, State),
            case Response of
                Count when is_integer(Count) -> #fuse_reply_write{ count = Count };
                eio -> #fuse_reply_err { err = eio};
                _ -> #fuse_reply_err { err = einval}
            end;
        _ ->
            #fuse_reply_err{ err = enoent }
    end,
    fuserlsrv:reply(Cont, Result).
    

access(Ctx, Ino, Mask, Cont, State) ->
    spawn_link(fun () -> access_async(Ctx, Ino, Mask, Cont, State) end),
    { noreply, State }.

access_async(_Ctx, _Ino, _Mask, Cont, _State) ->
    fuserlsrv:reply(Cont, #fuse_reply_err{ err = ok }).

flush (_Ctx, _Inode, _Fi, _Cont, State) ->
  { #fuse_reply_err{ err = ok }, State }.

forget (_Ctx, _Inode, _Nlookup, _Cont, State) ->
  { #fuse_reply_none{}, State }.

fsync (_Ctx, _Inode, _IsDataSync, _Fi, _Cont, State) ->
  { #fuse_reply_err{ err = ok }, State }.

fsyncdir (_Ctx, _Inode, _IsDataSync, _Fi, _Cont, State) ->
  { #fuse_reply_err{ err = ok }, State }.

getxattr(Ctx, Ino, Name, Size, Cont, State) ->
    spawn_link(fun () -> getxattr_async(Ctx,
                                        Ino,
                                        Name,
                                        Size,
                                        Cont,
                                        State) 
               end),
    { noreply, State }.

getxattr_async(_Ctx, _Ino, _Name, _Size, Cont, _State) ->
    fuserlsrv:reply(Cont, #fuse_reply_err{ err = enotsup }).

listxattr(Ctx, Ino, Size, Cont, State) ->
  spawn_link(fun () -> listxattr_async(Ctx, Ino, Size, Cont, State) end),
  { noreply, State }.

listxattr_async(_Ctx, _Ino, _Size, Cont, _State) ->
    fuserlsrv:reply(Cont, #fuse_reply_err{ err = erange }).

link(Ctx, Ino, NewParentIno, NewName, Cont, State) ->
    spawn_link(fun () -> link_async(Ctx, Ino, NewParentIno, NewName, Cont, State) end),
    {noreply, State}.

link_async(Ctx, Ino, NewParentIno, NewName, Cont, State) ->
   Result =
    case ets:lookup(State#amqpfs.inodes, Ino) of
        [{Ino,Path}] ->
            case ets:lookup(State#amqpfs.inodes, NewParentIno) of
                [{NewParentIno,NewPath}] ->
                    NewFullPath = amqpfs_util:concat_path([NewPath,binary_to_list(NewName)]),
                    Extra = case ets:lookup(State#amqpfs.names, Path) of
                                [{Path, {_,Type}}] ->
                                    Type;
                                _ ->
                                    {file, on_demand} % but this should never happen, I guess
                            end,
                    case remote(Path, {link, Path, NewFullPath}, Ctx, State) of
                        ok ->
                            {NewIno, _ } = make_inode(NewFullPath, Extra, State),
                            Stat = remote_getattr(NewFullPath, Ctx, State),
                            #fuse_reply_entry{ 
                                               fuse_entry_param = #fuse_entry_param{ ino = NewIno,
                                                                                     generation = 1,  
                                                                                     attr_timeout_ms = 1000,
                                                                                     entry_timeout_ms = 1000,
                                                                                     attr = Stat } };
                        Err ->
                            #fuse_reply_err{ err = Err }
                    end;
                _ ->
                    #fuse_reply_err{ err = enoent }
            end;
        _ ->
            #fuse_reply_err{ err = enoent }
    end,
    fuserlsrv:reply (Cont, Result).
                              
                        

mknod(Ctx, ParentIno, Name, Mode, Dev, Cont, State) ->
    spawn_link 
      (fun () -> 
               mknod_async(Ctx, ParentIno, Name, Mode, Dev, Cont, State) 
       end),
    {noreply, State}.

    

mknod_async(Ctx, ParentIno, Name, Mode, _Dev, Cont, State) ->
    Result = 
    case ets:lookup(State#amqpfs.inodes, ParentIno) of
        [{ParentIno,Path}] ->
            case remote(Path, {create, Path, Name, Mode}, Ctx, State) of
                ok ->
                    Extra =
                    case Mode band ?S_IFMT of
                        ?S_IFREG ->
                            {file, on_demand};
                        ?S_IFDIR ->
                            {directory, on_demand};
                        ?S_IFLNK ->
                            {link, on_demand} % links are not yet supported, though
                    end,
                    Param = #fuse_entry_param {
                      ino = make_inode(amqpfs_util:concat_path([Path,Name]), Extra, State),
                      generation = 1,
                      attr = remote_getattr(Path, Ctx, State),
                      attr_timeout_ms = 100,
                      entry_timeout_ms = 100
                     },
                    #fuse_reply_entry { fuse_entry_param = Param };
                Err ->
                    #fuse_reply_err { err = Err }
            end;
        _ ->
            #fuse_reply_err{ err = enoent }
    end,
    fuserlsrv:reply(Cont, Result).   


create(Ctx, ParentIno, Name, Mode, Fi, Cont, State) ->
    spawn_link 
      (fun () -> 
               create_async(Ctx, ParentIno, Name, Mode, Fi, Cont, State)
       end),
    { noreply, State }.

create_async(Ctx, ParentIno, Name, Mode, Fi, Cont, State) ->
    Result = 
    case ets:lookup(State#amqpfs.inodes, ParentIno) of
        [{ParentIno,Path}] ->
            case remote(Path, {create, Path, Name, Mode}, Ctx, State) of
                ok ->
                    Extra = 
                    case Mode band ?S_IFMT of
                        ?S_IFREG ->
                            {file, on_demand};
                        ?S_IFDIR ->
                            {directory, on_demand};
                        ?S_IFLNK ->
                            {link, on_demand} % links are not yet supported, though
                    end,
                    make_inode(amqpfs_util:concat_path([Path,Name]), Extra, State),
                    Response = remote(Path, {open, Path, Fi}, Ctx, State),
                    case Response of
                        ok -> #fuse_reply_open{fuse_file_info = Fi};
                        Err -> #fuse_reply_err { err = Err }
                    end;
                Err ->
                    #fuse_reply_err { err = Err }
            end;
        _ ->
            #fuse_reply_err{ err = enoent }
    end,
    fuserlsrv:reply(Cont, Result).   

release(Ctx, Ino, Fi, Cont, State) ->
    spawn_link 
      (fun () -> 
               release_async(Ctx, Ino, Fi, Cont, State)
       end),
    { noreply, State }.

release_async(Ctx, Ino, Fi, Cont, State) ->
    Result =
        case ets:lookup(State#amqpfs.inodes, Ino) of
            [{Ino,Path}] ->
                Response = remote(Path, {release, Path, Fi}, Ctx, State),
                case Response of
                    ok -> #fuse_reply_err{ err = ok };
                    eio -> #fuse_reply_err { err = eio};
                    _ -> #fuse_reply_err { err = einval}
                end;
            _ ->
            #fuse_reply_err{ err = enoent }
        end,
    fuserlsrv:reply(Cont, Result).

releasedir(_Ctx, _Ino, _Fi, _Cont, State) ->
  { #fuse_reply_err{ err = ok }, State }.

getlk(Ctx, Ino, Fi, Lock, Cont, State) ->
    spawn_link(fun () -> getlk_async(Ctx, Ino, Fi, Lock, Cont, State) end),
    {noreply, State}.

getlk_async(Ctx, Ino, Fi, Lock, Cont, State) ->
    Result =
        case ets:lookup(State#amqpfs.inodes, Ino) of
            [{Ino,Path}] ->
                case remote(Path, {get_lock, Path, Fi, Lock}, Ctx, State) of
                    #flock{}=Flock ->
                        #fuse_reply_lock{ flock = Flock };
                    Err ->
                        #fuse_reply_err{ err = Err }
                end;
        _ ->
                #fuse_reply_err{ err = enoent }
        end,
    fuserlsrv:reply (Cont, Result).

mkdir(Ctx, ParentIno, Name, Mode, Cont, State) ->
    mknod(Ctx, ParentIno, Name, Mode bor ?S_IFDIR, {0,0}, Cont, State).


%% removexattr(_Ctx, _Inode, _Name, _Cont, _State) ->
%%     io:format("ni: removexattr~n"),
%%     erlang:throw(not_implemented).

rename(Ctx, ParentIno, Name, NewParentIno, NewName, Cont, State) ->
    spawn_link(fun () -> rename_async(Ctx, ParentIno, Name, NewParentIno, NewName, Cont, State) end),
    {noreply, State}.

rename_async(Ctx, ParentIno, Name, NewParentIno, NewName, Cont, State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, ParentIno) of
        [{ParentIno,Path}] ->
            case ets:lookup(State#amqpfs.inodes, NewParentIno) of
                [{NewParentIno,NewPath}] ->
                    FullPath = amqpfs_util:concat_path([Path,binary_to_list(Name)]),
                    NewFullPath = amqpfs_util:concat_path([NewPath,binary_to_list(NewName)]),
                    remote(Path, {rename, FullPath, NewFullPath}, Ctx, State);
                _ ->
                    enoent
            end;
        _ ->
            enoent
    end,
    fuserlsrv:reply (Cont, #fuse_reply_err{ err = Result }).

                        


rmdir(Ctx, ParentIno, Name, Cont, State) ->
    spawn_link(fun () -> rmdir_async(Ctx, ParentIno, Name, Cont, State) end),
    { noreply, State }.

rmdir_async(Ctx, ParentIno, Name, Cont, State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, ParentIno) of
        [{ParentIno,Path}] ->
            FullPath = amqpfs_util:concat_path([Path,binary_to_list(Name)]),
            remote(Path, {rmdir, FullPath}, Ctx, State);
        _ ->
            enoent
    end,
    fuserlsrv:reply (Cont, #fuse_reply_err{ err = Result }).
            

setattr(Ctx, Ino, Attr, ToSet, Fi, Cont, State) ->
    spawn_link(fun () -> setattr_async(Ctx, Ino, Attr, ToSet, Fi, Cont, State) end),
    { noreply, State }.

setattr_async(Ctx, Ino, Attr, ToSet, _Fi, Cont, State) ->
    Result = 
    case ets:lookup(State#amqpfs.inodes, Ino) of
        [{Ino,Path}] ->
            case remote_getattr(Path, Ctx, State) of
                #stat{}=Stat -> 
                    Stat1 = remote_setattr(Path, Stat, Attr, ToSet, Ctx, State),
                    #fuse_reply_attr{ attr = Stat1 , attr_timeout_ms = 1000 };
                Err -> #fuse_reply_err { err = Err}
            end;
        [] ->
            #fuse_reply_err { err = enoent}
    end,
    fuserlsrv:reply (Cont, Result).



setlk(Ctx, Ino, Fi, Lock, Sleep, Cont, State) ->
    spawn_link(fun () -> setlk_async(Ctx, Ino, Fi, Lock, Sleep, Cont, State) end),
    {noreply, State}.

setlk_async(Ctx, Ino, Fi, Lock, Sleep, Cont, State) ->
    Result =
        case ets:lookup(State#amqpfs.inodes, Ino) of
            [{Ino,Path}] ->
                case remote(Path, {set_lock, Path, Fi, Lock, Sleep}, Ctx, State) of
                    Res ->
                        #fuse_reply_err{ err = Res }
                end;
        _ ->
                #fuse_reply_err{ err = enoent }
        end,
    fuserlsrv:reply (Cont, Result).

%% setxattr(_Ctx, _Inode, _Name, _Value, _Flags, _Cont, _State) ->
%%     io:format("ni: setxattr~n"),
%%     erlang:throw(not_implemented).

%% statfs(_Ctx, _Inode, _Cont, State) ->
%%     io:format("ni: statfs~n"),
%%     {noreply, State}.


%% symlink(_Ctx, _Link, _Inode, _Name, _Cont, _State) ->
%%     io:format("ni: symlink~n"),
%%     erlang:throw(not_implemented).

unlink(Ctx, ParentIno, Name, Cont, State) ->
    spawn_link(fun () -> unlink_async(Ctx, ParentIno, Name, Cont, State) end),
    { noreply, State }.

unlink_async(Ctx, ParentIno, Name, Cont, State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, ParentIno) of
        [{ParentIno,Path}] ->
            FullPath = amqpfs_util:concat_path([Path, Name]),
            remote(Path, {remove, FullPath}, Ctx, State);
        _ ->
            enoent
    end,
    fuserlsrv:reply (Cont, #fuse_reply_err{ err = Result }).
            
%%%%%%%%%%%

take_while (_, _, []) -> 
   [];
take_while (F, Acc, [ H | T ]) ->
   case F (H, Acc) of
    { continue, NewAcc } ->
       [ H | take_while (F, NewAcc, T) ];
     stop ->
       []
   end.

make_inode(Name,Extra, State) ->
  case ets:lookup (State#amqpfs.names, Name) of
      [{Name, { Ino, _ } }] ->
          { Ino, State };
      [] ->
          Inodes = State#amqpfs.inodes,
          Id = amqpfs_inode:alloc(),
          ets:insert(Inodes, {Id, Name}),
          ets:insert (State#amqpfs.names, {Name, {Id, Extra}}),
          { Id, State }
  end.

path_to_announced(Path, #amqpfs{ announcements = Announcements }=State) ->
    case ets:lookup(Announcements, Path) of
        [] ->
            path_to_announced(filename:dirname(Path), State);
        [{Path, _}] ->
            Path;
        Paths when is_list(Paths) andalso length(Paths) > 1 ->
            % that's a bag
            {Path, _} = hd(Paths),
            Path
    end.


register_response_route(Path, Command, #amqpfs{response_routes=Tab}) ->
    Route = list_to_binary(lists:flatten(io_lib:format("~w",[now()]))),
    ets:insert(Tab, {Route, self(), Path, Command}),
    Route.

unregister_response_route(Route, #amqpfs{response_routes=Tab}) ->
    ets:delete(Tab, Route).
            
remote_list_dir(Path, Ctx, State) ->
    remote(Path, {list_dir, Path}, Ctx, State).

remote_getattr(Path, Ctx, State) ->
    Stat0 = remote(Path, {getattr, Path}, Ctx, State),
    case ets:lookup (State#amqpfs.names, Path) of
        [{Path, {Ino, {directory, on_demand}}}] ->
            NLink = length(lists:filter(fun ({_Name, {Type, _}}) -> Type =:= directory end, remote_list_dir(Path, Ctx, State))) + 2, 
            % FIXME: shouldn't we assign executability if only this item is readable for particular category (owner/group/other)?
            Stat0#stat{ st_mode = ?S_IFDIR bor Stat0#stat.st_mode bor ?S_IXUSR bor ?S_IXGRP bor ?S_IXOTH, st_ino = Ino, st_nlink = NLink};
        [{Path, {Ino, {file, on_demand}}}] ->
            Stat0#stat{ st_mode = ?S_IFREG bor Stat0#stat.st_mode, st_ino = Ino, st_nlink = 1 };
        [] ->
            Stat0
    end.

remote_setattr(Path, Stat, Attr, ToSet, Ctx, State) ->
    remote(Path, {setattr, Path, Stat, Attr, ToSet}, Ctx, State).

remote(Path, Command, Ctx, #amqpfs{response_cache = Tab}=State) ->
    case ets:lookup(Tab, Command) of
        [{Command, _, -1, CachedData}] ->
            CachedData;
        [{Command, CachedAt, CacheTTL, CachedData}] ->
            Now = now(),
            case timer:now_diff(Now, CachedAt) >= CacheTTL of
                false ->
                    CachedData;
                true ->
                    ets:delete(Tab, Command),
                    remote_impl(Path, Command, Ctx, State)
            end;
        [] ->
            remote_impl(Path, Command, Ctx, State)
    end.

remote_impl(Path, Command, Ctx, #amqpfs{amqp_channel = Channel, response_cache = Tab}=State) ->
    Route = register_response_route(Path, Command, State),
    amqp_channel:call(Channel, #'basic.publish'{exchange = <<"amqpfs">>, routing_key = amqpfs_util:path_to_routing_key(Path)}, 
                      {amqp_msg, #'P_basic'{message_id = Route, headers = env_headers(State) ++ ctx_headers(Ctx) }, term_to_binary(Command)}),
    Response = 
        receive 
            {response, Data, 0} ->
                ets:delete(Tab, Command),
                Data;
            {response, Data, TTL} when is_integer(TTL) -> 
                ets:insert(Tab, {Command, now(), TTL, Data}),
                Data
        after ?ON_DEMAND_TIMEOUT ->
                []
        end,
    unregister_response_route(Route, State),
    Response.

       
env_headers(_State) ->
    {ok, Hostname} = inet:gethostname(),
    [{"node", longstr, atom_to_list(node())},
     {"hostname", longstr, Hostname}].

ctx_headers(#fuse_ctx{uid = UID, gid = GID, pid = PID}) ->
    [{"uid", long, UID},
     {"gid", long, GID},
     {"pid", long, PID}].
