-module (amqpfs_server).
-export ([start_link/0, start_link/2, start_link/3]).
%-behaviour (fuserl).
-export ([ code_change/3,
           handle_info/2,
           init/1,
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

  %%          getlk/6,
%%            link/6,
%%            mkdir/6,
%%            removexattr/5,
%%            rename/7,
%%            rmdir/5,
           setattr/7
%%            setlk/7,
%%            setxattr/7,
%%            statfs/4,
%%            symlink/6,
%%            unlink/5

          ]).

-include_lib("amqpfs/include/amqpfs.hrl").

-record (amqpfs, { inodes, 
                   names,
                   response_routes,
                   response_cache,
                   amqp_conn, amqp_channel, amqp_consumer_tag, amqp_response_consumer_tag }).

-define(ON_DEMAND_TIMEOUT, 60000).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link() ->
    start_link(false, proplists:get_value(mount_point, application:get_all_env(amqpfs),"/amqpfs")).

start_link (LinkedIn, Dir) ->
    start_link (LinkedIn, Dir, "").

start_link (LinkedIn, Dir, MountOpts) ->
    fuserlsrv:start_link (?MODULE, LinkedIn, MountOpts, Dir, [], []).

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
                     response_routes = ets:new(response_routes, [public, set]),
                     response_cache = ets:new(response_cache, [public, set]),
                     amqp_conn = AmqpConn,
                     amqp_channel = AmqpChannel,
                     amqp_consumer_tag = ConsumerTag,
                     amqp_response_consumer_tag = ResponseConsumerTag
                  },
    { ok, State }.

code_change (_OldVsn, State, _Extra) -> { ok, State }.

handle_info({#'basic.deliver'{consumer_tag=ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, 
                              exchange = <<"amqpfs.response">>, routing_key=_RoutingKey}, Content}, 
            #amqpfs{response_routes = Tab, amqp_response_consumer_tag = ConsumerTag}=State) ->
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
    case ets:lookup(Tab, Route) of 
        [{Route, Pid}] ->
            Pid ! {response, Response, TTL};
        _ ->
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

handle_info (_Msg, State) -> { noreply, State }.
terminate (_Reason, _State) -> ok.

handle_command({announce, directory, {Path, Contents}}, State) ->
    {_, State1} = make_inode(Path, {directory, Contents}, State),
    State1;

handle_command({cancel, directory, _Path}, State) ->
    State.

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
            Path1 = case Path of
                        "/" -> "";
                        _ -> Path
                    end,
            Result =
            case ets:lookup(State#amqpfs.names, Path) of
                [{Path, { ParentIno, {directory, on_demand}}}] ->
                    Response = remote_list_dir(Path, Ctx, State),
                    List = lists:map(fun ({P,E}) -> 
                                             Path2 = Path1 ++ "/" ++ P,
                                             {_Ino, _} = make_inode(Path2, E, State),
                                             P
                                     end, Response),
                    lookup_impl(BinPath, Path1, List, Ctx, State);
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
            Path2 = Path ++ "/" ++ binary_to_list(BinPath),
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
                Path1 = case Path of
                            "/" -> "";
                            _ -> Path
                        end,
                case ets:lookup(State#amqpfs.names, Path) of
                    [{Path, { Ino, {directory, on_demand}}}] ->
                        Response = remote_list_dir(Path, Ctx, State),
                        lists:foldl(fun ({P, E}, {L, Acc}) -> 
                                            Path2 = Path1 ++ "/" ++ P,
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


mknod(Ctx, ParentIno, Name, Mode, Dev, Cont, State) ->
    spawn_link 
      (fun () -> 
               mknod_async(Ctx, ParentIno, Name, Mode, Dev, Cont, State) 
       end).

mknod_async(Ctx, ParentIno, Name, Mode, _Dev, Cont, State) ->
    Result = 
    case ets:lookup(State#amqpfs.inodes, ParentIno) of
        [{ParentIno,Path}] ->
            case remote(Path, {create, Path, Name}, Ctx, State) of
                ok ->
                    remote_setattr(Path, remote_getattr(Path, Ctx, State), #stat{ st_mode = Mode }, ?FUSE_SET_ATTR_MODE, Ctx, State),
                    ok;
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
            case remote(Path, {create, Path, Name}, Ctx, State) of
                ok ->
                    remote_setattr(Path, remote_getattr(Path, Ctx, State), #stat{ st_mode = Mode }, ?FUSE_SET_ATTR_MODE, Ctx, State),
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

%% getlk(_Ctx, _Inode, _Fi, _Lock, _Cont, _State) ->
%%     io:format("ni: getlk~n"),
%%     erlang:throw(not_implemented).

%% link(_Ctx, _Ino, _NewParent, _NewName, _Cont, _State) ->
%%     io:format("ni: link~n"),
%%     erlang:throw(not_implemented).

%% mkdir(_Ctx, _ParentInode, _Name, _Mode, _Cont, _State) ->
%%     io:format("ni: mkdir~n"),
%%     erlang:throw(not_implemented).


%% removexattr(_Ctx, _Inode, _Name, _Cont, _State) ->
%%     io:format("ni: removexattr~n"),
%%     erlang:throw(not_implemented).

%% rename(_Ctx, _Parent, _Name, _NewParent, _NewName, _Cont, _State) ->
%%     io:format("ni: rename~n"),
%%     erlang:throw(not_implemented).

%% rmdir(_Ctx, _Inode, _Name, _Cont, _State) ->
%%     io:format("ni: rmdir~n"),
%%     erlang:throw(not_implemented).

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


%% setlk(_Ctx, _Inode, _Fi, _Lock, _Sleep, _Cont, _State) ->
%%     io:format("ni: setlk~n"),
%%     erlang:throw(not_implemented).

%% setxattr(_Ctx, _Inode, _Name, _Value, _Flags, _Cont, _State) ->
%%     io:format("ni: setxattr~n"),
%%     erlang:throw(not_implemented).

%% statfs(_Ctx, _Inode, _Cont, State) ->
%%     io:format("ni: statfs~n"),
%%     {noreply, State}.


%% symlink(_Ctx, _Link, _Inode, _Name, _Cont, _State) ->
%%     io:format("ni: symlink~n"),
%%     erlang:throw(not_implemented).

%% unlink(_Ctx, _Inode, _Name, _Cont, _State) ->
%%     io:format("ni: unlink~n"),
%%     erlang:throw(not_implemented).
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

make_inode (Name,Extra, State) ->
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



register_response_route(#amqpfs{response_routes=Tab}) ->
    Route = list_to_binary(lists:flatten(io_lib:format("~w",[now()]))),
    ets:insert(Tab, {Route, self()}),
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
    Route = register_response_route(State),
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
