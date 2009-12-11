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
           read/7,
           readdir/7,
           readlink/4 ]).

-include_lib("amqpfs/include/amqpfs.hrl").

-record (amqpfs, { inodes, 
                   names,
                   response_routes,
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

    State0 = #amqpfs{ inodes = ets:new(inodes, [public, ordered_set]),
                      names = ets:new(names, [public, set]),
                      response_routes = ets:new(response_routes, [public, set]),
                      amqp_conn = AmqpConn,
                      amqp_channel = AmqpChannel,
                      amqp_consumer_tag = ConsumerTag,
                      amqp_response_consumer_tag = ResponseConsumerTag
                  },
    State = lists:foldl(fun (D, StateAcc) -> 
                                case D of
                                    {Path, {directory, Contents}} ->
                                        case filename:basename(Path) of
                                            "" -> % root
                                                {_, StateAcc1} = make_inode("/", {directory, Contents}, StateAcc),
                                                StateAcc1;
                                            _ ->
                                                {_, StateAcc1} = ensure_path(filename:dirname(Path), StateAcc),
                                                {_, StateAcc2} = make_inode(Path, {directory, Contents}, StateAcc1),
                                                StateAcc2
                                        end;
                                    {Path, {file, X}} ->
                                        {_, StateAcc1} = make_inode(Path, {file, X}, StateAcc),
                                        StateAcc1
                                end
                        end, State0, 
                        [{"/", {directory, [".amqpfs"]}},
                         {"/.amqpfs", {directory, ["version","server"]}},
                         {"/.amqpfs/version", {file, undefined}},
                         {"/.amqpfs/server", {file, undefined}}
                        ]),
    { ok, State }.

code_change (_OldVsn, State, _Extra) -> { ok, State }.

handle_info({#'basic.deliver'{consumer_tag=ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange = <<"amqpfs.response">>, routing_key=_RoutingKey}, Content} , #amqpfs{response_routes = Tab, amqp_response_consumer_tag = ConsumerTag}=State) ->
    #amqp_msg{payload = Payload } = Content,
    #'P_basic'{content_type = ContentType, headers = _Headers, reply_to = Route} = Content#amqp_msg.props,
    Response = decode_payload(ContentType, Payload),
    case ets:lookup(Tab, Route) of 
        [{Route, Pid}] ->
            Pid ! {response, Response};
        _ ->
            discard
    end,
    {noreply, State};
    
handle_info({#'basic.deliver'{consumer_tag=ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange = <<"amqpfs.announce">>, routing_key=_RoutingKey}, Content} , #amqpfs{amqp_consumer_tag = ConsumerTag}=State) ->
    #amqp_msg{payload = Payload } = Content,
    #'P_basic'{content_type = ContentType, headers = _Headers} = Content#amqp_msg.props,
    Command = decode_payload(ContentType, Payload),
    {noreply, handle_command(Command, State)};

handle_info (_Msg, State) -> { noreply, State }.
terminate (_Reason, _State) -> ok.

-define (DIRATTR (X), #stat{ st_ino = (X), 
                             st_mode = ?S_IFDIR bor 8#0555, 
                             st_nlink = 1 }).
-define (LINKATTR, #stat{ st_mode = ?S_IFLNK bor 8#0555, st_nlink = 1 }).


handle_command({announce, directory, {Path, Contents}}, State) ->
    {_, State1} = ensure_path(Path, {directory, Contents}, State),
    {_, State2} = make_inode(Path, {directory, Contents}, State1),
    State2;

handle_command({announce, file, {Path, Extra}}, State) ->
    {_, State2} = make_inode(Path, {file, Extra}, State),
    State3 = add_item(filename:dirname(Path),filename:basename(Path), State2),
    State3;

handle_command({cancel, directory, _Path}, State) ->
    State;

handle_command({cancel, file, _Path}, State) ->
    State.

getattr(Ctx, Ino, Cont, State) ->
    spawn_link(fun () -> getattr_async(Ctx,
                                       Ino,
                                       Cont,
                                       State)
               end),
    { noreply, State }.

getattr_async(_Ctx, Ino, Cont, #amqpfs{amqp_channel = Channel}=State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, Ino) of
        [{Ino, Path}] ->
            case ets:lookup(State#amqpfs.names, Path) of
                [{Path, { Ino, {directory, _List} } }] ->
                    #fuse_reply_attr{ attr = ?DIRATTR (Ino), attr_timeout_ms = 1000 };
                [{"/.amqpfs/version", { Ino, {file, _} } }] ->
                    #fuse_reply_attr{ attr = #stat{ st_ino = Ino, st_size = length(?AMQPFS_VERSION), st_mode = ?S_IFREG bor 8#0444 }, attr_timeout_ms = 1000 };
                [{Path, { Ino, {file, undefined} } }] ->
                    #fuse_reply_attr{ attr = #stat{ st_ino = Ino, st_size = 0, st_mode = ?S_IFREG bor 8#0444 }, attr_timeout_ms = 1000 };
                [{Path, { Ino, {file, on_demand} } }] ->
                    Route = register_response_route(State),
                    amqp_channel:call(Channel, #'basic.publish'{exchange= <<"amqpfs">>, routing_key = amqpfs_util:path_to_routing_key(Path)}, {amqp_msg, #'P_basic'{message_id = Route}, term_to_binary({getattr, Path})}),
                    Response = 
                        receive 
                            {response, Data} -> Data
                        after ?ON_DEMAND_TIMEOUT ->
                                []
                        end,
                    unregister_response_route(Route, State),
                    case Response of
                        #stat{}=Stat -> #fuse_reply_attr{ attr = Stat#stat{ st_ino = Ino }, attr_timeout_ms = 1000 };
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

lookup_async(_Ctx, ParentIno, BinPath, Cont, State) ->
    case ets:lookup(State#amqpfs.inodes, ParentIno) of
        [{ParentIno,Path}] ->
            Path1 = case Path of
                        "/" -> "";
                        _ -> Path
                    end,
            Result =
            case ets:lookup(State#amqpfs.names, Path) of
                [{Path, { ParentIno, {directory, List} } }] when is_list(List) ->
                    lookup_impl(BinPath, Path1, List, State);
                [{Path, { ParentIno, {directory, on_demand}}}] ->
                    Response = directory_on_demand(Path, State),
                    List = lists:map(fun ({P,E}) -> 
                                             Path2 = Path1 ++ "/" ++ P,
                                             {_Ino, _} = make_inode(Path2, E, State),
                                             P
                                     end, Response),
                    lookup_impl(BinPath, Path1, List, State);
                _ ->
                    #fuse_reply_err{ err = enoent }
            end,
            fuserlsrv:reply (Cont, Result);
        _ ->
            fuserlsrv:reply (Cont, #fuse_reply_err{ err = enoent })
    end.

lookup_impl(BinPath, Path, List, State) ->
    case lists:any(fun (P) -> P == BinPath end,  lists:map(fun erlang:list_to_binary/1, List)) of
        true -> % there is something
            Path2 = Path ++ "/" ++ binary_to_list(BinPath),
            case ets:lookup(State#amqpfs.names, Path2) of
                [{Path2, {Ino, {directory, _List}}}] ->
                    #fuse_reply_entry{ 
                  fuse_entry_param = #fuse_entry_param{ ino = Ino,
                                                        generation = 1,  % (?)
                                                        attr_timeout_ms = 1000,
                                                        entry_timeout_ms = 1000,
                                                        attr = ?DIRATTR (Ino) } };
                [{Path2, {Ino, {file, _}}}] ->
                    #fuse_reply_entry{ 
                  fuse_entry_param = #fuse_entry_param{ ino = Ino,
                                                        generation = 1,  % (?)
                                                        attr_timeout_ms = 1000,
                                                        entry_timeout_ms = 1000,
                                                        attr = #stat{ st_ino = Ino, 
                                                                      st_mode = ?S_IFREG bor 8#0444, 
                                                                      st_size = 0 } } };
                _ ->
                    #fuse_reply_err{ err = enoent }
            end;
        false ->
            #fuse_reply_err{ err = enoent }
    end.


open(Ctx, Ino, Fi, Cont, State) ->
    spawn_link(fun () -> open_async(Ctx, Ino, Fi, Cont, State) end),
    { noreply, State }.

open_async(_Ctx, Ino, Fi, Cont, #amqpfs{amqp_channel = Channel}=State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, Ino) of
        [{Ino, "/.amqpfs/version"}] ->
            #fuse_reply_open{fuse_file_info = Fi};
        [{Ino,Path}] ->
            Route = register_response_route(State),
            amqp_channel:call(Channel, #'basic.publish'{exchange= <<"amqpfs">>, routing_key = amqpfs_util:path_to_routing_key(Path)}, {amqp_msg, #'P_basic'{message_id = Route}, term_to_binary({open, Path})}),
            Response = 
                receive 
                    {response, Data} -> Data
                after ?ON_DEMAND_TIMEOUT ->
                        []
                end,
            unregister_response_route(Route, State),
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

read_async(_Ctx, Ino, Size, Offset, _Fi, Cont,  #amqpfs{amqp_channel = Channel}=State) ->
    Result =
    case ets:lookup(State#amqpfs.inodes, Ino) of
        [{Ino, "/.amqpfs/version"}] ->
            #fuse_reply_buf { size = length(?AMQPFS_VERSION), buf = list_to_binary(?AMQPFS_VERSION) };
        [{Ino,Path}] ->
            Route = register_response_route(State),
            amqp_channel:call(Channel, #'basic.publish'{exchange= <<"amqpfs">>, routing_key = amqpfs_util:path_to_routing_key(Path)}, {amqp_msg, #'P_basic'{message_id = Route}, term_to_binary({read, Path, Size, Offset})}),
            Response = 
                receive 
                    {response, Data} -> Data
                after ?ON_DEMAND_TIMEOUT ->
                        []
                end,
            unregister_response_route(Route, State),
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

readdir_async(_Ctx, Ino, Size, Offset, _Fi, Cont, #amqpfs{}=State) ->
    {Contents, _} =
        case ets:lookup(State#amqpfs.inodes, Ino) of
            [{Ino,Path}] ->
                Path1 = case Path of
                            "/" -> "";
                            _ -> Path
                        end,
                case ets:lookup(State#amqpfs.names, Path) of
                    [{Path, { Ino, {directory, on_demand}}}] ->
                        Response = directory_on_demand(Path, State),
                        lists:foldl(fun ({P, E}, {L, Acc}) -> % it is a copy paste of the stuff below
                                            Path2 = Path1 ++ "/" ++ P,
                                            make_inode(Path2, E, State),
                                            case ets:lookup(State#amqpfs.names, Path2) of                      
                                                [{Path2, {ChildIno, {directory, _Extra}}}] ->
                                                    {L ++ [#direntry{ name = P, offset = Acc, stat = ?DIRATTR(ChildIno)}], Acc + 1};
                                                [{Path2, {ChildIno, {file, _}}}] ->
                                                    {L ++ [#direntry{ name = P, offset = Acc, stat = 
                                                                      #stat{ st_ino = ChildIno, 
                                                                             st_mode = ?S_IFREG bor 8#0444, 
                                                                             st_size = 0 }
                                                                     }], Acc + 1};
                                                _ ->
                                                    {L, Acc}
                                            end
                                    end, {[],3},  Response);
                    [{Path, { Ino, {directory, List } }}] when is_list(List) ->
                        lists:foldl(fun (P, {L, Acc}) ->
                                            Path2 = Path1 ++ "/" ++ P,
                                            case ets:lookup(State#amqpfs.names, Path2) of                      
                                                [{Path2, {ChildIno, {directory, _Extra}}}] ->
                                                    {L ++ [#direntry{ name = P, offset = Acc, stat = ?DIRATTR(ChildIno)}], Acc + 1};
                                                [{Path2, {ChildIno, {file, _}}}] ->
                                                    {L ++ [#direntry{ name = P, offset = Acc, stat = 
                                                                      #stat{ st_ino = ChildIno, 
                                                                             st_mode = ?S_IFREG bor 8#0444, 
                                                                             st_size = 0 }
                                                                     }], Acc + 1};
                                                _ ->
                                                    {L, Acc}
                                            end
                                    end, {[],3},  List);
                    _ ->
                        {[], 3}
                end;
            _ ->
                {[], 3}
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
            [ #direntry{ name = ".", offset = 1, stat = ?DIRATTR (Ino) },
              #direntry{ name = "..", offset = 2, stat = ?DIRATTR (Ino) }
             ] ++ Contents)),
    fuserlsrv:reply (Cont, #fuse_reply_direntrylist{ direntrylist = DirEntryList }).

readlink (_, _, _, State) ->
    { #fuse_reply_err{ err = einval }, State }.


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

ensure_path("/", State) ->
    make_inode("/", undefined, State);
ensure_path(Path, State0) ->
    ensure_path(Path, {directory, []}, State0).

ensure_path(Path, Extra, State0) ->
    Dir = filename:dirname(Path),
    Base = filename:basename(Path),
    {_, State1} = make_inode(Path, Extra, State0),
    {Ino, State2} = ensure_path(Dir, State1),
    State3 = add_item(Dir, Base, State2),
    {Ino, State3}.

add_item(Path, Item, State) ->    
        case ets:lookup(State#amqpfs.names, Path) of
            [{Path, {Ino, {directory, List}}}] ->
                case lists:any(fun (I) -> I == Item end, List) of
                    false ->
                        ets:insert(State#amqpfs.names, {Path, {Ino, {directory, List ++ [Item]}}});
                    _ ->
                        skip
                end;
        _ ->
            skip
    end,
    State.

register_response_route(#amqpfs{response_routes=Tab}) ->
    Route = list_to_binary(lists:flatten(io_lib:format("~w",[now()]))),
    ets:insert(Tab, {Route, self()}),
    Route.

unregister_response_route(Route, #amqpfs{response_routes=Tab}) ->
    ets:delete(Tab, Route).
            
directory_on_demand(Path, #amqpfs{amqp_channel = Channel}=State) ->
    Route = register_response_route(State),
    amqp_channel:call(Channel, #'basic.publish'{exchange= <<"amqpfs">>, routing_key = amqpfs_util:path_to_routing_key(Path)}, {amqp_msg, #'P_basic'{message_id = Route}, term_to_binary({list, directory, Path})}),
    Response = 
        receive 
            {response, Data} -> Data
        after ?ON_DEMAND_TIMEOUT ->
                []
        end,
    unregister_response_route(Route, State),        
    Response.
    
decode_payload(ContentType, Payload) ->    
    case ContentType of
        ?CONTENT_TYPE_BERT ->
            binary_to_term(Payload);
        ?CONTENT_TYPE_BIN ->
            Payload;
        _ ->
            binary_to_term(Payload) % by default, attempt BERT, but FIXME: it might be a bad idea in a long run
    end.
    

