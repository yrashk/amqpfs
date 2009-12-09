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

-include_lib ("fuserl/src/fuserl.hrl").
-include("rabbitmq-erlang-client/include/amqp_client.hrl").

-record (amqpfs, { inodes, 
                   names,
                   amqp_conn, amqp_channel, amqp_ticket, amqp_consumer_tag }).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link() ->
    start_link(false, "/amqpfs").

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
    #'access.request_ok'{ticket = Ticket} = amqp_channel:call(AmqpChannel, #'access.request'{realm = <<"/">>, 
                                                                                             exclusive = false,
                                                                                             passive = true,
                                                                                             active = true,
                                                                                             write = true,
                                                                                             read = true}),
    #'exchange.declare_ok'{} = amqp_channel:call(AmqpChannel, #'exchange.declare'{ticket = Ticket,
                                                                                  exchange = <<"amqpfs.announce">>,
                                                                                  type = <<"fanout">>,
                                                                                  passive = false, durable = true,
                                                                                  auto_delete = false, internal = false,
                                                                                  nowait = false, arguments = []}),
    Queue = list_to_binary("amqpfs.server->" ++ atom_to_list(node())),
    #'queue.declare_ok'{} = amqp_channel:call(AmqpChannel, #'queue.declare'{ticket = Ticket,
                                                                            queue = Queue,
                                                                            passive = false, durable = true,
                                                                            exclusive = false, auto_delete = false,
                                                                            nowait = false, arguments = []}),
    #'queue.bind_ok'{} = amqp_channel:call(AmqpChannel, #'queue.bind'{ticket = Ticket,
                                                                      queue = Queue, exchange = <<"amqpfs.announce">>,
                                                                      routing_key = <<"#">>,
                                                                      nowait = false, arguments = []}),
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:subscribe(AmqpChannel, #'basic.consume'{ticket = Ticket,
                                                                                                           queue = Queue,
                                                                                                           consumer_tag = <<"">>,
                                                                                                           no_local = false,
                                                                                                           no_ack = true,
                                                                                                           exclusive = false,
                                                                                                           nowait = false},
                                                                             self()),
    receive
          #'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
    end,
    State0 = #amqpfs{ inodes = gb_trees:from_orddict([{ 0, [] }]),
                      names = gb_trees:empty(),
                      amqp_conn = AmqpConn,
                      amqp_channel = AmqpChannel,
                      amqp_ticket = Ticket,
                      amqp_consumer_tag = ConsumerTag
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
                         {"/.amqpfs", {directory, ["version"]}},
                         {"/.amqpfs/version", {file, undefined}}
                        ]),
    { ok, State }.

code_change (_OldVsn, State, _Extra) -> { ok, State }.

handle_info({#'basic.deliver'{consumer_tag=ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange = <<"amqpfs.announce">>, routing_key=RoutingKey}, Content} , #amqpfs{amqp_consumer_tag = ConsumerTag}=State) ->
    #content{payload_fragments_rev = Payload, properties_bin = PropertiesBin, class_id = ClassId} = Content,
    #'P_basic'{content_type = ContentType, headers = Headers} = rabbit_framing:decode_properties(ClassId, PropertiesBin),
    Command =
    case ContentType of
        <<"application/x-bert">> ->
            binary_to_term(Payload);
        _ ->
            error
    end,
    {noreply, handle_command(Command, State)};

handle_info (_Msg, State) -> { noreply, State }.
terminate (_Reason, _State) -> ok.

-define (DIRATTR (X), #stat{ st_ino = (X), 
                             st_mode = ?S_IFDIR bor 8#0555, 
                             st_nlink = 1 }).
-define (LINKATTR, #stat{ st_mode = ?S_IFLNK bor 8#0555, st_nlink = 1 }).


handle_command({announce, directory, Path}, State) ->
    State;

handle_command({announce, file, Path}, State) ->
    State;

handle_command({cancel, directory, Path}, State) ->
    State;

handle_command({cancel, file, Path}, State) ->
    State.

getattr (_, Ino, _, State) ->
    case gb_trees:lookup(Ino, State#amqpfs.inodes) of
        {value, Path} ->
            case gb_trees:lookup(Path, State#amqpfs.names) of
                { value, { Ino, {directory, _List} } } ->
                            { #fuse_reply_attr{ attr = ?DIRATTR (Ino), attr_timeout_ms = 1000 }, State };
                { value, { Ino, {file, _} } } ->
                    { #fuse_reply_attr{ attr =    #stat{ st_ino = Ino, 
                                                         st_mode = ?S_IFREG bor 8#0444, 
                                                         st_size = 0 }, attr_timeout_ms = 1000 }, State };
                _ ->
                    { #fuse_reply_err{ err = enoent }, State }
            end;
        _ ->
            { #fuse_reply_err{ err = enoent }, State }
    end.


lookup (_, ParentIno, BinPath, _, State) ->
    case gb_trees:lookup(ParentIno, State#amqpfs.inodes) of
        {value, Path} ->
            Path1 = case Path of
                        "/" -> "";
                        _ -> Path
                    end,
            case gb_trees:lookup(Path, State#amqpfs.names) of
                { value, { ParentIno, {directory, List} } } ->
                    case lists:any(fun (P) -> P == BinPath end,  lists:map(fun erlang:list_to_binary/1, List)) of
                        true -> % there is something
                            case gb_trees:lookup(Path1 ++ "/" ++ binary_to_list(BinPath), State#amqpfs.names) of
                                {value, {Ino, {directory, _List}}} ->
                                    {#fuse_reply_entry{ 
                                          fuse_entry_param = #fuse_entry_param{ ino = Ino,
                                                                                generation = 1,  % (?)
                                                                                attr_timeout_ms = 1000,
                                                                                entry_timeout_ms = 1000,
                                                                                attr = ?DIRATTR (Ino) } },
                                     State};
                                {value, {Ino, {file, _}}} ->
                                    {#fuse_reply_entry{ 
                                          fuse_entry_param = #fuse_entry_param{ ino = Ino,
                                                                                generation = 1,  % (?)
                                                                                attr_timeout_ms = 1000,
                                                                                entry_timeout_ms = 1000,
                                                                                attr = #stat{ st_ino = Ino, 
                                                                                              st_mode = ?S_IFREG bor 8#0444, 
                                                                                              st_size = 0 } } },
                                     State};
                                _ ->
                                    { #fuse_reply_err{ err = enoent }, State }
                            end;
                        false ->
                            { #fuse_reply_err{ err = enoent }, State }
                    end;
                _ ->
                    { #fuse_reply_err{ err = einval }, State }
            end;
        _ ->
            { #fuse_reply_err{ err = enoent }, State }
    end;


lookup (_, _, _, _, State) ->
      { #fuse_reply_err{ err = enoent }, State }.

open (_, X, Fi = #fuse_file_info{}, _, State) when X >= 1, X =< 6 ->
      { #fuse_reply_err{ err = eacces }, State }.

read (_, X, Size, Offset, _Fi, _, State) ->
    { #fuse_reply_err{ err = einval }, State }.

readdir (_, Ino, Size, Offset, _Fi, _, State) ->
    {Contents, _} =
    case gb_trees:lookup(Ino, State#amqpfs.inodes) of
        {value, Path} ->
            Path1 = case Path of
                        "/" -> "";
                        _ -> Path
                    end,
            case gb_trees:lookup(Path, State#amqpfs.names) of
                { value, { Ino, {directory, List } }} ->
                    lists:foldl(fun (P, {L, Acc}) ->
                                        case gb_trees:lookup(Path1 ++ "/" ++ P, State#amqpfs.names) of                      
                                            {value, {ChildIno, {directory, _Extra}}} ->
                                                {L ++ [#direntry{ name = P, offset = Acc, stat = ?DIRATTR(ChildIno)}], Acc + 1};
                                            {value, {ChildIno, {file, _}}} ->
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
    { #fuse_reply_direntrylist{ direntrylist = DirEntryList }, State };


readdir (_, 2, Size, Offset, _Fi, _, State) ->
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
          [ #direntry{ name = ".", offset = 1, stat = ?DIRATTR (1) },
            #direntry{ name = "..", offset = 2, stat = ?DIRATTR (1) }
          ])),
  { #fuse_reply_direntrylist{ direntrylist = DirEntryList }, State }.

readlink (_, X, _, State) ->
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

make_inode (Name, Extra, State) ->
  case gb_trees:lookup (Name, State#amqpfs.names) of
      { value, { Ino, Extra } } ->
          { Ino, State };
      {value, { Ino, _ } } ->
          { Ino, State };
      none ->
          Inodes = State#amqpfs.inodes,
          { Max, _ } = gb_trees:largest (Inodes),
          NewInodes = gb_trees:insert (Max + 1, Name, Inodes),
          Names = State#amqpfs.names,
          NewNames = gb_trees:insert (Name, { Max + 1, Extra }, Names),
          { Max + 1, State#amqpfs{ inodes = NewInodes, names = NewNames } }
  end.

ensure_path("/", State) ->
    make_inode("/", undefined, State);
ensure_path(Path, State0) ->
    Dir = filename:dirname(Path),
    State1 = make_inode(Path, {diretory, []}, State0),
    make_inode(Dir, {directory, [Path]}, State1).
