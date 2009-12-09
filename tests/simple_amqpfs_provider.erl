-module(simple_amqpfs_provider). 
-export([start/0]).

-include("rabbitmq-erlang-client/include/amqp_client.hrl").

start() ->
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
    #'exchange.declare_ok'{} = amqp_channel:call(AmqpChannel, #'exchange.declare'{ticket = Ticket,
                                                                                  exchange = <<"amqpfs.response">>,
                                                                                  type = <<"direct">>,
                                                                                  passive = false, durable = false,
                                                                                  auto_delete = false, internal = false,
                                                                                  nowait = false, arguments = []}),
    #'exchange.declare_ok'{} = amqp_channel:call(AmqpChannel, #'exchange.declare'{ticket = Ticket,
                                                                                  exchange = <<"amqpfs">>,
                                                                                  type = <<"topic">>,
                                                                                  passive = false, durable = false,
                                                                                  auto_delete = false, internal = false,
                                                                                  nowait = false, arguments = []}),
    Queue = list_to_binary("simple_amqpfs_provider@" ++ atom_to_list(node())),
    #'queue.declare_ok'{} = amqp_channel:call(AmqpChannel, #'queue.declare'{ticket = Ticket,
                                                                            queue = Queue,
                                                                            passive = false, durable = false,
                                                                            exclusive = false, auto_delete = false,
                                                                            nowait = false, arguments = []}),
    #'queue.bind_ok'{} = amqp_channel:call(AmqpChannel, #'queue.bind'{ticket = Ticket,
                                                                      queue = Queue, exchange = <<"amqpfs">>,
                                                                      routing_key = amqpfs_util:path_to_matching_routing_key("/simple_on_demand"),
                                                                      nowait = false, arguments = []}),
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:subscribe(AmqpChannel, #'basic.consume'{ticket = Ticket,
                                                                                                           queue = Queue,
                                                                                                           consumer_tag = <<"">>,
                                                                                                           no_local = false,
                                                                                                           no_ack = true,
                                                                                                           exclusive = false,
                                                                                                           nowait = false}, self()),
    receive
          #'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
    end,
    amqpfs_announce:directory(AmqpChannel, Ticket, "/simple",[]),
    amqpfs_announce:directory(AmqpChannel, Ticket, "/simple_on_demand"),
    amqpfs_announce:file(AmqpChannel, Ticket, "/just_a_file"),
    amqpfs_announce:file(AmqpChannel, Ticket, "/simple/anotherfile"),
    amqpfs_announce:file(AmqpChannel, Ticket, "/simple/anotherfile1"),
    loop(AmqpChannel, Ticket).

loop(AmqpChannel, Ticket) ->
    receive
        {#'basic.deliver'{consumer_tag=ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange = <<"amqpfs">>, routing_key=RoutingKey}, Content} ->
            #amqp_msg{payload = Payload } = Content,
            #'P_basic'{content_type = ContentType, headers = Headers, message_id = MessageId} = Content#amqp_msg.props,
            Command =
                case ContentType of
                    _ ->
                        binary_to_term(Payload)
                end,
            case Command of
                {list, directory, Path} ->
                    io:format("list directory ~s for ~p ~n",[Path, MessageId]),
                    amqp_channel:call(AmqpChannel, #'basic.publish'{ticket=Ticket, exchange= <<"amqpfs.response">>}, {amqp_msg, #'P_basic'{reply_to = MessageId}, term_to_binary([{"bogus",{file, undefined}}])});
                Other ->
                    io:format("Unknown request ~p~n",[Other])
            end;
        OtherCommand ->
            io:format("Unknown command ~p~n",[OtherCommand])
    end,
    loop(AmqpChannel, Ticket).
            


                    
                    


