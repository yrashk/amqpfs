-module(root_amqpfs_provider). 

-export([init/1, 
         list_dir/2,
         handle_info/2,
         allow_request/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

-record(root_amqpfs_provider_extra,
        {
          items
         }).

init(#amqpfs_provider_state{ channel = Channel }=State) ->
    Queue = amqpfs_util:announce_queue_name("ROOT"),
    #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{
                                                queue = Queue,
                                                passive = false, durable = true,
                                                exclusive = false, auto_delete = false,
                                                nowait = false, arguments = []}),
    #'queue.bind_ok'{} = amqp_channel:call(Channel, #'queue.bind'{
                                             queue = Queue, exchange = <<"amqpfs.announce">>,
                                             routing_key = <<"">>,
                                             nowait = false, arguments = []}),
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:subscribe(Channel, #'basic.consume'{
                                                                               queue = Queue,
                                                                               consumer_tag = <<"">>,
                                                                               no_local = false,
                                                                               no_ack = true,
                                                                               exclusive = false,
                                                                               nowait = false}, self()),
    receive
        #'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
    end,
    State1 = State#amqpfs_provider_state { extra = #root_amqpfs_provider_extra{ items = ets:new(items, [public, set]) } },
    amqpfs_provider:announce(directory, "/", State1),
    State1.

list_dir("/", #amqpfs_provider_state{ extra = Extra }) ->
    Items = Extra#root_amqpfs_provider_extra.items,
    ets:tab2list(Items).
    
handle_info({#'basic.deliver'{consumer_tag=_ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange = <<"amqpfs.announce">>, routing_key=_RoutingKey}, Content}, State) ->
    #amqp_msg{payload = Payload } = Content,
    #'P_basic'{content_type = ContentType, headers = _Headers} = Content#amqp_msg.props,
    Command = amqpfs_util:decode_payload(ContentType, Payload),
    {noreply, handle_command(Command, State)};

handle_info(_,_) ->
    ignore.

handle_command({announce, directory, {Path, on_demand}}, #amqpfs_provider_state{ extra = Extra }) when Path /= "/" ->
    case filename:dirname(Path) of
        "/" ->
            Items = Extra#root_amqpfs_provider_extra.items,
            ets:insert(Items, {filename:basename(Path), {directory, on_demand}});
        _ ->
            ignore
    end;
            

handle_command(_,_) ->
    ignore.
    

allow_request(#amqpfs_provider_state{request_headers = Headers}) ->
    {ok, Hostname} = inet:gethostname(),
    HostnameBin = list_to_binary(Hostname),
    case lists:keysearch(<<"hostname">>, 1, Headers) of 
        {value, {<<"hostname">>, _, HostnameBin}} ->
            true;
        _ ->
            false
    end.
