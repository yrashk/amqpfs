-define(AMQPFS_VERSION, "0.1").

-include_lib ("fuserl/src/fuserl.hrl").
-include("rabbitmq-erlang-client/include/amqp_client.hrl").

-define(CONTENT_TYPE_BIN, <<"application/octet-stream">>).
-define(CONTENT_TYPE_BERT, <<"application/x-bert">>).

-record(amqpfs_provider_state,
        {
          module,
          args = [],
          connection,
          channel,
          request_headers,
          request_command,
          extra
         }).

-record (amqpfs, { inodes, 
                   names,
                   announcements,
                   response_routes,
                   response_cache,
                   response_policies,
                   response_buffers,
                   amqp_conn, amqp_channel, amqp_consumer_tag, amqp_response_consumer_tag }).

-define(DEFAULT_RESPONSE_POLICY, first).
-define(DEFAULT_RESPONSE_AGGREGATION, first).

-define(DEFAULT_RESPONSE_POLICIES,
        [{list_dir, ?DEFAULT_RESPONSE_POLICY, ?DEFAULT_RESPONSE_AGGREGATION},
         {create, ?DEFAULT_RESPONSE_POLICY, ?DEFAULT_RESPONSE_AGGREGATION},
         {open, ?DEFAULT_RESPONSE_POLICY, ?DEFAULT_RESPONSE_AGGREGATION}, 
         {read, ?DEFAULT_RESPONSE_POLICY, ?DEFAULT_RESPONSE_AGGREGATION},
         {write, ?DEFAULT_RESPONSE_POLICY, ?DEFAULT_RESPONSE_AGGREGATION},
         {getattr, ?DEFAULT_RESPONSE_POLICY, ?DEFAULT_RESPONSE_AGGREGATION},
         {setattr, ?DEFAULT_RESPONSE_POLICY, ?DEFAULT_RESPONSE_AGGREGATION},
         {release, ?DEFAULT_RESPONSE_POLICY, ?DEFAULT_RESPONSE_AGGREGATION}
        ]).
          
