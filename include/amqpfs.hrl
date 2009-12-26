-define(AMQPFS_VERSION, "0.1").

-include_lib ("fuserl/src/fuserl.hrl").
-include("rabbitmq-erlang-client/include/amqp_client.hrl").

-define(CONTENT_TYPE_BIN, <<"application/octet-stream">>).
-define(CONTENT_TYPE_BERT, <<"application/x-bert">>).

-record(amqpfs_provider_state,
        {
          module,
          args = [],
          app_id,
          user_id,
          connection,
          channel,
          request_headers,
          request_command,
          extra
         }).

-record (amqpfs, { inodes, 
                   names,
                   announcements,
                   providers,
                   fuse_continuations,
                   response_routes,
                   response_cache,
                   response_policies,
                   response_buffers,
                   amqp_conn, amqp_channel, amqp_consumer_tag, amqp_response_consumer_tag }).

-define(DEFAULT_RESPONSE_COLLECT, first).
-define(DEFAULT_RESPONSE_REDUCE, first).

-define(DEFAULT_RESPONSE_POLICIES,
        [{list_dir, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {create, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {open, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE}, 
         {read, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {write, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {getattr, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {setattr, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {release, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {rmdir, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {remove, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {get_lock, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {set_lock, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {rename, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {link, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {symlink, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {readlink, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {flush, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {access, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE},
         {statfs, ?DEFAULT_RESPONSE_COLLECT, ?DEFAULT_RESPONSE_REDUCE}
        ]).
          
