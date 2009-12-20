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
