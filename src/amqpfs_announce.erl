-module(amqpfs_announce).
-export([directory/2]).

-include_lib("amqpfs/include/amqpfs.hrl").

directory(Channel, Path) ->
    directory(Channel, Path, on_demand).

directory(Channel, Path, Contents) ->
    amqp_channel:call(Channel, #'basic.publish'{exchange= <<"amqpfs.announce">>}, {amqp_msg, #'P_basic'{}, term_to_binary({announce, directory, {Path,Contents}})}).
