-module(amqpfs_announce).
-export([directory/3, directory/4,
         file/3]).

-include_lib("amqpfs/include/amqpfs.hrl").

directory(Channel, Ticket, Path) ->
    directory(Channel, Ticket, Path, on_demand).

directory(Channel, Ticket, Path, Contents) ->
    amqp_channel:call(Channel, #'basic.publish'{ticket=Ticket, exchange= <<"amqpfs.announce">>}, {amqp_msg, #'P_basic'{}, term_to_binary({announce, directory, {Path,Contents}})}).

file(Channel, Ticket, Path) ->
    amqp_channel:call(Channel, #'basic.publish'{ticket=Ticket, exchange= <<"amqpfs.announce">>}, {amqp_msg, #'P_basic'{}, term_to_binary({announce, file, {Path,undefined}})}).


