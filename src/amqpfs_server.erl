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

-record (amqpfs, { inodes, names }).

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
  State = #amqpfs{ inodes = gb_trees:from_orddict ([ { 6, [] } ]),
                   names = gb_trees:empty () },
  { ok, State }.

code_change (_OldVsn, State, _Extra) -> { ok, State }.
handle_info (_Msg, State) -> { noreply, State }.
terminate (_Reason, _State) -> ok.

-define (DIRATTR (X), #stat{ st_ino = (X), 
                             st_mode = ?S_IFDIR bor 8#0555, 
                             st_nlink = 1 }).
-define (LINKATTR, #stat{ st_mode = ?S_IFLNK bor 8#0555, st_nlink = 1 }).

% / -> 1
% /.amqpfs -> 2

getattr (_, 1, _, State) ->
  { #fuse_reply_attr{ attr = ?DIRATTR (1), attr_timeout_ms = 1000 }, State };
getattr (_, 2, _, State) ->
  { #fuse_reply_attr{ attr = ?DIRATTR (2), attr_timeout_ms = 1000 }, State }.


lookup (_, 1, <<".amqpfs">>, _, State) ->
  { #fuse_reply_entry{ 
      fuse_entry_param = #fuse_entry_param{ ino = 6,
                                            generation = 1,  % (?)
                                            attr_timeout_ms = 1000,
                                            entry_timeout_ms = 1000,
                                            attr = ?DIRATTR (2) } },
    State };

lookup (_, 1, _, _, State) ->
    { #fuse_reply_err{ err = einval }, State }.

open (_, X, Fi = #fuse_file_info{}, _, State) when X >= 1, X =< 6 ->
      { #fuse_reply_err{ err = eacces }, State }.

read (_, X, Size, Offset, _Fi, _, State) ->
    { #fuse_reply_err{ err = einval }, State }.

readdir (_, 1, Size, Offset, _Fi, _, State) ->
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
            #direntry{ name = "..", offset = 2, stat = ?DIRATTR (1) },
            #direntry{ name = ".amqpfs", offset = 3, stat = ?DIRATTR(2) }
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

