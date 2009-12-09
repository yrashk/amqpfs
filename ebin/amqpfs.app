{application, amqpfs,
[{description, "AMQP FUSE FS"},
{author, "yrashk@scallable.com"},
{vsn, "0.1"},
{mod, {amqpfs_app, []}},
{modules, [
          amqpfs, amqpfs_sup, amqpfs_server
           ]},
{registered, [amqpfs_server]},
{applications, [kernel, stdlib, erabbitmq]}
]}.