{application, amqpfs,
[{description, "AMQPFS"},
{author, "yrashk@scallable.com"},
{vsn, "0.1"},
{mod, {amqpfs_app, []}},
{modules, [
           amqpfs, amqpfs_sup, amqpfs_server,
           root_amqpfs_provider, dot_amqpfs_provider
          ]},
{registered, [amqpfs]},
{applications, [kernel, stdlib, erabbitmq]}
]}.
