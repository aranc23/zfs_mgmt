# ZfsMgmt

zfs_mgmt aims to provide some useful helpers for managing zfs snapshots, and eventually send/recv duties via the zfsmgr script in bin/.

Currently only snapshot destruction is implemented by a policy specification stored in zfs properties.

## Installation

Currently zfs_mgmt is only useful for it's zfsmgr binary, although
eventually the library might be useful for writing other applications
around managing zfs.

Therefore, building the gem and installing, or running ruby inside the src/ directory would be most useful:

    $ ruby -I lib bin/zfsmgr

## Usage

    Commands:
      zfsmgr help [COMMAND]               # Describe available commands or one specific command
      zfsmgr snapshot SUBCOMMAND ...ARGS  # manage snapshots
      zfsmgr zfsget [ZFS]                 # execute zfs get for the given properties and types and parse the output into a nested hash
    
    
      zfsmgr snapshot destroy         # apply the snapshot destroy policy to zfs
      zfsmgr snapshot help [COMMAND]  # Describe subcommands or one specific subcommand
      zfsmgr snapshot policy          # print the policy table for zfs
    
    Options:
      [--noop], [--no-noop]        # pass -n option to zfs commands
      [--verbose], [--no-verbose]  # pass -v option to zfs commands
      [--debug], [--no-debug]      # set logging level to debug
      [--filter=FILTER]            # only act on zfs matching this regexp
                                   # Default: .+



## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/aranc23/zfs_mgmt.

## zfs user properties

Destruction of zfs snapshots is based on the following zfs user properties:

### zfsmgmt:manage
manage snapshots for this filesystem if this property is 'true' (string literal)
 
### zfsmgmt:policy

A policy specification consisting of the number of snapshots of a certain time frame to keep, such as:

- 30d 
- 8w15d
- 1y1m1y1d1h
- 72h

The order each timeframe is listed in does not matter, and the supported specs are as follows:

- h - hourly
- d - daily
- w - weekly
- m - monthly
- y - yearly

### zfsmgmt:minage
The minimum age of a snapshot before it will be considered for
deletion, as specified in seconds, or using a multiplier of:

- s (seconds, same as not specifiying a multiplier)
- m (minutes, x60)
- h (hours, x60x60)
- d (days, x24x60x60)
- w (weeks, x7x24x60x60)

The intended purpose of minage is to keep recent snapshots regardless
of policy, possibly to ensure zfs send/recv has recent snapshots to
work with, or simply out of paranoia.

### zfsmgmt:matchsnaps
If this property is set, the snapshot portion of a snapshot name
(right of the @) must match this as interpreted as a regular
expression in order to match the policy as specified above.  The
intended use is to match application specific snapshots, (ie: ^backup-
) in an environment where automatic snapshots are still created but
there is no need to keep them. Snapshots matching this pattern can and
will still be deleted if they aren't marked to be saved by the policy
in place for the zfs.

### zfsmgmt:ignoresnaps
Ignore snapshots matching this regexp pattern.  They are neither used
to match the specified policy for the zfs, nor will they be deleted.
The intended use is match zfs send/recv snapshots or hand-created
snapshots, etc.  ie: ^syncoid_

## Snapshot Management / zfs destroy
When destroying snapshots according to a given policy, all snapshots
should be considered for deletion and all snapshots should be
considered as potentially satisfying the retention policy regardless
of the name of the snapshot.  Only the creation property really
matters unless the user configures zfsmgmt otherwise.  If the user
wants to preserve a given snapshot it should be preserved using the
zfs hold mechanism or excluded by the ignoresnaps property.  This
allows zfs_mgmt to manage snapshots indepentantly of the mechanism
used to create them.

