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

The most common usage pattern would be to set zfs properties as explained below, then use **zfsmgr snapshot policy** to print a table of what would be kept and for what reason.  Then use **zfsmgr snapshot destroy --noop** to see what would be destroyed, and finally **zfsmgr snapshot destroy** without the --noop option to actually remove snapshots.

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


## Example output
    [aranc23@beast:~/src/zfs_mgmt] (master)$ zfs get all | egrep 'zfsmgmt.+local'
    backup                      zfsmgmt:manage        true                                          local
    backup                      zfsmgmt:policy        10y60m104w365d168h                            local
    backup                      zfsmgmt:minage        7D                                            local
    backup                      zfsmgmt:ignoresnaps   ^syncoid_                                     local
    backup/beast/data/archive   zfsmgmt:policy        1h                                            local
    backup/beast/data/archive   zfsmgmt:minage        1s                                            local
    backup/beast/data/archive   zfsmgmt:matchsnaps    archive                                       local

    [aranc23@beast:~/src/zfs_mgmt] (master)$ ruby -I lib bin/zfsmgr snapshot policy --filter pics                                                                 
    +------------------------------------------------------------+---------------------------+--------------------+------------+--------------+---------+--------+
    |                            snap                            |         creation          |       hourly       |   daily    |    weekly    | monthly | yearly |
    +------------------------------------------------------------+---------------------------+--------------------+------------+--------------+---------+--------+
    | backup/beast/data/pics@autosnap-2020-02-27T12:17:01-0600   | 2020-02-27T12:17:01-06:00 | 2020-02-27 Hour 12 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T11:17:01-0600   | 2020-02-27T11:17:01-06:00 | 2020-02-27 Hour 11 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T10:17:01-0600   | 2020-02-27T10:17:01-06:00 | 2020-02-27 Hour 10 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T09:17:01-0600   | 2020-02-27T09:17:02-06:00 | 2020-02-27 Hour 09 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T08:17:01-0600   | 2020-02-27T08:17:01-06:00 | 2020-02-27 Hour 08 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T07:17:01-0600   | 2020-02-27T07:17:01-06:00 | 2020-02-27 Hour 07 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T06:17:01-0600   | 2020-02-27T06:17:01-06:00 | 2020-02-27 Hour 06 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T05:17:01-0600   | 2020-02-27T05:17:01-06:00 | 2020-02-27 Hour 05 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T04:17:01-0600   | 2020-02-27T04:17:02-06:00 | 2020-02-27 Hour 04 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T03:17:01-0600   | 2020-02-27T03:17:01-06:00 | 2020-02-27 Hour 03 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T02:17:01-0600   | 2020-02-27T02:17:01-06:00 | 2020-02-27 Hour 02 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T01:17:01-0600   | 2020-02-27T01:17:02-06:00 | 2020-02-27 Hour 01 |            |              |         |        |
    | backup/beast/data/pics@autosnap-2020-02-27T00:17:01-0600   | 2020-02-27T00:17:01-06:00 | 2020-02-27 Hour 00 | 2020-02-27 |              |         |        |
    ...
    | backup/beast/data/pics@zfssendman-20140604092215           | 2014-06-04T09:22:43-05:00 |                    | 2014-06-04 | 2014 Week 22 | 2014-06 |        |
    | backup/beast/data/pics@migrate3                            | 2014-05-26T08:17:31-05:00 |                    | 2014-05-26 |              |         |        |
    | backup/beast/data/pics@migrate2                            | 2014-05-25T21:57:28-05:00 |                    | 2014-05-25 | 2014 Week 21 |         |        |
    | backup/beast/data/pics@migrate1                            | 2014-05-24T10:31:56-05:00 |                    | 2014-05-24 | 2014 Week 20 | 2014-05 | 2014   |
    | backup/beast/data/pics@20131108144154                      | 2013-11-08T14:41:57-06:00 |                    | 2013-11-08 | 2013 Week 44 | 2013-11 | 2013   |
    +------------------------------------------------------------+---------------------------+--------------------+------------+--------------+---------+--------+

    [aranc23@beast:~/src/zfs_mgmt] (master)$ ruby -I lib bin/zfsmgr snapshot destroy --filter pics --noop
    I, [2020-02-27T16:27:33.381645 #4914]  INFO -- : deleting 21 snapshots for backup/beast/data/pics
    I, [2020-02-27T16:27:33.381731 #4914]  INFO -- : zfs destroy -pn backup/beast/data/pics@autosnap_2020-02-19_21:00:05_hourly,autosnap_2020-02-19_22:00:05_hourly,autosnap_2020-02-19_23:00:01_hourly,autosnap_2020-02-20_00:00:05_daily,autosnap_2020-02-20_01:00:04_hourly,autosnap_2020-02-20_02:00:04_hourly,autosnap_2020-02-20_03:00:04_hourly,autosnap_2020-02-20_04:00:05_hourly,autosnap_2020-02-20_05:00:05_hourly,autosnap_2020-02-20_07:00:04_hourly,autosnap_2020-02-20_08:00:01_hourly,autosnap_2020-02-20_09:00:05_hourly,autosnap_2020-02-20_10:00:05_hourly,autosnap_2020-02-20_11:00:05_hourly,autosnap_2020-02-20_12:00:05_hourly,autosnap_2020-02-20_13:00:01_hourly,autosnap_2020-02-20_14:00:05_hourly,autosnap_2020-02-20_15:00:05_hourly,autosnap_2020-02-20_16:00:05_hourly,autosnap_2020-02-20_17:00:05_hourly,autosnap_2020-02-20_18:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-19_21:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-19_22:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-19_23:00:01_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_00:00:05_daily
    destroy backup/beast/data/pics@autosnap_2020-02-20_01:00:04_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_02:00:04_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_03:00:04_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_04:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_05:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_07:00:04_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_08:00:01_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_09:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_10:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_11:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_12:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_13:00:01_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_14:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_15:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_16:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_17:00:05_hourly
    destroy backup/beast/data/pics@autosnap_2020-02-20_18:00:05_hourly
    reclaim 0

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

A policy specification consisting of the number of snapshots of a
certain time frame to keep.  A zfs must have a valid policy
specification or zfs_mgmt will not destroy any snapshots.

Examples:
- 30d ( 30 daily snapshots )
- 8w15d (8 weekly, and 15 daily snapshots)
- 1y1m1y1d1h (1 of each time frame worth of snapshots)
- 72h (72 hourly snapshots)

The order in which each timeframe is listed in does not matter, and the supported specs are as follows:

- h - hourly
- d - daily
- w - weekly (Sunday)
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

