# ZfsMgmt

zfs_mgmt aims to provide some useful helpers for managing zfs
snapshots, and eventually send/recv duties via the zfsmgr script in
bin/.

Snapshot creation and deletion by policy along with zfs send wrapping
are implementation by specifications stored in zfs properties.

## Installation

Currently zfs_mgmt is only useful for it's zfsmgr binary, although
eventually the library might be useful for writing other applications
around managing zfs.

Therefore, building the gem and installing, or running ruby inside the src/ directory would be most useful:

    $ ruby -I lib bin/zfsmgr

## Usage

The most common usage pattern would be to set zfs properties as
explained below, then use **zfsmgr snapshot policy** to print a table
of what would be kept and for what reason.  Then use **zfsmgr snapshot destroy --noop**
to see what would be destroyed, and finally **zfsmgr snapshot destroy**
without the --noop option to actually remove snapshots.

    $ zfsmgr
    Commands:
      zfsmgr help [COMMAND]               # Describe available commands or one specific command
      zfsmgr list SUBCOMMAND ...ARGS      # list filesystems
      zfsmgr restic SUBCOMMAND ...ARGS    # backup zfs to restic
      zfsmgr snapshot SUBCOMMAND ...ARGS  # manage snapshots
      zfsmgr zfsget [ZFS]                 # execute zfs get for the given properties and types and parse the output into a nested hash

### list stale

**zfsmgr list stale will** will list all zfs with "stale" snapshots,
that is the newest snapshot is older than 1d or older than the --age
parameter.

### restic backup

zfsmgr can pipe zfs send output into restic, to allow storing zfs
streams in restic repositories.  zfsmgr takes an opinionated approach
to this task, implementing a traditional full/differential/incremental
backup scheme.  This should allow a recent snapshot to be restored
requiring only 3 restic snapshots to recreate the filesystem. See the
properties section for restic related zfs properties.

    $ zfsmgr restic backup full


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

After checking out the repo, run `bin/setup` to install
dependencies. Then, run `rake spec` to run the tests. You can also run
`bin/console` for an interactive prompt that will allow you to
experiment.

To install this gem onto your local machine, run `bundle exec rake
install`. To release a new version, update the version number in
`version.rb`, and then run `bundle exec rake release`, which will
create a git tag for the version, push git commits and tags, and push
the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/aranc23/zfs_mgmt.

## Configuration File

`zfsmgr` loads `*.toml` configuration files from the following directory (unless `--config-dir <path>` is specified):

- `/etc/zfsmgr/*.toml` when running as `root`
- `$XDG_CONFIG_HOME/zfsmgr/*.toml` when non-root and `XDG_CONFIG_HOME` is set
- `~/.config/zfsmgr/*.toml` when non-root and `XDG_CONFIG_HOME` is unset

Files are loaded in lexicographical `sort()` order (for example `00-base.toml` followed by `50-local.toml`) and deep-merged, with later files replacing earlier scalars, arrays, and per-dataset options.

Precedence is: **CLI options > per-dataset config (child > parent) > top-level config > ZFS dataset user properties (`zfsmgmt:*`)**.

### Format

At the top level, `excludes` and `[overrides]` set default `zfs recv` `-x` and `-o` options. Per-dataset options are configured under `[zfs."pool/dataset"]` using single-bracket TOML tables (not double-bracket `[[zfs."pool/dataset"]]` array-of-tables syntax). Per-dataset `excludes` and `[zfs."pool/dataset".overrides]` are also supported using single-bracket tables. Dataset names containing `/` (or other characters not allowed in TOML bare keys) must be quoted:

```toml
#:schema https://raw.githubusercontent.com/aranc23/zfs_mgmt/master/schema/config.schema.json

excludes = ["mountpoint", "sharenfs"]

[overrides]
canmount = "noauto"
readonly = "on"

[zfs."tank"]
manage = true
policy = "10y6m4w30d24h"
snapshot = true

[zfs."tank/data"]
send = true
destination = "zfs_recv@backup.example.com:backup/tank"
excludes = ["quota"]

[zfs."tank/data".overrides]
canmount = "**"
compression = "zstd"
```

Dataset options configured in TOML inherit to child datasets (e.g. `tank` -> `tank/data`) with the same source semantics as `zfsmgmt:*` user properties.

Unlike scalar options (where a higher-precedence source replaces lower ones), `excludes` and `overrides` are **merged** from lowest precedence (`zfsmgmt:excludes` / `zfsmgmt:overrides`) up through top-level config, per-dataset config (ancestor to child), and CLI `--exclude` / `--option` flags. To unset a value set at a lower precedence level, use the `**` knockout prefix:
- In `overrides`: set `prop = "**"` in TOML (or `prop=**` / `**prop` on the CLI or `zfsmgmt:overrides`) to remove `prop` from lower levels, or `"**"` alone to clear all lower-level overrides.
- In `excludes`: include `"**prop"` to remove `prop` from lower levels, or `"**"` to clear all lower-level excludes.
- A higher-precedence layer can re-add an exclude or override that was knocked out at an intermediate layer.

See [examples/config.toml](examples/config.toml) for a full example and [schema/config.schema.json](schema/config.schema.json) for the JSON Schema (used by TOML editors/validators via the `#:schema` directive; regenerate with `bundle exec rake schema`).

## zfs user properties

Destruction of zfs snapshots is based on the following zfs user
properties:

### zfsmgmt:manage
manage snapshots for this filesystem if this property is 'true'
(string literal)
 
### zfsmgmt:policy

A policy specification consisting of the number of snapshots of a
certain time frame to keep.  A zfs must have a valid policy
specification or zfs_mgmt will not destroy any snapshots.

Examples:
- 30d ( 30 daily snapshots )
- 8w15d (8 weekly, and 15 daily snapshots)
- 1y1m1y1d1h (1 of each time frame worth of snapshots)
- 72h (72 hourly snapshots)

The order in which each timeframe is listed in does not matter, and
the supported specs are as follows:

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

### zfsmgmt:prefersnaps
Prefer snapshots matching this regexp pattern.  The oldest/youngest
snapshot matching this pattern will be used depending on strategy, but
if no snapshots are found matching this pattern use any snapshot as
constrained by the matchsnaps and ignoresnaps options (if set.)

### zfsmgmt:strategy (default: oldest)
Save the oldest snapshot fitting a given time frame as specificed by
the policy, unless this value is set to "youngest" in which case use
the most recent snapshot for any given time frame.  The default is
oldest and unless the property is set to youngest oldest will be used.

### zfsmgmt:snapshot
If this property is 'true' then create a snapshot in the format of
zfsmgmt-%FT%T%z.  If this property is 'recursive' then create a
recursive snapshot of this zfs, but only on zfs where this property is
local.  If this property is set to the string 'local' and the property
is set locally, it will create a snapshot.  The intention is that you
would use 'local' when you want a zfs snapshot for the filesystem, but
NOT it's descendant filesystems.

### zfsmgmt:snap_prefix
Change the zfsmgmt portion of created snapshots, ie: 'autosnap' would
create snapshots called autosnap-%FT%T%z.

### zfsmgmt:snap_timestamp
strftime format string used when creating snapshot names, default
being %FT%T%z.

### zfsmgmt:send
Enable `zfs send` for this dataset. Accepts boolean values (`true`/`on`)
or `'local'`:
- `true` or `on`: enables `zfs send` when the property's ZFS source is
  `local` or `inherited`, and disables it when the source is `received`
  or when `zfsmgmt:send_replicate` is inherited from an ancestor.
- `local`: enables `zfs send` only when the property's ZFS source is
  `local` (not inherited or received).

### zfsmgmt:remote *(deprecated)*
Deprecated. Remote SSH target for `zfs recv` and remote `zfs get` checks
(e.g. `user@host`). Specify the remote host directly in `zfsmgmt:destination`
instead (e.g. `user@host:backup/tank`). When set, logs a deprecation warning
and prepends the remote host to `destination` only if `destination` does not
already include a `hostname:` or `user@hostname:` prefix.

### zfsmgmt:destination
Base destination ZFS dataset path for `zfs send`/`recv` (required for
`zfsmgr send all`). Supports local and remote forms:
- `zpool/zfs/zfs2` (local)
- `hostname:zpool/zfs/zfs2` (remote via SSH)
- `user@hostname:zpool/zfs/zfs2` (remote via SSH with user)

Path resolution rules:
- When the property's ZFS source is `local`, the destination path
  resolves to `<destination>/<dataset basename>`.
- When the property's ZFS source is `inherited from <ancestor>`, the
  destination path resolves to
  `<destination>/<ancestor basename>/<relative path>`.
- When the source is `received` (or unrecognized), no destination is
  resolved.

### zfs send / recv flags

The following boolean options (`true`/`on`, or `false`/`off` in ZFS user properties; `true`/`false` in `[zfs."<dataset>"]` TOML config) map directly to `zfs send` or `zfs recv` command-line flags and can be overridden via `zfsmgr send all` CLI flags:

| User Property | Config Key | ZFS Option | Default | Description |
| --- | --- | --- | --- | --- |
| `zfsmgmt:send_intermediary` | `send_intermediary` | `zfs send -I` | `false` | Include intermediary snapshots |
| `zfsmgmt:send_backup` | `send_backup` | `zfs send -b` | `false` | Send received property values |
| `zfsmgmt:send_compressed` | `send_compressed` | `zfs send -c` | `false` | Send compressed blocks |
| `zfsmgmt:send_embed` | `send_embed` | `zfs send -e` | `false` | Send embedded data |
| `zfsmgmt:send_holds` | `send_holds` | `zfs send -h` | `false` | Send snapshot holds |
| `zfsmgmt:send_large_block` | `send_large_block` | `zfs send -L` | `false` | Allow large blocks |
| `zfsmgmt:send_props` | `send_props` | `zfs send -p` | `false` | Send properties |
| `zfsmgmt:send_raw` | `send_raw` | `zfs send -w` | `false` | Send raw encrypted stream |
| `zfsmgmt:recv_state` | `recv_state` | `zfs recv -s` | `true` | Save partial receive state for resuming |
| `zfsmgmt:recv_noop` | `recv_noop` | `zfs recv -n` | `false` | Dry-run (do not actually receive) |
| `zfsmgmt:recv_drop_holds` | `recv_drop_holds` | `zfs recv -h` | `false` | Skip receiving holds |
| `zfsmgmt:recv_unmount` | `recv_unmount` | `zfs recv -u` | `false` | Do not mount received filesystem |
| `zfsmgmt:recv_force` | `recv_force` | `zfs recv -F` | `false` | Force rollback |

### zfsmgmt:send_replicate (default: false)
Boolean (`true`/`on`). Pass `-R` (replication stream) to `zfs send` and
use the newest snapshot for initial send. Only takes effect on the
dataset where `zfsmgmt:send_replicate` is `local`; descendant datasets
inheriting it have both `send` and `send_replicate` disabled so they are
not sent separately.

### zfsmgmt:recv_discard_first (default: false)
Boolean (`true`/`on`). Pass `-d` (discard the first element of the sent
snapshot's filesystem name) to `zfs recv`, passing the base
`zfsmgmt:destination` as the `zfs recv` target and
stripping the first path element for remote existence/snapshot checks.

### zfsmgmt:excludes
Comma-separated list of ZFS properties to exclude on `zfs recv` (`-x`),
for example `mountpoint,sharenfs`. Merged with top-level config,
per-dataset config, and CLI `--exclude` (`-x`) options. Prefix a
property with `**` (e.g. `**mountpoint`) to knock out an excluded
property, or use `**` to clear lower-precedence excludes.

### zfsmgmt:overrides
Comma-separated list of `property=value` overrides to pass to `zfs recv`
(`-o`), for example `canmount=noauto,readonly=on`. Merged with top-level
config, per-dataset config, and CLI `--option` (`-o`) options. Use
`property=**` (or `**property`) to knock out an override, or `**` to
clear lower-precedence overrides.

### zfsmgmt:restic_backup
boolean, send this zfs to restic

### zfsmgmt:restic_repository
send the zfs to this repository, optional, rely on restic environment
variables otherwise

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

