require "spec_helper"

RSpec.describe ZfsMgmt::Zfs do
  describe "EasyTalk schema and validations" do
    it "includes EasyTalk::Model and generates a JSON schema with all zfsmgmt properties" do
      expect(described_class.ancestors).to include(EasyTalk::Model)

      schema = described_class.json_schema
      expect(schema["type"]).to eq("object")
      expect(schema["required"]).to eq(["name"])

      props = schema["properties"]
      expect(props.keys).to include(
        "name",
        "manage",
        "policy",
        "strategy",
        "minage",
        "matchsnaps",
        "ignoresnaps",
        "prefersnaps",
        "snapshot",
        "snapshot_recursive",
        "snap_prefix",
        "snap_timestamp",
        "send",
        "remote",
        "destination",
        "send_intermediary",
        "send_backup",
        "send_compressed",
        "send_embed",
        "send_holds",
        "send_large_block",
        "send_props",
        "send_raw",
        "send_replicate",
        "recv_state",
        "recv_noop",
        "recv_drop_holds",
        "recv_unmount",
        "recv_discard_first",
        "recv_force",
        "mbuffer",
        "mbuffer_size",
        "excludes",
        "overrides",
        "ssh_config",
        "restic_backup",
        "restic_repository"
      )
      expect(props["strategy"]["enum"]).to eq(%w[oldest youngest])
      expect(props["recv_state"]["default"]).to eq(true)
      expect(props["mbuffer"]["default"]).to eq(true)
      expect(props["send"]["type"]).to eq("boolean")
      expect(props["excludes"]["type"]).to eq("array")
      expect(props["excludes"]["items"]).to eq({ "type" => "string" })
      expect(props["overrides"]["type"]).to eq("object")
      expect(props["overrides"]["additionalProperties"]).to eq({ "type" => "string" })
      expect(props["ssh_config"]["type"]).to eq("object")
      expect(props["ssh_config"]["additionalProperties"]).to eq({ "type" => "string" })
    end

    it "applies sensible defaults and validates correctly" do
      zfs = described_class.new(name: "tank/data")
      expect(zfs).to be_valid
      expect(zfs.manage).to eq(false)
      expect(zfs.strategy).to eq("oldest")
      expect(zfs.snapshot).to eq(false)
      expect(zfs.snapshot_recursive).to eq(false)
      expect(zfs.snap_prefix).to eq("zfsmgmt")
      expect(zfs.snap_timestamp).to eq("%FT%T%z")
      expect(zfs.send_enabled).to eq(false)
      expect(zfs.send?).to eq(false)
      expect(zfs.volume?).to eq(false)
      expect(zfs.recv_state).to eq(true)
      expect(zfs.mbuffer).to eq(true)
      expect(zfs.mbuffer_size).to be_nil
      expect(zfs.excludes).to eq([])
      expect(zfs.overrides).to eq({})
      expect(zfs.override_options).to eq([])
      expect(zfs.ssh_config).to eq({})
      expect(zfs.restic_backup).to eq(false)
    end

    it "accepts :send key in initializer without clobbering Object#send" do
      zfs = described_class.new(name: "tank/data", send: true)
      expect(zfs.send_enabled).to eq(true)
      expect(zfs.send?).to eq(true)
      expect(zfs.send(:name)).to eq("tank/data")
      expect(zfs).to be_valid
    end

    it "rejects invalid strategy, policy, minage, and type values" do
      expect(described_class.new(name: "tank/data", strategy: "newest")).not_to be_valid
      expect(described_class.new(name: "tank/data", policy: "bogus")).not_to be_valid
      expect(described_class.new(name: "tank/data", minage: "invalid")).not_to be_valid
      expect(described_class.new(name: "tank/data", type: "snapshot")).not_to be_valid
      expect(described_class.new(name: "tank/data", policy: "10y60m104w365d168h", minage: "7D", strategy: "youngest", type: "filesystem")).to be_valid
      expect(described_class.new(name: "tank/zvol", type: "volume")).to be_valid
    end

    it "validates excludes and overrides types and omits ** knockouts from override_options" do
      expect(described_class.new(name: "tank/data", excludes: "mountpoint")).not_to be_valid
      expect(described_class.new(name: "tank/data", excludes: [123])).not_to be_valid

      arr_ovr = described_class.new(name: "tank/data", overrides: [{ "canmount" => "off" }])
      expect(arr_ovr).not_to be_valid
      expect(arr_ovr.errors[:overrides].join).to include('[zfs."tank/data".overrides]')

      expect(described_class.new(name: "tank/data", overrides: { "canmount" => 123 })).not_to be_valid

      valid_zfs = described_class.new(
        name: "tank/data",
        excludes: %w[mountpoint sharenfs],
        overrides: { "canmount" => "**", "readonly" => "on" }
      )
      expect(valid_zfs).to be_valid
      expect(valid_zfs.override_options).to eq(["readonly=on"])
    end

    it "validates destination accepts a string or array of strings and rejects other types" do
      expect(described_class.new(name: "tank/data", destination: "backup/tank")).to be_valid
      expect(described_class.new(name: "tank/data", destination: ["backup/tank", "host:offsite/tank"])).to be_valid
      expect(described_class.new(name: "tank/data", destination: 123)).not_to be_valid
      expect(described_class.new(name: "tank/data", destination: [123])).not_to be_valid
    end

    it "validates ssh_config types without validating ssh option names or values" do
      arr_ssh = described_class.new(name: "tank/data", ssh_config: [{ "BatchMode" => "no" }])
      expect(arr_ssh).not_to be_valid
      expect(arr_ssh.errors[:ssh_config].join).to include('[zfs."tank/data".ssh_config]')

      expect(described_class.new(name: "tank/data", ssh_config: "BatchMode=no")).not_to be_valid
      expect(described_class.new(name: "tank/data", ssh_config: { "ConnectTimeout" => 30 })).not_to be_valid

      valid_ssh = described_class.new(
        name: "tank/data",
        ssh_config: { "BatchMode" => "no", "ArbitraryOption" => "anything" }
      )
      expect(valid_ssh).to be_valid
    end

    it "validates mbuffer is a boolean and mbuffer_size is a string" do
      expect(described_class.new(name: "tank/data", mbuffer: false, mbuffer_size: "512M")).to be_valid
      expect(described_class.new(name: "tank/data", mbuffer: "false")).not_to be_valid
      expect(described_class.new(name: "tank/data", mbuffer_size: 512)).not_to be_valid
    end
  end

  describe ".from_zfs_props" do
    context "snapshot source interpretation" do
      it "enables non-recursive snapshot when zfsmgmt:snapshot is true/on regardless of source" do
        zfs_inherited = described_class.from_zfs_props("tank/sub", {
          "zfsmgmt:snapshot" => "true",
          "zfsmgmt:snapshot@source" => "inherited from tank"
        })
        expect(zfs_inherited.snapshot).to eq(true)
        expect(zfs_inherited.snapshot_recursive).to eq(false)

        zfs_received = described_class.from_zfs_props("backup/sub", {
          "zfsmgmt:snapshot" => "on",
          "zfsmgmt:snapshot@source" => "received"
        })
        expect(zfs_received.snapshot).to eq(true)
        expect(zfs_received.snapshot_recursive).to eq(false)
      end

      it "enables recursive snapshot only when source is local or received" do
        local_rec = described_class.from_zfs_props("tank/rec", {
          "zfsmgmt:snapshot" => "recursive",
          "zfsmgmt:snapshot@source" => "local"
        })
        expect(local_rec.snapshot).to eq(true)
        expect(local_rec.snapshot_recursive).to eq(true)

        recv_rec = described_class.from_zfs_props("backup/rec", {
          "zfsmgmt:snapshot" => "recursive",
          "zfsmgmt:snapshot@source" => "received"
        })
        expect(recv_rec.snapshot).to eq(true)
        expect(recv_rec.snapshot_recursive).to eq(true)

        inherited_rec = described_class.from_zfs_props("tank/rec/child", {
          "zfsmgmt:snapshot" => "recursive",
          "zfsmgmt:snapshot@source" => "inherited from tank/rec"
        })
        expect(inherited_rec.snapshot).to eq(false)
        expect(inherited_rec.snapshot_recursive).to eq(false)
      end

      it "enables local snapshot only when source is local or received" do
        local_snap = described_class.from_zfs_props("tank/only", {
          "zfsmgmt:snapshot" => "local",
          "zfsmgmt:snapshot@source" => "local"
        })
        expect(local_snap.snapshot).to eq(true)
        expect(local_snap.snapshot_recursive).to eq(false)

        inherited_local = described_class.from_zfs_props("tank/only/child", {
          "zfsmgmt:snapshot" => "local",
          "zfsmgmt:snapshot@source" => "inherited from tank/only"
        })
        expect(inherited_local.snapshot).to eq(false)
        expect(inherited_local.snapshot_recursive).to eq(false)
      end
    end

    context "send and send_replicate source interpretation" do
      it "enables send when true/on with local or inherited source, and disables when received" do
        local_send = described_class.from_zfs_props("tank/fs", {
          "zfsmgmt:send" => "true",
          "zfsmgmt:send@source" => "local"
        })
        expect(local_send.send_enabled).to eq(true)

        inherited_send = described_class.from_zfs_props("tank/fs/sub", {
          "zfsmgmt:send" => "on",
          "zfsmgmt:send@source" => "inherited from tank/fs"
        })
        expect(inherited_send.send_enabled).to eq(true)

        received_send = described_class.from_zfs_props("backup/fs", {
          "zfsmgmt:send" => "true",
          "zfsmgmt:send@source" => "received"
        })
        expect(received_send.send_enabled).to eq(false)
      end

      it "supports zfsmgmt:send=local only when source is local" do
        local_only = described_class.from_zfs_props("tank/fs", {
          "zfsmgmt:send" => "local",
          "zfsmgmt:send@source" => "local"
        })
        expect(local_only.send_enabled).to eq(true)

        inherited_local = described_class.from_zfs_props("tank/fs/child", {
          "zfsmgmt:send" => "local",
          "zfsmgmt:send@source" => "inherited from tank/fs"
        })
        expect(inherited_local.send_enabled).to eq(false)
      end

      it "disables send and send_replicate for descendants inheriting send_replicate" do
        parent = described_class.from_zfs_props("tank/rep", {
          "zfsmgmt:send" => "true",
          "zfsmgmt:send@source" => "local",
          "zfsmgmt:send_replicate" => "true",
          "zfsmgmt:send_replicate@source" => "local"
        })
        expect(parent.send_enabled).to eq(true)
        expect(parent.send_replicate).to eq(true)

        child = described_class.from_zfs_props("tank/rep/child", {
          "zfsmgmt:send" => "true",
          "zfsmgmt:send@source" => "inherited from tank/rep",
          "zfsmgmt:send_replicate" => "true",
          "zfsmgmt:send_replicate@source" => "inherited from tank/rep"
        })
        expect(child.send_enabled).to eq(false)
        expect(child.send_replicate).to eq(false)
      end
    end

    context "destination source interpretation" do
      it "resolves local destination by appending dataset basename" do
        zfs = described_class.from_zfs_props("tank/sub/fs", {
          "zfsmgmt:destination" => "backup/pool",
          "zfsmgmt:destination@source" => "local"
        })
        expect(zfs.destination).to eq("backup/pool/fs")
        expect(zfs.resolved_destination).to eq("backup/pool/fs")
        expect(zfs.resolved_destination("other/pool")).to eq("other/pool/fs")
      end

      it "resolves inherited destination preserving relative path from ancestor" do
        zfs = described_class.from_zfs_props("tank/sub/fs", {
          "zfsmgmt:destination" => "backup/dest",
          "zfsmgmt:destination@source" => "inherited from tank"
        })
        expect(zfs.destination).to eq("backup/dest/tank/sub/fs")
        expect(zfs.resolved_destination).to eq("backup/dest/tank/sub/fs")
        expect(zfs.resolved_destination("other/dest")).to eq("other/dest/tank/sub/fs")
        expect(zfs.base_destination).to eq("backup/dest")
        expect(zfs.base_destination("other/dest")).to eq("other/dest")
        expect(zfs.discard_first_destination).to eq("backup/dest/sub/fs")
        expect(zfs.discard_first_destination("other/dest")).to eq("other/dest/sub/fs")
      end

      it "leaves destination nil when source is received or unrecognized" do
        zfs = described_class.from_zfs_props("backup/tank/fs", {
          "zfsmgmt:destination" => "backup/dest",
          "zfsmgmt:destination@source" => "received"
        })
        expect(zfs.destination).to be_nil
        expect(zfs.base_destination).to be_nil
        expect(zfs.discard_first_destination).to be_nil
      end

      it "supports hostname:path and user@hostname:path destination syntax" do
        host_zfs = described_class.from_zfs_props("tank/sub/fs", {
          "zfsmgmt:destination" => "drhost:backup/pool",
          "zfsmgmt:destination@source" => "local"
        })
        expect(host_zfs.destination).to eq("drhost:backup/pool/fs")
        expect(host_zfs.destination_remote).to eq("drhost")
        expect(host_zfs.destination_path).to eq("backup/pool/fs")
        expect(host_zfs.base_destination).to eq("drhost:backup/pool")
        expect(host_zfs.resolved_destination("other@host2:other/pool")).to eq("other@host2:other/pool/fs")
        expect(host_zfs.resolved_destination("localpool/ds")).to eq("localpool/ds/fs")

        user_host_zfs = described_class.from_zfs_props("tank/sub/fs", {
          "zfsmgmt:destination" => "zfs_recv@drhost:backup/dest",
          "zfsmgmt:destination@source" => "inherited from tank"
        })
        expect(user_host_zfs.destination).to eq("zfs_recv@drhost:backup/dest/tank/sub/fs")
        expect(user_host_zfs.destination_remote).to eq("zfs_recv@drhost")
        expect(user_host_zfs.destination_path).to eq("backup/dest/tank/sub/fs")
        expect(user_host_zfs.base_destination).to eq("zfs_recv@drhost:backup/dest")
        expect(user_host_zfs.discard_first_destination).to eq("zfs_recv@drhost:backup/dest/sub/fs")
        expect(user_host_zfs.discard_first_destination("host2:other/dest")).to eq("host2:other/dest/sub/fs")
      end

      it "warns when deprecated remote is set and only prepends it if destination lacks a host" do
        expect($logger).to receive(:warn).with(/remote option is deprecated \(tank\/sub\/fs\)/)
        no_host = described_class.from_zfs_props("tank/sub/fs", {
          "zfsmgmt:remote" => "root@drhost",
          "zfsmgmt:destination" => "backup/pool",
          "zfsmgmt:destination@source" => "local"
        })
        expect(no_host.destination).to eq("root@drhost:backup/pool/fs")
        expect(no_host.base_destination).to eq("root@drhost:backup/pool")
        expect(no_host.destination_explicit_remote).to be_nil

        expect($logger).to receive(:warn).with(/remote option is deprecated \(tank\/sub\/fs\)/)
        with_host = described_class.from_zfs_props("tank/sub/fs", {
          "zfsmgmt:remote" => "root@oldhost",
          "zfsmgmt:destination" => "zfs_recv@newhost:backup/pool",
          "zfsmgmt:destination@source" => "local"
        })
        expect(with_host.destination).to eq("zfs_recv@newhost:backup/pool/fs")
        expect(with_host.destination_remote).to eq("zfs_recv@newhost")
        expect(with_host.destination_explicit_remote).to eq("zfs_recv@newhost")
      end

      it "supports comma-separated zfsmgmt:destination for local and inherited sources" do
        local_multi = described_class.from_zfs_props("tank/sub/fs", {
          "zfsmgmt:destination" => "backup/pool, zfs_recv@drhost:offsite/pool",
          "zfsmgmt:destination@source" => "local"
        })
        expect(local_multi.destination).to eq(["backup/pool/fs", "zfs_recv@drhost:offsite/pool/fs"])
        expect(local_multi.destinations).to eq(["backup/pool/fs", "zfs_recv@drhost:offsite/pool/fs"])
        expect(local_multi.base_destination).to eq(["backup/pool", "zfs_recv@drhost:offsite/pool"])
        expect(local_multi.destination_entries).to eq([
          { resolved: "backup/pool/fs", base: "backup/pool", explicit_remote: nil },
          { resolved: "zfs_recv@drhost:offsite/pool/fs", base: "zfs_recv@drhost:offsite/pool", explicit_remote: "zfs_recv@drhost" }
        ])

        inherited_multi = described_class.from_zfs_props("tank/sub/fs", {
          "zfsmgmt:destination" => "backup/dest,drhost:offsite/dest",
          "zfsmgmt:destination@source" => "inherited from tank"
        })
        expect(inherited_multi.destinations).to eq(["backup/dest/tank/sub/fs", "drhost:offsite/dest/tank/sub/fs"])
        expect(inherited_multi.base_destination).to eq(["backup/dest", "drhost:offsite/dest"])
        expect(inherited_multi.discard_first_destination).to eq(["backup/dest/sub/fs", "drhost:offsite/dest/sub/fs"])
      end

      it "supports array destination in Zfs.new and from_zfs_props, applying deprecated remote only when host is omitted" do
        direct = described_class.new(name: "tank/data", destination: ["backup/data", "drhost:offsite/data"])
        expect(direct).to be_valid
        expect(direct.destinations).to eq(["backup/data", "drhost:offsite/data"])
        expect(direct.destination_entries).to eq([
          { resolved: "backup/data", base: "backup/data", explicit_remote: nil },
          { resolved: "drhost:offsite/data", base: "drhost:offsite/data", explicit_remote: "drhost" }
        ])

        expect($logger).to receive(:warn).with(/remote option is deprecated \(tank\/sub\/fs\)/)
        multi_with_remote = described_class.from_zfs_props("tank/sub/fs", {
          "zfsmgmt:remote" => "root@fallback",
          "zfsmgmt:destination" => ["backup/pool", "explicit@host:offsite/pool"],
          "zfsmgmt:destination@source" => "local"
        })
        expect(multi_with_remote.destinations).to eq([
          "root@fallback:backup/pool/fs",
          "explicit@host:offsite/pool/fs"
        ])
        expect(multi_with_remote.destination_entries).to eq([
          { resolved: "root@fallback:backup/pool/fs", base: "root@fallback:backup/pool", explicit_remote: nil },
          { resolved: "explicit@host:offsite/pool/fs", base: "explicit@host:offsite/pool", explicit_remote: "explicit@host" }
        ])
      end
    end

    it "maps type and volume? from ZFS properties" do
      vol = described_class.from_zfs_props("tank/zvol", { "type" => "volume" })
      expect(vol.type).to eq("volume")
      expect(vol.volume?).to eq(true)

      fs = described_class.from_zfs_props("tank/fs", { "type" => "filesystem" })
      expect(fs.type).to eq("filesystem")
      expect(fs.volume?).to eq(false)
    end

    it "maps all send, recv, snapshot policy, and restic properties" do
      zfs = described_class.from_zfs_props("tank/app", {
        "zfsmgmt:manage" => "on",
        "zfsmgmt:policy" => "30d",
        "zfsmgmt:strategy" => "youngest",
        "zfsmgmt:minage" => "2h",
        "zfsmgmt:matchsnaps" => "^auto-",
        "zfsmgmt:ignoresnaps" => "^syncoid_",
        "zfsmgmt:prefersnaps" => "^daily-",
        "zfsmgmt:snap_prefix" => "custom",
        "zfsmgmt:snap_timestamp" => "%F",
        "zfsmgmt:remote" => "root@drhost",
        "zfsmgmt:send_intermediary" => "true",
        "zfsmgmt:send_compressed" => "on",
        "zfsmgmt:send_raw" => "true",
        "zfsmgmt:recv_state" => "off",
        "zfsmgmt:mbuffer" => "off",
        "zfsmgmt:mbuffer_size" => "256M",
        "zfsmgmt:recv_unmount" => "true",
        "zfsmgmt:recv_force" => "on",
        "zfsmgmt:restic_backup" => "true",
        "zfsmgmt:restic_repository" => "/srv/restic"
      })

      expect(zfs).to be_valid
      expect(zfs.manage).to eq(true)
      expect(zfs.policy).to eq("30d")
      expect(zfs.strategy).to eq("youngest")
      expect(zfs.minage).to eq("2h")
      expect(zfs.matchsnaps).to eq("^auto-")
      expect(zfs.ignoresnaps).to eq("^syncoid_")
      expect(zfs.prefersnaps).to eq("^daily-")
      expect(zfs.snap_prefix).to eq("custom")
      expect(zfs.snap_timestamp).to eq("%F")
      expect(zfs.remote).to eq("root@drhost")
      expect(zfs.send_intermediary).to eq(true)
      expect(zfs.send_compressed).to eq(true)
      expect(zfs.send_raw).to eq(true)
      expect(zfs.send_props).to eq(false)
      expect(zfs.recv_state).to eq(false)
      expect(zfs.mbuffer).to eq(false)
      expect(zfs.mbuffer_size).to eq("256M")
      expect(zfs.recv_unmount).to eq(true)
      expect(zfs.recv_force).to eq(true)
      expect(zfs.restic_backup).to eq(true)
      expect(zfs.restic_repository).to eq("/srv/restic")
    end

    it "warns when an unknown zfsmgmt:* property is set" do
      expect($logger).to receive(:warn).with("unknown zfsmgmt property 'zfsmgmt:snaphsot' on tank/app")
      expect($logger).to receive(:warn).with("unknown zfsmgmt property 'zfsmgmt:bogus' on tank/app")

      described_class.from_zfs_props("tank/app", {
        "type" => "filesystem",
        "mountpoint" => "/app",
        "zfsmgmt:manage" => "true",
        "zfsmgmt:manage@source" => "local",
        "zfsmgmt:snaphsot" => "true",
        "zfsmgmt:snaphsot@source" => "local",
        "zfsmgmt:bogus" => "123"
      })
    end

    it "does not warn for any valid zfsmgmt:* properties or @source metadata" do
      expect($logger).not_to receive(:warn).with(/unknown zfsmgmt property/)
      allow($logger).to receive(:warn).with(/remote option is deprecated/)

      props = described_class::VALID_ZFS_PROPS.each_with_object({}) do |prop, h|
        h[prop] = "true"
        h["#{prop}@source"] = "local"
      end
      props["zfsmgmt:strategy"] = "oldest"
      props["zfsmgmt:policy"] = "7d"
      props["zfsmgmt:minage"] = "1h"
      props["zfsmgmt:excludes"] = "mountpoint,sharenfs"
      props["zfsmgmt:overrides"] = "canmount=noauto,readonly=on"
      props["zfsmgmt:ssh_config"] = "BatchMode=no,Compression=yes"
      props["zfsmgmt:mbuffer_size"] = "512M"

      described_class.from_zfs_props("tank/app", props)
    end

    context "excludes, overrides, and ssh_config parsing" do
      it "parses comma-separated zfsmgmt:excludes and zfsmgmt:overrides" do
        zfs = described_class.from_zfs_props("tank/app", {
          "zfsmgmt:excludes" => " mountpoint , sharenfs , **quota ",
          "zfsmgmt:overrides" => " canmount=noauto , sharenfs=rw=10.0.0.0/8 , readonly=** , **atime "
        })
        expect(zfs.excludes).to eq(%w[mountpoint sharenfs **quota])
        expect(zfs.overrides).to eq({
          "canmount" => "noauto",
          "sharenfs" => "rw=10.0.0.0/8",
          "readonly" => "**",
          "atime" => "**"
        })
        expect(zfs.override_options).to eq(["canmount=noauto", "sharenfs=rw=10.0.0.0/8"])
      end

      it "supports bare ** knockout in zfsmgmt:overrides and warns on malformed entries" do
        expect($logger).to receive(:warn).with("ignoring malformed override 'bogus' on tank/app")
        zfs = described_class.from_zfs_props("tank/app", {
          "zfsmgmt:overrides" => "**,bogus,compression=zstd"
        })
        expect(zfs.overrides).to eq({
          "**" => "**",
          "compression" => "zstd"
        })
      end

      it "parses comma-separated zfsmgmt:ssh_config including ** knockouts and warns on malformed entries" do
        expect($logger).to receive(:warn).with("ignoring malformed ssh_config 'bogus' on tank/app")
        zfs = described_class.from_zfs_props("tank/app", {
          "zfsmgmt:ssh_config" => " BatchMode=no , Compression=yes , bogus , ServerAliveInterval=** , **IPQoS "
        })
        expect(zfs.ssh_config).to eq({
          "BatchMode" => "no",
          "Compression" => "yes",
          "ServerAliveInterval" => "**",
          "IPQoS" => "**"
        })
      end
    end
  end

  describe "lazy snapshot retrieval" do
    it "does not call zfsget until #snapshots is accessed, and memoizes the result" do
      zfs = described_class.new(name: "tank/data")
      expect(zfs.snapshots_loaded?).to eq(false)

      snaps = { "tank/data@s1" => { "creation" => 1000, "userrefs" => 0 } }
      expect(ZfsMgmt).to receive(:zfsget).once.with(
        properties: %w[name creation userrefs used written referenced],
        types: ["snapshot"],
        zfs: "tank/data"
      ).and_return(snaps)

      expect(zfs.snapshots).to eq(snaps)
      expect(zfs.snapshots_loaded?).to eq(true)
      # Second call uses memoized value
      expect(zfs.snapshots).to eq(snaps)
    end

    it "re-fetches snapshots when #reload_snapshots! is called" do
      zfs = described_class.new(name: "tank/data", snapshots: { "tank/data@old" => { "creation" => 100 } })
      expect(zfs.snapshots_loaded?).to eq(true)

      fresh = { "tank/data@new" => { "creation" => 200 } }
      expect(ZfsMgmt).to receive(:zfsget).once.with(
        properties: %w[name creation userrefs used written referenced],
        types: ["snapshot"],
        zfs: "tank/data"
      ).and_return(fresh)

      expect(zfs.reload_snapshots!).to eq(fresh)
      expect(zfs.snapshots).to eq(fresh)
    end
  end
end
