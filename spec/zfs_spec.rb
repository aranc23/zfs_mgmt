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
        "recv_discard_last",
        "recv_discard_first",
        "recv_force",
        "restic_backup",
        "restic_repository"
      )
      expect(props["strategy"]["enum"]).to eq(%w[oldest youngest])
      expect(props["recv_state"]["default"]).to eq(true)
      expect(props["send"]["type"]).to eq("boolean")
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
      expect(zfs.restic_backup).to eq(false)
    end

    it "accepts :send key in initializer without clobbering Object#send" do
      zfs = described_class.new(name: "tank/data", send: true)
      expect(zfs.send_enabled).to eq(true)
      expect(zfs.send?).to eq(true)
      expect(zfs.send(:name)).to eq("tank/data")
      expect(zfs).to be_valid
    end

    it "rejects invalid strategy, policy, and minage values" do
      expect(described_class.new(name: "tank/data", strategy: "newest")).not_to be_valid
      expect(described_class.new(name: "tank/data", policy: "bogus")).not_to be_valid
      expect(described_class.new(name: "tank/data", minage: "invalid")).not_to be_valid
      expect(described_class.new(name: "tank/data", policy: "10y60m104w365d168h", minage: "7D", strategy: "youngest")).to be_valid
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
      end

      it "leaves destination nil when source is received or unrecognized" do
        zfs = described_class.from_zfs_props("backup/tank/fs", {
          "zfsmgmt:destination" => "backup/dest",
          "zfsmgmt:destination@source" => "received"
        })
        expect(zfs.destination).to be_nil
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
      expect(zfs.recv_unmount).to eq(true)
      expect(zfs.recv_force).to eq(true)
      expect(zfs.restic_backup).to eq(true)
      expect(zfs.restic_repository).to eq("/srv/restic")
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
