require "spec_helper"
require "tmpdir"

RSpec.describe ZfsMgmt::Config do
  describe "EasyTalk schema and validations" do
    it "includes EasyTalk::Model and generates a JSON schema with excludes, overrides, and zfs" do
      expect(described_class.ancestors).to include(EasyTalk::Model)

      schema = described_class.json_schema
      expect(schema["type"]).to eq("object")

      props = schema["properties"]
      expect(props.keys).to include("excludes", "overrides", "zfs")
      expect(props["excludes"]["type"]).to eq("array")
      expect(props["excludes"]["items"]).to eq({ "type" => "string" })
      expect(props["overrides"]["type"]).to eq("array")
      expect(props["overrides"]["items"]).to eq({ "type" => "string" })
      expect(props["zfs"]["type"]).to eq("object")
      expect(props["zfs"]["additionalProperties"]["type"]).to eq("object")
      expect(props["zfs"]["additionalProperties"]["properties"].keys).to include("send", "destination", "remote")
    end

    it "applies sensible defaults and validates valid configs" do
      cfg = described_class.new
      expect(cfg).to be_valid
      expect(cfg.excludes).to eq([])
      expect(cfg.overrides).to eq([])
      expect(cfg.zfs).to eq({})
    end

    it "rejects invalid nested Zfs properties" do
      cfg = described_class.from_hash(
        "zfs" => {
          "tank/data" => {
            "policy" => "bogus",
            "strategy" => "invalid"
          }
        }
      )
      expect(cfg).not_to be_valid
      expect(cfg.errors[:zfs].join).to include("tank/data")
    end
  end

  describe ".load_dir and .load_files" do
    it "returns an empty valid Config when the directory is missing or empty" do
      expect(described_class.load_dir("/nonexistent/zfsmgr/config/dir")).to be_valid

      Dir.mktmpdir do |dir|
        cfg = described_class.load_dir(dir)
        expect(cfg).to be_valid
        expect(cfg.excludes).to eq([])
        expect(cfg.overrides).to eq([])
        expect(cfg.zfs).to eq({})
      end
    end

    it "loads and merges *.toml files in sort() order with later files replacing earlier values" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "00-base.toml"), <<~TOML)
          excludes = ["mountpoint", "sharenfs"]
          overrides = ["readonly=on"]

          [zfs."pool/zfs"]
          manage = true
          policy = "30d"
          remote = "root@oldhost"

          [zfs."pool/zfs/zfs2"]
          send = true
          destination = "otherpool"
          remote = "root@host"
        TOML

        File.write(File.join(dir, "50-local.toml"), <<~TOML)
          excludes = ["mountpoint"]

          [zfs."pool/zfs"]
          remote = "root@newhost"

          [zfs."pool/zfs/zfs2"]
          send_compressed = true
        TOML

        cfg = described_class.load_dir(dir)
        expect(cfg).to be_valid
        expect(cfg.excludes).to eq(["mountpoint"])
        expect(cfg.overrides).to eq(["readonly=on"])

        parent = cfg.zfs["pool/zfs"]
        expect(parent).to be_a(ZfsMgmt::Zfs)
        expect(parent.manage).to eq(true)
        expect(parent.policy).to eq("30d")
        expect(parent.remote).to eq("root@newhost")

        child = cfg.zfs["pool/zfs/zfs2"]
        expect(child).to be_a(ZfsMgmt::Zfs)
        expect(child.send_enabled).to eq(true)
        expect(child.destination).to eq("otherpool/zfs2")
        expect(child.remote).to eq("root@host")
        expect(child.send_compressed).to eq(true)
      end
    end

    it "supports [[zfs.\"dataset\"]] array-of-tables syntax in TOML" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "10-send.toml"), <<~TOML)
          [[zfs."pool/zfs/zfs2"]]
          send = true
          destination = "otherpool"
          remote = "root@host"
        TOML

        cfg = described_class.load_dir(dir)
        expect(cfg.zfs["pool/zfs/zfs2"].send_enabled).to eq(true)
        expect(cfg.zfs["pool/zfs/zfs2"].destination).to eq("otherpool/zfs2")
        expect(cfg.zfs["pool/zfs/zfs2"].remote).to eq("root@host")
      end
    end

    it "raises ConfigError and logs an error on invalid TOML syntax" do
      Dir.mktmpdir do |dir|
        bad_path = File.join(dir, "00-broken.toml")
        File.write(bad_path, "excludes = [unclosed")

        expect($logger).to receive(:error).with(/failed to parse config file/)
        expect { described_class.load_dir(dir) }.to raise_error(ZfsMgmt::ConfigError, /failed to parse config file/)
      end
    end

    it "raises ConfigError and logs an error when merged config fails validation" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "00-invalid.toml"), <<~TOML)
          [zfs."pool/bad"]
          policy = "not-a-policy"
        TOML

        expect($logger).to receive(:error).with(/invalid configuration/)
        expect { described_class.load_dir(dir) }.to raise_error(ZfsMgmt::ConfigError, /invalid configuration/)
      end
    end

    it "warns on unknown top-level and per-dataset config keys" do
      expect($logger).to receive(:warn).with("unknown config key 'bogus_top'")
      expect($logger).to receive(:warn).with("unknown config property 'snaphsot' on pool/ds")

      cfg = described_class.from_hash(
        "bogus_top" => true,
        "zfs" => {
          "pool/ds" => {
            "manage" => true,
            "snaphsot" => true
          }
        }
      )
      expect(cfg).to be_valid
      expect(cfg.zfs["pool/ds"].manage).to eq(true)
    end
  end

  describe "#zfs_props_for manual inheritance" do
    it "propagates ancestor properties down to children and grandchildren with @source metadata" do
      cfg = described_class.from_hash(
        "zfs" => {
          "pool" => {
            "manage" => true,
            "policy" => "30d",
            "destination" => "backup/dest",
            "remote" => "root@drhost"
          },
          "pool/a" => {
            "policy" => "7d",
            "send" => true
          }
        }
      )

      child_props = cfg.zfs_props_for("pool/a")
      expect(child_props["zfsmgmt:manage"]).to eq("true")
      expect(child_props["zfsmgmt:manage@source"]).to eq("inherited from pool")
      expect(child_props["zfsmgmt:policy"]).to eq("7d")
      expect(child_props["zfsmgmt:policy@source"]).to eq("local")
      expect(child_props["zfsmgmt:destination"]).to eq("backup/dest")
      expect(child_props["zfsmgmt:destination@source"]).to eq("inherited from pool")
      expect(child_props["zfsmgmt:send"]).to eq("true")
      expect(child_props["zfsmgmt:send@source"]).to eq("local")

      grandchild_props = cfg.zfs_props_for("pool/a/b")
      expect(grandchild_props["zfsmgmt:policy"]).to eq("7d")
      expect(grandchild_props["zfsmgmt:policy@source"]).to eq("inherited from pool/a")
      expect(grandchild_props["zfsmgmt:destination@source"]).to eq("inherited from pool")

      grandchild = ZfsMgmt::Zfs.from_zfs_props("pool/a/b", grandchild_props)
      expect(grandchild.destination).to eq("backup/dest/pool/a/b")
      expect(grandchild.manage).to eq(true)
      expect(grandchild.send_enabled).to eq(true)
    end

    it "handles source-sensitive properties (send=local, send_replicate, snapshot=recursive/local) like ZFS" do
      cfg = described_class.from_hash(
        "zfs" => {
          "pool/rep" => {
            "send" => true,
            "send_replicate" => true,
            "snapshot" => "recursive"
          },
          "pool/only" => {
            "send" => "local",
            "snapshot" => "local"
          }
        }
      )

      rep_parent = ZfsMgmt::Zfs.from_zfs_props("pool/rep", cfg.zfs_props_for("pool/rep"))
      expect(rep_parent.send_enabled).to eq(true)
      expect(rep_parent.send_replicate).to eq(true)
      expect(rep_parent.snapshot).to eq(true)
      expect(rep_parent.snapshot_recursive).to eq(true)

      rep_child = ZfsMgmt::Zfs.from_zfs_props("pool/rep/sub", cfg.zfs_props_for("pool/rep/sub"))
      expect(rep_child.send_enabled).to eq(false)
      expect(rep_child.send_replicate).to eq(false)
      expect(rep_child.snapshot).to eq(false)
      expect(rep_child.snapshot_recursive).to eq(false)

      only_parent = ZfsMgmt::Zfs.from_zfs_props("pool/only", cfg.zfs_props_for("pool/only"))
      expect(only_parent.send_enabled).to eq(true)
      expect(only_parent.snapshot).to eq(true)
      expect(only_parent.snapshot_recursive).to eq(false)

      only_child = ZfsMgmt::Zfs.from_zfs_props("pool/only/sub", cfg.zfs_props_for("pool/only/sub"))
      expect(only_child.send_enabled).to eq(false)
      expect(only_child.snapshot).to eq(false)
    end

    it "only emits explicitly configured properties so unspecified defaults do not clobber ZFS user properties" do
      cfg = described_class.from_hash(
        "zfs" => {
          "pool/ds" => {
            "remote" => "root@config-host"
          }
        }
      )

      props = cfg.zfs_props_for("pool/ds")
      expect(props).to eq({
        "zfsmgmt:remote" => "root@config-host",
        "zfsmgmt:remote@source" => "local"
      })
    end
  end
end
