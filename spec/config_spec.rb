require "spec_helper"
require "tmpdir"

RSpec.describe ZfsMgmt::Config do
  describe "EasyTalk schema and validations" do
    it "includes EasyTalk::Model and generates a JSON schema with excludes, overrides, and zfs" do
      expect(described_class.ancestors).to include(EasyTalk::Model)

      schema = described_class.json_schema
      expect(schema["type"]).to eq("object")

      props = schema["properties"]
      expect(props.keys).to include("excludes", "overrides", "ssh_config", "mbuffer", "mbuffer_size", "zfs")
      expect(props["excludes"]["type"]).to eq("array")
      expect(props["excludes"]["items"]).to eq({ "type" => "string" })
      expect(props["overrides"]["type"]).to eq("object")
      expect(props["overrides"]["additionalProperties"]).to eq({ "type" => "string" })
      expect(props["ssh_config"]["type"]).to eq("object")
      expect(props["ssh_config"]["additionalProperties"]).to eq({ "type" => "string" })
      expect(props["mbuffer"]["type"]).to eq("boolean")
      expect(props["mbuffer_size"]["type"]).to eq("string")
      expect(props["zfs"]["type"]).to eq("object")
      expect(props["zfs"]["additionalProperties"]["type"]).to eq("object")
      expect(props["zfs"]["additionalProperties"]["properties"].keys).to include("send", "destination", "remote", "excludes", "overrides", "ssh_config", "mbuffer", "mbuffer_size")
    end

    it "applies sensible defaults and validates valid configs" do
      cfg = described_class.new
      expect(cfg).to be_valid
      expect(cfg.excludes).to eq([])
      expect(cfg.overrides).to eq({})
      expect(cfg.override_options).to eq([])
      expect(cfg.ssh_config).to eq({})
      expect(cfg.mbuffer).to be_nil
      expect(cfg.mbuffer_size).to be_nil
      expect(cfg.zfs).to eq({})
    end

    it "rejects invalid overrides, ssh_config, mbuffer, mbuffer_size, and excludes values" do
      non_hash = described_class.from_hash("overrides" => ["canmount=noauto"])
      expect(non_hash).not_to be_valid
      expect(non_hash.errors[:overrides]).not_to be_empty

      bad_val = described_class.from_hash("overrides" => { "canmount" => 123 })
      expect(bad_val).not_to be_valid
      expect(bad_val.errors[:overrides]).not_to be_empty

      arr_ssh = described_class.from_hash("ssh_config" => [{ "BatchMode" => "no" }])
      expect(arr_ssh).not_to be_valid
      expect(arr_ssh.errors[:ssh_config].join).to include("[ssh_config]")

      bad_ssh_val = described_class.from_hash("ssh_config" => { "ConnectTimeout" => 10 })
      expect(bad_ssh_val).not_to be_valid
      expect(bad_ssh_val.errors[:ssh_config]).not_to be_empty

      bad_mbuffer = described_class.from_hash("mbuffer" => "false")
      expect(bad_mbuffer).not_to be_valid
      expect(bad_mbuffer.errors[:mbuffer]).not_to be_empty

      bad_mbuffer_size = described_class.from_hash("mbuffer_size" => 512)
      expect(bad_mbuffer_size).not_to be_valid
      expect(bad_mbuffer_size.errors[:mbuffer_size]).not_to be_empty

      bad_per_zfs_mbuffer = described_class.from_hash("zfs" => { "pool/ds" => { "mbuffer" => "no" } })
      expect(bad_per_zfs_mbuffer).not_to be_valid
      expect(bad_per_zfs_mbuffer.errors[:zfs].join).to include("pool/ds: mbuffer must be a boolean")

      bad_per_zfs_mbuffer_size = described_class.from_hash("zfs" => { "pool/ds" => { "mbuffer_size" => 256 } })
      expect(bad_per_zfs_mbuffer_size).not_to be_valid
      expect(bad_per_zfs_mbuffer_size.errors[:zfs].join).to include("pool/ds: mbuffer_size must be a string")

      bad_excl = described_class.from_hash("excludes" => "mountpoint")
      expect(bad_excl).not_to be_valid
      expect(bad_excl.errors[:excludes]).not_to be_empty

      bad_excl_elem = described_class.from_hash("excludes" => [123])
      expect(bad_excl_elem).not_to be_valid
      expect(bad_excl_elem.errors[:excludes]).not_to be_empty
    end

    it "omits ** knockout entries from override_options" do
      cfg = described_class.from_hash("overrides" => { "canmount" => "**", "readonly" => "on" })
      expect(cfg).to be_valid
      expect(cfg.override_options).to eq(["readonly=on"])
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
        expect(cfg.overrides).to eq({})
        expect(cfg.zfs).to eq({})
      end
    end

    it "loads and merges *.toml files in sort() order with later files replacing earlier values" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "00-base.toml"), <<~TOML)
          excludes = ["mountpoint", "sharenfs"]

          [overrides]
          readonly = "on"
          canmount = "off"

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

          [overrides]
          canmount = "noauto"
          atime = "off"

          [zfs."pool/zfs"]
          remote = "root@newhost"

          [zfs."pool/zfs/zfs2"]
          send_compressed = true
        TOML

        cfg = described_class.load_dir(dir)
        expect(cfg).to be_valid
        expect(cfg.excludes).to eq(["mountpoint"])
        expect(cfg.overrides).to eq({
          "readonly" => "on",
          "canmount" => "noauto",
          "atime" => "off"
        })
        expect(cfg.override_options).to eq(["readonly=on", "canmount=noauto", "atime=off"])

        parent = cfg.zfs["pool/zfs"]
        expect(parent).to be_a(ZfsMgmt::Zfs)
        expect(parent.manage).to eq(true)
        expect(parent.policy).to eq("30d")
        expect(parent.remote).to eq("root@newhost")

        child = cfg.zfs["pool/zfs/zfs2"]
        expect(child).to be_a(ZfsMgmt::Zfs)
        expect(child.send_enabled).to eq(true)
        expect(child.destination).to eq("root@host:otherpool/zfs2")
        expect(child.destination_remote).to eq("root@host")
        expect(child.destination_path).to eq("otherpool/zfs2")
        expect(child.remote).to eq("root@host")
        expect(child.send_compressed).to eq(true)
      end
    end

    it "rejects [[zfs.\"dataset\"]] array-of-tables syntax in TOML" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "10-send.toml"), <<~TOML)
          [[zfs."pool/zfs/zfs2"]]
          send = true
          destination = "otherpool"
          remote = "root@host"
        TOML

        expect($logger).to receive(:error).with(/invalid configuration.*pool\/zfs\/zfs2.*must be a Zfs configuration table/)
        expect { described_class.load_dir(dir) }.to raise_error(
          ZfsMgmt::ConfigError,
          /invalid configuration.*pool\/zfs\/zfs2.*must be a Zfs configuration table/
        )
      end

      cfg = described_class.from_hash(
        "zfs" => {
          "pool/zfs/zfs2" => [{ "send" => true }]
        }
      )
      expect(cfg).not_to be_valid
      expect(cfg.errors[:zfs].join).to include("pool/zfs/zfs2")
      expect(cfg.errors[:zfs].join).to include("must be a Zfs configuration table")
    end

    it "loads per-zfs excludes and [zfs.\"dataset\".overrides] from TOML files and rejects [[zfs.\"dataset\".overrides]]" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "10-per-zfs.toml"), <<~TOML)
          excludes = ["mountpoint"]

          [overrides]
          canmount = "noauto"
          readonly = "on"

          [zfs."pool/zfs"]
          excludes = ["sharenfs"]

          [zfs."pool/zfs".overrides]
          compression = "lz4"

          [zfs."pool/zfs/sub"]
          excludes = ["**sharenfs", "quota"]

          [zfs."pool/zfs/sub".overrides]
          canmount = "**"
          atime = "off"
        TOML

        cfg = described_class.load_dir(dir)
        expect(cfg).to be_valid
        expect(cfg.zfs["pool/zfs"].excludes).to eq(["sharenfs"])
        expect(cfg.zfs["pool/zfs"].overrides).to eq({ "compression" => "lz4" })
        expect(cfg.zfs["pool/zfs/sub"].excludes).to eq(["**sharenfs", "quota"])
        expect(cfg.zfs["pool/zfs/sub"].overrides).to eq({
          "compression" => "lz4",
          "canmount" => "**",
          "atime" => "off"
        })
      end

      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "10-bad.toml"), <<~TOML)
          [zfs."pool/zfs/zfs2"]
          send = true

          [[zfs."pool/zfs/zfs2".overrides]]
          stuff = "nonsense"
        TOML

        expect($logger).to receive(:error).with(/invalid configuration.*pool\/zfs\/zfs2.*overrides must be a table/)
        expect { described_class.load_dir(dir) }.to raise_error(
          ZfsMgmt::ConfigError,
          /invalid configuration.*pool\/zfs\/zfs2.*overrides must be a table/
        )
      end

      bad_per_zfs_excl = described_class.from_hash("zfs" => { "pool/ds" => { "excludes" => "mountpoint" } })
      expect(bad_per_zfs_excl).not_to be_valid
      expect(bad_per_zfs_excl.errors[:zfs].join).to include("pool/ds: excludes must be an array of strings")

      bad_per_zfs_ovr = described_class.from_hash("zfs" => { "pool/ds" => { "overrides" => { "canmount" => 123 } } })
      expect(bad_per_zfs_ovr).not_to be_valid
      expect(bad_per_zfs_ovr.errors[:zfs].join).to include("pool/ds: overrides.canmount: property and value must be strings")

      bad_dest_int = described_class.from_hash("zfs" => { "pool/ds" => { "destination" => 123 } })
      expect(bad_dest_int).not_to be_valid
      expect(bad_dest_int.errors[:zfs].join).to include("pool/ds: destination must be a string or an array of strings")

      bad_dest_arr = described_class.from_hash("zfs" => { "pool/ds" => { "destination" => ["backup/a", 456] } })
      expect(bad_dest_arr).not_to be_valid
      expect(bad_dest_arr.errors[:zfs].join).to include("pool/ds: destination must be a string or an array of strings")
    end

    it "supports array destinations in TOML config including inheritance and child override" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "10-multi.toml"), <<~TOML)
          [zfs."pool/a"]
          send = true
          destination = ["backup/a", "zfs_recv@drhost:offsite/a"]

          [zfs."pool/a/child"]
          send = true

          [zfs."pool/a/override"]
          send = true
          destination = "single/dest"
        TOML

        cfg = described_class.load_dir(dir)
        expect(cfg).to be_valid
        expect(cfg.zfs["pool/a"].destinations).to eq(["backup/a/a", "zfs_recv@drhost:offsite/a/a"])
        expect(cfg.zfs["pool/a/child"].destinations).to eq(["backup/a/a/child", "zfs_recv@drhost:offsite/a/a/child"])
        expect(cfg.zfs["pool/a/override"].destinations).to eq(["single/dest/override"])
      end
    end

    it "supports host:path and user@host:path in destination and warns on deprecated remote" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "10-dest.toml"), <<~TOML)
          [zfs."pool/a"]
          destination = "backuphost:tank/dest"

          [zfs."pool/b"]
          destination = "zfs_recv@backuphost:tank/dest"

          [zfs."pool/c"]
          remote = "old@host"
          destination = "tank/dest"

          [zfs."pool/d"]
          remote = "old@host"
          destination = "new@host:tank/dest"
        TOML

        expect($logger).to receive(:warn).with(/remote option is deprecated \(pool\/c\)/)
        expect($logger).to receive(:warn).with(/remote option is deprecated \(pool\/d\)/)

        cfg = described_class.load_dir(dir)
        expect(cfg).to be_valid

        expect(cfg.zfs["pool/a"].destination).to eq("backuphost:tank/dest/a")
        expect(cfg.zfs["pool/a"].destination_remote).to eq("backuphost")
        expect(cfg.zfs["pool/a"].destination_path).to eq("tank/dest/a")

        expect(cfg.zfs["pool/b"].destination).to eq("zfs_recv@backuphost:tank/dest/b")
        expect(cfg.zfs["pool/b"].destination_remote).to eq("zfs_recv@backuphost")
        expect(cfg.zfs["pool/b"].destination_path).to eq("tank/dest/b")

        expect(cfg.zfs["pool/c"].destination).to eq("old@host:tank/dest/c")
        expect(cfg.zfs["pool/d"].destination).to eq("new@host:tank/dest/d")
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
      expect(grandchild.destination).to eq("root@drhost:backup/dest/pool/a/b")
      expect(grandchild.destination_path).to eq("backup/dest/pool/a/b")
      expect(grandchild.destination_remote).to eq("root@drhost")
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

    it "merges per-dataset excludes and overrides down the hierarchy while preserving knockout markers" do
      cfg = described_class.from_hash(
        "zfs" => {
          "pool" => {
            "excludes" => %w[mountpoint sharenfs],
            "overrides" => { "canmount" => "noauto", "readonly" => "on" }
          },
          "pool/child" => {
            "excludes" => %w[**sharenfs quota],
            "overrides" => { "readonly" => "**", "compression" => "zstd" }
          }
        }
      )

      expect(cfg.zfs_excludes_layers_for("pool/child/grand")).to eq([
        %w[mountpoint sharenfs],
        %w[**sharenfs quota]
      ])
      expect(cfg.zfs_overrides_layers_for("pool/child/grand")).to eq([
        { "canmount" => "noauto", "readonly" => "on" },
        { "readonly" => "**", "compression" => "zstd" }
      ])

      props = cfg.zfs_props_for("pool/child/grand")
      expect(props["zfsmgmt:excludes"]).to eq(%w[mountpoint **sharenfs quota])
      expect(props["zfsmgmt:excludes@source"]).to eq("inherited from pool/child")
      expect(props["zfsmgmt:overrides"]).to eq({
        "canmount" => "noauto",
        "readonly" => "**",
        "compression" => "zstd"
      })
      expect(props["zfsmgmt:overrides@source"]).to eq("inherited from pool/child")
    end

    it "loads, inherits, and validates top-level [ssh_config] and [zfs.\"dataset\".ssh_config]" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "10-ssh.toml"), <<~TOML)
          [ssh_config]
          Compression = "on"
          ServerAliveInterval = "60"

          [zfs."pool/zfs"]
          [zfs."pool/zfs".ssh_config]
          ConnectTimeout = "15"

          [zfs."pool/zfs/zfs2"]
          [zfs."pool/zfs/zfs2".ssh_config]
          BatchMode = "no"
          Compression = "yes"
        TOML

        cfg = described_class.load_dir(dir)
        expect(cfg).to be_valid
        expect(cfg.ssh_config).to eq({
          "Compression" => "on",
          "ServerAliveInterval" => "60"
        })
        expect(cfg.zfs["pool/zfs"].ssh_config).to eq({ "ConnectTimeout" => "15" })
        expect(cfg.zfs["pool/zfs/zfs2"].ssh_config).to eq({
          "ConnectTimeout" => "15",
          "BatchMode" => "no",
          "Compression" => "yes"
        })
        expect(cfg.zfs_ssh_config_layers_for("pool/zfs/zfs2/sub")).to eq([
          { "ConnectTimeout" => "15" },
          { "BatchMode" => "no", "Compression" => "yes" }
        ])
      end

      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "10-bad-ssh.toml"), <<~TOML)
          [zfs."pool/zfs/zfs2"]
          [[zfs."pool/zfs/zfs2".ssh_config]]
          BatchMode = "no"
        TOML

        expect($logger).to receive(:error).with(/invalid configuration.*pool\/zfs\/zfs2.*ssh_config must be a table/)
        expect { described_class.load_dir(dir) }.to raise_error(
          ZfsMgmt::ConfigError,
          /invalid configuration.*pool\/zfs\/zfs2.*ssh_config must be a table/
        )
      end

      bad_per_zfs_ssh = described_class.from_hash("zfs" => { "pool/ds" => { "ssh_config" => { "ConnectTimeout" => 10 } } })
      expect(bad_per_zfs_ssh).not_to be_valid
      expect(bad_per_zfs_ssh.errors[:zfs].join).to include("pool/ds: ssh_config.ConnectTimeout: property and value must be strings")
    end

    it "loads and merges top-level and per-dataset mbuffer and mbuffer_size options with inheritance" do
      Dir.mktmpdir do |dir|
        File.write(File.join(dir, "00-base.toml"), <<~TOML)
          mbuffer = true
          mbuffer_size = "256M"

          [zfs."pool"]
          mbuffer = false
          mbuffer_size = "512M"

          [zfs."pool/child"]
          mbuffer = true
        TOML

        File.write(File.join(dir, "50-local.toml"), <<~TOML)
          mbuffer = false
          mbuffer_size = "128M"

          [zfs."pool/child"]
          mbuffer_size = "1G"
        TOML

        cfg = described_class.load_dir(dir)
        expect(cfg).to be_valid
        expect(cfg.mbuffer).to eq(false)
        expect(cfg.mbuffer_size).to eq("128M")
        expect(cfg.zfs["pool"].mbuffer).to eq(false)
        expect(cfg.zfs["pool"].mbuffer_size).to eq("512M")
        expect(cfg.zfs["pool/child"].mbuffer).to eq(true)
        expect(cfg.zfs["pool/child"].mbuffer_size).to eq("1G")

        grandchild = ZfsMgmt::Zfs.from_zfs_props("pool/child/grand", cfg.zfs_props_for("pool/child/grand"))
        expect(grandchild.mbuffer).to eq(true)
        expect(grandchild.mbuffer_size).to eq("1G")
      end
    end
  end

  describe "examples/config.toml" do
    it "parses cleanly without warnings and produces a valid Config" do
      sample_path = File.expand_path("../examples/config.toml", __dir__)
      expect($logger).not_to receive(:warn)

      cfg = described_class.load_files([sample_path])
      expect(cfg).to be_valid
      expect(cfg.mbuffer).to eq(true)
      expect(cfg.mbuffer_size).to eq("512M")
      expect(cfg.excludes).to eq(%w[mountpoint sharenfs sharesmb])
      expect(cfg.overrides).to eq({ "canmount" => "noauto", "readonly" => "on" })
      expect(cfg.override_options).to eq(["canmount=noauto", "readonly=on"])
      expect(cfg.ssh_config).to eq({ "Compression" => "yes" })
      expect(cfg.zfs.keys).to match_array(%w[tank tank/data tank/archive])
      expect(cfg.zfs["tank"].policy).to eq("10y6m4w30d24h")
      expect(cfg.zfs["tank/data"].send_enabled).to eq(true)
      expect(cfg.zfs["tank/data"].manage).to eq(true)
      expect(cfg.zfs["tank/data"].destination).to eq("zfs_recv@backup.example.com:backup/tank/data")
      expect(cfg.zfs["tank/data"].destination_remote).to eq("zfs_recv@backup.example.com")
      expect(cfg.zfs["tank/data"].destination_path).to eq("backup/tank/data")
      expect(cfg.zfs["tank/data"].mbuffer_size).to eq("1G")
      expect(cfg.zfs["tank/data"].excludes).to eq(["quota"])
      expect(cfg.zfs["tank/data"].overrides).to eq({ "canmount" => "**", "compression" => "zstd" })
      expect(cfg.zfs["tank/data"].override_options).to eq(["compression=zstd"])
      expect(cfg.zfs["tank/data"].ssh_config).to eq({ "ConnectTimeout" => "30" })
      expect(cfg.zfs["tank/archive"].mbuffer).to eq(false)
      expect(cfg.zfs["tank/archive"].destinations).to eq([
        "backup/archive/archive",
        "zfs_recv@offsite.example.com:offsite/archive/archive"
      ])
      expect(cfg.zfs["tank/archive"].restic_backup).to eq(true)
      expect(cfg.zfs["tank/archive"].snapshot_recursive).to eq(true)
    end
  end

  describe ".config_json_schema and schema/config.schema.json" do
    it "produces a draft-07 schema tailored for TOML config validation" do
      schema = described_class.config_json_schema
      expect(schema["$schema"]).to eq("http://json-schema.org/draft-07/schema#")
      expect(schema["$id"]).to eq("https://raw.githubusercontent.com/aranc23/zfs_mgmt/master/schema/config.schema.json")

      zfs_schema = schema.dig("properties", "zfs", "additionalProperties")
      expect(zfs_schema).to be_a(Hash)
      expect(zfs_schema["required"]).to be_nil

      snapshot_schema = zfs_schema.dig("properties", "snapshot")
      expect(snapshot_schema["oneOf"]).to eq([
        { "type" => "boolean" },
        { "type" => "string", "enum" => %w[recursive local] }
      ])

      send_schema = zfs_schema.dig("properties", "send")
      expect(send_schema["oneOf"]).to eq([
        { "type" => "boolean" },
        { "type" => "string", "enum" => %w[local] }
      ])

      dest_schema = zfs_schema.dig("properties", "destination")
      expect(dest_schema["oneOf"]).to eq([
        { "type" => "string" },
        { "type" => "array", "items" => { "type" => "string" } }
      ])
    end

    it "matches the committed schema/config.schema.json file" do
      schema_path = File.expand_path("../schema/config.schema.json", __dir__)
      expect(File).to exist(schema_path)

      parsed = JSON.parse(File.read(schema_path))
      expect(parsed).to eq(described_class.config_json_schema)
    end

    it "generates the schema via the rake schema:generate task" do
      require "rake"
      rake = Rake::Application.new
      Rake.application = rake
      load File.expand_path("../Rakefile", __dir__)

      Dir.mktmpdir do |dir|
        out = File.join(dir, "config.schema.json")
        expect { rake["schema:generate"].invoke(out) }.to output(/Wrote JSON schema to #{Regexp.escape(out)}/).to_stdout
        expect(JSON.parse(File.read(out))).to eq(described_class.config_json_schema)
      end
    ensure
      Rake.application = nil
    end
  end
end
