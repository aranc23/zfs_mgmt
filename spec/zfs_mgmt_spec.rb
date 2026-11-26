require "spec_helper"
require "stringio"
require "tmpdir"

RSpec.describe ZfsMgmt do
  it "has a version number" do
    expect(ZfsMgmt::VERSION).not_to be nil
  end

  describe ".snapshot_policy" do
    let(:zfs) { "tank/data" }
    let(:props) do
      {
        "zfsmgmt:policy" => "2d",
        "zfsmgmt:manage" => "true"
      }
    end

    # 4 daily snapshots, 1 day apart
    let(:t4) { Time.new(2026, 11, 17, 12, 0, 0).to_i }
    let(:t3) { Time.new(2026, 11, 16, 12, 0, 0).to_i }
    let(:t2) { Time.new(2026, 11, 15, 12, 0, 0).to_i }
    let(:t1) { Time.new(2026, 11, 14, 12, 0, 0).to_i }

    let(:snaps) do
      {
        "tank/data@snap-2026-11-17" => { "creation" => t4 },
        "tank/data@snap-2026-11-16" => { "creation" => t3 },
        "tank/data@snap-2026-11-15" => { "creation" => t2 },
        "tank/data@snap-2026-11-14" => { "creation" => t1 }
      }
    end

    let(:zfs_obj) do
      ZfsMgmt::Zfs.new(
        name: zfs,
        policy: "2d",
        manage: true,
        snapshots: snaps
      )
    end

    before do
      allow(ZfsMgmt).to receive(:zfs_managed_list).and_return([zfs_obj])
    end

    context "when show_destroy is false" do
      it "only prints kept snapshots in the table" do
        output = capture_stdout do
          ZfsMgmt.snapshot_policy(show_destroy: false)
        end

        expect(output).to include("snap-2026-11-17")
        expect(output).to include("snap-2026-11-16")
        expect(output).not_to include("snap-2026-11-15")
        expect(output).not_to include("snap-2026-11-14")
      end
    end

    context "when show_destroy is true" do
      it "interleaves snapshots to be deleted with kept snapshots in chronological order" do
        output = capture_stdout do
          ZfsMgmt.snapshot_policy(show_destroy: true)
        end

        expect(output).to include("snap-2026-11-17")
        expect(output).to include("snap-2026-11-16")
        expect(output).to include("snap-2026-11-15")
        expect(output).to include("snap-2026-11-14")

        # Verify ordering: newest first
        pos_t4 = output.index("snap-2026-11-17")
        pos_t3 = output.index("snap-2026-11-16")
        pos_t2 = output.index("snap-2026-11-15")
        pos_t1 = output.index("snap-2026-11-14")

        expect(pos_t4).to be < pos_t3
        expect(pos_t3).to be < pos_t2
        expect(pos_t2).to be < pos_t1
      end

      it "leaves reason columns blank for snapshots marked to be deleted" do
        output = capture_stdout do
          ZfsMgmt.snapshot_policy(show_destroy: true)
        end

        # Kept rows have daily date in daily column
        expect(output).to match(/snap-2026-11-17\s+\|\s+\S+\s+\|\s+\|\s+2026-11-17\s+\|\s+\|\s+\|\s+\|/)
        expect(output).to match(/snap-2026-11-16\s+\|\s+\S+\s+\|\s+\|\s+2026-11-16\s+\|\s+\|\s+\|\s+\|/)
        # Deleted rows have blank policy columns
        expect(output).to match(/snap-2026-11-15\s+\|\s+\S+\s+\|\s+\|\s+\|\s+\|\s+\|\s+\|/)
        expect(output).to match(/snap-2026-11-14\s+\|\s+\S+\s+\|\s+\|\s+\|\s+\|\s+\|\s+\|/)
      end
    end
  end

  describe ZfsMgmt::ZfsMgr::Snapshot do
    describe "#policy" do
      it "defaults show_destroy to false" do
        expect(ZfsMgmt).to receive(:snapshot_policy).with(filter: ".+", show_destroy: false)
        described_class.start(["policy"])
      end

      it "passes show_destroy: true when --show-destroy flag is given" do
        expect(ZfsMgmt).to receive(:snapshot_policy).with(filter: ".+", show_destroy: true)
        described_class.start(["policy", "--show-destroy"])
      end
    end
  end

  describe ".send_state_db_path" do
    before do
      ZfsMgmt.global_options = {}
    end

    it "uses /var/lib/zfs_mgmt/send_state.pstore when running as root" do
      allow(Process).to receive(:euid).and_return(0)
      expect(ZfsMgmt.send_state_db_path).to eq("/var/lib/zfs_mgmt/send_state.pstore")
    end

    it "uses XDG_STATE_HOME/zfs_mgmt/send_state.pstore when set for non-root user" do
      allow(Process).to receive(:euid).and_return(1000)
      allow(ENV).to receive(:[]).and_call_original
      allow(ENV).to receive(:[]).with("XDG_STATE_HOME").and_return("/custom/xdg/state")
      expect(ZfsMgmt.send_state_db_path).to eq("/custom/xdg/state/zfs_mgmt/send_state.pstore")
    end

    it "defaults to ~/.local/state/zfs_mgmt/send_state.pstore for non-root user when XDG_STATE_HOME is unset" do
      allow(Process).to receive(:euid).and_return(1000)
      allow(ENV).to receive(:[]).and_call_original
      allow(ENV).to receive(:[]).with("XDG_STATE_HOME").and_return(nil)
      allow(Dir).to receive(:home).and_return("/home/testuser")
      expect(ZfsMgmt.send_state_db_path).to eq("/home/testuser/.local/state/zfs_mgmt/send_state.pstore")
    end
  end

  describe "send state tracking and management" do
    around do |example|
      Dir.mktmpdir do |tmpdir|
        @db_path = File.join(tmpdir, "state", "send_state.pstore")
        ZfsMgmt.global_options = {
          state_db: @db_path,
          zfs_binary: "zfs",
          mbuffer_binary: "mbuffer"
        }
        example.run
      end
    end

    it "records send state as a JSON hash string in PStore and overwrites previous entry" do
      ts1 = 1_700_000_000
      ts2 = 1_700_003_600
      status_fail = instance_double(Process::Status, exitstatus: 1)
      status_ok = instance_double(Process::Status, exitstatus: 0)

      ZfsMgmt.record_send_state("zpool/zfs/zfssub", status_fail, timestamp: ts1)

      store = PStore.new(@db_path)
      raw = store.transaction(true) { store["zpool/zfs/zfssub"] }
      expect(raw).to be_a(String)
      expect(JSON.parse(raw)).to eq({ "timestamp" => ts1, "exit" => 1 })

      ZfsMgmt.record_send_state("zpool/zfs/zfssub", status_ok, timestamp: ts2)
      entries = ZfsMgmt.send_state_entries
      expect(entries["zpool/zfs/zfssub"]).to eq({ "timestamp" => ts2, "exit" => 0 })
    end

    it "deletes a single entry with send_state_delete" do
      ZfsMgmt.record_send_state("tank/a", 0, timestamp: 1_700_000_000)
      ZfsMgmt.record_send_state("tank/b", 1, timestamp: 1_700_000_100)

      ZfsMgmt.send_state_delete("tank/a")
      entries = ZfsMgmt.send_state_entries
      expect(entries.keys).to eq(["tank/b"])
    end

    it "wipes all entries with send_state_wipe" do
      ZfsMgmt.record_send_state("tank/a", 0, timestamp: 1_700_000_000)
      ZfsMgmt.record_send_state("tank/b", 1, timestamp: 1_700_000_100)

      ZfsMgmt.send_state_wipe
      expect(ZfsMgmt.send_state_entries).to be_empty
    end

    describe ".send_state_show" do
      let(:now) { Time.new(2026, 11, 18, 12, 0, 0).to_i }

      before do
        allow(Time).to receive(:now).and_return(Time.at(now))
      end

      it "prints a table of values in the db when check is false" do
        ts = now - 3600
        ZfsMgmt.record_send_state("zpool/zfs/zfssub", 0, timestamp: ts)

        output = capture_stdout do
          ZfsMgmt.send_state_show(check: false)
        end

        expect(output).to include("zfs")
        expect(output).to include("timestamp")
        expect(output).to include("exit")
        expect(output).to include("zpool/zfs/zfssub")
        expect(output).to include(ZfsMgmt.local_epoch_to_datetime(ts).to_s)
      end

      it "prints Checkmk local check output with OK (0), WARN (1 >36h), and CRIT (2 >72h or exit != 0)" do
        ts_ok = now - (10 * 3600)         # 10h ago -> OK (0)
        ts_warn = now - (37 * 3600)       # 37h ago (> 36h) -> WARN (1)
        ts_crit_age = now - (73 * 3600)   # 73h ago (> 72h) -> CRIT (2)
        ts_crit_exit = now - (1 * 3600)   # 1h ago, exit 1 -> CRIT (2)

        ZfsMgmt.record_send_state("tank/ok", 0, timestamp: ts_ok)
        ZfsMgmt.record_send_state("tank/warn", 0, timestamp: ts_warn)
        ZfsMgmt.record_send_state("tank/crit_age", 0, timestamp: ts_crit_age)
        ZfsMgmt.record_send_state("tank/crit_exit", 1, timestamp: ts_crit_exit)

        output = capture_stdout do
          ZfsMgmt.send_state_show(check: true)
        end

        lines = output.strip.split("\n")
        expect(lines.length).to eq(4)
        expect(lines).to include("0 \"zfs_send tank/ok\" age=36000;129600;259200 last send #{ZfsMgmt.local_epoch_to_datetime(ts_ok)} exit 0")
        expect(lines).to include("1 \"zfs_send tank/warn\" age=133200;129600;259200 last send #{ZfsMgmt.local_epoch_to_datetime(ts_warn)} exit 0")
        expect(lines).to include("2 \"zfs_send tank/crit_age\" age=262800;129600;259200 last send #{ZfsMgmt.local_epoch_to_datetime(ts_crit_age)} exit 0")
        expect(lines).to include("2 \"zfs_send tank/crit_exit\" age=3600;129600;259200 last send #{ZfsMgmt.local_epoch_to_datetime(ts_crit_exit)} exit 1")
      end
    end

    describe ".zfs_send integration" do
      let(:zfs) { "tank/fs" }
      let(:props) do
        {
          "zfsmgmt:destination" => "backup/pool",
          "zfsmgmt:destination@source" => "local"
        }
      end
      let(:snaps) do
        {
          "tank/fs@snap1" => { "creation" => 1000 },
          "tank/fs@snap2" => { "creation" => 2000 }
        }
      end

      it "records completion time and exit code when an incremental zfs send executes" do
        allow(ZfsMgmt).to receive(:zfsget).with(zfs: "backup/pool/fs", command_prefix: []).and_return({
          "backup/pool/fs" => {}
        })
        allow(ZfsMgmt).to receive(:zfsget).with(
          zfs: "backup/pool/fs",
          types: ["snapshot"],
          command_prefix: [],
          properties: ["creation", "userrefs"]
        ).and_return({
          "backup/pool/fs@snap1" => { "creation" => 1000, "userrefs" => 0 }
        })

        status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)
        expect(ZfsMgmt).to receive(:system_com).and_return(status_ok)

        ZfsMgmt.zfs_send({ mbuffer: true }, zfs, props, snaps)

        entries = ZfsMgmt.send_state_entries
        expect(entries).to have_key("tank/fs")
        expect(entries["tank/fs"]["exit"]).to eq(0)
        expect(entries["tank/fs"]["timestamp"]).to be_a(Integer)
      end

      it "does not record state when most recent snapshot already exists on remote" do
        allow(ZfsMgmt).to receive(:zfsget).with(zfs: "backup/pool/fs", command_prefix: []).and_return({
          "backup/pool/fs" => {}
        })
        allow(ZfsMgmt).to receive(:zfsget).with(
          zfs: "backup/pool/fs",
          types: ["snapshot"],
          command_prefix: [],
          properties: ["creation", "userrefs"]
        ).and_return({
          "backup/pool/fs@snap2" => { "creation" => 2000, "userrefs" => 0 }
        })

        expect(ZfsMgmt).not_to receive(:system_com)
        ZfsMgmt.zfs_send({ mbuffer: true }, zfs, props, snaps)
        expect(ZfsMgmt.send_state_entries).to be_empty
      end

      it "does not record state when noop is enabled" do
        allow(ZfsMgmt).to receive(:zfsget).with(zfs: "backup/pool/fs", command_prefix: []).and_return({
          "backup/pool/fs" => {}
        })
        allow(ZfsMgmt).to receive(:zfsget).with(
          zfs: "backup/pool/fs",
          types: ["snapshot"],
          command_prefix: [],
          properties: ["creation", "userrefs"]
        ).and_return({
          "backup/pool/fs@snap1" => { "creation" => 1000, "userrefs" => 0 }
        })

        status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)
        expect(ZfsMgmt).to receive(:system_com).and_return(status_ok)

        ZfsMgmt.zfs_send({ mbuffer: true, noop: true }, zfs, props, snaps)
        expect(ZfsMgmt.send_state_entries).to be_empty
      end
    end
  end

  describe ZfsMgmt::ZfsMgr::Send do
    it "dispatches `send state` to send_state_show(check: false)" do
      expect(ZfsMgmt).to receive(:send_state_show).with(check: false, filter: ".+")
      described_class.start(["state"])
    end

    it "dispatches `send state --check` to send_state_show(check: true)" do
      expect(ZfsMgmt).to receive(:send_state_show).with(check: true, filter: ".+")
      described_class.start(["state", "--check"])
    end

    it "dispatches `send state delete <zfs>` to send_state_delete" do
      expect(ZfsMgmt).to receive(:send_state_delete).with("zpool/zfs/zfssub")
      described_class.start(["state", "delete", "zpool/zfs/zfssub"])
    end

    it "dispatches `send state wipe` to send_state_wipe" do
      expect(ZfsMgmt).to receive(:send_state_wipe)
      described_class.start(["state", "wipe"])
    end

    describe "#all recv_state CLI flags" do
      before do
        allow(ZfsMgmt).to receive(:lock).and_return(nil)
        allow(ZfsMgmt).to receive(:unlock)
      end

      it "passes recv_state: false when --no-recv-state is given" do
        expect(ZfsMgmt).to receive(:zfs_send_all) do |opts|
          expect(opts[:recv_state]).to eq(false)
        end
        described_class.start(["all", "--no-recv-state"])
      end

      it "passes recv_state: true when --recv-state is given" do
        expect(ZfsMgmt).to receive(:zfs_send_all) do |opts|
          expect(opts[:recv_state]).to eq(true)
        end
        described_class.start(["all", "--recv-state"])
      end

      it "passes recv_state: true when -s is given" do
        expect(ZfsMgmt).to receive(:zfs_send_all) do |opts|
          expect(opts[:recv_state]).to eq(true)
        end
        described_class.start(["all", "-s"])
      end
    end
  end

  describe ".zfs_recv_com" do
    before do
      ZfsMgmt.global_options = {
        zfs_binary: "zfs",
        mbuffer_binary: "mbuffer"
      }
    end

    it "includes -s by default when neither CLI option nor zfsmgmt:recv_state property is set" do
      com = ZfsMgmt.zfs_recv_com({}, [], {}, "backup/pool/fs")
      expect(com).to eq(["zfs", "recv", "-s", '"backup/pool/fs"'])
    end

    it "includes -s when zfsmgmt:recv_state is true or on" do
      expect(ZfsMgmt.zfs_recv_com({}, [], { "zfsmgmt:recv_state" => "true" }, "backup/pool/fs")).to include("-s")
      expect(ZfsMgmt.zfs_recv_com({}, [], { "zfsmgmt:recv_state" => "on" }, "backup/pool/fs")).to include("-s")
    end

    it "omits -s when zfsmgmt:recv_state is false or off" do
      com_false = ZfsMgmt.zfs_recv_com({}, [], { "zfsmgmt:recv_state" => "false" }, "backup/pool/fs")
      expect(com_false).to eq(["zfs", "recv", '"backup/pool/fs"'])

      com_off = ZfsMgmt.zfs_recv_com({}, [], { "zfsmgmt:recv_state" => "off" }, "backup/pool/fs")
      expect(com_off).to eq(["zfs", "recv", '"backup/pool/fs"'])
    end

    it "omits -s when CLI option recv_state is false, overriding property" do
      com = ZfsMgmt.zfs_recv_com(
        Thor::CoreExt::HashWithIndifferentAccess.new(recv_state: false),
        [],
        { "zfsmgmt:recv_state" => "true" },
        "backup/pool/fs"
      )
      expect(com).to eq(["zfs", "recv", '"backup/pool/fs"'])
    end

    it "includes -s when CLI option recv_state is true, overriding zfsmgmt:recv_state=false" do
      com = ZfsMgmt.zfs_recv_com(
        Thor::CoreExt::HashWithIndifferentAccess.new(recv_state: true),
        [],
        { "zfsmgmt:recv_state" => "false" },
        "backup/pool/fs"
      )
      expect(com).to eq(["zfs", "recv", "-s", '"backup/pool/fs"'])
    end

    it "supports all recv flags, excludes, options, extra_opts, and remote ssh wrapping with mbuffer" do
      opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        noop: true,
        drop_holds: true,
        unmount: true,
        discard_first: true,
        force: true,
        verbose: "recv",
        exclude: ["mountpoint", "sharenfs"],
        option: ["readonly=on"],
        remote: "backup@remotehost",
        mbuffer: true,
        mbuffer_size: "512M"
      )
      com = ZfsMgmt.zfs_recv_com(opts, ["-o", "canmount=noauto"], {}, "backup/pool/fs")
      expect(com).to eq([
        "ssh",
        "backup@remotehost",
        "'mbuffer -q -m 512M | zfs recv -s -n -h -u -d -F -v -x mountpoint -x sharenfs -o readonly=on -o canmount=noauto \"backup/pool/fs\"'"
      ])
    end

    it "reads recv flags and remote host from zfsmgmt properties when CLI options are not set" do
      props = {
        "zfsmgmt:recv_force" => "true",
        "zfsmgmt:recv_unmount" => "on",
        "zfsmgmt:remote" => "root@drhost"
      }
      com = ZfsMgmt.zfs_recv_com({}, [], props, "backup/pool/fs")
      expect(com).to eq([
        "ssh",
        "root@drhost",
        "'zfs recv -s -u -F \"backup/pool/fs\"'"
      ])
    end

    it "extracts ssh remote from target or destination and strips host prefix from recv target" do
      com = ZfsMgmt.zfs_recv_com({}, [], {}, "zfs_recv@backuphost:backup/pool/fs")
      expect(com).to eq([
        "ssh",
        "zfs_recv@backuphost",
        "'zfs recv -s \"backup/pool/fs\"'"
      ])

      host_only = ZfsMgmt.zfs_recv_com({}, [], {}, "backuphost:backup/pool/fs")
      expect(host_only).to eq([
        "ssh",
        "backuphost",
        "'zfs recv -s \"backup/pool/fs\"'"
      ])
    end

    it "omits volume-unsupported properties from -x and -o when receiving a volume" do
      opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        exclude: %w[mountpoint canmount quota sharenfs],
        option: %w[mountpoint=none canmount=off quota=10G readonly=on]
      )
      vol_props = { "type" => "volume" }
      com = ZfsMgmt.zfs_recv_com(opts, [], vol_props, "backup/pool/zvol")
      expect(com).to eq([
        "zfs", "recv", "-s", "-x", "sharenfs", "-o", "readonly=on", '"backup/pool/zvol"'
      ])

      vol_obj = ZfsMgmt::Zfs.new(name: "tank/zvol", type: "volume")
      com_obj = ZfsMgmt.zfs_recv_com(opts, [], vol_obj, "backup/pool/zvol")
      expect(com_obj).to eq([
        "zfs", "recv", "-s", "-x", "sharenfs", "-o", "readonly=on", '"backup/pool/zvol"'
      ])
    end

    it "passes mountpoint, canmount, and quota in -x and -o when receiving a filesystem" do
      opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        exclude: %w[mountpoint canmount quota],
        option: %w[mountpoint=none canmount=off quota=10G]
      )
      fs_props = { "type" => "filesystem" }
      com = ZfsMgmt.zfs_recv_com(opts, [], fs_props, "backup/pool/fs")
      expect(com).to eq([
        "zfs", "recv", "-s",
        "-x", "mountpoint", "-x", "canmount", "-x", "quota",
        "-o", "mountpoint=none", "-o", "canmount=off", "-o", "quota=10G",
        '"backup/pool/fs"'
      ])
    end

    it "merges config.excludes and config.overrides with CLI options and supports ** knockouts" do
      ZfsMgmt.config = ZfsMgmt::Config.from_hash(
        "excludes" => %w[mountpoint sharenfs],
        "overrides" => { "canmount" => "off", "readonly" => "on" }
      )

      fs_com = ZfsMgmt.zfs_recv_com({}, [], { "type" => "filesystem" }, "backup/pool/fs")
      expect(fs_com).to eq([
        "zfs", "recv", "-s",
        "-x", "mountpoint", "-x", "sharenfs",
        "-o", "canmount=off", "-o", "readonly=on",
        '"backup/pool/fs"'
      ])

      vol_com = ZfsMgmt.zfs_recv_com({}, [], { "type" => "volume" }, "backup/pool/zvol")
      expect(vol_com).to eq([
        "zfs", "recv", "-s",
        "-x", "sharenfs",
        "-o", "readonly=on",
        '"backup/pool/zvol"'
      ])

      cli_opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        exclude: ["**sharenfs", "quota"],
        option: ["canmount=**", "compression=lz4"]
      )
      cli_com = ZfsMgmt.zfs_recv_com(cli_opts, [], { "type" => "filesystem" }, "backup/pool/fs")
      expect(cli_com).to eq([
        "zfs", "recv", "-s",
        "-x", "mountpoint", "-x", "quota",
        "-o", "readonly=on", "-o", "compression=lz4",
        '"backup/pool/fs"'
      ])

      clear_opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        exclude: ["**", "quota"],
        option: ["**", "compression=zstd"]
      )
      clear_com = ZfsMgmt.zfs_recv_com(clear_opts, [], { "type" => "filesystem" }, "backup/pool/fs")
      expect(clear_com).to eq([
        "zfs", "recv", "-s",
        "-x", "quota",
        "-o", "compression=zstd",
        '"backup/pool/fs"'
      ])
    end

    it "merges zfsmgmt:excludes and zfsmgmt:overrides across all precedence layers with ** knockouts" do
      ZfsMgmt.config = ZfsMgmt::Config.from_hash(
        "excludes" => %w[mountpoint sharenfs],
        "overrides" => { "canmount" => "noauto", "readonly" => "on" },
        "zfs" => {
          "tank" => {
            "excludes" => %w[sharesmb],
            "overrides" => { "atime" => "off", "readonly" => "**" }
          },
          "tank/data" => {
            "excludes" => %w[**sharenfs quota],
            "overrides" => { "compression" => "zstd", "canmount" => "**" }
          }
        }
      )

      zfs_props = {
        "type" => "filesystem",
        "zfsmgmt:excludes" => "aclinherit,mountpoint",
        "zfsmgmt:overrides" => "exec=off,atime=on,readonly=off"
      }
      zfs_obj = ZfsMgmt.build_zfs_from_props("tank/data/sub", zfs_props)
      cli_opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        exclude: ["**aclinherit", "recordsize"],
        option: ["readonly=on", "exec=**"]
      )

      com = ZfsMgmt.zfs_recv_com(cli_opts, [], zfs_obj, "backup/tank/data/sub")
      expect(com).to eq([
        "zfs", "recv", "-s",
        "-x", "mountpoint", "-x", "sharesmb", "-x", "quota", "-x", "recordsize",
        "-o", "atime=off", "-o", "compression=zstd", "-o", "readonly=on",
        '"backup/tank/data/sub"'
      ])
    end

    it "filters volume-unsupported properties after merging across ZFS props, config, and CLI" do
      ZfsMgmt.config = ZfsMgmt::Config.from_hash(
        "excludes" => %w[mountpoint],
        "overrides" => { "canmount" => "noauto" },
        "zfs" => {
          "tank/zvol" => {
            "excludes" => %w[quota sharenfs],
            "overrides" => { "readonly" => "on" }
          }
        }
      )

      zfs_obj = ZfsMgmt.build_zfs_from_props("tank/zvol", {
        "type" => "volume",
        "zfsmgmt:excludes" => "sharesmb",
        "zfsmgmt:overrides" => "quota=10G,compression=lz4"
      })
      cli_opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        exclude: ["canmount"],
        option: ["mountpoint=none", "atime=off"]
      )

      com = ZfsMgmt.zfs_recv_com(cli_opts, [], zfs_obj, "backup/tank/zvol")
      expect(com).to eq([
        "zfs", "recv", "-s",
        "-x", "sharesmb", "-x", "sharenfs",
        "-o", "compression=lz4", "-o", "readonly=on", "-o", "atime=off",
        '"backup/tank/zvol"'
      ])
    end
  end

  describe ".merge_excludes and .merge_overrides" do
    it "merges excludes in order, deduplicates, and applies ** knockouts and re-additions" do
      merged = ZfsMgmt.merge_excludes(
        "mountpoint, sharenfs",
        %w[sharenfs quota **mountpoint],
        %w[mountpoint atime]
      )
      expect(merged).to eq(%w[sharenfs quota mountpoint atime])

      cleared = ZfsMgmt.merge_excludes(%w[mountpoint sharenfs], %w[** quota])
      expect(cleared).to eq(%w[quota])
    end

    it "merges overrides in order, preserves = in values, and handles ** knockouts" do
      merged = ZfsMgmt.merge_overrides(
        "sharenfs=rw=10.0.0.0/8,canmount=noauto,readonly=on",
        { "canmount" => "**", "atime" => "off" },
        ["readonly=**", "canmount=off"]
      )
      expect(merged).to eq({
        "sharenfs" => "rw=10.0.0.0/8",
        "atime" => "off",
        "canmount" => "off"
      })

      cleared = ZfsMgmt.merge_overrides({ "a" => "1", "b" => "2" }, ["**", "c=3"])
      expect(cleared).to eq({ "c" => "3" })

      preserved = ZfsMgmt.merge_overrides({ "a" => "1" }, { "a" => "**", "b" => "2" }, strip_knockouts: false)
      expect(preserved).to eq({ "a" => "**", "b" => "2" })
    end
  end

  describe ".custom_properties" do
    it "returns all expected zfsmgmt:* property names" do
      props = ZfsMgmt.custom_properties
      expect(props).to include(
        "zfsmgmt:policy",
        "zfsmgmt:manage",
        "zfsmgmt:strategy",
        "zfsmgmt:minage",
        "zfsmgmt:matchsnaps",
        "zfsmgmt:ignoresnaps",
        "zfsmgmt:prefersnaps",
        "zfsmgmt:snapshot",
        "zfsmgmt:snap_prefix",
        "zfsmgmt:snap_timestamp",
        "zfsmgmt:send",
        "zfsmgmt:remote",
        "zfsmgmt:destination",
        "zfsmgmt:recv_state",
        "zfsmgmt:excludes",
        "zfsmgmt:overrides"
      )
      expect(props.all? { |p| p.start_with?("zfsmgmt:") }).to be(true)
    end
  end

  describe ".timespec_to_seconds" do
    it "converts seconds, minutes, hours, days, weeks, and unitless numbers" do
      expect(ZfsMgmt.timespec_to_seconds("45")).to eq(45)
      expect(ZfsMgmt.timespec_to_seconds("30s")).to eq(30)
      expect(ZfsMgmt.timespec_to_seconds("5m")).to eq(300)
      expect(ZfsMgmt.timespec_to_seconds("2h")).to eq(7200)
      expect(ZfsMgmt.timespec_to_seconds("3d")).to eq(259_200)
      expect(ZfsMgmt.timespec_to_seconds("1w")).to eq(604_800)
      expect(ZfsMgmt.timespec_to_seconds("2D")).to eq(172_800)
    end

    it "raises an error for invalid timespec strings" do
      expect { ZfsMgmt.timespec_to_seconds("abc") }.to raise_error(StandardError)
    end
  end

  describe ".policy_parser" do
    it "parses combined timeframe policy strings case-insensitively" do
      res = ZfsMgmt.policy_parser("24h14d4w12m2y")
      expect(res).to eq({
        "hourly" => 24,
        "daily" => 14,
        "weekly" => 4,
        "monthly" => 12,
        "yearly" => 2
      })
    end

    it "defaults unspecified timeframes to 0" do
      res = ZfsMgmt.policy_parser("7D")
      expect(res).to eq({
        "hourly" => 0,
        "daily" => 7,
        "weekly" => 0,
        "monthly" => 0,
        "yearly" => 0
      })
    end

    it "raises ArgumentError when policy string contains no valid tokens" do
      expect { ZfsMgmt.policy_parser("invalid") }.to raise_error(ArgumentError, /unable to parse the policy configuration/)
    end
  end

  describe "utility helpers" do
    it "quotes strings with .sq and .dq" do
      expect(ZfsMgmt.sq("hello")).to eq("'hello'")
      expect(ZfsMgmt.dq("hello")).to eq('"hello"')
    end

    it "checks boolean property strings with .prop_on? and .prop_off?" do
      expect(ZfsMgmt.prop_on?("true")).to be(true)
      expect(ZfsMgmt.prop_on?("on")).to be(true)
      expect(ZfsMgmt.prop_on?("false")).to be(false)

      expect(ZfsMgmt.prop_off?("false")).to be(true)
      expect(ZfsMgmt.prop_off?("off")).to be(true)
      expect(ZfsMgmt.prop_off?("true")).to be(false)
    end

    it "matches filesystem names with .match_filter?" do
      expect(ZfsMgmt.match_filter?("tank/data/sub", "^tank/data")).to be_truthy
      expect(ZfsMgmt.match_filter?("rpool/ROOT", "^tank/data")).to be_nil
    end

    it "compares hash values against Array, Hash, Method, Regexp, and literal values with .key_comp?" do
      h = {
        "flag" => "on",
        "mode" => "recursive",
        "num" => 42
      }
      expect(ZfsMgmt.key_comp?(h, "missing")).to be(false)
      expect(ZfsMgmt.key_comp?(h, "flag")).to be(true)
      expect(ZfsMgmt.key_comp?(h, "mode", ["local", "recursive"])).to be(true)
      expect(ZfsMgmt.key_comp?(h, "mode", { "recursive" => 1 })).to be(true)
      expect(ZfsMgmt.key_comp?(h, "mode", /^recur/)).to be_truthy
      expect(ZfsMgmt.key_comp?(h, "num", 42)).to be(true)
      expect(ZfsMgmt.key_comp?(h, "num", 99)).to be(false)
    end

    it "sets $logger severity with .set_log_level and falls back to ZFS_MGMT_LOGLEVEL" do
      old_level = $logger.level
      old_env = ENV["ZFS_MGMT_LOGLEVEL"]
      begin
        ZfsMgmt.set_log_level("debug")
        expect($logger.level).to eq(Logger::DEBUG)
        ZfsMgmt.set_log_level("INFO")
        expect($logger.level).to eq(Logger::INFO)
        ZfsMgmt.set_log_level("warn")
        expect($logger.level).to eq(Logger::WARN)
        ZfsMgmt.set_log_level("error")
        expect($logger.level).to eq(Logger::ERROR)
        ZfsMgmt.set_log_level("fatal")
        expect($logger.level).to eq(Logger::FATAL)
        ZfsMgmt.set_log_level("unknown")
        expect($logger.level).to eq(Logger::UNKNOWN)
        ZfsMgmt.set_log_level("3")
        expect($logger.level).to eq(3)

        ENV["ZFS_MGMT_LOGLEVEL"] = "error"
        ZfsMgmt.set_log_level
        expect($logger.level).to eq(Logger::ERROR)

        ENV["ZFS_MGMT_LOGLEVEL"] = "fatal"
        ZfsMgmt.set_log_level(nil)
        expect($logger.level).to eq(Logger::FATAL)
      ensure
        ENV["ZFS_MGMT_LOGLEVEL"] = old_env
        $logger.level = old_level
      end
    end
  end

  describe ".zfsget" do
    before do
      ZfsMgmt.global_options = { zfs_binary: "zfs" }
    end

    it "parses tab-delimited zfs get output and translates integer properties and sources" do
      stdout = [
        "tank/data\tcreation\t1700000000\t-",
        "tank/data\tuserrefs\t2\t-",
        "tank/data\tzfsmgmt:manage\ttrue\tlocal",
        "tank/data\tzfsmgmt:policy\t14d\tinherited from tank"
      ].join("\n") + "\n"
      status = instance_double(Process::Status, signaled?: false, success?: true)

      expect(Open3).to receive(:capture3).with("zfs get -Hp creation,userrefs,zfsmgmt:manage -t filesystem tank/data")
        .and_return([stdout, "", status])

      res = ZfsMgmt.zfsget(
        properties: ["creation", "userrefs", "zfsmgmt:manage"],
        types: ["filesystem"],
        zfs: "tank/data"
      )

      expect(res).to eq({
        "tank/data" => {
          "creation" => 1_700_000_000,
          "userrefs" => 2,
          "zfsmgmt:manage" => "true",
          "zfsmgmt:manage@source" => "local",
          "zfsmgmt:policy" => "14d",
          "zfsmgmt:policy@source" => "inherited from tank"
        }
      })
    end

    it "raises ZfsDatasetNotFoundError without logging an error when dataset does not exist" do
      status = instance_double(Process::Status, signaled?: false, success?: false, exitstatus: 1)
      expect(Open3).to receive(:capture3).and_return(["", "cannot open 'tank/nonexistent': dataset does not exist\n", status])
      expect($logger).not_to receive(:error)

      expect {
        ZfsMgmt.zfsget(zfs: "tank/nonexistent")
      }.to raise_error(ZfsMgmt::ZfsDatasetNotFoundError, /dataset does not exist: tank\/nonexistent/)
    end

    it "raises ZfsGetError and logs an error when command exits non-zero for other failures" do
      status = instance_double(Process::Status, signaled?: false, success?: false, exitstatus: 255)
      expect(Open3).to receive(:capture3).and_return(["out line\n", "ssh: connect to host backup port 22: Connection refused\n", status])
      expect($logger).to receive(:error).with(/failed to execute.*exit status 255/)
      expect($logger).to receive(:error).with("stderr: ssh: connect to host backup port 22: Connection refused")

      expect {
        ZfsMgmt.zfsget(zfs: "tank/data", command_prefix: ["ssh", "backup"])
      }.to raise_error(ZfsMgmt::ZfsGetError, /failed to execute.*exit status 255/)
    end

    it "raises ZfsGetError when process is signaled" do
      status = instance_double(Process::Status, signaled?: true, termsig: 15)
      expect(Open3).to receive(:capture3).and_return(["", "", status])

      expect {
        ZfsMgmt.zfsget(zfs: "tank/data")
      }.to raise_error(ZfsMgmt::ZfsGetError, /process was signalled.*termsig 15/)
    end
  end

  describe ".zfs_managed_list" do
    it "filters filesystems by regex and property_match and returns Zfs instances without eagerly fetching snapshots" do
      allow(ZfsMgmt).to receive(:zfsget).with(properties: ["all"]).and_return({
        "tank/managed" => { "zfsmgmt:manage" => "true", "zfsmgmt:policy" => "7d" },
        "tank/unmanaged" => { "zfsmgmt:manage" => "false" },
        "tank/also_managed" => { "zfsmgmt:manage" => "on" },
        "other/managed" => { "zfsmgmt:manage" => "true" }
      })

      expect(ZfsMgmt).not_to receive(:zfsget).with(hash_including(types: ["snapshot"]))

      res = ZfsMgmt.zfs_managed_list(filter: "^tank/")
      expect(res.length).to eq(2)
      expect(res).to all(be_a(ZfsMgmt::Zfs))
      expect(res.map(&:name)).to eq(["tank/managed", "tank/also_managed"])
      expect(res.first.manage).to eq(true)
      expect(res.first.policy).to eq("7d")
      expect(res.first.snapshots_loaded?).to eq(false)
    end

    it "supports filtering by snapshot: true and send_enabled: true with source resolution" do
      allow(ZfsMgmt).to receive(:zfsget).with(properties: ["all"]).and_return({
        "tank/snap_local" => { "zfsmgmt:snapshot" => "recursive", "zfsmgmt:snapshot@source" => "local" },
        "tank/snap_inherited" => { "zfsmgmt:snapshot" => "recursive", "zfsmgmt:snapshot@source" => "inherited from tank/snap_local" },
        "tank/send_ok" => { "zfsmgmt:send" => "true", "zfsmgmt:send@source" => "local", "zfsmgmt:destination" => "b/p", "zfsmgmt:destination@source" => "local" },
        "tank/send_recv" => { "zfsmgmt:send" => "true", "zfsmgmt:send@source" => "received", "zfsmgmt:destination" => "b/p", "zfsmgmt:destination@source" => "received" }
      })

      snap_list = ZfsMgmt.zfs_managed_list(property_match: { snapshot: true })
      expect(snap_list.map(&:name)).to eq(["tank/snap_local"])
      expect(snap_list.first.snapshot_recursive).to eq(true)

      send_list = ZfsMgmt.zfs_managed_list(property_match: { send_enabled: true })
      expect(send_list.map(&:name)).to eq(["tank/send_ok"])
      expect(send_list.first.destination).to eq("b/p/send_ok")
    end

    it "merges config properties over ZFS user properties and propagates config inheritance" do
      ZfsMgmt.config = ZfsMgmt::Config.from_hash(
        "zfs" => {
          "tank" => {
            "manage" => true,
            "policy" => "30d",
            "destination" => "configpool"
          },
          "tank/child" => {
            "send" => true,
            "remote" => "root@config-remote"
          }
        }
      )

      allow(ZfsMgmt).to receive(:zfsget).with(properties: ["all"]).and_return({
        "tank/child" => {
          "zfsmgmt:manage" => "false",
          "zfsmgmt:policy" => "7d",
          "zfsmgmt:minage" => "2h",
          "zfsmgmt:remote" => "root@zfs-remote"
        }
      })

      res = ZfsMgmt.zfs_managed_list
      expect(res.length).to eq(1)
      child = res.first
      expect(child.name).to eq("tank/child")
      # Config overrides ZFS user properties and inherits from tank in config
      expect(child.manage).to eq(true)
      expect(child.policy).to eq("30d")
      expect(child.send_enabled).to eq(true)
      expect(child.remote).to eq("root@config-remote")
      expect(child.destination).to eq("root@config-remote:configpool/tank/child")
      # ZFS user properties not specified in config are preserved
      expect(child.minage).to eq("2h")
    end
  end

  describe ".config_dir" do
    before do
      ZfsMgmt.global_options = {}
    end

    it "uses /etc/zfsmgr when running as root" do
      allow(Process).to receive(:euid).and_return(0)
      expect(ZfsMgmt.config_dir).to eq("/etc/zfsmgr")
    end

    it "uses XDG_CONFIG_HOME/zfsmgr when set for non-root user" do
      allow(Process).to receive(:euid).and_return(1000)
      allow(ENV).to receive(:[]).and_call_original
      allow(ENV).to receive(:[]).with("XDG_CONFIG_HOME").and_return("/custom/xdg/config")
      expect(ZfsMgmt.config_dir).to eq("/custom/xdg/config/zfsmgr")
    end

    it "defaults to ~/.config/zfsmgr for non-root user when XDG_CONFIG_HOME is unset" do
      allow(Process).to receive(:euid).and_return(1000)
      allow(ENV).to receive(:[]).and_call_original
      allow(ENV).to receive(:[]).with("XDG_CONFIG_HOME").and_return(nil)
      allow(Dir).to receive(:home).and_return("/home/testuser")
      expect(ZfsMgmt.config_dir).to eq("/home/testuser/.config/zfsmgr")
    end

    it "uses global_options[:config_dir] when specified" do
      ZfsMgmt.global_options = { config_dir: "/tmp/custom_zfsmgr" }
      expect(ZfsMgmt.config_dir).to eq("/tmp/custom_zfsmgr")
    end
  end

  describe ".snapshot_destroy_policy" do
    let(:zfs) { "tank/data" }
    # Same day (2026-11-17) at 08:00, 12:00, 16:00, plus previous day (2026-11-16) at 10:00
    let(:t_day2_late)  { Time.new(2026, 11, 17, 16, 0, 0).to_i }
    let(:t_day2_mid)   { Time.new(2026, 11, 17, 12, 0, 0).to_i }
    let(:t_day2_early) { Time.new(2026, 11, 17, 8, 0, 0).to_i }
    let(:t_day1)       { Time.new(2026, 11, 16, 10, 0, 0).to_i }

    let(:snaps) do
      {
        "tank/data@auto-3" => { "creation" => t_day2_late },
        "tank/data@pref-2" => { "creation" => t_day2_mid },
        "tank/data@auto-1" => { "creation" => t_day2_early },
        "tank/data@day1"   => { "creation" => t_day1 }
      }
    end

    it "selects oldest snapshot in period by default and never deletes the newest overall snapshot" do
      props = { "zfsmgmt:policy" => "1d" }
      saved, saved_snaps, deleteme = ZfsMgmt.snapshot_destroy_policy(zfs, props, snaps)

      expect(saved["daily"]["2026-11-17"]).to eq("tank/data@auto-1")
      expect(saved_snaps).to eq(["tank/data@auto-1"])
      # auto-3 is the very newest snapshot overall, so it is protected from deletion
      expect(deleteme).to contain_exactly("tank/data@pref-2", "tank/data@day1")
    end

    it "selects youngest snapshot in period when strategy is youngest" do
      props = {
        "zfsmgmt:policy" => "1d",
        "zfsmgmt:strategy" => "youngest"
      }
      saved, saved_snaps, deleteme = ZfsMgmt.snapshot_destroy_policy(zfs, props, snaps)

      expect(saved["daily"]["2026-11-17"]).to eq("tank/data@auto-3")
      expect(saved_snaps).to eq(["tank/data@auto-3"])
      expect(deleteme).to contain_exactly("tank/data@pref-2", "tank/data@auto-1", "tank/data@day1")
    end

    it "prefers snapshots matching zfsmgmt:prefersnaps even under youngest strategy" do
      props = {
        "zfsmgmt:policy" => "1d",
        "zfsmgmt:strategy" => "youngest",
        "zfsmgmt:prefersnaps" => "^pref-"
      }
      saved, _saved_snaps, _deleteme = ZfsMgmt.snapshot_destroy_policy(zfs, props, snaps)
      expect(saved["daily"]["2026-11-17"]).to eq("tank/data@pref-2")
    end

    it "skips snapshots matching zfsmgmt:ignoresnaps from both policy saving and deletion" do
      props = {
        "zfsmgmt:policy" => "1d",
        "zfsmgmt:ignoresnaps" => "^day1|^auto-1"
      }
      saved, _saved_snaps, deleteme = ZfsMgmt.snapshot_destroy_policy(zfs, props, snaps)
      expect(saved["daily"]["2026-11-17"]).to eq("tank/data@pref-2")
      expect(deleteme).to be_empty
    end

    it "only considers snapshots matching zfsmgmt:matchsnaps for policy saving" do
      props = {
        "zfsmgmt:policy" => "1d",
        "zfsmgmt:matchsnaps" => "^pref-"
      }
      saved, _saved_snaps, deleteme = ZfsMgmt.snapshot_destroy_policy(zfs, props, snaps)
      expect(saved["daily"]["2026-11-17"]).to eq("tank/data@pref-2")
      expect(deleteme).to contain_exactly("tank/data@auto-1", "tank/data@day1")
    end

    it "protects snapshots younger than zfsmgmt:minage from deletion" do
      allow(Time).to receive(:now).and_return(Time.at(t_day2_late + 600))
      props = {
        "zfsmgmt:policy" => "1d",
        "zfsmgmt:minage" => "6h"
      }
      _saved, _saved_snaps, deleteme = ZfsMgmt.snapshot_destroy_policy(zfs, props, snaps)
      # pref-2 is only 4h10m old (< 6h minage), so only day1 is deleted
      expect(deleteme).to eq(["tank/data@day1"])
    end
  end

  describe ".snapshot_destroy" do
    before do
      ZfsMgmt.global_options = { zfs_binary: "zfs" }
    end

    it "builds a batched zfs destroy command with oldest snapshots first and respects noop and verbose" do
      snaps = {
        "tank/data@s3" => { "creation" => Time.new(2026, 11, 17, 12, 0, 0).to_i },
        "tank/data@s2" => { "creation" => Time.new(2026, 11, 16, 12, 0, 0).to_i },
        "tank/data@s1" => { "creation" => Time.new(2026, 11, 15, 12, 0, 0).to_i }
      }
      zfs_obj = ZfsMgmt::Zfs.new(name: "tank/data", manage: true, policy: "1d", snapshots: snaps)
      allow(ZfsMgmt).to receive(:zfs_managed_list).and_return([zfs_obj])

      expect(ZfsMgmt).to receive(:system_com).with(["zfs", "destroy", "-d", "-n", "-v", "tank/data@s1,s2"])
      ZfsMgmt.snapshot_destroy(noop: true, verbose: true, filter: "^tank/")
    end

    it "skips filesystems with missing or invalid zfsmgmt:policy without fetching snapshots for missing policy" do
      snaps = { "tank/badpolicy@s1" => { "creation" => 1000 } }
      nopolicy = ZfsMgmt::Zfs.new(name: "tank/nopolicy", manage: true)
      badpolicy = ZfsMgmt::Zfs.new(name: "tank/badpolicy", manage: true, policy: "bogus", snapshots: snaps)
      allow(ZfsMgmt).to receive(:zfs_managed_list).and_return([nopolicy, badpolicy])

      expect(ZfsMgmt).not_to receive(:zfsget)
      expect(ZfsMgmt).not_to receive(:system_com)
      ZfsMgmt.snapshot_destroy
      expect(nopolicy.snapshots_loaded?).to eq(false)
    end
  end

  describe ".snapshot_create" do
    before do
      ZfsMgmt.global_options = { "zfs_binary" => "zfs" }
    end

    it "creates snapshots for filesystems with zfsmgmt:snapshot enabled, including recursive and custom prefix/timestamp" do
      fixed_dt = DateTime.parse("2026-11-18T10:20:30+00:00")
      allow(DateTime).to receive(:now).and_return(fixed_dt)

      allow(ZfsMgmt).to receive(:zfsget).and_return({
        "tank/simple" => {
          "zfsmgmt:snapshot" => "true"
        },
        "tank/rec" => {
          "zfsmgmt:snapshot" => "recursive",
          "zfsmgmt:snapshot@source" => "local",
          "zfsmgmt:snap_prefix" => "hourly",
          "zfsmgmt:snap_timestamp" => "%F"
        },
        "tank/rec_inherited" => {
          "zfsmgmt:snapshot" => "recursive",
          "zfsmgmt:snapshot@source" => "inherited from tank/rec"
        },
        "tank/off" => {
          "zfsmgmt:snapshot" => "false"
        }
      })

      expect(ZfsMgmt).to receive(:system_com).with(
        ["zfs", "snapshot", "tank/simple@zfsmgmt-#{fixed_dt.strftime('%FT%T%z')}"],
        true
      )
      expect(ZfsMgmt).to receive(:system_com).with(
        ["zfs", "snapshot", "-r", "tank/rec@hourly-2026-11-18"],
        true
      )

      ZfsMgmt.snapshot_create(noop: true, filter: "^tank/")
    end
  end

  describe ".zfs_send_com, .mbuffer_command, and .pv_command" do
    before do
      ZfsMgmt.global_options = {
        zfs_binary: "zfs",
        mbuffer_binary: "mbuffer"
      }
    end

    it "constructs zfs_send_com with CLI and property flags" do
      opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        verbose: "send",
        raw: true,
        props: false
      )
      props = {
        "zfsmgmt:send_props" => "true",
        "zfsmgmt:send_holds" => "on",
        "zfsmgmt:send_compressed" => "true"
      }
      com = ZfsMgmt.zfs_send_com(opts, props, ["-i", '"@snap1"'], "tank/fs@snap2")
      expect(com).to eq(["zfs", "send", "-v", "-P", "-c", "-h", "-w", "-i", '"@snap1"', '"tank/fs@snap2"', "|"])
    end

    it "constructs mbuffer_command with quiet and size flags" do
      expect(ZfsMgmt.mbuffer_command({ mbuffer_size: "1G" })).to eq(["mbuffer", "-q", "-m", "1G", "|"])
      expect(ZfsMgmt.mbuffer_command({ verbose: "mbuffer" })).to eq(["mbuffer", "|"])
    end

    it "constructs pv_command with and without size estimate" do
      expect(ZfsMgmt.pv_command({ pv_binary: "pv" }, 123_456)).to eq(["pv", "-prb", "-e", "-s", 123_456, "|"])
      expect(ZfsMgmt.pv_command({ pv_binary: "pv" }, nil)).to eq(["pv", "-prb", "|"])
    end
  end

  describe ".zfs_send additional scenarios" do
    let(:zfs) { "tank/sub/fs" }
    let(:snaps) do
      {
        "tank/sub/fs@snap1" => { "creation" => 1000 },
        "tank/sub/fs@snap2" => { "creation" => 2000 }
      }
    end

    around do |example|
      Dir.mktmpdir do |tmpdir|
        ZfsMgmt.global_options = {
          state_db: File.join(tmpdir, "send_state.pstore"),
          zfs_binary: "zfs",
          mbuffer_binary: "mbuffer"
        }
        example.run
      end
    end

    it "sends initial snapshot and incremental update when destination is missing and destination is inherited" do
      props = {
        "zfsmgmt:destination" => "backup/dest",
        "zfsmgmt:destination@source" => "inherited from tank",
        "zfsmgmt:send_intermediary" => "true"
      }
      # Inherited destination path: File.join("backup/dest", "tank", "/sub/fs") -> "backup/dest/tank/sub/fs"
      dest = "backup/dest/tank/sub/fs"

      expect(ZfsMgmt).to receive(:zfsget).with(zfs: dest, command_prefix: [])
        .and_raise(ZfsMgmt::ZfsDatasetNotFoundError.new("dataset does not exist: #{dest}"))

      status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)
      # First system_com: initial send of oldest snapshot (snap1)
      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", '"tank/sub/fs@snap1"', "|",
        "zfs", "recv", "-s", "\"#{dest}\""
      ]).ordered.and_return(status_ok)

      # Then zfs_send checks remote snapshots and sends incremental (-I) to snap2
      expect(ZfsMgmt).to receive(:zfsget).with(
        zfs: dest,
        types: ["snapshot"],
        command_prefix: [],
        properties: ["creation", "userrefs"]
      ).and_return({
        "#{dest}@snap1" => { "creation" => 1000, "userrefs" => 0 }
      })

      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", "-I", '"@snap1"', '"tank/sub/fs@snap2"', "|",
        "zfs", "recv", "-s", "\"#{dest}\""
      ]).ordered.and_return(status_ok)

      ZfsMgmt.zfs_send({}, zfs, props, snaps)
    end

    it "logs an error and aborts send when checking destination fails with a non-missing ZfsGetError" do
      props = {
        "zfsmgmt:destination" => "backup/dest",
        "zfsmgmt:destination@source" => "local"
      }
      dest = "backup/dest/fs"

      expect(ZfsMgmt).to receive(:zfsget).with(zfs: dest, command_prefix: [])
        .and_raise(ZfsMgmt::ZfsGetError.new("ssh connection failed"))
      expect($logger).to receive(:error).with("unable to check remote filesystem #{dest}")
      expect(ZfsMgmt).not_to receive(:system_com)

      ZfsMgmt.zfs_send({}, zfs, props, snaps)
    end

    it "resumes an interrupted receive when receive_resume_token is present on destination" do
      props = {
        "zfsmgmt:destination" => "backup/pool",
        "zfsmgmt:destination@source" => "local"
      }
      dest = "backup/pool/fs"

      expect(ZfsMgmt).to receive(:zfsget).with(zfs: dest, command_prefix: []).and_return({
        dest => { "receive_resume_token" => "1-abc-token" }
      })

      status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)
      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", "-t", "1-abc-token", "|",
        "zfs", "recv", "-s", "\"#{dest}\""
      ]).and_return(status_ok)

      expect(ZfsMgmt).to receive(:zfsget).with(
        zfs: dest,
        types: ["snapshot"],
        command_prefix: [],
        properties: ["creation", "userrefs"]
      ).and_return({
        "#{dest}@snap2" => { "creation" => 2000, "userrefs" => 0 }
      })

      ZfsMgmt.zfs_send({}, zfs, props, snaps)
    end

    it "uses base destination for zfs recv -d and strips first path element for zfsget checks when discard_first is enabled" do
      props = {
        "zfsmgmt:destination" => "backup/dest",
        "zfsmgmt:destination@source" => "inherited from tank",
        "zfsmgmt:recv_discard_first" => "true"
      }
      check_dest = "backup/dest/sub/fs"
      recv_target = "backup/dest"

      expect(ZfsMgmt).to receive(:zfsget).with(zfs: check_dest, command_prefix: [])
        .and_raise(ZfsMgmt::ZfsDatasetNotFoundError.new("dataset does not exist: #{check_dest}"))

      status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)
      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", '"tank/sub/fs@snap1"', "|",
        "zfs", "recv", "-s", "-d", "\"#{recv_target}\""
      ]).ordered.and_return(status_ok)

      expect(ZfsMgmt).to receive(:zfsget).with(
        zfs: check_dest,
        types: ["snapshot"],
        command_prefix: [],
        properties: ["creation", "userrefs"]
      ).and_return({
        "#{check_dest}@snap1" => { "creation" => 1000, "userrefs" => 0 }
      })

      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", "-i", '"@snap1"', '"tank/sub/fs@snap2"', "|",
        "zfs", "recv", "-s", "-d", "\"#{recv_target}\""
      ]).ordered.and_return(status_ok)

      ZfsMgmt.zfs_send({}, zfs, props, snaps)
    end

    it "allows CLI discard_first: false to override zfsmgmt:recv_discard_first=true" do
      props = {
        "zfsmgmt:destination" => "backup/dest",
        "zfsmgmt:destination@source" => "inherited from tank",
        "zfsmgmt:recv_discard_first" => "true"
      }
      dest = "backup/dest/tank/sub/fs"
      opts = Thor::CoreExt::HashWithIndifferentAccess.new(discard_first: false)

      expect(ZfsMgmt).to receive(:zfsget).with(zfs: dest, command_prefix: []).and_return({
        dest => {}
      })
      expect(ZfsMgmt).to receive(:zfsget).with(
        zfs: dest,
        types: ["snapshot"],
        command_prefix: [],
        properties: ["creation", "userrefs"]
      ).and_return({
        "#{dest}@snap2" => { "creation" => 2000, "userrefs" => 0 }
      })

      ZfsMgmt.zfs_send(opts, zfs, props, snaps)
    end

    it "enforces CLI > config > ZFS user property precedence in .zfs_send" do
      ZfsMgmt.config = ZfsMgmt::Config.from_hash(
        "excludes" => ["mountpoint"],
        "overrides" => { "readonly" => "on" },
        "zfs" => {
          "tank/sub/fs" => {
            "destination" => "config/dest",
            "remote" => "config@host",
            "send_intermediary" => true,
            "recv_unmount" => true
          }
        }
      )

      props = {
        "zfsmgmt:destination" => "zfs/dest",
        "zfsmgmt:destination@source" => "local",
        "zfsmgmt:remote" => "zfs@host",
        "zfsmgmt:send_intermediary" => "false",
        "zfsmgmt:send_compressed" => "true"
      }

      # CLI overrides remote and intermediary, while config overrides destination & unmount,
      # and ZFS property supplies send_compressed (since config didn't set it).
      opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        remote: "cli@host",
        intermediary: false
      )
      dest = "config/dest/fs"

      expect(ZfsMgmt).to receive(:zfsget).with(zfs: dest, command_prefix: ["ssh", "cli@host"]).and_return({
        dest => {}
      })
      expect(ZfsMgmt).to receive(:zfsget).with(
        zfs: dest,
        types: ["snapshot"],
        command_prefix: ["ssh", "cli@host"],
        properties: ["creation", "userrefs"]
      ).and_return({
        "#{dest}@snap1" => { "creation" => 1000, "userrefs" => 0 }
      })

      status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)
      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", "-c", "-i", '"@snap1"', '"tank/sub/fs@snap2"', "|",
        "ssh", "cli@host",
        "'zfs recv -s -u -x mountpoint -o readonly=on \"#{dest}\"'"
      ]).and_return(status_ok)

      ZfsMgmt.zfs_send(opts, zfs, props, snaps)
    end

    it "applies merged per-zfs excludes and overrides with ** knockouts in .zfs_send" do
      ZfsMgmt.config = ZfsMgmt::Config.from_hash(
        "excludes" => %w[mountpoint sharenfs],
        "overrides" => { "canmount" => "noauto", "readonly" => "on" },
        "zfs" => {
          "tank/sub/fs" => {
            "destination" => "config/dest",
            "excludes" => %w[**sharenfs quota],
            "overrides" => { "canmount" => "**", "compression" => "zstd" }
          }
        }
      )

      props = {
        "type" => "filesystem",
        "zfsmgmt:excludes" => "sharesmb",
        "zfsmgmt:overrides" => "atime=off"
      }
      dest = "config/dest/fs"

      expect(ZfsMgmt).to receive(:zfsget).with(zfs: dest, command_prefix: []).and_return({ dest => {} })
      expect(ZfsMgmt).to receive(:zfsget).with(
        zfs: dest,
        types: ["snapshot"],
        command_prefix: [],
        properties: ["creation", "userrefs"]
      ).and_return({
        "#{dest}@snap1" => { "creation" => 1000, "userrefs" => 0 }
      })

      opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        option: ["atime=**"]
      )

      status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)
      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", "-i", '"@snap1"', '"tank/sub/fs@snap2"', "|",
        "zfs", "recv", "-s",
        "-x", "sharesmb", "-x", "mountpoint", "-x", "quota",
        "-o", "readonly=on", "-o", "compression=zstd",
        "\"#{dest}\""
      ]).and_return(status_ok)

      ZfsMgmt.zfs_send(opts, zfs, props, snaps)
    end

    it "supports hostname:path and user@hostname:path destinations in .zfs_send without remote option" do
      props = {
        "zfsmgmt:destination" => "zfs_recv@drhost:backup/pool",
        "zfsmgmt:destination@source" => "local"
      }
      dest = "backup/pool/fs"

      expect($logger).not_to receive(:warn).with(/remote option is deprecated/)
      expect(ZfsMgmt).to receive(:zfsget).with(zfs: dest, command_prefix: ["ssh", "zfs_recv@drhost"]).and_return({
        dest => {}
      })
      expect(ZfsMgmt).to receive(:zfsget).with(
        zfs: dest,
        types: ["snapshot"],
        command_prefix: ["ssh", "zfs_recv@drhost"],
        properties: ["creation", "userrefs"]
      ).and_return({
        "#{dest}@snap1" => { "creation" => 1000, "userrefs" => 0 }
      })

      status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)
      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", "-i", '"@snap1"', '"tank/sub/fs@snap2"', "|",
        "ssh", "zfs_recv@drhost",
        "'zfs recv -s \"#{dest}\"'"
      ]).and_return(status_ok)

      ZfsMgmt.zfs_send({}, zfs, props, snaps)
    end

    it "supports [user@]host:destination with discard_first and CLI destination overrides" do
      props = {
        "zfsmgmt:destination" => "drhost:backup/dest",
        "zfsmgmt:destination@source" => "inherited from tank",
        "zfsmgmt:recv_discard_first" => "true",
        "zfsmgmt:remote" => "ignored@oldhost"
      }
      check_dest = "other/dest/sub/fs"
      recv_target = "other/dest"
      opts = Thor::CoreExt::HashWithIndifferentAccess.new(destination: "cli@newhost:other/dest")

      expect($logger).to receive(:warn).with(/remote option is deprecated/)
      expect(ZfsMgmt).to receive(:zfsget).with(zfs: check_dest, command_prefix: ["ssh", "cli@newhost"])
        .and_raise(ZfsMgmt::ZfsDatasetNotFoundError.new("dataset does not exist: #{check_dest}"))

      status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)
      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", '"tank/sub/fs@snap1"', "|",
        "ssh", "cli@newhost",
        "'zfs recv -s -d \"#{recv_target}\"'"
      ]).ordered.and_return(status_ok)

      expect(ZfsMgmt).to receive(:zfsget).with(
        zfs: check_dest,
        types: ["snapshot"],
        command_prefix: ["ssh", "cli@newhost"],
        properties: ["creation", "userrefs"]
      ).and_return({
        "#{check_dest}@snap1" => { "creation" => 1000, "userrefs" => 0 }
      })

      expect(ZfsMgmt).to receive(:system_com).with([
        "zfs", "send", "-i", '"@snap1"', '"tank/sub/fs@snap2"', "|",
        "ssh", "cli@newhost",
        "'zfs recv -s -d \"#{recv_target}\"'"
      ]).ordered.and_return(status_ok)

      ZfsMgmt.zfs_send(opts, zfs, props, snaps)
    end

    it "does not let deprecated remote override an explicit host in destination" do
      props = {
        "zfsmgmt:destination" => "explicit@host:backup/pool",
        "zfsmgmt:destination@source" => "local",
        "zfsmgmt:remote" => "deprecated@host"
      }
      opts = Thor::CoreExt::HashWithIndifferentAccess.new(remote: "cli@host")
      dest = "backup/pool/fs"

      expect($logger).to receive(:warn).with(/remote option is deprecated/).at_least(:once)
      expect(ZfsMgmt).to receive(:zfsget).with(zfs: dest, command_prefix: ["ssh", "explicit@host"]).and_return({
        dest => {}
      })
      expect(ZfsMgmt).to receive(:zfsget).with(
        zfs: dest,
        types: ["snapshot"],
        command_prefix: ["ssh", "explicit@host"],
        properties: ["creation", "userrefs"]
      ).and_return({
        "#{dest}@snap2" => { "creation" => 2000, "userrefs" => 0 }
      })

      ZfsMgmt.zfs_send(opts, zfs, props, snaps)
    end
  end

  describe ".parse_destination and .format_destination" do
    it "parses local, hostname:path, and user@hostname:path destinations" do
      expect(ZfsMgmt.parse_destination(nil)).to eq([nil, nil])
      expect(ZfsMgmt.parse_destination("zpool/zfs/zfs2/etc")).to eq([nil, "zpool/zfs/zfs2/etc"])
      expect(ZfsMgmt.parse_destination("hostname:zpool/zfs/zfs2/etc")).to eq(["hostname", "zpool/zfs/zfs2/etc"])
      expect(ZfsMgmt.parse_destination("user@hostname:zpool/zfs/zfs2/etc")).to eq(["user@hostname", "zpool/zfs/zfs2/etc"])
    end

    it "formats destinations with optional remote prefix" do
      expect(ZfsMgmt.format_destination(nil, "zpool/ds")).to eq("zpool/ds")
      expect(ZfsMgmt.format_destination("", "zpool/ds")).to eq("zpool/ds")
      expect(ZfsMgmt.format_destination("host", "zpool/ds")).to eq("host:zpool/ds")
      expect(ZfsMgmt.format_destination("user@host", "zpool/ds")).to eq("user@host:zpool/ds")
    end
  end

  describe ".zfs_send_all" do
    it "skips received filesystems, non-local replicated descendants, and filesystems without destination" do
      snaps = { "tank/valid@s1" => { "creation" => 1000 } }
      received = ZfsMgmt::Zfs.from_zfs_props("tank/received", {
        "zfsmgmt:send" => "true", "zfsmgmt:send@source" => "received",
        "zfsmgmt:destination" => "b/p", "zfsmgmt:destination@source" => "received"
      })
      rep_child = ZfsMgmt::Zfs.from_zfs_props("tank/rep_child", {
        "zfsmgmt:send" => "true", "zfsmgmt:send@source" => "inherited from tank",
        "zfsmgmt:send_replicate" => "true", "zfsmgmt:send_replicate@source" => "inherited from tank",
        "zfsmgmt:destination" => "b/p", "zfsmgmt:destination@source" => "inherited from tank"
      })
      nodest = ZfsMgmt::Zfs.from_zfs_props("tank/nodest", {
        "zfsmgmt:send" => "true", "zfsmgmt:send@source" => "local"
      })
      valid = ZfsMgmt::Zfs.from_zfs_props("tank/valid", {
        "zfsmgmt:send" => "true", "zfsmgmt:send@source" => "local",
        "zfsmgmt:destination" => "b/p", "zfsmgmt:destination@source" => "local"
      })
      valid.snapshots = snaps

      allow(ZfsMgmt).to receive(:zfs_managed_list).and_return([received, rep_child, nodest, valid])

      expect(ZfsMgmt).not_to receive(:zfsget)
      expect(ZfsMgmt).to receive(:zfs_send).once.with({ filter: ".+" }, valid)

      ZfsMgmt.zfs_send_all({ filter: ".+" })
      expect(received.snapshots_loaded?).to eq(false)
      expect(rep_child.snapshots_loaded?).to eq(false)
      expect(nodest.snapshots_loaded?).to eq(false)
    end
  end

  describe ".lock and .unlock" do
    it "returns nil when lock option is false" do
      expect(ZfsMgmt.lock({ lock: false })).to be_nil
    end

    it "acquires and releases an exclusive file lock" do
      Dir.mktmpdir do |tmpdir|
        lock_file = File.join(tmpdir, "zfsmgr.lock")
        lock = ZfsMgmt.lock({ lock: true, lock_file: lock_file, lock_wait: 2 })
        expect(lock).to be_a(File)
        expect(File.exist?(lock_file)).to be(true)
        ZfsMgmt.unlock(lock)
        expect(lock.closed?).to be(true)
      end
    end

    it "raises Errno::ENOENT when lock file directory does not exist" do
      expect {
        ZfsMgmt.lock({ lock: true, lock_file: "/nonexistent_dir_12345/zfsmgr.lock", lock_wait: 1 })
      }.to raise_error(Errno::ENOENT)
    end
  end

  describe ZfsMgmt::Restic do
    describe ".valid_chain" do
      let(:snap_l0) do
        {
          "zfsmgmt:snapshot" => "tank/fs@s1",
          "zfsmgmt:level" => 0
        }
      end
      let(:snap_l1) do
        {
          "zfsmgmt:snapshot" => "tank/fs@s2",
          "zfsmgmt:parent" => "tank/fs@s1",
          "zfsmgmt:level" => 1
        }
      end
      let(:snap_broken) do
        {
          "zfsmgmt:snapshot" => "tank/fs@s3",
          "zfsmgmt:parent" => "tank/fs@missing",
          "zfsmgmt:level" => 2
        }
      end
      let(:index) do
        {
          "tank/fs@s1" => snap_l0,
          "tank/fs@s2" => snap_l1,
          "tank/fs@s3" => snap_broken
        }
      end

      it "returns the full chain when all parents exist down to level 0" do
        chain = ZfsMgmt::Restic.valid_chain(snap_l1, [snap_l0, snap_l1], index, [])
        expect(chain).to eq([snap_l1, snap_l0])
      end

      it "returns an empty array when a parent in the chain is missing" do
        chain = ZfsMgmt::Restic.valid_chain(snap_broken, [snap_l0, snap_l1, snap_broken], index, [])
        expect(chain).to eq([])
      end
    end

    describe ".backup" do
      before do
        ZfsMgmt.global_options = { "zfs_binary" => "zfs" }
      end

      it "performs an incremental restic backup, sets hold on current snapshot, and releases obsolete holds" do
        zfs_snapshots = {
          "tank/fs@old_held" => { "creation" => 1000, "userrefs" => 1 },
          "tank/fs@s1"       => { "creation" => 2000, "userrefs" => 1 },
          "tank/fs@s2"       => { "creation" => 3000, "userrefs" => 0 }
        }
        zfs_obj = ZfsMgmt::Zfs.new(
          name: "tank/fs",
          restic_backup: true,
          restic_repository: "/srv/restic-repo",
          snapshots: zfs_snapshots
        )

        allow(ZfsMgmt).to receive(:zfs_managed_list).and_return([zfs_obj])

        restic_snap_s1 = {
          "time" => "2026-11-16T12:00:00Z",
          "date_time" => DateTime.parse("2026-11-16T12:00:00Z"),
          "zfsmgmt:zfs" => "tank/fs",
          "zfsmgmt:snapshot" => "tank/fs@s1",
          "zfsmgmt:level" => 0
        }
        allow(ZfsMgmt::Restic).to receive(:restic_snapshots).and_return([
          [restic_snap_s1],
          { "tank/fs@s1" => restic_snap_s1 }
        ])

        expect(ZfsMgmt).to receive(:zfs_holds).with("tank/fs@s2").and_return([])
        expect(ZfsMgmt).to receive(:zfs_hold).with("zfsmgmt_restic", "tank/fs@s2")
        expect(ZfsMgmt).to receive(:system_com) do |com|
          expect(com).to include("zfs", "send", "-w", "-h", "-p", "-i", "tank/fs@s1", "tank/fs@s2")
          expect(com).to include("restic", "backup", "--stdin", "--stdin-filename", "tank/fs")
          expect(com).to include("--repo", "/srv/restic-repo")
          expect(com).to include('"zfsmgmt:level=1"', '"zfsmgmt:parent=tank/fs@s1"')
        end
        expect(ZfsMgmt).to receive(:zfs_holds).with("tank/fs@old_held").and_return(["zfsmgmt_restic"])
        expect(ZfsMgmt).to receive(:zfs_release).with("zfsmgmt_restic", "tank/fs@old_held")

        opts = Thor::CoreExt::HashWithIndifferentAccess.new(
          filter: ".+",
          restic_binary: "restic",
          buffer: "256m",
          verbose: 1
        )
        ZfsMgmt::Restic.backup(backup_level: 2, options: opts)
      end

      it "skips backup when latest snapshot already exists in restic index" do
        zfs_snapshots = { "tank/fs@s1" => { "creation" => 1000, "userrefs" => 1 } }
        zfs_obj = ZfsMgmt::Zfs.new(name: "tank/fs", restic_backup: true, snapshots: zfs_snapshots)
        allow(ZfsMgmt).to receive(:zfs_managed_list).and_return([zfs_obj])

        restic_snap_s1 = { "zfsmgmt:snapshot" => "tank/fs@s1", "zfsmgmt:level" => 0 }
        allow(ZfsMgmt::Restic).to receive(:restic_snapshots).and_return([
          [restic_snap_s1],
          { "tank/fs@s1" => restic_snap_s1 }
        ])

        expect(ZfsMgmt).not_to receive(:system_com)
        ZfsMgmt::Restic.backup(backup_level: 2, options: { "filter" => ".+" })
      end
    end
  end

  describe ZfsMgmt::ZfsMgr::List do
    let(:now) { Time.new(2026, 11, 18, 12, 0, 0).to_i }

    before do
      allow(Time).to receive(:now).and_return(Time.at(now))
    end

    describe "#stale" do
      let(:stale_time) { now - (3 * 86_400) } # 3 days ago
      let(:fresh_time) { now - 3600 }         # 1 hour ago

      before do
        allow(ZfsMgmt).to receive(:zfs_managed_list).and_return([
          ZfsMgmt::Zfs.new(name: "tank/stale", snapshots: { "tank/stale@oldsnap" => { "creation" => stale_time } }),
          ZfsMgmt::Zfs.new(name: "tank/fresh", snapshots: { "tank/fresh@newsnap" => { "creation" => fresh_time } })
        ])
      end

      it "prints a table of filesystems whose newest snapshot is older than the threshold" do
        output = capture_stdout do
          described_class.start(["stale", "--age", "1d", "--format", "table"])
        end
        expect(output).to include("tank/stale")
        expect(output).to include("oldsnap")
        expect(output).not_to include("tank/fresh")
      end

      it "prints tab-separated output when --format tab is given" do
        output = capture_stdout do
          described_class.start(["stale", "--age", "1d", "--format", "tab"])
        end
        expect(output).to eq("tank/stale\toldsnap\t#{Time.at(stale_time)}\n")
      end
    end

    describe "#holds" do
      before do
        allow(ZfsMgmt).to receive(:zfs_managed_list).and_return([
          ZfsMgmt::Zfs.new(
            name: "tank/fs",
            snapshots: {
              "tank/fs@s1" => { "creation" => 1000, "userrefs" => 0 },
              "tank/fs@s2" => { "creation" => 2000, "userrefs" => 2 }
            }
          )
        ])
        allow(ZfsMgmt).to receive(:zfs_holds).with("tank/fs@s2").and_return(["holdA", "holdB"])
      end

      it "prints a table of held snapshots by default" do
        output = capture_stdout do
          described_class.start(["holds"])
        end
        expect(output).to include("tank/fs@s2")
        expect(output).to include("holdA,holdB")
        expect(output).not_to include("tank/fs@s1")
      end

      it "prints zfs release commands when --format release is specified" do
        output = capture_stdout do
          described_class.start(["holds", "--format", "release"])
        end
        expect(output).to eq("zfs release holdA tank/fs@s2\nzfs release holdB tank/fs@s2\n")
      end
    end
  end

  describe ZfsMgmt::ZfsMgr::Backup do
    it "dispatches incremental, differential, and full backup commands with appropriate backup levels" do
      expect(ZfsMgmt::Restic).to receive(:backup).with(backup_level: 3, options: hash_including("level" => 3))
      described_class.start(["incremental", "--level", "3"])

      expect(ZfsMgmt::Restic).to receive(:backup).with(backup_level: 1, options: anything)
      described_class.start(["differential"])

      expect(ZfsMgmt::Restic).to receive(:backup).with(backup_level: 0, options: anything)
      described_class.start(["full"])
    end
  end

  describe "additional ZfsMgmt::ZfsMgr::Snapshot CLI commands" do
    before do
      allow(ZfsMgmt).to receive(:lock).and_return(nil)
      allow(ZfsMgmt).to receive(:unlock)
    end

    it "dispatches `snapshot destroy --noop --verbose` to ZfsMgmt.snapshot_destroy" do
      expect(ZfsMgmt).to receive(:snapshot_destroy).with(noop: true, verbose: true, filter: "^tank/")
      ZfsMgmt::ZfsMgr::Snapshot.start(["destroy", "--noop", "--verbose", "--filter", "^tank/"])
    end

    it "dispatches `snapshot create --noop` to ZfsMgmt.snapshot_create" do
      expect(ZfsMgmt).to receive(:snapshot_create).with(noop: true, filter: ".+")
      ZfsMgmt::ZfsMgr::Snapshot.start(["create", "--noop"])
    end
  end

  describe ".zfs_holds, .zfs_hold, and .zfs_release" do
    before do
      ZfsMgmt.global_options = { "zfs_binary" => "zfs" }
    end

    it "parses tab-separated hold names from zfs holds -H output" do
      allow(ZfsMgmt).to receive(:`).with("zfs holds -H tank/fs@s1")
        .and_return("tank/fs@s1\thold1\tWed Nov 18 12:00 2026\ntank/fs@s1\thold2\tWed Nov 18 12:01 2026\n")
      system("true")

      expect(ZfsMgmt.zfs_holds("tank/fs@s1")).to eq(["hold1", "hold2"])
    end

    it "invokes system_com with zfs hold and zfs release" do
      expect(ZfsMgmt).to receive(:system_com).with(["zfs", "hold", "mytag", "tank/fs@s1"]) { system("true") }
      ZfsMgmt.zfs_hold("mytag", "tank/fs@s1")

      expect(ZfsMgmt).to receive(:system_com).with(["zfs", "release", "mytag", "tank/fs@s1"]) { system("true") }
      ZfsMgmt.zfs_release("mytag", "tank/fs@s1")
    end
  end

  describe "ZfsMgmt::Restic.restic_snapshots" do
    it "parses restic snapshots JSON output and indexes snapshots by zfsmgmt:snapshot tag" do
      json_out = JSON.generate([
        {
          "id" => "abc12345",
          "time" => "2026-11-17T12:00:00Z",
          "tags" => [
            "zfsmgmt",
            "zfsmgmt:zfs=tank/fs",
            "zfsmgmt:snapshot=tank/fs@s1",
            "zfsmgmt:level=0"
          ]
        }
      ])

      opts = Thor::CoreExt::HashWithIndifferentAccess.new(
        restic_binary: "restic",
        password_file: "/etc/restic.pw",
        repo: "/srv/restic"
      )

      expect(ZfsMgmt::Restic).to receive(:`)
        .with("restic snapshots --json --tag zfsmgmt --path /tank/fs -p /etc/restic.pw --repo /srv/restic")
        .and_return(json_out)
      system("true")

      snaps, index = ZfsMgmt::Restic.restic_snapshots("tank/fs", opts, {})
      expect(snaps.length).to eq(1)
      expect(snaps.first["zfsmgmt:level"]).to eq(0)
      expect(snaps.first["zfsmgmt:zfs"]).to eq("tank/fs")
      expect(index["tank/fs@s1"]).to eq(snaps.first)
    end
  end

  def capture_stdout
    old_stdout = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = old_stdout
  end
end
