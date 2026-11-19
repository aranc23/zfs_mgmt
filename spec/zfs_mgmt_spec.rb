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

    before do
      allow(ZfsMgmt).to receive(:zfs_managed_list).and_return([[zfs, props, snaps]])
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
