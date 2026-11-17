require "spec_helper"
require "stringio"

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

  def capture_stdout
    old_stdout = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = old_stdout
  end
end
