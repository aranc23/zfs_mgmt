require "spec_helper"
require "rake"
require "tmpdir"

RSpec.describe "zfs_mgmt.gemspec and packaging" do
  let(:repo_root) { File.expand_path("..", __dir__) }
  let(:gemspec) { Gem::Specification.load(File.join(repo_root, "zfs_mgmt.gemspec")) }

  it "includes zfsmgr and excludes legacy shell/perl wrappers from gem executables" do
    expect(gemspec.executables).to contain_exactly("zfsmgr", "readsnaps", "zfs-list-snapshots")
    expect(gemspec.executables).not_to include("zfsrecvman", "zfssendman", "zfssnapman")
  end

  it "includes schema/config.schema.json and excludes legacy scripts from gem files while keeping them in the repo" do
    expect(gemspec.files).to include("schema/config.schema.json", "bin/zfsmgr")
    expect(gemspec.files).not_to include("bin/zfsrecvman", "bin/zfssendman", "bin/zfssnapman", "bin/zfsfuncs")

    %w[bin/zfsrecvman bin/zfssendman bin/zfssnapman bin/zfsfuncs].each do |legacy_file|
      expect(File).to exist(File.join(repo_root, legacy_file))
    end
  end

  describe "rake install:system" do
    def with_isolated_rake_and_env(env_overrides)
      old_env = {}
      env_overrides.each do |k, v|
        old_env[k] = ENV.key?(k) ? ENV[k] : :__unset__
        v.nil? ? ENV.delete(k) : ENV[k] = v
      end

      rake = Rake::Application.new
      Rake.application = rake
      load File.join(repo_root, "Rakefile")
      if rake.lookup(:build)
        rake["build"].clear
      else
        rake.define_task(Rake::Task, :build)
      end

      yield rake
    ensure
      Rake.application = nil
      old_env.each do |k, v|
        v == :__unset__ ? ENV.delete(k) : ENV[k] = v
      end
    end

    def create_fake_gem_bin(dir, args_log)
      gem_bin = File.join(dir, "fake_gem")
      File.write(gem_bin, <<~SH)
        #!/bin/sh
        printf '%s\n' "$@" > "#{args_log}"
      SH
      File.chmod(0o755, gem_bin)
      gem_bin
    end

    it "installs into BINDIR and creates a compatibility symlink when COMPAT_BINDIR exists" do
      Dir.mktmpdir do |dir|
        bindir = File.join(dir, "usr_bin")
        compat_bindir = File.join(dir, "usr_local_bin")
        FileUtils.mkdir_p([bindir, compat_bindir])

        gem_file = File.join(dir, "zfs_mgmt-test.gem")
        File.write(gem_file, "")
        args_log = File.join(dir, "gem_args.txt")
        gem_bin = create_fake_gem_bin(dir, args_log)

        env = {
          "BINDIR" => bindir,
          "COMPAT_BINDIR" => compat_bindir,
          "GEM_BIN" => gem_bin,
          "GEM_FILE" => gem_file
        }

        with_isolated_rake_and_env(env) do |rake|
          expect { rake["install:system"].invoke }.to output(
            /Linked #{Regexp.escape(File.join(compat_bindir, "zfsmgr"))} -> #{Regexp.escape(File.join(bindir, "zfsmgr"))}/
          ).to_stdout
        end

        expect(File.read(args_log).lines(chomp: true)).to eq([
          "install", "--no-document", "--bindir", bindir, gem_file
        ])
        link_path = File.join(compat_bindir, "zfsmgr")
        expect(File.symlink?(link_path)).to eq(true)
        expect(File.readlink(link_path)).to eq(File.join(bindir, "zfsmgr"))
      end
    end

    it "skips compatibility symlink cleanly when COMPAT_BINDIR does not exist" do
      Dir.mktmpdir do |dir|
        bindir = File.join(dir, "usr_bin")
        compat_bindir = File.join(dir, "missing_local_bin")
        FileUtils.mkdir_p(bindir)

        gem_file = File.join(dir, "zfs_mgmt-test.gem")
        File.write(gem_file, "")
        args_log = File.join(dir, "gem_args.txt")
        gem_bin = create_fake_gem_bin(dir, args_log)

        env = {
          "BINDIR" => bindir,
          "COMPAT_BINDIR" => compat_bindir,
          "GEM_BIN" => gem_bin,
          "GEM_FILE" => gem_file
        }

        with_isolated_rake_and_env(env) do |rake|
          expect { rake["install:system"].invoke }.to output(
            /Skipping compatibility symlink: #{Regexp.escape(compat_bindir)} does not exist/
          ).to_stdout
        end

        expect(File.read(args_log).lines(chomp: true)).to eq([
          "install", "--no-document", "--bindir", bindir, gem_file
        ])
        expect(File.exist?(compat_bindir)).to eq(false)
      end
    end
  end
end
