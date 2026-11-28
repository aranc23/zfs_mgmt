require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

namespace :schema do
  desc "Generate JSON Schema for zfsmgr config files (default: schema/config.schema.json)"
  task :generate, [:output] do |_t, args|
    $LOAD_PATH.unshift(File.expand_path("lib", __dir__))
    require "json"
    require "fileutils"
    require "zfs_mgmt"

    output = args[:output] || File.expand_path("schema/config.schema.json", __dir__)
    FileUtils.mkdir_p(File.dirname(output))
    File.write(output, JSON.pretty_generate(ZfsMgmt::Config.config_json_schema) + "\n")
    puts "Wrote JSON schema to #{output}"
  end
end

desc "Generate JSON Schema for zfsmgr config files"
task :schema => "schema:generate"

namespace :install do
  desc "Build and install zfs_mgmt into BINDIR (default: /usr/bin) with compatibility symlink in COMPAT_BINDIR (default: /usr/local/bin)"
  task :system => :build do
    require "fileutils"
    $LOAD_PATH.unshift(File.expand_path("lib", __dir__))
    require "zfs_mgmt/version"

    bindir = ENV.fetch("BINDIR", "/usr/bin")
    compat_bindir = ENV.fetch("COMPAT_BINDIR", "/usr/local/bin")
    gem_bin = ENV.fetch("GEM_BIN", "gem")
    gem_file = ENV["GEM_FILE"] || File.expand_path("pkg/zfs_mgmt-#{ZfsMgmt::VERSION}.gem", __dir__)

    sh gem_bin, "install", "--no-document", "--bindir", bindir, gem_file

    if compat_bindir && !compat_bindir.empty? && compat_bindir != bindir
      if Dir.exist?(compat_bindir)
        target = File.join(bindir, "zfsmgr")
        link = File.join(compat_bindir, "zfsmgr")
        FileUtils.ln_sf(target, link)
        puts "Linked #{link} -> #{target}"
      else
        puts "Skipping compatibility symlink: #{compat_bindir} does not exist"
      end
    end
  end
end

task :default => :spec
