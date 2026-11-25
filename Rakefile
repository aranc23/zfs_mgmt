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

task :default => :spec
