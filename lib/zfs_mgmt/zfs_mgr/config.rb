# config management subcommands

class ZfsMgmt::ZfsMgr::Config < Thor
  def self.exit_on_failure?
    true
  end

  desc "validate [FILE ...]", "validate one or more config files (or all files in config directory with --all)"
  method_option :all, :type => :boolean, :default => false, :aliases => '-a',
                :desc => 'validate all *.toml config files in the config directory'
  method_option :quiet, :type => :boolean, :default => false, :aliases => '-q',
                :desc => 'suppress output and exit non-zero if validation fails'
  def validate(*files)
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ok = ZfsMgmt.validate_config(
      files: files,
      all: options[:all],
      quiet: options[:quiet]
    )
    exit(1) unless ok
  end
end
