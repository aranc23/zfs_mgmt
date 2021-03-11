
class ZfsMgmt::ZfsMgr::Backup < Thor
  include ZfsMgmt::Restic
  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  class_option :restic_binary, :type => :string, :default => 'restic',
               :desc => 'restic binary'
  class_option :verbose, :alias => '-v', :type => :numeric,
               :desc => 'verbosity level for restic'
  class_option :buffer, :type => :string, :default => '256m',
               :desc => 'buffer size for mbuffer'
  class_option :password_file, :alias => '-p', :type => :string,
               :desc => 'passed to restic'
  class_option :limit_upload, :type => :numeric,
               :desc => 'passed to restic'
  class_option :repo, :type => :string,
               :desc => 'passed to restic'
  desc "incremental", "perform incremental backup"
  method_option :level, :desc => "backup level in integer form", :default => 2, :type => :numeric
  method_option :intermediary, :alias => '-I', :desc => "pass -I (intermediary) option to zfs send", :default => false, :type => :boolean
  def incremental()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt::Restic.backup(backup_level: options[:level], options: options)
  end
  desc "differential", "perform differential backup"
  method_option :intermediary, :alias => '-I', :desc => "pass -I (intermediary) option to zfs send", :default => false, :type => :boolean
  def differential()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt::Restic.backup(backup_level: 1, options: options)
  end
  desc "full", "perform full backup"
  def full()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt::Restic.backup(backup_level: 0, options: options)
  end
end

class ZfsMgmt::ZfsMgr::Restic < Thor
  desc "backup SUBCOMMAND ...ARGS", "backup all configured zfs to restic"
  subcommand "backup", ZfsMgmt::ZfsMgr::Backup
end
