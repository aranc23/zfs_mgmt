
module ZfsMgmt::Restic
  def self.backup(backup_type: 'incremental', force: false, filter: '.+')
    pp backup_type, force, filter
  end
end

class ZfsMgmtResticBackup < Thor
  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  class_option :force, :type => :boolean, :default => false,
               :desc => 'force create this backup type, fail if it cannot be forced'
  desc "incremental", "perform incremental backup"
  def incremental()
    ZfsMgmt::Restic.backup(backup_type: 'incremental', force: options[:force])
  end
  desc "differential", "perform differential backup"
  def differential()
    ZfsMgmt::Restic.backup(backup_type: 'differential', force: options[:force])
  end
  desc "full", "perform full backup"
  def full()
    ZfsMgmt::Restic.backup(backup_type: 'full', force: options[:force])
  end
end

class ZfsMgmtRestic < Thor
  desc "restic SUBCOMMAND ...ARGS", "backup all configured zfs to restic"
  subcommand "backup", ZfsMgmtResticBackup

end

  
