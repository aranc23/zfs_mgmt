# implement snapshot management

class ZfsMgmt::ZfsMgr::Snapshot < Thor
  class_option :noop, :type => :boolean, :default => false,
               :desc => 'pass -n option to zfs commands'
  class_option :verbose, :type => :boolean, :default => false,
               :desc => 'pass -v option to zfs commands'
  class_option :loglevel, :type => :string, :default => 'info',
               :enum => ['debug','error','fatal','info','warn'],
               :desc => 'set logging level to specified severity'
  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  desc "destroy", "apply the snapshot destroy policy to zfs"
  def destroy()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt.snapshot_destroy(noop: options[:noop], verbopt: options[:verbose], filter: options[:filter])
  end
  desc "policy", "print the policy table for zfs"
  def policy()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt.snapshot_policy(verbopt: options[:verbose], filter: options[:filter])
  end
  desc "create", "execute zfs snapshot based on zfs properties"
  def create()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt.snapshot_create(noop: options[:noop], verbopt: options[:verbose], filter: options[:filter])
  end
end
