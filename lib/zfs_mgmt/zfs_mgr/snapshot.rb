# implement snapshot management

class ZfsMgmt::ZfsMgr::Snapshot < Thor
  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  desc "destroy", "apply the snapshot destroy policy to zfs"
  method_option :noop, :type => :boolean, :default => false,
                :desc => 'pass -n option to zfs commands'
  method_option :verbose, :type => :boolean, :default => false,
                :desc => 'pass -v option to zfs commands'
  def destroy()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt.snapshot_destroy(noop: options[:noop], verbose: options[:verbose], filter: options[:filter])
  end
  desc "policy", "print the policy table for zfs"
  def policy()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt.snapshot_policy(filter: options[:filter])
  end
  desc "create", "execute zfs snapshot based on zfs properties"
  method_option :noop, :type => :boolean, :default => false,
                :desc => 'log snapshot commands without running zfs snapshot'
  def create()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt.snapshot_create(noop: options[:noop], filter: options[:filter])
  end
end
