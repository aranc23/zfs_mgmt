# implement snapshot management

class ZfsMgmt::ZfsMgr::Snapshot < Thor
  class_option :noop, :type => :boolean, :default => false,
               :desc => 'pass -n option to zfs commands'
  class_option :verbose, :type => :boolean, :default => false,
               :desc => 'pass -v option to zfs commands'
  class_option :debug, :type => :boolean, :default => false,
               :desc => 'set logging level to debug'
  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  desc "destroy", "apply the snapshot destroy policy to zfs"
  def destroy()
    ZfsMgmt.snapshot_destroy(noop: options[:noop], verbopt: options[:verbose], debugopt: options[:debug], filter: options[:filter])
  end
  desc "policy", "print the policy table for zfs"
  def policy()
    ZfsMgmt.snapshot_policy(verbopt: options[:verbose], debugopt: options[:debug], filter: options[:filter])
  end
  desc "create", "execute zfs snapshot based on zfs properties"
  def create()
    ZfsMgmt.snapshot_create(verbopt: options[:verbose], debugopt: options[:debug], filter: options[:filter])
  end
end
