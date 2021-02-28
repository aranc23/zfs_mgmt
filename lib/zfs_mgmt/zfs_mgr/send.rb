# zfs send stuff

class ZfsMgmt::ZfsMgr::Send < Thor
  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  desc "all", "send all zfs configured via user properties"
  method_option :remote, :type => :string,
                :desc => 'remote specification like root@otherhost or localhost'
  method_option :destination, :type => :string,
                :desc => 'destination path like otherpool/ourpool'
  method_option :noop, :type => :string, :alias => '-n',
                :desc => 'pass the -n option to zfs recv'
  method_option :verbose, :type => :boolean, :alias => 'v', :default => false,
                :desc => 'pass the -v option to zfs send'
  method_option :intermediary, :alias => '-I', :desc => "pass -I (intermediary) option to zfs send", :default => false, :type => :boolean
  method_option :properties, :alias => '-p', :desc => "pass -p (properties) option to zfs send", :default => true, :type => :boolean
  method_option :raw, :alias => '-w', :desc => "pass -w (raw) option to zfs send", :default => false, :type => :boolean
  method_option :large_block, :alias => '-L', :desc => "pass -L (large block) option to zfs send", :default => true, :type => :boolean
  method_option :embed, :alias => '-e', :desc => "pass -e (embed) option to zfs send", :default => true, :type => :boolean
  method_option :compressed, :alias => '-c', :desc => "pass -c (compressed) option to zfs send", :default => true, :type => :boolean
  method_option :unmount, :alias => '-u', :desc => "pass -u (unmount) option to zfs receive", :default => true, :type => :boolean
  method_option :mbuffer, :desc => "insert mbuffer between send and recv", :default => true, :type => :boolean
  def all()
    ZfsMgmt.global_options = options

    ZfsMgmt.zfs_managed_list(filter: options[:filter],
                             property_match: { 'zfsmgmt:send' => 'true' }).each do |blob|
      zfs,props,snaps = blob
      if props['zfsmgmt:send@source'] == 'received'
        $logger.debug("skipping received filesystem")
        next
      end
      unless props['zfsmgmt:destination']
        $logger.error("#{zfs}: you must specify a destination zfs path via the user property zfsmgmt:destination, even if using --destination on the command line, skipping")
        next
      end
      ZfsMgmt.zfs_send(options,zfs,props,snaps)
    end
  end
end
