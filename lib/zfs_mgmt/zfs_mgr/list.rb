# class list

class ZfsMgmt::ZfsMgr::List < Thor
  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  desc "stale", "list all zfs with stale snapshots"
  method_option :age, :desc => "timeframe outside of which the zfs will be considered stale", :default => '1d'
  def stale()
    ZfsMgmt.global_options = options
    cutoff = Time.at(Time.now.to_i -  ZfsMgmt.timespec_to_seconds(options[:age]))
    table = Text::Table.new
    table.head = ['zfs','snapshot','age']
    table.rows = []
    ZfsMgmt.zfs_managed_list(filter: options[:filter]).each do |blob|
      zfs,props,snaps = blob
      last = snaps.keys.sort { |a,b| snaps[a]['creation'] <=> snaps[b]['creation'] }.last
      snap_time = Time.at(snaps[last]['creation'])
      if snap_time < cutoff
        table.rows << [zfs,last.split('@')[1],snap_time]
      end
    end
    if table.rows.count > 0
      print table.to_s
    end
  end
end
