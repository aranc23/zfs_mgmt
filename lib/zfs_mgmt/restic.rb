
module ZfsMgmt::Restic
  def self.backup(backup_type: 'incremental',
                  force: false,
                  filter: '.+',
                  restic_binary: 'restic')
    pp backup_type, force, filter
    ZfsMgmt.zfs_managed_list(filter: '.+',
                             properties: ['name',
                                          'zfsmgmt:restic_backup',
                                          'zfsmgmt:restic_repository',
                                         ],
                             property_match: { 'zfsmgmt:restic_backup' => 'true' }).each do |blob|
      zfs,props,snaps = blob

      last = snaps.keys.sort { |a,b| snaps[a]['creation'] <=> snaps[b]['creation'] }.last
      snap_time = Time.at(snaps[last]['creation'])
      com = [ 'zfs', 'send', '-L', '-w', '-h', '-p', last ]
      com.push( '|', 'mbuffer', '-m', '256m', '-q' )
      com.push( '|', restic_binary, 'backup', '--stdin',
                '--stdin-filename', zfs, '--time', "\"#{snap_time.strftime('%F %T')}\"" )
      [ "zfsmgmt:snapshot=#{last}",
        "zfsmgmt:zfs=#{zfs}",
        "zfsmgmt:level=#{backup_type}" ].each do |tag|
        com.push( '--tag', "\"#{tag}\"" )
      end
      if props.has_key?('zfsmgmt:restic_repository')
        com.push( '-r', props['zfsmgmt:restic_repository'] )
      end
      print "#{com.join(' ')}\n"
      system(com.join(' '))
    end
  end
end

class ZfsMgmtResticBackup < Thor
  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  class_option :force, :type => :boolean, :default => false,
               :desc => 'force create this backup type, fail if it cannot be forced'
  class_option :restic_binary, :type => :string, :default => 'restic',
               :desc => 'restic binary'
  desc "incremental", "perform incremental backup"
  def incremental()
    ZfsMgmt::Restic.backup(backup_type: 'incremental',
                           force: options[:force],
                           restic_binary: options[:restic_binary])
  end
  desc "differential", "perform differential backup"
  def differential()
    ZfsMgmt::Restic.backup(backup_type: 'differential', force: options[:force],
                           restic_binary: options[:restic_binary])
  end
  desc "full", "perform full backup"
  def full()
    ZfsMgmt::Restic.backup(backup_type: 'full', force: options[:force],
                           restic_binary: options[:restic_binary])
  end
end

class ZfsMgmtRestic < Thor
  desc "restic SUBCOMMAND ...ARGS", "backup all configured zfs to restic"
  subcommand "backup", ZfsMgmtResticBackup

end

  
