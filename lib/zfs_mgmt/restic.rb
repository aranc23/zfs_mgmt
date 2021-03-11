require "json"

module ZfsMgmt::Restic
  def self.restic_snapshots(zfs,options,props)
    # query the restic database
    com = [ options[:restic_binary],
            'snapshots',
            '--json',
            '--tag', 'zfsmgmt',
            '--path', "/#{zfs}",
          ]
    if options.has_key?('password_file')
      com.push('-p',options['password_file'])
    end
    if options.has_key?('repo')
      com.push('--repo', options['repo'])
    elsif props.has_key?('zfsmgmt:restic_repository')
      com.push( '--repo', props['zfsmgmt:restic_repository'] )
    end

    $logger.info("#{com.join(' ')}")
    restic_output = %x(#{com.join(' ')})
    unless $?.success?
      $logger.error("unable to query the restic database")
      raise "unable to query the restic database"
    end
    restic_snapshots = JSON.parse(restic_output)
    restic_snapshot_zfs_snapshot_index = {}
    restic_snapshots.each do |snappy|
      snappy['date_time'] = DateTime.parse(snappy['time'])
      if snappy.has_key?('tags')
        snappy['tags'].each do |t|
          if m = /^(zfsmgmt:.+?)=(.+)/.match(t)
            if ['zfsmgmt:level'].include?(m[1])
              snappy[m[1]] = m[2].to_i
            else
              snappy[m[1]] = m[2]
            end
            if m[1] == 'zfsmgmt:snapshot'
              restic_snapshot_zfs_snapshot_index[m[2]] = snappy
            end
          end
        end
      end
    end
    return([restic_snapshots,restic_snapshot_zfs_snapshot_index])
  end

  def self.valid_chain(snap,restic_snapshots,restic_snapshot_zfs_snapshot_index,a)
    if snap['zfsmgmt:level'] == 0
      a.push(snap)
      $logger.debug("found complete chain culminating in full backup of: #{snap['zfsmgmt:snapshot']}")
      return a
    elsif restic_snapshot_zfs_snapshot_index.has_key?(snap['zfsmgmt:parent'])
      a.push(snap)
      $logger.debug("found another link in the chain: #{snap['zfsmgmt:snapshot']} => #{snap['zfsmgmt:parent']}")
      return valid_chain(restic_snapshot_zfs_snapshot_index[snap['zfsmgmt:parent']],restic_snapshots,restic_snapshot_zfs_snapshot_index,a)
    else
      $logger.error("broken chain: looking for the parent of #{snap['zfsmgmt:snapshot']} (#{snap['zfsmgmt:parent']}) and failed to find")
      return []
    end
  end
      
      
  def self.backup(backup_level: 2,
                  options: {})
    ZfsMgmt.zfs_managed_list(filter: options['filter'],
                             properties: ['name',
                                          'zfsmgmt:restic_backup',
                                          'zfsmgmt:restic_repository',
                                          'userrefs',
                                         ],
                             property_match: { 'zfsmgmt:restic_backup' => ['on','true'] }).each do |blob|
      zfs,props,zfs_snapshots = blob
      last_zfs_snapshot = zfs_snapshots.keys.sort { |a,b| zfs_snapshots[a]['creation'] <=> zfs_snapshots[b]['creation'] }.last
      zfs_snap_time = Time.at(zfs_snapshots[last_zfs_snapshot]['creation'])

      level = 0
      chain = []
      zfs_snap_parent = ''
      restic_snap_parent = ''
      (restic_snapshots,restic_snapshot_zfs_snapshot_index) = restic_snapshots(zfs,options,props)
      if restic_snapshot_zfs_snapshot_index.has_key?(last_zfs_snapshot)
        $logger.warn("backup of this snapshot #{last_zfs_snapshot} already exists in restic, cannot continue with backup of #{zfs}")
        next # next zfs filesystem to be backed up
      end
      if backup_level > 0 and restic_snapshots.count > 0
        # reverse (oldest first) sorted restic snapshots
        restic_snap_parent = restic_snapshots.filter { |rsnap|
          rsnap.has_key?('zfsmgmt:zfs') and rsnap['zfsmgmt:zfs'] == zfs and
            rsnap.has_key?('zfsmgmt:level') and rsnap['zfsmgmt:level'] < backup_level }.sort {
          |a,b| a['date_time'] <=> b['date_time'] }.last
        if restic_snap_parent and
          zfs_snapshots.has_key?(restic_snap_parent['zfsmgmt:snapshot']) and
          chain = valid_chain(restic_snap_parent,restic_snapshots,restic_snapshot_zfs_snapshot_index,[]) and
          chain.length > 0
          
          level = restic_snap_parent['zfsmgmt:level'] + 1
          zfs_snap_parent = restic_snap_parent['zfsmgmt:snapshot']
          $logger.debug("restic_snap_parent: level: #{restic_snap_parent['zfsmgmt:level']} snapshot: #{zfs_snap_parent}")
        else
          $logger.error("restic_snap_parent rejected: level: #{restic_snap_parent['zfsmgmt:level']} snapshot: #{restic_snap_parent['zfsmgmt:snapshot']}")
        end
        $logger.debug("chain of snapshots: #{chain}")
      end
      tags = [ 'zfsmgmt',
               "zfsmgmt:snapshot=#{last_zfs_snapshot}",
               "zfsmgmt:zfs=#{zfs}",
               "zfsmgmt:level=#{level}" ]
      com = [ ZfsMgmt.global_options['zfs_binary'], 'send', '-w', '-h', '-p' ]
      if level > 0
        if options[:intermediary]
          com.push('-I')
        else
          com.push('-i')
        end
        com.push(zfs_snap_parent)
        tags.push("zfsmgmt:parent=#{zfs_snap_parent}")
      end
      com.push( last_zfs_snapshot )
      com.push( '|', 'mbuffer', '-m', options[:buffer], '-q' )
      com.push( '|', options[:restic_binary], 'backup', '--stdin',
                '--stdin-filename', zfs, '--time', "\"#{zfs_snap_time.strftime('%F %T')}\"" )
      tags.each do |tag|
        com.push( '--tag', "\"#{tag}\"" )
      end
      if options.has_key?('limit_upload')
        com.push('--limit-upload', options['limit_upload'])
      end
      if options.has_key?('password_file')
        com.push('-p',options['password_file'])
      end
      if options.has_key?('repo')
        com.push('--repo', options['repo'])
      elsif props.has_key?('zfsmgmt:restic_repository')
        com.push( '--repo', props['zfsmgmt:restic_repository'] )
      end
      if options[:verbose]
        com.push('--verbose',options[:verbose])
      elsif $stdout.isatty
        com.push('-v')
      end
      unless ZfsMgmt.zfs_holds(last_zfs_snapshot).include?('zfsmgmt_restic')
        ZfsMgmt.zfs_hold('zfsmgmt_restic',last_zfs_snapshot)
      end
      ZfsMgmt.system_com(com)
      chain_snaps = chain.map do |rsnap|
        rsnap['zfsmgmt:snapshot']
      end
      zfs_snapshots.each do |s,d|
        d['userrefs'] == 0 and next
        chain_snaps.include?(s) and next
        s == last_zfs_snapshot and next
        if ZfsMgmt.zfs_holds(s).include?('zfsmgmt_restic')
          ZfsMgmt.zfs_release('zfsmgmt_restic',s)
        end
      end
    end
  end
end

