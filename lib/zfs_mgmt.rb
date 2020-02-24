# coding: utf-8
require "zfs_mgmt/version"
require 'pp'
require 'date'
require 'logger'
require 'text-table'
require 'open3'

$logger = Logger.new(STDERR)

$date_patterns = {
  'hourly' => '%F Hour %H',
  'daily' => '%F',
  'weekly' => '%Y Week %U', # week, starting on sunday
  'monthly' => '%Y-%m',
  'yearly' => '%Y',
}

module ZfsMgmt
  def self.custom_properties()
    return [
      'weekly',
      'daily',
      'hourly',
      'monthly',
      'yearly',
      'manage',
      'minage',
    ].map do |p|
      ['zfsmgmt',p].join(':')
    end
  end
  def self.readsnaps()
    a=[]
    File.open('/etc/zfs-list-snapshots.txt',mode='r') do |s|
      while l = s.gets
        a << l
      end
    end
    a
  end
  def self.timespec_to_seconds(spec)
    specs = {
      's' => 1,
      'm' => 60,
      'h' => 60*60,
      'd' => 24*60*60,
      'w' => 7*24*60*60,
    }
    md = /^(\d+)([smhdw]?)/.match(spec)
    if md.length == 2
      return spec.to_i
    elsif md.length == 3
      return md[1].to_i * specs[md[2]]
    else
      return spec
    end
  end
      
  def self.zfsget(properties: ['name'],types: ['filesystem','volume'],fs: '')
    results={}
    com = ['zfs', 'get', '-Hp', properties.join(','), '-t', types.join(','), fs]
    so,se,status = Open3.capture3(com.join(' '))
    if status.signaled?
      $logger.error("process was signalled \"#{com.join(' ')}\", termsig #{status.termsig}")
      raise 'ZfsGetError'
    end
    unless status.success?
      $logger.error("failed to execute \"#{com.join(' ')}\", exit status #{status.exitstatus}")
      so.split("\n").each { |l| $logger.debug("stdout: #{l}") }
      se.split("\n").each { |l| $logger.error("stderr: #{l}") }
      raise 'ZfsGetError'
    end
    so.split("\n").each do |line|
      params = line.split("\t")
      unless results.has_key?(params[0])
        results[params[0]] = {}
      end
      if params[2] != '-'
        results[params[0]][params[1]] = params[2]
      end
      if params[3] != '-'
        results[params[0]]["#{params[1]}@source"] = params[3]
      end
    end
    return results
  end
  def self.local_epoch_to_datetime(e)
    return Time.at(e).to_datetime
  end
  def self.find_saved_reason(saved,snap)
    results = {}
    $date_patterns.each do |d,dk|
      if saved.has_key?(d)
        saved[d].each do |k,s|
          if snap == s
            results[d]=k
            break
          end
        end
      end
    end
    return [results['hourly'],results['daily'],results['weekly'],results['monthly'],results['yearly']]
  end
  def self.snapshot_destroy_policy(zfs,props,snaps)
    minage = 0
    if props.has_key?('zfsmgmt:minage')
      minage = timespec_to_seconds(props['zfsmgmt:minage'])
    end
    sorted = snaps.keys.sort { |a,b| snaps[b]['creation'] <=> snaps[a]['creation'] }
    # never consider the latest snapshot for anything
    newest_snapshot_name = sorted.shift
    
    counters = {}
    saved = {}

    # set the counters variable to track the number of saved daily/hourly/etc. snapshots
    $date_patterns.each do |d,p|
      saved[d] = {}
      if props.has_key?("zfsmgmt:#{d}")
        counters[d] = props["zfsmgmt:#{d}"].to_i
      else
        counters[d] = 0
      end
    end

    sorted.each do |snap_name|
      snaptime = local_epoch_to_datetime(snaps[snap_name]['creation'])
      $date_patterns.each do |d,p|
        pat = snaptime.strftime(p)
        if saved[d].has_key?(pat)
          # update the existing current save snapshot for this timeframe
          $logger.debug("updating the saved snapshot for \"#{pat}\" to #{snap_name} at #{snaptime}")
          saved[d][pat] = snap_name
        elsif counters[d] > 0
          # new pattern, and we want to save more snaps of this type
          $logger.debug("new pattern \"#{pat}\" n#{counters[d]} #{d} snapshot}, saving #{snap_name} at #{snaptime}")
          counters[d] -= 1
          saved[d][pat] = snap_name
        end
      end
    end
    
    # create a list of unique saved snap shots
    saved_snaps = []
    saved.each do |d,saved|
      saved_snaps += saved.values()
    end
    saved_snaps = saved_snaps.sort.uniq
    
    # delete everything not in the list of saved snapshots
    deleteme = sorted - saved_snaps
    deleteme = deleteme.select { |snap|
      if minage > 0 and Time.at(snaps[snap]['creation'] + minage) > Time.now()
        $logger.debug("skipping due to minage: #{snap} #{local_epoch_to_datetime(snaps[snap]['creation']).strftime('%F %T')}")
        false
      else
        true
      end
    }
    return saved,saved_snaps,deleteme
  end
  def self.snapshot_destroy(noop: false, verbopt: false, debugopt: false, filter: '.+')
    if debugopt
      $logger.level = Logger::DEBUG
    else
      $logger.level = Logger::INFO
    end
    self.zfsget(properties: custom_properties()).each do |zfs,props|
      unless /#{filter}/ =~ zfs
        next
      end
      unless props.has_key?('zfsmgmt:manage') and props['zfsmgmt:manage'] == 'true'
        next
      end
      snaps = self.zfsget(properties: ['name','creation','userrefs'],types: ['snapshot'], fs: zfs)
      if snaps.length == 0
        $logger.warn("unable to process this zfs, no snapshots at all: #{zfs}")
        next
      end
      sanity_check = false
      $date_patterns.each do |d,p|
        if props.has_key?("zfsmgmt:#{d}")
          sanity_check = true
          break
        end
      end
      unless sanity_check == true
        $logger.error("zfs_mgmt is configured to manage #{zfs}, but there is no valid #{$date_patterns.keys.join('/')} configuration, skipping")
        next # zfs
      end
      # these are integers and probably should be converted by zfsget
      snaps.each do |s,h|
        ['creation','userrefs'].each do |p|
          if h.has_key?(p)
            snaps[s][p] = snaps[s][p].to_i
          end
        end
      end
      # call the function that decides who to save and who to delete
      (saved,saved_snaps,deleteme) = snapshot_destroy_policy(zfs,props,snaps)

      if verbopt
        # print a table of saved snapshots with the reasons it is being saved
        table = Text::Table.new
        table.head = ['snap','creation','hourly','daily','weekly','monthly','yearly']
        table.rows = []
        saved_snaps.sort { |a,b| snaps[b]['creation'] <=> snaps[a]['creation'] }.each do |snap|
          table.rows << [snap,local_epoch_to_datetime(snaps[snap]['creation'])] + find_saved_reason(saved,snap)
        end
        print table.to_s
      end
      
      $logger.info("deleting #{deleteme.length} snapshots for #{zfs}")
      if deleteme.length > 0
        deleteme.each do |snap_name|
          $logger.debug("delete: #{snap_name} #{local_epoch_to_datetime(snaps[snap_name]['creation']).strftime('%F %T')}")
        end
        bigarg = "#{zfs}@#{deleteme.map { |s| s.split('@')[1] }.join(',')}"
        com_base = "zfs destroy -p"
        if noop
          com_base = "#{com_base}n"
        end
        if verbopt
          com_base = "#{com_base}v"
        end
        com = "#{com_base} #{bigarg}"
        # this is just a guess about how big things can be before running zfs will fail
        if bigarg.length >= 131072 or com.length >= (2097152-10000) 
          deleteme.each do |snap_name|
            minicom="#{com_base} #{snap_name}"
            $logger.info(minicom)
            system(minicom)
          end
        else
          $logger.info(com)
          system(com)
        end
      end
    end
  end
  def self.popper(*args)
    Open3.popen3(args) {|stdin, stdout, stderr, wait_thr|
    pid = wait_thr.pid # pid of the started process.
    wait
    exit_status = wait_thr.value # Process::Status object returned.
  }
  end
end
