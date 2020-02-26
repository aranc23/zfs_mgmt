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
$time_specs = {
  's' => 1,
  'm' => 60,
  'h' => 60*60,
  'd' => 24*60*60,
  'w' => 7*24*60*60,
}

$properties_xlate = {
  'userrefs' => ->(x) { x.to_i },
  'creation' => ->(x) { x.to_i },
}

module ZfsMgmt
  def self.custom_properties()
    return [
      'policy',
      'manage',
      'minage',
      'matchsnaps',
      'ignoresnaps',
    ].map do |p|
      ['zfsmgmt',p].join(':')
    end
  end
  def self.timespec_to_seconds(spec)
    md = /^(\d+)([smhdw]?)/.match(spec)
    unless md.length == 3
      raise 'SpecParseError'
    end
    if md[2] and md[2].length > 0
      return md[1].to_i * $time_specs[md[2]]
    else
      return md[1].to_i
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
        if $properties_xlate.has_key?(params[1])
          results[params[0]][params[1]] = $properties_xlate[params[1]].call(params[2])
        else
          results[params[0]][params[1]] = params[2]
        end
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
    
    counters = policy_parser(props['zfsmgmt:policy'])
    $logger.debug("#{counters}")
    saved = {}

    # set the counters variable to track the number of saved daily/hourly/etc. snapshots
    $date_patterns.each do |d,p|
      saved[d] = {}
    end

    sorted.each do |snap_name|
      if  props.has_key?('zfsmgmt:ignoresnaps') and /#{props['zfsmgmt:ignoresnaps']}/ =~ snap_name
        $logger.debug("skipping #{snap_name} because it matches ignoresnaps pattern: #{props['zfsmgmt:ignoresnaps']}")
        next
      end
      if  props.has_key?('zfsmgmt:matchsnaps') and not /#{props['zfsmgmt:matchsnaps']}/ =~ snap_name
        $logger.debug("skipping #{snap_name} because it does not match matchsnaps pattern: #{props['zfsmgmt:matchsnaps']}")
        next
      end
      snaptime = local_epoch_to_datetime(snaps[snap_name]['creation'])
      $date_patterns.each do |d,p|
        pat = snaptime.strftime(p)
        if saved[d].has_key?(pat)
          # update the existing current save snapshot for this timeframe
          $logger.debug("updating the saved snapshot for \"#{pat}\" to #{snap_name} at #{snaptime}")
          saved[d][pat] = snap_name
        elsif counters[d] > 0
          # new pattern, and we want to save more snaps of this type
          $logger.debug("new pattern \"#{pat}\" n#{counters[d]} #{d} snapshot, saving #{snap_name} at #{snaptime}")
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
      if props.has_key?('zfsmgmt:ignoresnaps') and /#{props['zfsmgmt:ignoresnaps']}/ =~ snap
        $logger.debug("skipping #{snap} because it matches ignoresnaps pattern: #{props['zfsmgmt:ignoresnaps']}")
        false
      elsif minage > 0 and Time.at(snaps[snap]['creation'] + minage) > Time.now()
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
      unless props.has_key?('zfsmgmt:policy') and policy = policy_parser(props['zfsmgmt:policy'])
        $logger.error("zfs_mgmt is configured to manage #{zfs}, but there is no valid policy configuration, skipping")
        next # zfs
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
      com_base = "zfs destroy -p"
      if noop
        com_base = "#{com_base}n"
      end
      if verbopt
        com_base = "#{com_base}v"
      end
      deleteme.each do |snap_name|
        $logger.debug("delete: #{snap_name} #{local_epoch_to_datetime(snaps[snap_name]['creation']).strftime('%F %T')}")
      end
      while deleteme.length > 0
        bigarg = "#{zfs}@#{deleteme.map { |s| s.split('@')[1] }.join(',')}"
        com = "#{com_base} #{bigarg}"
        $logger.debug("size of bigarg: #{bigarg.length} size of com: #{com.length}")
        if bigarg.length >= 131072 or com.length >= (2097152-10000)
          # too big, so just delete one snapshot and try again
          com = "#{com_base} #{deleteme.shift}"
          $logger.info(com)
          system(com)
          next
        else
          $logger.info(com)
          system(com)
          break
        end
      end
    end
  end

  def self.policy_parser(str)
    res = {}
    map = {}
    $date_patterns.keys.each do |tf|
      res[tf]=0
      map[tf[0]] = tf
    end
    p = str.scan(/\d+[#{map.keys.join('')}]/)
    unless p.length > 0
      raise "unable to parse the policy configuration #{str}"
    end
    p.each do |pi|
      scn = pi.scan(/(\d+)([#{map.keys.join('')}])/)
      res[map[scn[0][1]]] = scn[0][0].to_i
    end
    res
  end
end
