# coding: utf-8
require 'easy_talk'
require 'set'

module ZfsMgmt
  class TypedHashBuilder
    def initialize(_name, type, constraints = {})
      @type = type
      @constraints = constraints || {}
    end

    def build
      schema = { type: 'object' }
      schema[:description] = @constraints[:description] if @constraints[:description]
      schema[:default] = @constraints[:default] if @constraints.key?(:default)
      value_type = @type.respond_to?(:values) ? @type.values : nil
      value_type = value_type.raw_type if value_type.respond_to?(:raw_type)
      if value_type
        schema[:additionalProperties] = EasyTalk::Property.new(:_value, value_type, {}).build
      end
      schema
    end
  end
end

unless EasyTalk::Builders::Registry.registered?('T::Types::TypedHash')
  EasyTalk::Builders::Registry.register('T::Types::TypedHash', ZfsMgmt::TypedHashBuilder, collection: true)
end

module ZfsMgmt
  class Zfs
    include EasyTalk::Model

    attr_reader :provided_attributes, :destination_base

    SEND_BOOL_PROPS = %i[
      send_intermediary
      send_backup
      send_compressed
      send_embed
      send_holds
      send_large_block
      send_props
      send_raw
    ].freeze

    RECV_BOOL_PROPS = %i[
      recv_noop
      recv_drop_holds
      recv_unmount
      recv_discard_first
      recv_force
    ].freeze

    VALID_ZFS_PROPS = (
      %w[
        manage policy strategy minage matchsnaps ignoresnaps prefersnaps
        snapshot snap_prefix snap_timestamp
        send remote destination send_replicate
        recv_state
        excludes overrides
        restic_backup restic_repository
      ] +
      SEND_BOOL_PROPS.map(&:to_s) +
      RECV_BOOL_PROPS.map(&:to_s)
    ).map { |p| "zfsmgmt:#{p}" }.freeze

    VALID_CONFIG_KEYS = (
      VALID_ZFS_PROPS.map { |p| p.sub(/\Azfsmgmt:/, '') } +
      %w[name type snapshot_recursive send_enabled]
    ).uniq.freeze

    define_schema do
      title 'Zfs'
      description 'Encapsulates zfsmgmt:* user properties for a ZFS dataset'

      property :name, String,
               description: 'Full name of the ZFS dataset (e.g. tank/data)'
      property :type, String, optional: true, enum: %w[filesystem volume],
               description: 'ZFS dataset type (filesystem or volume)'

      # Snapshot retention policy properties
      property :manage, T::Boolean, optional: true, default: false,
               description: 'Whether snapshot retention management is enabled for this dataset'
      property :policy, String, optional: true, pattern: '\A(\d+[hdwmyHDWMY])+\z',
               description: 'Snapshot retention policy specification (e.g. 10y60m104w365d168h)'
      property :strategy, String, optional: true, enum: %w[oldest youngest], default: 'oldest',
               description: 'Which snapshot to retain within each policy timeframe'
      property :minage, String, optional: true, pattern: '\A\d+[smhdwSMHDW]?\z',
               description: 'Minimum age before a snapshot is eligible for destruction (e.g. 7D)'
      property :matchsnaps, String, optional: true,
               description: 'Regular expression that snapshot names must match to satisfy retention policy'
      property :ignoresnaps, String, optional: true,
               description: 'Regular expression for snapshot names to ignore during policy evaluation and destruction'
      property :prefersnaps, String, optional: true,
               description: 'Regular expression for snapshot names preferred when selecting snapshots to retain'

      # Snapshot creation properties
      property :snapshot, T::Boolean, optional: true, default: false,
               description: 'Whether snapshots should be created for this dataset'
      property :snapshot_recursive, T::Boolean, optional: true, default: false,
               description: 'Whether snapshots should be created recursively (-r) for this dataset'
      property :snap_prefix, String, optional: true, default: 'zfsmgmt',
               description: 'Prefix used when naming created snapshots'
      property :snap_timestamp, String, optional: true, default: '%FT%T%z',
               description: 'strftime format string used when naming created snapshots'

      # ZFS send/recv properties
      property :send_enabled, T::Boolean, optional: true, default: false, as: :send,
               description: 'Whether zfs send is enabled for this dataset'
      property :remote, String, optional: true,
               description: 'Remote SSH target for zfs recv (e.g. user@host)'
      property :destination, String, optional: true,
               description: 'Resolved destination ZFS dataset path for zfs send/recv'

      property :send_intermediary, T::Boolean, optional: true, default: false,
               description: 'Pass -I (intermediary snapshots) to zfs send'
      property :send_backup, T::Boolean, optional: true, default: false,
               description: 'Pass -b (backup) to zfs send'
      property :send_compressed, T::Boolean, optional: true, default: false,
               description: 'Pass -c (compressed) to zfs send'
      property :send_embed, T::Boolean, optional: true, default: false,
               description: 'Pass -e (embed) to zfs send'
      property :send_holds, T::Boolean, optional: true, default: false,
               description: 'Pass -h (holds) to zfs send'
      property :send_large_block, T::Boolean, optional: true, default: false,
               description: 'Pass -L (large block) to zfs send'
      property :send_props, T::Boolean, optional: true, default: false,
               description: 'Pass -p (properties) to zfs send'
      property :send_raw, T::Boolean, optional: true, default: false,
               description: 'Pass -w (raw) to zfs send'
      property :send_replicate, T::Boolean, optional: true, default: false,
               description: 'Pass -R (replicate) to zfs send'

      property :recv_state, T::Boolean, optional: true, default: true,
               description: 'Pass -s (save partial receive state) to zfs recv'
      property :recv_noop, T::Boolean, optional: true, default: false,
               description: 'Pass -n (noop) to zfs recv'
      property :recv_drop_holds, T::Boolean, optional: true, default: false,
               description: 'Pass -h (drop holds) to zfs recv'
      property :recv_unmount, T::Boolean, optional: true, default: false,
               description: 'Pass -u (unmount) to zfs recv'
      property :recv_discard_first, T::Boolean, optional: true, default: false,
               description: 'Pass -d (discard first path element) to zfs recv'
      property :recv_force, T::Boolean, optional: true, default: false,
               description: 'Pass -F (force rollback) to zfs recv'
      property :excludes, T::Array[String], optional: true,
               description: 'ZFS properties to exclude on zfs recv (-x)'
      property :overrides, T::Hash[String, String], optional: true,
               description: 'ZFS property overrides to set on zfs recv (-o property=value)'

      # Restic backup properties
      property :restic_backup, T::Boolean, optional: true, default: false,
               description: 'Whether restic backup is enabled for this dataset'
      property :restic_repository, String, optional: true,
               description: 'Target restic repository for backups'
    end

    validate :validate_excludes
    validate :validate_overrides

    def initialize(attributes = {})
      attrs = attributes.transform_keys(&:to_sym)
      @snapshots = attrs.delete(:snapshots) if attrs.key?(:snapshots)
      @destination_relative_path = attrs.delete(:destination_relative_path) if attrs.key?(:destination_relative_path)
      @destination_base = attrs.delete(:destination_base) if attrs.key?(:destination_base)
      @config_merged = attrs.key?(:config_merged) ? !!attrs.delete(:config_merged) : false
      if attrs.key?(:send) && !attrs.key?(:send_enabled)
        attrs[:send_enabled] = attrs.delete(:send)
      end
      @provided_attributes = attrs.keys.to_set
      attrs[:excludes] = [] unless attrs.key?(:excludes)
      if attrs.key?(:overrides)
        if attrs[:overrides].is_a?(Hash)
          attrs[:overrides] = attrs[:overrides].each_with_object({}) { |(k, v), h| h[k.to_s] = v }
        end
      else
        attrs[:overrides] = {}
      end
      super(attrs)
    end

    def config_merged?
      @config_merged
    end

    def override_options
      return [] unless overrides.is_a?(Hash)

      overrides.reject { |_k, v| v == ZfsMgmt::KNOCKOUT_PREFIX }.map { |k, v| "#{k}=#{v}" }
    end

    def send=(value)
      self.send_enabled = value
    end

    def send?
      send_enabled == true
    end

    def volume?
      type == 'volume'
    end

    def snapshots
      return @snapshots unless @snapshots.nil?

      @snapshots = ZfsMgmt.zfsget(
        properties: %w[name creation userrefs used written referenced],
        types: ['snapshot'],
        zfs: name
      )
    end

    def snapshots=(value)
      @snapshots = value
    end

    def snapshots_loaded?
      !@snapshots.nil?
    end

    def reload_snapshots!
      @snapshots = nil
      snapshots
    end

    def resolved_destination(override = nil)
      return destination unless override

      rel = @destination_relative_path || File.basename(name)
      File.join(override, rel)
    end

    def base_destination(override = nil)
      return override if override

      @destination_base || destination
    end

    def discard_first_destination(override = nil)
      base = base_destination(override)
      return nil unless base

      rel = name.include?('/') ? name.split('/', 2).last : nil
      rel && !rel.empty? ? File.join(base, rel) : base
    end

    def self.warn_unknown_zfs_props(name, props)
      props.each_key do |key|
        key_str = key.to_s
        next unless key_str.start_with?('zfsmgmt:')
        next if key_str.end_with?('@source')
        next if VALID_ZFS_PROPS.include?(key_str)

        target = name.nil? || name.empty? ? '' : " on #{name}"
        $logger&.warn("unknown zfsmgmt property '#{key_str}'#{target}")
      end
    end

    def self.from_zfs_props(name, props = {}, config_merged: false)
      props ||= {}
      warn_unknown_zfs_props(name, props)
      attrs = { name: name, config_merged: config_merged }
      attrs[:type] = props['type'] if props.key?('type')

      attrs[:manage] = ZfsMgmt.prop_on?(props['zfsmgmt:manage'])
      attrs[:policy] = props['zfsmgmt:policy'] if props.key?('zfsmgmt:policy')
      attrs[:strategy] = props['zfsmgmt:strategy'] if props.key?('zfsmgmt:strategy')
      attrs[:minage] = props['zfsmgmt:minage'] if props.key?('zfsmgmt:minage')
      attrs[:matchsnaps] = props['zfsmgmt:matchsnaps'] if props.key?('zfsmgmt:matchsnaps')
      attrs[:ignoresnaps] = props['zfsmgmt:ignoresnaps'] if props.key?('zfsmgmt:ignoresnaps')
      attrs[:prefersnaps] = props['zfsmgmt:prefersnaps'] if props.key?('zfsmgmt:prefersnaps')

      # Interpret zfsmgmt:snapshot taking its @source into account
      snap_val = props['zfsmgmt:snapshot']
      snap_src = props['zfsmgmt:snapshot@source']
      if ZfsMgmt.prop_on?(snap_val)
        attrs[:snapshot] = true
        attrs[:snapshot_recursive] = false
      elsif %w[recursive local].include?(snap_val) && %w[local received].include?(snap_src)
        attrs[:snapshot] = true
        attrs[:snapshot_recursive] = (snap_val == 'recursive')
      else
        attrs[:snapshot] = false
        attrs[:snapshot_recursive] = false
      end

      attrs[:snap_prefix] = props['zfsmgmt:snap_prefix'] if props.key?('zfsmgmt:snap_prefix')
      attrs[:snap_timestamp] = props['zfsmgmt:snap_timestamp'] if props.key?('zfsmgmt:snap_timestamp')

      # Interpret zfsmgmt:send and zfsmgmt:send_replicate taking @source into account
      send_val = props['zfsmgmt:send']
      send_src = props['zfsmgmt:send@source']
      rep_val = props['zfsmgmt:send_replicate']
      rep_src = props['zfsmgmt:send_replicate@source']

      replicated_descendant = ZfsMgmt.prop_on?(rep_val) && !rep_src.nil? && rep_src != 'local'
      attrs[:send_replicate] = ZfsMgmt.prop_on?(rep_val) && !replicated_descendant

      attrs[:send_enabled] =
        if send_src == 'received' || replicated_descendant
          false
        elsif ZfsMgmt.prop_on?(send_val)
          true
        elsif send_val == 'local' && send_src == 'local'
          true
        else
          false
        end

      attrs[:remote] = props['zfsmgmt:remote'] if props.key?('zfsmgmt:remote')

      # Interpret zfsmgmt:destination taking @source into account
      if props.key?('zfsmgmt:destination')
        dest_base = props['zfsmgmt:destination']
        dest_src = props['zfsmgmt:destination@source']
        if dest_src == 'local'
          rel = File.basename(name)
          attrs[:destination] = File.join(dest_base, rel)
          attrs[:destination_relative_path] = rel
          attrs[:destination_base] = dest_base
        elsif dest_src && (m = /\Ainherited from (.+)\z/.match(dest_src))
          rel = File.join(File.basename(m[1]), name.sub(m[1], ''))
          attrs[:destination] = File.join(dest_base, rel)
          attrs[:destination_relative_path] = rel
          attrs[:destination_base] = dest_base
        end
      end

      SEND_BOOL_PROPS.each do |prop|
        key = "zfsmgmt:#{prop}"
        attrs[prop] = ZfsMgmt.prop_on?(props[key]) if props.key?(key)
      end

      attrs[:recv_state] = !ZfsMgmt.prop_off?(props['zfsmgmt:recv_state']) if props.key?('zfsmgmt:recv_state')

      RECV_BOOL_PROPS.each do |prop|
        key = "zfsmgmt:#{prop}"
        attrs[prop] = ZfsMgmt.prop_on?(props[key]) if props.key?(key)
      end

      if props.key?('zfsmgmt:excludes')
        attrs[:excludes] = ZfsMgmt.parse_excludes(props['zfsmgmt:excludes'])
      end

      if props.key?('zfsmgmt:overrides')
        ctx = (name.nil? || name.empty?) ? nil : name
        attrs[:overrides] = ZfsMgmt.parse_overrides(props['zfsmgmt:overrides'], context: ctx)
      end

      attrs[:restic_backup] = ZfsMgmt.prop_on?(props['zfsmgmt:restic_backup']) if props.key?('zfsmgmt:restic_backup')
      attrs[:restic_repository] = props['zfsmgmt:restic_repository'] if props.key?('zfsmgmt:restic_repository')

      new(attrs)
    end

    private

    def validate_excludes
      return if excludes.nil?

      unless excludes.is_a?(Array) && excludes.all? { |item| item.is_a?(String) }
        errors.add(:excludes, 'must be an array of strings')
      end
    end

    def validate_overrides
      return if overrides.nil?

      if overrides.is_a?(Array)
        errors.add(:overrides, "must be a hash of property=value pairs (use [zfs.\"#{name}\".overrides], not [[zfs.\"#{name}\".overrides]])")
        return
      end

      unless overrides.is_a?(Hash)
        errors.add(:overrides, 'must be a hash of property=value pairs')
        return
      end

      overrides.each do |k, v|
        unless k.is_a?(String) && v.is_a?(String)
          errors.add(:overrides, "#{k}: property and value must be strings")
        end
      end
    end
  end
end
