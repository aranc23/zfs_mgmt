# coding: utf-8
require 'easy_talk'
require 'tomlrb'
require 'deep_merge/core'
require 'json'
require 'zfs_mgmt/zfs'

module ZfsMgmt
  class ConfigError < StandardError; end

  class Config
    include EasyTalk::Model

    TOP_LEVEL_KEYS = %w[excludes overrides ssh_config mbuffer mbuffer_size zfs].freeze
    JSON_SCHEMA_DRAFT = 'http://json-schema.org/draft-07/schema#'
    JSON_SCHEMA_URL = 'https://raw.githubusercontent.com/aranc23/zfs_mgmt/master/schema/config.schema.json'

    define_schema do
      title 'Config'
      description 'Top-level zfsmgr configuration loaded from TOML files'

      property :excludes, T::Array[String], optional: true,
               description: 'ZFS properties to exclude on zfs recv (-x)'
      property :overrides, T::Hash[String, String], optional: true,
               description: 'ZFS property overrides to set on zfs recv (-o property=value)'
      property :ssh_config, T::Hash[String, String], optional: true,
               description: 'SSH options passed via -o option=value (merged with BatchMode=yes default)'
      property :mbuffer, T::Boolean, optional: true,
               description: 'Whether to insert mbuffer between zfs send and zfs recv (defaults to true)'
      property :mbuffer_size, String, optional: true,
               description: 'Buffer size passed to mbuffer -m option (e.g. 512M, 1G)'
      property :zfs, T::Hash[String, ZfsMgmt::Zfs], optional: true,
               description: 'Per-dataset ZFS configuration keyed by dataset name'
    end

    validate :validate_excludes
    validate :validate_overrides
    validate :validate_ssh_config
    validate :validate_mbuffer
    validate :validate_mbuffer_size
    validate :validate_zfs_entries

    def initialize(attributes = {})
      @raw_zfs = {}
      raw_attrs = (attributes || {}).each_with_object({}) { |(k, v), h| h[k.to_s] = v }

      raw_attrs.each_key do |k|
        next if TOP_LEVEL_KEYS.include?(k)

        $logger&.warn("unknown config key '#{k}'")
      end

      super_attrs = {}
      super_attrs[:excludes] = raw_attrs.key?('excludes') ? raw_attrs['excludes'] : []
      if raw_attrs.key?('overrides')
        raw_overrides = raw_attrs['overrides']
        super_attrs[:overrides] = if raw_overrides.is_a?(Hash)
                                    raw_overrides.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
                                  else
                                    raw_overrides
                                  end
      else
        super_attrs[:overrides] = {}
      end

      if raw_attrs.key?('ssh_config')
        raw_ssh_config = raw_attrs['ssh_config']
        super_attrs[:ssh_config] = if raw_ssh_config.is_a?(Hash)
                                     raw_ssh_config.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
                                   else
                                     raw_ssh_config
                                   end
      else
        super_attrs[:ssh_config] = {}
      end

      super_attrs[:mbuffer] = raw_attrs['mbuffer'] if raw_attrs.key?('mbuffer')
      super_attrs[:mbuffer_size] = raw_attrs['mbuffer_size'] if raw_attrs.key?('mbuffer_size')

      if raw_attrs.key?('zfs')
        raw_zfs_input = raw_attrs['zfs']
        if raw_zfs_input.is_a?(Hash)
          super_attrs[:zfs] = normalize_zfs_entries(raw_zfs_input)
        else
          super_attrs[:zfs] = raw_zfs_input
        end
      else
        super_attrs[:zfs] = {}
      end

      super(super_attrs)
    end

    def override_options
      return [] unless overrides.is_a?(Hash)

      overrides.reject { |_k, v| v == ZfsMgmt::KNOCKOUT_PREFIX }.map { |k, v| "#{k}=#{v}" }
    end

    def zfs_excludes_layers_for(dataset_name)
      layers = []
      dataset_ancestors(dataset_name).each do |ancestor|
        raw_entry = @raw_zfs[ancestor]
        next unless raw_entry && raw_entry.key?('excludes')

        val = raw_entry['excludes']
        if val.is_a?(Array) && val.all? { |item| item.is_a?(String) }
          layers << val
        end
      end
      layers
    end

    def zfs_overrides_layers_for(dataset_name)
      zfs_hash_layers_for(dataset_name, 'overrides')
    end

    def zfs_ssh_config_layers_for(dataset_name)
      zfs_hash_layers_for(dataset_name, 'ssh_config')
    end

    def zfs_props_for(dataset_name)
      result = {}
      ancestors = dataset_ancestors(dataset_name)
      return result if ancestors.empty?

      excludes_layers = []
      excludes_source = nil
      overrides_layers = []
      overrides_source = nil
      ssh_config_layers = []
      ssh_config_source = nil

      ancestors.each do |ancestor|
        raw_entry = @raw_zfs[ancestor]
        next unless raw_entry

        source = (ancestor == dataset_name ? 'local' : "inherited from #{ancestor}")
        raw_entry.each do |k, v|
          next if v.nil?

          if k == 'type'
            result['type'] = v.to_s if ancestor == dataset_name
            next
          end
          next if k == 'name'

          if k == 'excludes'
            if v.is_a?(Array) && v.all? { |item| item.is_a?(String) }
              excludes_layers << v
              excludes_source = source
            end
            next
          end

          if k == 'overrides'
            if v.is_a?(Hash) && v.all? { |ok, ov| ok.is_a?(String) && ov.is_a?(String) }
              overrides_layers << v
              overrides_source = source
            end
            next
          end

          if k == 'ssh_config'
            if v.is_a?(Hash) && v.all? { |sk, sv| sk.is_a?(String) && sv.is_a?(String) }
              ssh_config_layers << v
              ssh_config_source = source
            end
            next
          end

          if k == 'snapshot_recursive'
            if v == true
              result['zfsmgmt:snapshot'] = 'recursive'
              result['zfsmgmt:snapshot@source'] = source
            end
            next
          end

          if k == 'destination'
            if v.is_a?(String)
              result['zfsmgmt:destination'] = v
              result['zfsmgmt:destination@source'] = source
            elsif v.is_a?(Array) && v.all? { |item| item.is_a?(String) }
              result['zfsmgmt:destination'] = v.dup
              result['zfsmgmt:destination@source'] = source
            end
            next
          end

          if k == 'source'
            if v.is_a?(String)
              result['zfsmgmt:source'] = v
              result['zfsmgmt:source@source'] = source
            end
            next
          end

          prop_key = case k
                     when 'send_enabled' then 'send'
                     when 'recv_enabled' then 'recv'
                     else k
                     end
          next unless ZfsMgmt::Zfs::VALID_ZFS_PROPS.include?("zfsmgmt:#{prop_key}")

          str_val = case v
                    when true then 'true'
                    when false then 'false'
                    else v.to_s
                    end
          result["zfsmgmt:#{prop_key}"] = str_val
          result["zfsmgmt:#{prop_key}@source"] = source
        end
      end

      unless excludes_layers.empty?
        result['zfsmgmt:excludes'] = ZfsMgmt.merge_excludes(*excludes_layers, strip_knockouts: false)
        result['zfsmgmt:excludes@source'] = excludes_source
      end

      unless overrides_layers.empty?
        result['zfsmgmt:overrides'] = ZfsMgmt.merge_overrides(*overrides_layers, strip_knockouts: false)
        result['zfsmgmt:overrides@source'] = overrides_source
      end

      unless ssh_config_layers.empty?
        result['zfsmgmt:ssh_config'] = ZfsMgmt.merge_ssh_config(*ssh_config_layers, strip_knockouts: false)
        result['zfsmgmt:ssh_config@source'] = ssh_config_source
      end

      result
    end

    def self.from_hash(hash)
      new(hash || {})
    end

    def self.schema_path
      File.expand_path('../../schema/config.schema.json', __dir__)
    end

    def self.config_json_schema
      base = JSON.parse(JSON.generate(json_schema))
      schema = {
        '$schema' => JSON_SCHEMA_DRAFT,
        '$id' => JSON_SCHEMA_URL
      }.merge(base)

      zfs_schema = schema.dig('properties', 'zfs', 'additionalProperties')
      if zfs_schema.is_a?(Hash)
        if zfs_schema['required'].is_a?(Array)
          zfs_schema['required'].delete('name')
          zfs_schema.delete('required') if zfs_schema['required'].empty?
        end

        zfs_props = zfs_schema['properties']
        if zfs_props.is_a?(Hash)
          zfs_props['snapshot'] = {
            'description' => 'Whether snapshots should be created for this dataset (true, false, "recursive", or "local")',
            'oneOf' => [
              { 'type' => 'boolean' },
              { 'type' => 'string', 'enum' => %w[recursive local] }
            ],
            'default' => false
          }
          zfs_props['send'] = {
            'description' => 'Whether zfs send is enabled for this dataset (true, false, or "local")',
            'oneOf' => [
              { 'type' => 'boolean' },
              { 'type' => 'string', 'enum' => %w[local] }
            ],
            'default' => false
          }
          zfs_props['destination'] = {
            'description' => 'Destination ZFS dataset path(s) for zfs send/recv, optionally prefixed with [user@]host: (e.g. backup/tank, user@host:backup/tank, or an array of destinations)',
            'oneOf' => [
              { 'type' => 'string' },
              { 'type' => 'array', 'items' => { 'type' => 'string' } }
            ]
          }
          zfs_props['recv'] = {
            'description' => 'Whether zfs recv is enabled for this dataset (true, false, or "local")',
            'oneOf' => [
              { 'type' => 'boolean' },
              { 'type' => 'string', 'enum' => %w[local] }
            ],
            'default' => false
          }
        end
      end

      schema
    end

    def self.load_files(paths)
      merged = {}
      paths.sort.each do |path|
        begin
          parsed = Tomlrb.load_file(path)
        rescue Tomlrb::ParseError => e
          $logger&.error("failed to parse config file #{path}: #{e.message}")
          raise ConfigError, "failed to parse config file #{path}: #{e.message}"
        end

        next if parsed.nil?
        unless parsed.is_a?(Hash)
          $logger&.error("invalid config file #{path}: expected top-level table")
          raise ConfigError, "invalid config file #{path}: expected top-level table"
        end

        DeepMerge.deep_merge!(parsed, merged, overwrite_arrays: true)
      end

      config = from_hash(merged)
      unless config.valid?
        msg = "invalid configuration: #{config.errors.full_messages.join('; ')}"
        $logger&.error(msg)
        raise ConfigError, msg
      end
      config
    end

    def self.load_dir(dir)
      return new if dir.nil? || !Dir.exist?(dir)

      files = Dir.glob(File.join(dir, '*.toml')).sort
      load_files(files)
    end

    private

    def zfs_hash_layers_for(dataset_name, key)
      layers = []
      dataset_ancestors(dataset_name).each do |ancestor|
        raw_entry = @raw_zfs[ancestor]
        next unless raw_entry && raw_entry.key?(key)

        val = raw_entry[key]
        if val.is_a?(Hash) && val.all? { |k, v| k.is_a?(String) && v.is_a?(String) }
          layers << val
        end
      end
      layers
    end

    def dataset_ancestors(dataset_name)
      return [] if dataset_name.nil? || dataset_name.empty? || @raw_zfs.empty?

      parts = dataset_name.to_s.split('/')
      (0...parts.length).map { |i| parts[0..i].join('/') }
    end

    def normalize_zfs_entries(raw_zfs_input)
      models = {}
      deferred_raw = []

      raw_zfs_input.each do |ds_key, entry|
        ds_name = ds_key.to_s
        if entry.is_a?(ZfsMgmt::Zfs)
          @raw_zfs[ds_name] = extract_raw_from_model(entry)
          models[ds_name] = entry
        elsif entry.is_a?(Hash)
          @raw_zfs[ds_name] = filter_zfs_entry_keys(ds_name, entry)
          deferred_raw << ds_name
        else
          models[ds_name] = entry
        end
      end

      deferred_raw.each do |ds_name|
        models[ds_name] = ZfsMgmt::Zfs.from_zfs_props(ds_name, zfs_props_for(ds_name))
      end

      models
    end

    def filter_zfs_entry_keys(ds_name, hash)
      filtered = {}
      hash.each do |k, v|
        k_str = k.to_s
        unless ZfsMgmt::Zfs::VALID_CONFIG_KEYS.include?(k_str)
          $logger&.warn("unknown config property '#{k_str}' on #{ds_name}")
          next
        end
        if (k_str == 'overrides' || k_str == 'ssh_config') && v.is_a?(Hash)
          filtered[k_str] = v.each_with_object({}) { |(ok, ov), oh| oh[ok.to_s] = ov }
        else
          filtered[k_str] = v
        end
      end
      filtered
    end

    def extract_raw_from_model(model)
      raw = {}
      provided = model.provided_attributes || Set.new
      provided.each do |attr|
        key = case attr
              when :send_enabled then 'send'
              when :recv_enabled then 'recv'
              else attr.to_s
              end
        raw[key] = model.public_send(attr)
      end
      raw['destination'] = model.destination_base if model.destination_base
      raw['source'] = model.source_base if model.source_base
      raw
    end

    def validate_excludes
      unless excludes.is_a?(Array) && excludes.all? { |item| item.is_a?(String) }
        errors.add(:excludes, 'must be an array of strings')
      end
    end

    def validate_overrides
      validate_top_level_hash(:overrides, overrides, 'hash of property=value pairs')
    end

    def validate_ssh_config
      validate_top_level_hash(:ssh_config, ssh_config, 'hash of option=value pairs', table_hint: 'use [ssh_config], not [[ssh_config]]')
    end

    def validate_mbuffer
      return if mbuffer.nil? || mbuffer == true || mbuffer == false

      errors.add(:mbuffer, 'must be a boolean')
    end

    def validate_mbuffer_size
      return if mbuffer_size.nil? || mbuffer_size.is_a?(String)

      errors.add(:mbuffer_size, 'must be a string')
    end

    def validate_top_level_hash(attr, value, expected_desc, table_hint: nil)
      if value.is_a?(Array) && table_hint
        errors.add(attr, "must be a #{expected_desc} (#{table_hint})")
        return
      end

      unless value.is_a?(Hash)
        errors.add(attr, "must be a #{expected_desc}")
        return
      end

      value.each do |k, v|
        unless k.is_a?(String) && v.is_a?(String)
          errors.add(attr, "#{k}: property and value must be strings")
        end
      end
    end

    def validate_zfs_entries
      unless zfs.is_a?(Hash)
        errors.add(:zfs, 'must be a hash keyed by dataset name')
        return
      end

      zfs.each do |dataset_name, zfs_obj|
        unless zfs_obj.is_a?(ZfsMgmt::Zfs)
          if zfs_obj.is_a?(Array)
            errors.add(:zfs, "#{dataset_name}: must be a Zfs configuration table (use [zfs.\"#{dataset_name}\"], not [[zfs.\"#{dataset_name}\"]])")
          else
            errors.add(:zfs, "#{dataset_name}: must be a Zfs configuration table")
          end
          next
        end
        raw_entry = @raw_zfs[dataset_name]
        if raw_entry
          if raw_entry.key?('destination')
            raw_dest = raw_entry['destination']
            valid_dest = raw_dest.is_a?(String) || (raw_dest.is_a?(Array) && raw_dest.all? { |item| item.is_a?(String) })
            unless valid_dest
              errors.add(:zfs, "#{dataset_name}: destination must be a string or an array of strings")
            end
          end
          if raw_entry.key?('source') && !raw_entry['source'].is_a?(String)
            errors.add(:zfs, "#{dataset_name}: source must be a string")
          end
          if raw_entry.key?('excludes')
            raw_excl = raw_entry['excludes']
            unless raw_excl.is_a?(Array) && raw_excl.all? { |item| item.is_a?(String) }
              errors.add(:zfs, "#{dataset_name}: excludes must be an array of strings")
            end
          end
          if raw_entry.key?('overrides')
            validate_per_zfs_hash(dataset_name, 'overrides', raw_entry['overrides'], 'table of property=value pairs', 'hash of property=value pairs')
          end
          if raw_entry.key?('ssh_config')
            validate_per_zfs_hash(dataset_name, 'ssh_config', raw_entry['ssh_config'], 'table of option=value pairs', 'hash of option=value pairs')
          end
          if raw_entry.key?('mbuffer')
            raw_mbuffer = raw_entry['mbuffer']
            unless raw_mbuffer == true || raw_mbuffer == false
              errors.add(:zfs, "#{dataset_name}: mbuffer must be a boolean")
            end
          end
          if raw_entry.key?('mbuffer_size')
            unless raw_entry['mbuffer_size'].is_a?(String)
              errors.add(:zfs, "#{dataset_name}: mbuffer_size must be a string")
            end
          end
        end
        unless zfs_obj.valid?
          errors.add(:zfs, "#{dataset_name}: #{zfs_obj.errors.full_messages.join(', ')}")
        end
      end
    end

    def validate_per_zfs_hash(dataset_name, key, raw_val, table_desc, hash_desc)
      if raw_val.is_a?(Array)
        errors.add(:zfs, "#{dataset_name}: #{key} must be a #{table_desc} (use [zfs.\"#{dataset_name}\".#{key}], not [[zfs.\"#{dataset_name}\".#{key}]])")
      elsif !raw_val.is_a?(Hash)
        errors.add(:zfs, "#{dataset_name}: #{key} must be a #{hash_desc}")
      else
        raw_val.each do |k, v|
          unless k.is_a?(String) && v.is_a?(String)
            errors.add(:zfs, "#{dataset_name}: #{key}.#{k}: property and value must be strings")
          end
        end
      end
    end
  end
end
