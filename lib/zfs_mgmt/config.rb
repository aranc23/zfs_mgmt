# coding: utf-8
require 'easy_talk'
require 'tomlrb'
require 'deep_merge/core'
require 'json'
require 'zfs_mgmt/zfs'

module ZfsMgmt
  class ConfigError < StandardError; end

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
  class Config
    include EasyTalk::Model

    TOP_LEVEL_KEYS = %w[excludes overrides zfs].freeze
    JSON_SCHEMA_DRAFT = 'http://json-schema.org/draft-07/schema#'
    JSON_SCHEMA_URL = 'https://raw.githubusercontent.com/aranc23/zfs_mgmt/master/schema/config.schema.json'

    define_schema do
      title 'Config'
      description 'Top-level zfsmgr configuration loaded from TOML files'

      property :excludes, T::Array[String], optional: true,
               description: 'ZFS properties to exclude on zfs recv (-x)'
      property :overrides, T::Hash[String, String], optional: true,
               description: 'ZFS property overrides to set on zfs recv (-o property=value)'
      property :zfs, T::Hash[String, ZfsMgmt::Zfs], optional: true,
               description: 'Per-dataset ZFS configuration keyed by dataset name'
    end

    validate :validate_overrides
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

      overrides.map { |k, v| "#{k}=#{v}" }
    end

    def zfs_props_for(dataset_name)
      result = {}
      return result if dataset_name.nil? || dataset_name.empty? || @raw_zfs.empty?

      parts = dataset_name.to_s.split('/')
      ancestors = (0...parts.length).map { |i| parts[0..i].join('/') }

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

          if k == 'snapshot_recursive'
            if v == true
              result['zfsmgmt:snapshot'] = 'recursive'
              result['zfsmgmt:snapshot@source'] = source
            end
            next
          end

          prop_key = (k == 'send_enabled' ? 'send' : k)
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

      result
    end

    def self.from_hash(hash)
      new(hash || {})
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

        normalize_parsed_zfs_tables!(parsed)
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

    def normalize_zfs_entries(raw_zfs_input)
      models = {}
      deferred_raw = []

      raw_zfs_input.each do |ds_key, entry|
        ds_name = ds_key.to_s
        if entry.is_a?(ZfsMgmt::Zfs)
          @raw_zfs[ds_name] = extract_raw_from_model(entry)
          models[ds_name] = entry
        else
          collapsed = collapse_entry_hashes(entry)
          if collapsed.is_a?(Hash)
            @raw_zfs[ds_name] = filter_zfs_entry_keys(ds_name, collapsed)
            deferred_raw << ds_name
          else
            models[ds_name] = collapsed
          end
        end
      end

      deferred_raw.each do |ds_name|
        models[ds_name] = ZfsMgmt::Zfs.from_zfs_props(ds_name, zfs_props_for(ds_name))
      end

      models
    end

    def collapse_entry_hashes(entry)
      return entry unless entry.is_a?(Array) && entry.all? { |item| item.is_a?(Hash) }

      entry.each_with_object({}) do |item, acc|
        str_item = item.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
        DeepMerge.deep_merge!(str_item, acc, overwrite_arrays: true)
      end
    end

    def filter_zfs_entry_keys(ds_name, hash)
      filtered = {}
      hash.each do |k, v|
        k_str = k.to_s
        unless ZfsMgmt::Zfs::VALID_CONFIG_KEYS.include?(k_str)
          $logger&.warn("unknown config property '#{k_str}' on #{ds_name}")
          next
        end
        filtered[k_str] = v
      end
      filtered
    end

    def extract_raw_from_model(model)
      raw = {}
      provided = model.provided_attributes || Set.new
      provided.each do |attr|
        key = attr == :send_enabled ? 'send' : attr.to_s
        raw[key] = model.public_send(attr)
      end
      raw['destination'] = model.destination_base if model.destination_base
      raw
    end

    def self.normalize_parsed_zfs_tables!(parsed)
      return unless parsed['zfs'].is_a?(Hash)

      parsed['zfs'].each do |ds_name, entry|
        next unless entry.is_a?(Array) && entry.all? { |item| item.is_a?(Hash) }

        merged_entry = {}
        entry.each do |item|
          DeepMerge.deep_merge!(item, merged_entry, overwrite_arrays: true)
        end
        parsed['zfs'][ds_name] = merged_entry
      end
    end
    private_class_method :normalize_parsed_zfs_tables!

    def validate_overrides
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

    def validate_zfs_entries
      unless zfs.is_a?(Hash)
        errors.add(:zfs, 'must be a hash keyed by dataset name')
        return
      end

      zfs.each do |dataset_name, zfs_obj|
        unless zfs_obj.is_a?(ZfsMgmt::Zfs)
          errors.add(:zfs, "#{dataset_name}: must be a Zfs configuration table")
          next
        end
        unless zfs_obj.valid?
          errors.add(:zfs, "#{dataset_name}: #{zfs_obj.errors.full_messages.join(', ')}")
        end
      end
    end
  end
end
