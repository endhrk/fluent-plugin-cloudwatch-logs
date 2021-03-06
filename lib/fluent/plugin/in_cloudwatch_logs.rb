module Fluent
  class CloudwatchLogsInput < Input
    Plugin.register_input('cloudwatch_logs', self)

    unless method_defined?(:log)
      define_method("log") { $log }
    end

    unless method_defined?(:router)
      define_method("router") { Engine }
    end

    config_param :aws_key_id, :string, :default => nil
    config_param :aws_sec_key, :string, :default => nil
    config_param :region, :string, :default => nil
    config_param :tag, :string
    config_param :log_group_name, :string, :default => nil
    config_param :log_stream_name, :string, :default => nil
    config_param :state_file, :string
    config_param :fetch_interval, :time, default: 60
    config_param :http_proxy, :string, default: nil

    def initialize
      super

      require 'aws-sdk-core'
      require 'yaml'
    end

    def configure(conf)
      super
      configure_parser(conf)
    end

    def start
      super

      options = {}
      options[:credentials] = Aws::Credentials.new(@aws_key_id, @aws_sec_key) if @aws_key_id && @aws_sec_key
      options[:region] = @region if @region
      options[:http_proxy] = @http_proxy if @http_proxy
      @logs = Aws::CloudWatchLogs::Client.new(options)

      @updated = Time.now
      @finished = false
      @mutex   = Mutex.new
      @thread = Thread.new(&method(:run))
      @monitor = Thread.new(&method(:monitor))
    end

    def shutdown
      super

      @finished = true
      @thread.join
    end

    private
    def configure_parser(conf)
      if conf['format']
        @parser = TextParser.new
        @parser.configure(conf)
      end
    end

    def next_token(group_name, stream_name)
      return nil unless File.exist?(@state_file)
      token_config = YAML.load_file(@state_file)
      if token_config[group_name].nil?
        return nil
      elsif token_config[group_name][stream_name].nil?
        return nil
      else
        return token_config[group_name][stream_name].chomp
      end
    end

    def store_next_token(group_name, stream_name, token)
      new_config = Hash.new { |hash,key| hash[key] = Hash.new {} }
      if File.exist?(@state_file)
        current_config = YAML.load_file(@state_file)
        new_config.update(current_config)
      end
      new_config[group_name][stream_name] = token
      open(@state_file, 'w') do |f|
        YAML.dump(new_config, f)
      end
    end

    def monitor
      log.info "cloudwatch_logs: monitor thread starting"
      unless @finished
          sleep @fetch_interval / 2
          @mutex.synchronize do
              log.debug "cloudwatch_logs: last update [#{@tag}/#{@log_group_name}/#{@log_stream_name}] at #{@updated}"
              now = Time.now
              if @updated < now - @interval * 2
                  log.warn "cloudwatch_logs: [#{@tag}/#{@log_group_name}/#{@log_stream_name}] thread is not working after #{@updated}. Restarting"
                  @thread.kill
                  @updated = now
                  @thread = Thread.new(&method(:run))
              end
          end
      end
    end

    def run
      log.info "cloudwatch_logs: watch thread starting"
      @next_fetch_time = Time.now

      until @finished
        if Time.now > @next_fetch_time
          @next_fetch_time += @fetch_interval

          target = []
          if @log_group_name.to_s == ''
            get_group_names.each do |group_name|
              get_stream_names(group_name).each do |stream_name|
                target.push([group_name, stream_name])
              end
            end
          elsif @log_stream_name.to_s == ''
            get_stream_names(@log_group_name).each do |stream_name|
              target.push([@log_group_name, stream_name])
            end
          else
            target = [[@log_group_name, @log_stream_name]]
          end

          log.info "cloudwatch_logs: #{target.length} streams found"

          target.each do |group_name, stream_name|
            events = get_events(group_name, stream_name)
            output_events(events)
            @mutex.synchronize do
              @updated = Time.now
            end
          end
        end
        sleep 1
      end
    end

    def get_events(group_name, stream_name)
      log.info "cloudwatch_logs: start get_events #{@tag} #{group_name}, #{stream_name}"
      request = {
        log_group_name: group_name,
        log_stream_name: stream_name
      }
      request[:next_token] = next_token(group_name, stream_name) if next_token(group_name, stream_name)
      log.info "#{@tag}: #{group_name}, #{stream_name}: #{request}"
      response = @logs.get_log_events(request)
      store_next_token(group_name, stream_name, response.next_forward_token)

      response.events
    end

    def output_events(events)
      log.info "cloudwatch_logs: start to output #{@tag} #{events.length} events"
      count = 0

      events.each do |event|
        begin
          if @parser
            record = @parser.parse(event.message)
            router.emit(@tag, record[0], record[1])
          else
            time = (event.timestamp / 1000).floor
            /(\{.*\})/ =~ event.message
            splited_message = $1
            unless splited_message.nil? || splited_message.empty?
              record = JSON.parse(splited_message)
              router.emit(@tag, time, record)
            else
	      record = JSON.generate({"message" => event.message, "@log_name" => @tag})
              log.debug "#{@tag} record=#{record}"
              router.emit(@tag, time, record)
            end
          end
          count += 1
        rescue => ex
          log.error "#{ex.message}"
          next
        end
      end
      log.info "cloudwatch_logs: end of output #{@tag} #{count}/#{events.length}"
    end

    def get_group_names
      @logs.describe_log_groups().log_groups.map{|group| group.log_group_name}
    end

    def get_stream_names(group_name)
      @logs.describe_log_streams(log_group_name: group_name).log_streams.map{|stream| stream.log_stream_name}
    end
  end
end
