#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
module Fluent
  class FileOutput < TimeSlicedOutput
    Plugin.register_output('file', self)

    SUPPORTED_COMPRESS = {
      'gz' => :gz,
      'gzip' => :gz,
    }
  
    CHUNK_ID_PLACE_HOLDER = '${chunk_id}'

    config_param :path, :string

    config_param :time_format, :string, :default => nil

    config_param :compress, :default => nil do |val|
      c = SUPPORTED_COMPRESS[val]
      unless c
        raise ConfigError, "Unsupported compression algorithm '#{val}'"
      end
      c
    end

    config_param :symlink_path, :string, :default => nil

    def initialize
      require 'zlib'
      require 'time'
      super
    end

    def configure(conf)
      if path = conf['path']
        @path = path
      end
      unless @path
        raise ConfigError, "'path' parameter is required on file output"
      end

      #if pos = @path.index('*')
        #@path_prefix = @path[0,pos]
        #@path_suffix = @path[pos+1..-1]
        #conf['buffer_path'] ||= "#{@path}"
      #else
        #@path_prefix = @path+"."
        #@path_suffix = ".log"
        #conf['buffer_path'] ||= "#{@path}.*"
      #end

      if @compress == :gz
        begin
          Open3.capture3('gzip -V')
        rescue Errno::ENOENT
          raise ConfigError, "'gzip' utility must be in PATH for compression"
        end
      end

      super

      @timef = TimeFormatter.new(@time_format, @localtime)

      @buffer.symlink_path = @symlink_path if @symlink_path
    end

    def format(tag, time, record)
      time_str = @timef.format(time)
      "#{time_str}\t#{tag}\t#{Yajl.dump(record)}\n"
    end
  
    def path_format(chunk_key)
      Time.strptime(chunk_key, @time_slice_format).strftime(path)
    end

    def chunk_unique_id_to_str(unique_id)
      unique_id.unpack('C*').map{|x| x.to_s(16).rjust(2,'0')}.join('')
    end

    def write(chunk)
      suffix = case @compress
      when nil
        '.'
      when :gz
        ".gz"
      end
      

      i = 0
      begin
	path =  if path_format(chunk.key) == @path
		  "#{@path}.#{chunk.key}_#{i}#{suffix}"
		else
		  #{path_format(chunk.key)}_#{i}#{suffix}"
		end	
        i += 1
      end while File.exist?(path)
      FileUtils.mkdir_p File.dirname(path)
      
      case @compress
      when nil
        File.open(path, "a", DEFAULT_FILE_PERMISSION) {|f|
          chunk.write_to(f)
        }
      when :gz
        IO.popen('gzip', 'w', :out => [path,'w']) { |f|
          chunk.write_to(f) 
        }
      end

      return path  # for test
    end

    def secondary_init(primary)
      # don't warn even if primary.class is not FileOutput
    end
  end
end
