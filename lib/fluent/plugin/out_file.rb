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

    COMPRESS_SUPPORTED = {
      'gz'   => :gz,
      'gzip' => :gz,
      'lzop' => :lzo
    }

    COMPRESS_EXECUTABLE = {
      :gz  => 'gzip',
      :lzo => 'lzop'
    }

    COMPRESS_EXTENSION = {
      :gz  => 'gz',
      :lzo => 'lzo'
    }

    config_param :path, :string

    config_param :time_format, :string, :default => nil

    config_param :compress, :default => nil do |val|
      c = COMPRESS_SUPPORTED[val]
      unless c
        raise ConfigError, "Unsupported compression algorithm '#{val}'"
      end
      c
    end

    config_param :symlink_path, :string, :default => nil

    def initialize
      require 'time'
      require 'open3'
      super
    end

    def configure(conf)
      if path = conf['path']
        @path = path
      end
      unless @path
        raise ConfigError, "'path' parameter is required on file output"
      end

      if pos = @path.index('*')
        @path_prefix = @path[0,pos]
        @path_suffix = @path[pos+1..-1]
        conf['buffer_path'] ||= "#{@path}"
      else
        @path_prefix = @path+"."
        @path_suffix = ".log"
        conf['buffer_path'] ||= "#{@path}.*"
      end

      check_compress_method(@compress)

      super

      @timef = TimeFormatter.new(@time_format, @localtime)

      @buffer.symlink_path = @symlink_path if @symlink_path
    end

    def check_compress_method(method)
      if method
        begin
          Open3.capture3("#{COMPRESS_EXECUTABLE[method]} -V")
        rescue Errno::ENOENT
          raise ConfigError, "'#{COMPRESS_EXECUTABLE[method]}' utility must be in PATH for compression"
        end
      end
    end

    def compressed_write(chunk, path, executable)
      IO.pipe do |r,w|
        Kernel.fork do 
          w.close
          $stdin.reopen(r)
          $stdout.reopen(File.new(path,'w'))
          Kernel.exec(executable)
        end

        r.close

        chunk.write_to(w)

        w.close

        Process.wait
      end
    end

    def format(tag, time, record)
      time_str = @timef.format(time)
      "#{time_str}\t#{tag}\t#{Yajl.dump(record)}\n"
    end

    def write(chunk)
      suffix =  if @compress
                  ".#{COMPRESS_EXTENSION[@compress]}"
                else
                  ''
                end

      i = 0
      begin
        path = "#{@path_prefix}#{chunk.key}_#{i}#{@path_suffix}#{suffix}"
        i += 1
      end while File.exist?(path)
      FileUtils.mkdir_p File.dirname(path)

      if @compress
        compressed_write(chunk, path, COMPRESS_EXECUTABLE[@compress])
      else
        File.open(path, "a", DEFAULT_FILE_PERMISSION) {|f|
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
