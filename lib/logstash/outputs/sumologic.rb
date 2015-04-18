# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"

class LogStash::Outputs::Sumologic < LogStash::Outputs::Base
  # Got a Sumologic account? This output lets you `POST` events to your Sumologic. 
  #

  config_name "sumologic"

  # The hostname to send logs to. This should target the sumologic http input
  # server which is usually "collectors.sumologic.com"
  config :host, :validate => :string, :default => "collectors.sumologic.com"

  # The path to use to to send logs to. 
  config :path, :validate => :string, :default => "/receiver/v1/http/"

  # The sumologic http input key to send to.
  # This is visible in the Sumologic hosted http source page as something like this:
  # ....
  #     https://collectors.sumologic.com/receiver/v1/http/8TU2xK1CFVu8UT.....jhScYADl8U_SqmiyD2tA==
  #                                                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  #                                                       \---------->     key      <-------------/
  # ....
  # You can use `%{foo}` field lookups here if you need to pull the api key from
  # the event. This is mainly aimed at multitenant hosting providers who want
  # to offer shipping a customer's logs to that customer's loggly account.
  config :key, :validate => :string, :required => true
 
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
 # URL to use
  config :url, :validate => :string, :required => :true

  # What verb to use
  # only put and post are supported for now
  config :http_method, :validate => ["post"], :required => :true

  # Content type
  #
  # If not specified, this defaults to the following:
  #
  # * if format is "json", "application/json"
  # * if format is "message", "text/plain"
  # * if format is "form", "application/x-www-form-urlencoded"
  config :content_type, :validate => :string

  # This lets you choose the structure and parts of the event that are sent.
  #
  #
  # For example:
  # [source,ruby]
  #    mapping => ["foo", "%{host}", "bar", "%{type}"]
  config :mapping, :validate => :hash

  # Set the format of the http body.
  #
  # If form, then the body will be the mapping (or whole event) converted
  # into a query parameter string, e.g. `foo=bar&baz=fizz...`
  #
  # If message, then the body will be the result of formatting the event according to message
  #
  # Otherwise, the event is sent as json.
  config :format, :validate => ["json", "form", "message"], :default => "json"

  config :message, :validate => :string


  public
  def register
    require "ftw"
    require "uri"
    @agent = FTW::Agent.new

    if @content_type.nil?
      case @format
        when "form" ; @content_type = "application/x-www-form-urlencoded"
        when "json" ; @content_type = "application/json"
        when "message" ; @content_type = "text/plain"
      end
    end
    if @format == "message"
      if @message.nil?
        raise "message must be set if message format is used"
      end
      unless @mapping.nil?
        @logger.warn "mapping is not supported and will be ignored if message format is used"
      end
    end
  end # def register

  public
  def receive(event)
    return unless output?(event)
 
    if event == LogStash::SHUTDOWN
      finished
      return
    end
	
	
    if @mapping
      evt = Hash.new
      @mapping.each do |k,v|
        evt[k] = event.sprintf(v)
      end
    else
      evt = event.to_hash
    end
	
	# Send the event over https.
    url = URI.parse("https://#{@host}#{@path}#{event.sprintf(@key)}")
    @logger.info("Sumologic URL", :url => url)
	
    request = @agent.post(event.sprintf(@url))

 
    request["Content-Type"] = @content_type

    begin
      if @format == "json"
        request.body = LogStash::Json.dump(evt)
      elsif @format == "message"
        request.body = event.sprintf(@message)
      else
        request.body = encode(evt)
      end
      #puts "#{request.port} / #{request.protocol}"
      #puts request
      #puts
      #puts request.body
      response = @agent.execute(request)

      # Consume body to let this connection be reused
      rbody = ""
      response.read_body { |c| rbody << c }
      #puts rbody
    rescue Exception => e
      @logger.warn("Unhandled exception", :request => request, :response => response, :exception => e, :stacktrace => e.backtrace)
    end
  end # def receive

  def encode(hash)
    return hash.collect do |key, value|
      CGI.escape(key) + "=" + CGI.escape(value)
    end.join("&")
  end # def encode
end
