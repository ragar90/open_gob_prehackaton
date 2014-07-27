require "net/http"
require "uri"
require 'json'
require 'active_support/all'

TABLES = {}
PARENTS_REFERENCES = {}
CHILDREN_REFERENCES = {}
ONE_TO_MANY_RELATIONSHIP = []

def pull_data(route,page = 1)
	url_string = "http://api.gobiernoabierto.gob.sv/#{route}?page=#{page}"
	puts url_string
	url = URI.parse(url_string)
	req = Net::HTTP::Get.new(url_string)
	req['Authorization'] = 'Token token="a1d461bec350c9a3ff62b6f684f10d5e"'
	http = Net::HTTP.new(url.host, url.port)
	res = http.request(req)
	data = JSON.parse res.body
end