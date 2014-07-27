require 'rubygems'
require 'mongo'
require "net/http"
require "uri"
require 'json'
require 'active_support/all'

include Mongo

ROUTES = ["academic_grades","accountants","budget_units","carreers","categories","cities","civil_organization_types","civil_organizations","companies","company_penalties","cooperative_types","cooperatives","cultural_fee_place_contacts","cultural_fee_places","cultural_fee_price_types","cultural_fee_types","cultural_fees","delation_infos","delation_institutions","delegation_infos","delegations","disabled_associations","doctor_especialities","doctors","electricity_charge_types","electricity_companies","electricity_demand_levels","electricity_prices","electricity_rate_types","executing_works","finance_types","finances","fodes_cities_transfer_infos","fodes_cities_transfers","food_establishment_areas","food_establishment_health_communities","food_establishment_types","food_establishments","fovial_companies","health_establishments","hydro_departments","hydro_establishments","hydro_municipalities","hydro_prices","hydro_references","hydro_routes","hydro_zones","information_officers","information_standard_categories","information_standard_infos","information_standard_frames","information_standards","institution_consultants","institution_event_participants","institution_events","institution_information_standards_settings","institution_officials","institution_organizational_structures","institution_remunerations","institution_service_categories","institution_service_steps","institution_services","institution_travels","institution_types","institutions","lawyers","line_of_works","medicine_categories","medicines","nationalities","occupations","procurements","product_brands","product_categories","product_presentations","product_probes","products","radial_concessions","radial_frequencies","refuges","resources_to_private_recipients","risk_prevention_consultants","roads","school_infos","schools","shopping_establishments","solvent_companies","sports_federation_transfers","sports_grants","states","syndicate_categories","syndicates","telephone_companies_concessions","universities","woman_cities","woman_city_services"]

PARENTS_REFERENCES = {}
CHILDREN_REFERENCES = {}
ONE_TO_MANY_RELATIONSHIP = []

desc "Download the data from the API and converts it into No relational data"
task :download_data do
	puts "Retrieving Data from Open Government API...."
	total = ROUTES.length
	counter = 0.0
	percent = 0.0
	ROUTES.each do |route|
		get_data_from_api(route)
		counter +=1.0
		percent = ((counter/total)*100.0).round(1)
		puts "Completed #{percent}% downloading #{route.titleize} route"
	end
end

def get_data_from_api(route)
	begin
		# Getting the data from the API
		page = 1
		parents_references = []
		while true
			url_string = "http://api.gobiernoabierto.gob.sv/#{route}?page=#{page}"
			puts url_string
			url = URI.parse(url_string)
			req = Net::HTTP::Get.new(url_string)
			req['Authorization'] = 'Token token="a1d461bec350c9a3ff62b6f684f10d5e"'
			http = Net::HTTP.new(url.host, url.port)
			res = http.request(req)
			data = JSON.parse res.body
			count = data.length
			break if count == 0
			# Parsing the data to a hash format
			store_data_on_mongo(data,route)
			page += 1
		end
	rescue Exception => e
		puts "#{route} failed"
		puts e.message
    puts e.backtrace
	end
end

def change_relational_fields_to_no_relational
	puts "Transforming Data from Relational to Documents...."
	puts "Transforming Data from parents references to Documents...."
	total = TABLES.length
	counter = 0.0
	percent = 0.0
	mongo_client = MongoClient.new("localhost", 27017)
	db = mongo_client.db("gabdata_development")
	
	ROUTES.each do |table|
		coll = db.collection(table)
		coll.keys
	end
end

def store_data_on_mongo(table, table_name)
	mongo_client = MongoClient.new("localhost", 27017)
	db = mongo_client.db("gabdata_development")
	total = table.length
	counter = 0.0
	percent = 0.0
	coll = db.collection(table_name)
	table.each do |record|
		record.delete("id")
		coll.insert(record)
	end
end
