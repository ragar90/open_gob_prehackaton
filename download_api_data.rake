require 'rubygems'
require 'mongo'
require "net/http"
require "uri"
require 'json'

ROUTES = ["academic_grades","accountants","budget_units","carreers","categories","cities","civil_organization_types","civil_organizations","companies","company_penalties","cooperative_types","cooperatives","cultural_fee_place_contacts","cultural_fee_places","cultural_fee_price_types","cultural_fee_types","cultural_fees","delation_infos","delation_institutions","delegation_infos","delegations","disabled_associations","doctor_especialities","doctors","documents","electricity_charge_types","electricity_companies","electricity_demand_levels","electricity_prices","electricity_rate_types","executing_works","finance_types","finances","fodes_cities_transfer_infos","fodes_cities_transfers","food_establishment_areas","food_establishment_health_communities","food_establishment_types","food_establishments","fovial_companies","health_establishments","hydro_departments","hydro_establishments","hydro_municipalities","hydro_prices","hydro_references","hydro_routes","hydro_zones","information_officers","information_standard_categories","information_standard_infos","information_standard_frames","information_standards","institution_consultants","institution_event_participants","institution_events","institution_information_standards_settings","institution_officials","institution_organizational_structures","institution_remunerations","institution_service_categories","institution_service_steps","institution_services","institution_travels","institution_types","institutions","lawyers","line_of_works","medicine_categories","medicines","nationalities","occupations","procurements","product_brands","product_categories","product_presentations","product_probes","products","radial_concessions","radial_frequencies","refuges","resources_to_private_recipients","risk_prevention_consultants","roads","school_infos","schools","shopping_establishments","solvent_companies","sports_federation_transfers","sports_grants","states","syndicate_categories","syndicates","telephone_companies_concessions","universities","woman_cities","woman_city_services"]
tables = {}
dependent_relationships = {}

namespace :open_gob do
	desc "Download the data from the API and converts it into No relational data"
	task :download_data do
		ROUTES.each do |route|
			get_data_from_api(route)
		end
	end
end

def get_data_from_api(route)
	# Getting the data from the API	
	url_string = "http://api.gobiernoabierto.gob.sv/#{route}"
	url = URI.parse(url_string)
	req = Net::HTTP::Get.new(url.path)
	req['Authorization'] = 'Token token="a1d461bec350c9a3ff62b6f684f10d5e"'
	http = Net::HTTP.new(url.host, url.port)
	res = http.request(req)
	data = JSON.parse res.body
	# Parsing the data to a hash format
	table = data.map{|record| [record["id"],record] }
	# Storing data on hash collection
	tables[route] = table
	# Storing relationships on hash collection
	dependent_relationships[route] = data.first.keys.select{ |k| k=~/\w+_id/ }.map{|relation| relation.gsub("_id","")}
end

def change_relational_fields_to_no_relational
	tables.each_pair do |name, table|
		table.values.each do |record|
			dependent_relationships[name].each do |relation|
				id = record["#{relation}_id"]
				record[relation] =  tables[relation][id]
			end
		end
	end
end

def store_data_on_mongo
	db = Mongo::Connection.new("localhost").db("open_gob")
end
