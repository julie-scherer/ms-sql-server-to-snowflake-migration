utils-dict:
	cd staging_utils; \
	source .venv/bin/activate; \
	python3 ddl_to_dict/create_creo_utils_dict.py;\

utils-csv:
	cd staging_utils; \
	source .venv/bin/activate; \
	python3 dict_to_csv/export_json_to_csv.py;\


# CREATE TABLE and COPY INTO Worksheets for testing
create-dev:
	export TESTING=True; \
	python3 generate_ddl/write_sf_ddl_worksheet.py; \

copy-dev:
	export TESTING=True; \
	python3 generate_ddl/write_sf_copy_into_worksheet.py; \

# CREATE TABLE and COPY INTO Worksheets for production
create-prod:
	export TESTING=False; \
	python3 generate_ddl/write_sf_ddl_worksheet.py; \

copy-prod:
	export TESTING=False; \
	python3 generate_ddl/write_sf_copy_into_worksheet.py; \
