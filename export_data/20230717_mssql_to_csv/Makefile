copy-dev:
	export TESTING=True; \
	python3 scripts/write_sf_copy_into_worksheet.py; \

copy-prod:
	export TESTING=False; \
	python3 scripts/write_sf_copy_into_worksheet.py; \

create-dev:
	export TESTING=True; \
	python3 scripts/write_sf_ddl_worksheet.py; \

create-prod:
	export TESTING=False; \
	python3 scripts/write_sf_ddl_worksheet.py; \

csvs:
	# python scripts/export_mssql_to_csv.py # PC
	python3 scripts/export_mssql_to_csv.py # Mac