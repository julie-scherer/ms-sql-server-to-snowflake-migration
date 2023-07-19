create-dev:
	export TESTING=True; \
	python3 scripts/write_sf_ddl_worksheet.py; \

create-prod:
	export TESTING=False; \
	python3 scripts/write_sf_ddl_worksheet.py; \

copy-dev:
	export TESTING=True; \
	python3 scripts/write_sf_copy_into_worksheet.py; \

copy-prod:
	export TESTING=False; \
	python3 scripts/write_sf_copy_into_worksheet.py; \
