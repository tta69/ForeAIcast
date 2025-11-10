.PHONY: import-worldcities
DB ?= postgresql://USER:PASS@localhost:5432/ForeAIcast
CSV ?= /srv/data/worldcities.csv

import-worldcities:
	./scripts/import_worldcities.sh --db "$(DB)" --csv "$(CSV)"
