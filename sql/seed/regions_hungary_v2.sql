-- =========================================================
-- 🌍 REGIONS – HUNGARY (Meteorológiai és gazdasági régiók) v2 (idempotens)
-- Projekt: ForeAIcast / MilyenIdőLeszHolnap.hu
-- =========================================================

-- 0) Bővítmény a kis/nagybetű-független slug-hoz
CREATE EXTENSION IF NOT EXISTS citext;

-- 1) Tábla (ha még nincs)
CREATE TABLE IF NOT EXISTS public.regions (
  id SERIAL PRIMARY KEY,
  country_id INTEGER NOT NULL REFERENCES public.countries(id) ON DELETE CASCADE,
  name_hu TEXT NOT NULL,
  name_en TEXT,
  slug TEXT NOT NULL,            -- később citext-té alakítjuk feltételesen
  description TEXT,
  counties TEXT[] NOT NULL,
  lat NUMERIC(8,4),
  lon NUMERIC(8,4),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- 2) slug → citext (csak ha még nem citext)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns c
    JOIN pg_type t ON t.typname = c.udt_name
    WHERE c.table_schema='public' AND c.table_name='regions'
      AND c.column_name='slug' AND t.typname='citext'
  ) THEN
    ALTER TABLE public.regions
      ALTER COLUMN slug TYPE citext;
  END IF;
END$$;

-- 3) Egyediség a slug-ra (csak ha még nincs)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.regions'::regclass
      AND conname  = 'regions_slug_key'
  ) THEN
    ALTER TABLE public.regions
      ADD CONSTRAINT regions_slug_key UNIQUE (slug);
  END IF;
END$$;

-- 4) updated_at automatikus frissítése UPDATE-re
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_regions_updated_at ON public.regions;
CREATE TRIGGER trg_regions_updated_at
BEFORE UPDATE ON public.regions
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- 5) Koordináta tartomány ellenőrzés (HU-re szabva) – csak ha még nincs
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.regions'::regclass AND conname='regions_lat_chk'
  ) THEN
    ALTER TABLE public.regions
      ADD CONSTRAINT regions_lat_chk CHECK (lat IS NULL OR (lat BETWEEN 45.5 AND 48.7));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.regions'::regclass AND conname='regions_lon_chk'
  ) THEN
    ALTER TABLE public.regions
      ADD CONSTRAINT regions_lon_chk CHECK (lon IS NULL OR (lon BETWEEN 16.0 AND 23.0));
  END IF;
END$$;

-- 6) Seed rekordok – 8 régió (idempotens betöltés)
WITH hu AS (SELECT id AS country_id FROM public.countries WHERE iso2='HU')
INSERT INTO public.regions (country_id, name_hu, name_en, slug, description, counties, lat, lon)
VALUES
  ((SELECT country_id FROM hu),'Nyugat-Dunántúl','Western Transdanubia','nyugat-dunantul',
   'Atlanti hatású, csapadékosabb, hűvösebb. Balaton-felvidéktől az Őrségig.',
   ARRAY['Győr-Moson-Sopron','Vas','Zala'], 47.0000,16.7000),
  ((SELECT country_id FROM hu),'Közép-Dunántúl','Central Transdanubia','kozep-dunantul',
   'Dombos, mérsékelt éghajlat. Balaton és Vértes–Bakony között.',
   ARRAY['Veszprém','Fejér','Komárom-Esztergom'], 47.2000,18.2000),
  ((SELECT country_id FROM hu),'Dél-Dunántúl','Southern Transdanubia','del-dunantul',
   'Melegebb mikroklíma, borvidékek. Villány, Pécs, Mecsek.',
   ARRAY['Somogy','Tolna','Baranya'], 46.1000,18.1000),
  ((SELECT country_id FROM hu),'Közép-Magyarország','Central Hungary','kozep-magyarorszag',
   'Budapest és Pest megye. Urbanizált, ipar/szolgáltatás.',
   ARRAY['Budapest','Pest'], 47.5000,19.1000),
  ((SELECT country_id FROM hu),'Észak-Magyarország','Northern Hungary','eszak-magyarorszag',
   'Hegyi klíma, hűvösebb. Bükk, Mátra, Tokaj.',
   ARRAY['Heves','Borsod-Abaúj-Zemplén','Nógrád'], 48.1000,20.3000),
  ((SELECT country_id FROM hu),'Észak-Alföld','Northern Great Plain','eszak-alfold',
   'Kontinentális, nagy hőingás. Hortobágy, Debrecen.',
   ARRAY['Szabolcs-Szatmár-Bereg','Hajdú-Bihar','Jász-Nagykun-Szolnok'], 47.4000,21.3000),
  ((SELECT country_id FROM hu),'Dél-Alföld','Southern Great Plain','del-alfold',
   'Száraz, meleg nyár. Kecskemét, Szeged, Békéscsaba.',
   ARRAY['Bács-Kiskun','Csongrád-Csanád','Békés'], 46.5000,19.9000),
  ((SELECT country_id FROM hu),'Balaton térsége','Lake Balaton Area','balaton-tersege',
   'Speciális mikroklíma. Északi part vulkanikus, déli part lapályos.',
   ARRAY['Veszprém','Somogy','Zala'], 46.8000,17.5000)
ON CONFLICT (slug) DO NOTHING;

-- 7) (Opcionális) Normalizálás: régió–megye kapcsolótábla
DO $$
BEGIN
  IF to_regclass('public.region_counties') IS NULL THEN
    EXECUTE $$CREATE TABLE public.region_counties (
      region_id INTEGER NOT NULL REFERENCES public.regions(id) ON DELETE CASCADE,
      county_id INTEGER NOT NULL REFERENCES public.counties(id) ON DELETE CASCADE,
      PRIMARY KEY (region_id, county_id)
    )$$;
  END IF;
END$$;

-- 7/a) Egyszeri migráció a regions.counties tömbből (ha még üres a kapcsoló)
INSERT INTO public.region_counties(region_id, county_id)
SELECT r.id, c.id
FROM public.regions r
CROSS JOIN LATERAL unnest(r.counties) AS n(name_hu)
JOIN public.counties c ON c.name_hu = n.name_hu
ON CONFLICT DO NOTHING;

-- 8) (Opcionális) Nézet a város → régió visszafejtéshez (megye alapján)
CREATE OR REPLACE VIEW public.city_region AS
SELECT
  ci.id         AS city_id,
  ci.name_hu    AS city_name,
  co.name_hu    AS county_name,
  r.id          AS region_id,
  r.name_hu     AS region_name,
  r.slug        AS region_slug
FROM public.cities   ci
JOIN public.counties co ON co.id = ci.county_id
JOIN public.region_counties rc ON rc.county_id = co.id
JOIN public.regions r ON r.id = rc.region_id;

-- 9) Gyors ellenőrzések
-- SELECT COUNT(*) FROM public.regions;
-- SELECT id, name_hu, slug FROM public.regions ORDER BY id;
-- SELECT r.name_hu, COUNT(*) AS counties_in_region
-- FROM public.regions r JOIN public.region_counties rc ON rc.region_id = r.id
-- GROUP BY r.name_hu ORDER BY r.name_hu;
