-- VIEW: poketcg_cards_base

CREATE OR REPLACE VIEW structured.poketcg_cards_base AS
SELECT
    rawdata->>'id'::text AS id
    , rawdata->>'name'::text AS name
    , rawdata->>'supertype'::text AS supertype
    , rawdata->>'level'::text AS level
    , rawdata->>'hp'::text AS hp
    , rawdata->>'evolvesFrom'::text AS evolvesFrom
    , COALESCE(rawdata->>'convertedRetreatCost', NULL)::integer AS convertedRetreatCost
    , rawdata->>'number'::text AS number
    , rawdata->>'artist'::text AS artist
    , rawdata->>'rarity'::text AS rarity
    , rawdata->>'flavorText'::text AS flavorText
    , rawdata->>'regulationMark'::text AS regulationMark
    , rawdata->'ancientTrait'->>'name'::text AS ancientTrait_name
    , rawdata->'ancientTrait'->>'text'::text AS ancientTrait_text
    , rawdata->'set'->>'id'::text AS set_id
    , rawdata->'legalities'->>'standard'::text AS legalities_standard
    , rawdata->'legalities'->>'expanded'::text AS legalities_expanded
    , rawdata->'legalities'->>'unlimited'::text AS legalities_unlimited
    , rawdata->'images'->>'small'::text AS images_small
    , rawdata->'images'->>'large'::text AS images_large
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_tcgplayer

CREATE OR REPLACE VIEW structured.poketcg_cards_tcgplayer AS 
SELECT
    rawdata->>'id'::text AS card_id
    , rawdata->'tcgplayer'->>'url'::text AS url
    , rawdata->'tcgplayer'->>'updatedAt'::text AS updatedAt
    , COALESCE(rawdata->'tcgplayer'->'prices'->'normal'->>'low', NULL)::decimal AS price_normal_low
    , COALESCE(rawdata->'tcgplayer'->'prices'->'normal'->>'mid', NULL)::decimal AS price_normal_mid
    , COALESCE(rawdata->'tcgplayer'->'prices'->'normal'->>'high', NULL)::decimal AS price_normal_high
    , COALESCE(rawdata->'tcgplayer'->'prices'->'normal'->>'market', NULL)::decimal AS price_normal_market
    , COALESCE(rawdata->'tcgplayer'->'prices'->'normal'->>'directLow', NULL)::decimal AS price_normal_directLow
    , COALESCE(rawdata->'tcgplayer'->'prices'->'holofoil'->>'low', NULL)::decimal AS price_holofoil_low
    , COALESCE(rawdata->'tcgplayer'->'prices'->'holofoil'->>'mid', NULL)::decimal AS price_holofoil_mid
    , COALESCE(rawdata->'tcgplayer'->'prices'->'holofoil'->>'high', NULL)::decimal AS price_holofoil_high
    , COALESCE(rawdata->'tcgplayer'->'prices'->'holofoil'->>'market', NULL)::decimal AS price_holofoil_market
    , COALESCE(rawdata->'tcgplayer'->'prices'->'holofoil'->>'directLow', NULL)::decimal AS price_holofoil_directLow
    , COALESCE(rawdata->'tcgplayer'->'prices'->'reverseHolofoil'->>'low', NULL)::decimal AS price_reverseHolofoil_low
    , COALESCE(rawdata->'tcgplayer'->'prices'->'reverseHolofoil'->>'mid', NULL)::decimal AS price_reverseHolofoil_mid
    , COALESCE(rawdata->'tcgplayer'->'prices'->'reverseHolofoil'->>'high', NULL)::decimal AS price_reverseHolofoil_high
    , COALESCE(rawdata->'tcgplayer'->'prices'->'reverseHolofoil'->>'market', NULL)::decimal AS price_reverseHolofoil_market
    , COALESCE(rawdata->'tcgplayer'->'prices'->'reverseHolofoil'->>'directLow', NULL)::decimal AS price_reverseHolofoil_directLow
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionHolofoil'->>'low', NULL)::decimal AS price_1stEditionHolofoil_low
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionHolofoil'->>'mid', NULL)::decimal AS price_1stEditionHolofoil_mid
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionHolofoil'->>'high', NULL)::decimal AS price_1stEditionHolofoil_high
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionHolofoil'->>'market', NULL)::decimal AS price_1stEditionHolofoil_market
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionHolofoil'->>'directLow', NULL)::decimal AS price_1stEditionHolofoil_directLow
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionNormal'->>'low', NULL)::decimal AS price_1stEditionNormal_low
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionNormal'->>'mid', NULL)::decimal AS price_1stEditionNormal_mid
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionNormal'->>'high', NULL)::decimal AS price_1stEditionNormal_high
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionNormal'->>'market', NULL)::decimal AS price_1stEditionNormal_market
    , COALESCE(rawdata->'tcgplayer'->'prices'->'1stEditionNormal'->>'directLow', NULL)::decimal AS price_1stEditionNormal_directLow
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_cardmarket

CREATE OR REPLACE VIEW structured.poketcg_cards_cardmarket AS 
SELECT
    rawdata->>'id'::text AS card_id
    , rawdata->'cardmarket'->>'url'::text AS url
    , rawdata->'cardmarket'->>'updatedAt'::text AS updatedAt
    , COALESCE(rawdata->'cardmarket'->'prices'->>'averageSellPrice', NULL)::decimal AS prices_averageSellPrice
    , COALESCE(rawdata->'cardmarket'->'prices'->>'lowPrice', NULL)::decimal AS prices_lowPrice
    , COALESCE(rawdata->'cardmarket'->'prices'->>'trendPrice', NULL)::decimal AS prices_trendPrice
    , COALESCE(rawdata->'cardmarket'->'prices'->>'germanProLow', NULL)::decimal AS prices_germanProLow
    , COALESCE(rawdata->'cardmarket'->'prices'->>'suggestedPrice', NULL)::decimal AS prices_suggestedPrice
    , COALESCE(rawdata->'cardmarket'->'prices'->>'reverseHoloSell', NULL)::decimal AS prices_reverseHoloSell
    , COALESCE(rawdata->'cardmarket'->'prices'->>'reverseHoloLow', NULL)::decimal AS prices_reverseHoloLow
    , COALESCE(rawdata->'cardmarket'->'prices'->>'reverseHoloTrend', NULL)::decimal AS prices_reverseHoloTrend
    , COALESCE(rawdata->'cardmarket'->'prices'->>'lowPriceExPlus', NULL)::decimal AS prices_lowPriceExPlus
    , COALESCE(rawdata->'cardmarket'->'prices'->>'avg1', NULL)::decimal AS prices_avg1
    , COALESCE(rawdata->'cardmarket'->'prices'->>'avg7', NULL)::decimal AS prices_avg7
    , COALESCE(rawdata->'cardmarket'->'prices'->>'avg30', NULL)::decimal AS prices_avg30
    , COALESCE(rawdata->'cardmarket'->'prices'->>'reverseHoloAvg1', NULL)::decimal AS prices_reverseHoloAvg1
    , COALESCE(rawdata->'cardmarket'->'prices'->>'reverseHoloAvg7', NULL)::decimal AS prices_reverseHoloAvg7
    , COALESCE(rawdata->'cardmarket'->'prices'->>'reverseHoloAvg30', NULL)::decimal AS prices_reverseHoloAvg30
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_types

CREATE OR REPLACE VIEW structured.poketcg_cards_types AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements_text(rawdata->'types') AS types    
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_subtypes

CREATE OR REPLACE VIEW structured.poketcg_cards_subtypes AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements_text(rawdata->'subtypes') AS subtypes    
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_evolvesTo

CREATE OR REPLACE VIEW structured.poketcg_cards_evolvesTo AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements_text(rawdata->'evolvesTo') AS evolvesTo    
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_rules

CREATE OR REPLACE VIEW structured.poketcg_cards_rules AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements_text(rawdata->'rules') AS rules    
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_nationalPokedexNumbers

CREATE OR REPLACE VIEW structured.poketcg_cards_nationalPokedexNumbers AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements(rawdata->'nationalPokedexNumbers')::integer AS nationalPokedexNumbers    
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_retreatCost

CREATE OR REPLACE VIEW structured.poketcg_cards_retreatCost AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements_text(rawdata->'retreatCost') AS retreatCost    
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_abilities

CREATE OR REPLACE VIEW structured.poketcg_cards_abilities AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements(rawdata->'abilities')->>'name'::text AS ability_name
    , jsonb_array_elements(rawdata->'abilities')->>'text'::text AS ability_text
    , jsonb_array_elements(rawdata->'abilities')->>'type'::text AS ability_type    
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_attacks

CREATE OR REPLACE VIEW structured.poketcg_cards_attacks AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements(rawdata->'attacks')->>'name'::text AS attack_name
    , jsonb_array_elements(rawdata->'attacks')->>'costs'::text AS attack_costs
    , jsonb_array_elements(rawdata->'attacks')->>'text'::text AS attack_text
    , jsonb_array_elements(rawdata->'attacks')->>'damage'::text AS attack_damage
    , jsonb_array_elements(rawdata->'attacks')->>'convertedEnergyCost'::text AS attack_convertedEnergyCost  
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_weaknesses

CREATE OR REPLACE VIEW structured.poketcg_cards_weaknesses AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements(rawdata->'weaknesses')->>'type'::text AS weakness_type
    , jsonb_array_elements(rawdata->'weaknesses')->>'value'::text AS weakness_value  
FROM currentraw.poketcg_cards;

-- VIEW: poketcg_cards_resistances

CREATE OR REPLACE VIEW structured.poketcg_cards_resistances AS
SELECT
    rawdata->>'id'::text AS card_id
    , jsonb_array_elements(rawdata->'resistances')->>'type'::text AS resistance_type
    , jsonb_array_elements(rawdata->'resistances')->>'value'::text AS resistance_value  
FROM currentraw.poketcg_cards;

-- VIEW cardmarket_pricehistory

CREATE OR REPLACE VIEW structured.cardmarket_pricehistory AS
SELECT
	rawdata->>'card_id'::text AS card_id
	, to_date(jsonb_array_elements_text(rawdata->'date'), 'DD.MM.YYYY') AS reference_date
	, jsonb_array_elements_text(rawdata->'avgsellprice')::float AS avgsellprice
FROM currentraw.cardmarket_pricehistory;