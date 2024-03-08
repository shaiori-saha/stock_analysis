--non-trading-dates without weekends
DROP TABLE IF EXISTS NON_TRADE_DAYS;

SELECT EX_DATE.date::timestamp::date INTO NON_TRADE_DAYS FROM 
    (SELECT * FROM generate_series(
            (select min("EntryDate") as min_date from "DBU_DAILY_AGGR_VIEW"), 
            (select max("EntryDate") as max_date from "DBU_DAILY_AGGR_VIEW"), 
            interval '1 day') 
            AS dates(date) WHERE dates NOT IN 
            (SELECT DISTINCT "EntryDate"  FROM "DBU_DAILY_AGGR_VIEW") ) 
            AS EX_DATE(date)
WHERE (EXTRACT(DOW FROM EX_DATE.date)!=6 AND EXTRACT(DOW FROM EX_DATE.date)!=0);

SELECT * FROM (SELECT s::timestamp::date FROM generate_series(
            (select min("EntryDate") as min_date from "DBU_DAILY_AGGR_VIEW"), 
            (select max("EntryDate") as max_date from "DBU_DAILY_AGGR_VIEW"), 
            interval '1 day') as s(date) WHERE (EXTRACT(DOW FROM s.date)!=6 AND EXTRACT(DOW FROM s.date)!=0)
EXCEPT SELECT * FROM non_trade_days) AS EX_DATE(date) Order by EX_DATE desc;

-- working days
SELECT DISTINCT "EntryDate"  FROM "DBU_DAILY_AGGR_VIEW" Order by "EntryDate" desc;

-- max(top 10) volume trade in last week, month
SELECT "Mnemonic", SUM("TradedVolume") AS "TotalTrades" FROM "DBU_DAILY_AGGR_VIEW" WHERE "EntryDate">(CURRENT_DATE-8)
GROUP BY "Mnemonic" ORDER BY "TotalTrades" DESC LIMIT 25;
-- max(top 10) volume trade in in time period

SELECT DISTINCT "Mnemonic" FROM "DBU_DAILY_AGGR_VIEW";

SELECT "Mnemonic", Max("EntryDate") AS "LastTraded" FROM "DBU_DAILY_AGGR_VIEW" GROUP BY "Mnemonic"
HAVING Max("EntryDate")<(CURRENT_DATE-8);

DO $$ 
DECLARE 
LDATE DATE:= TO_DATE('20191225','YYYYMMDD');
BEGIN
EXECUTE 'CREATE TABLE "RETIRED_STOCKS" AS SELECT "Mnemonic", Max("EntryDate") AS "LastTraded" FROM "DBU_DAILY_AGGR_VIEW" GROUP BY "Mnemonic"
HAVING Max("EntryDate")< $1' USING LDATE;
END $$;

-- max(top 10) in(de-)crease in value from date
-- max(top 10) relative in(de-)crease in value from date

-- num trades for each stock current week, month
-- price change for each stock current week, month
SELECT a."Mnemonic", (b."StartPrice" - a."StartPrice") as "Gain" FROM "DBU_DAILY_AGGR_VIEW" a 
JOIN "DBU_DAILY_AGGR_VIEW" b ON a."Mnemonic" = b."Mnemonic" 
WHERE b."EntryDate"=(CURRENT_DATE-2) AND a."EntryDate"=(CURRENT_DATE-8) ORDER BY "Gain" DESC
;

SELECT a."Mnemonic", ((b."StartPrice" - a."StartPrice") / (a."StartPrice"))  as "Relative Gain" FROM "DBU_DAILY_AGGR_VIEW" a 
JOIN "DBU_DAILY_AGGR_VIEW" b ON a."Mnemonic" = b."Mnemonic" 
WHERE b."EntryDate"=(CURRENT_DATE-4) AND a."EntryDate"=(CURRENT_DATE-28) ORDER BY "Relative Gain" DESC
;

-- volatility calculation for each stock last 90 days
SELECT "Mnemonic", SUM("StartPrice" ^ 2)/COUNT(1) - (AVG("StartPrice") ^ 2) AS VOLATILITY
FROM "DBU_DAILY_AGGR_VIEW"  WHERE "EntryDate">(CURRENT_DATE-90) GROUP BY "Mnemonic";

-- data integrity copy end price to all 4 price if trade absent - procedure + cursor
CREATE OR REPLACE PROCEDURE ssss(inout stock1 TEXT)
AS $$
DECLARE
all_stocks CURSOR FOR SELECT DISTINCT "Mnemonic" FROM "DBU_DAILY_AGGR_VIEW";
cur_stock RECORD;
stocks TEXT DEFAULT '';
BEGIN
    OPEN all_stocks;
    LOOP
        FETCH all_stocks INTO cur_stock;
        EXIT WHEN NOT FOUND;

        stocks := stocks || ',' ||cur_stock."Mnemonic";
     END LOOP;

     CLOSE all_stocks;
     stock1=stocks;

END;
$$ LANGUAGE plpgsql;

CALL ssss('');

CALL fill_untraded_days();

SELECT DISTINCT "EntryDate" FROM "DBU_DAILY_AGGR_VIEW"
WHERE "EntryDate" NOT IN (
SELECT DISTINCT "EntryDate" FROM "DBU_DAILY_AGGR_VIEW" WHERE "Mnemonic"='WHT1')
AND "EntryDate">(SELECT min("EntryDate") FROM "DBU_DAILY_AGGR_VIEW" WHERE "Mnemonic"='WHT1')
ORDER BY "EntryDate" desc
;

------------------------------------------------------

CREATE OR REPLACE PROCEDURE fill_untraded_days()
LANGUAGE plpgsql
AS $$
DECLARE
    all_stocks CURSOR FOR SELECT "Mnemonic", SUM("TradedVolume" * "StartPrice") AS 
    "TotalTrades" FROM "DBU_DAILY_AGGR_VIEW" WHERE "EntryDate">(CURRENT_DATE-90)
    GROUP BY "Mnemonic" ORDER BY "TotalTrades" DESC LIMIT 250;
    cur_stock RECORD;
    stocks TEXT DEFAULT '';
    missed_dates RECORD;
    last_trade_date RECORD;

    missing_dates CURSOR (stock TEXT) FOR SELECT DISTINCT "EntryDate" FROM "DBU_DAILY_AGGR_VIEW"
    WHERE "EntryDate" NOT IN (
    SELECT DISTINCT "EntryDate" FROM "DBU_DAILY_AGGR_VIEW" WHERE "Mnemonic"= stock)
    AND "EntryDate">(SELECT min("EntryDate") FROM "DBU_DAILY_AGGR_VIEW" WHERE "Mnemonic"= stock)
    AND "EntryDate"<(SELECT max("EntryDate") FROM "DBU_DAILY_AGGR_VIEW" WHERE "Mnemonic"= stock)
    ORDER BY "EntryDate" desc;

    last_traded_day CURSOR (missed_date DATE, stock TEXT) FOR SELECT * 
    FROM "DBU_DAILY_AGGR_VIEW"
    WHERE "EntryDate" < missed_date AND "Mnemonic"= stock
    ORDER BY "EntryDate" desc LIMIT 1;

BEGIN
    OPEN all_stocks;
    LOOP
        FETCH all_stocks INTO cur_stock;
        EXIT WHEN NOT FOUND;

        OPEN missing_dates(cur_stock."Mnemonic");
        LOOP
            FETCH missing_dates INTO missed_dates;
            EXIT WHEN NOT FOUND;

                OPEN last_traded_day(missed_dates."EntryDate", cur_stock."Mnemonic");
                FETCH last_traded_day INTO last_trade_date;
                INSERT INTO "DBU_DAILY_AGGR_VIEW" VALUES(cur_stock."Mnemonic",
                            last_trade_date."EndPrice", last_trade_date."EndPrice",
                            last_trade_date."EndPrice", last_trade_date."EndPrice",
                            0, 0, missed_dates."EntryDate");
                CLOSE last_traded_day;
        END LOOP;
        CLOSE missing_dates;
        
     END LOOP;

     CLOSE all_stocks;
    COMMIT;

END;
$$;

-- weekly aggregate -> Materialized view + refresh concurrent

SELECT "Mnemonic", MIN("MinPrice"), MAX("MaxPrice"), SUM("TradedVolume"), SUM("NumTrades"),  
CONCAT(TO_CHAR(DATE_PART('isoyear',"EntryDate"), '9999'), 
 TO_CHAR(DATE_PART('week',"EntryDate"), '999')) AS "NoWeek" FROM "DBU_DAILY_AGGR_VIEW" WHERE "EntryDate">(CURRENT_DATE-90)
    GROUP BY "Mnemonic","NoWeek" limit 25;

--------------------------------------------------------------- working
DROP MATERIALIZED VIEW "DBU_WEEKLY_AGGR_VIEW";

DROP VIEW temp_view_week_start_end_price;

DROP VIEW temp_view_week_start_price; 
DROP VIEW temp_view_week_start_date;

CREATE VIEW temp_view_week_start_date AS SELECT "Mnemonic", MIN("EntryDate") as "StartDate", 
CONCAT(TO_CHAR(DATE_PART('isoyear',"EntryDate"), '9999'), 
 TO_CHAR(DATE_PART('week',"EntryDate"), '000')) AS "NoWeek" FROM "DBU_DAILY_AGGR_VIEW" 
WHERE "EntryDate">(CURRENT_DATE-380) GROUP BY "Mnemonic","NoWeek" ORDER BY "NoWeek";


CREATE VIEW temp_view_week_start_price AS SELECT a."Mnemonic", MIN(b."StartPrice") as "StartPrice",
CONCAT(TO_CHAR(DATE_PART('isoyear',MIN(a."StartDate")), '9999'), 
 TO_CHAR(DATE_PART('week',MIN(a."StartDate")), '000')) AS "NoWeek" FROM temp_view_week_start_date a
JOIN "DBU_DAILY_AGGR_VIEW" b ON  a."Mnemonic"=b."Mnemonic" AND a."StartDate" = b."EntryDate"
WHERE a."StartDate">(CURRENT_DATE-380) AND b."EntryDate">(CURRENT_DATE-380) 
    GROUP BY a."Mnemonic","NoWeek" ORDER BY "NoWeek" desc;
	
DROP VIEW temp_view_week_end_price;
DROP VIEW temp_view_week_end_date; 

CREATE VIEW temp_view_week_end_date AS SELECT "Mnemonic", MAX("EntryDate") as "EndDate", 
CONCAT(TO_CHAR(DATE_PART('isoyear',"EntryDate"), '9999'), 
 TO_CHAR(DATE_PART('week',"EntryDate"), '000')) AS "NoWeek" FROM "DBU_DAILY_AGGR_VIEW" 
WHERE "EntryDate">(CURRENT_DATE-380) GROUP BY "Mnemonic","NoWeek" ORDER BY "NoWeek";


CREATE VIEW temp_view_week_end_price AS SELECT a."Mnemonic", MIN(b."EndPrice") as "EndPrice",
CONCAT(TO_CHAR(DATE_PART('isoyear',MIN(a."EndDate")), '9999'), 
 TO_CHAR(DATE_PART('week',MIN(a."EndDate")), '000')) AS "NoWeek" FROM temp_view_week_end_date a
JOIN "DBU_DAILY_AGGR_VIEW" b ON  a."Mnemonic"=b."Mnemonic" AND a."EndDate" = b."EntryDate"
WHERE a."EndDate">(CURRENT_DATE-380) AND b."EntryDate">(CURRENT_DATE-380) 
    GROUP BY a."Mnemonic","NoWeek" ORDER BY "NoWeek" desc ;

CREATE VIEW temp_view_week_start_end_price AS SELECT a."Mnemonic", 
a."StartPrice" as "StartPrice", b."EndPrice" as "EndPrice", a."NoWeek" as "NoWeek" FROM temp_view_week_start_price a 
JOIN temp_view_week_end_price b ON a."Mnemonic"=b."Mnemonic" AND a."NoWeek" = b."NoWeek";

DROP VIEW temp_view_week_min_max_vol;

CREATE VIEW temp_view_week_min_max_vol AS
SELECT "Mnemonic", MIN("MinPrice") as "MinPrice", 
MAX("MaxPrice") as "MaxPrice", SUM("TradedVolume") as "TradedVolume", 
SUM("NumTrades") as "NumTrades",  
CONCAT(TO_CHAR(DATE_PART('isoyear',"EntryDate"), '9999'), 
 TO_CHAR(DATE_PART('week',"EntryDate"), '000')) AS "NoWeek" 
 FROM "DBU_DAILY_AGGR_VIEW" WHERE "EntryDate">(CURRENT_DATE-380)
    GROUP BY "Mnemonic","NoWeek";

CREATE MATERIALIZED VIEW "DBU_WEEKLY_AGGR_VIEW" AS 
SELECT a."Mnemonic" as "Mnemonic", MIN(a."MinPrice") as "MinPrice", MAX(a."MaxPrice") as "MaxPrice", 
MIN(b."StartPrice") as "StartPrice", MIN(b."EndPrice") as "Endprice", 
SUM(a."TradedVolume") as "TradedVolume", SUM(a."NumTrades") as "NumTrades",  
a."NoWeek" AS "NoWeek" FROM temp_view_week_min_max_vol a 
JOIN temp_view_week_start_end_price b ON a."Mnemonic"=b."Mnemonic" AND a."NoWeek" = b."NoWeek"
GROUP BY a."Mnemonic", a."NoWeek" ORDER BY a."NoWeek" DESC WITH NO DATA;

-- REFRESH MATERIALIZED VIEW CONCURRENTLY "DBU_WEEKLY_AGGR_VIEW";

REFRESH MATERIALIZED VIEW "DBU_WEEKLY_AGGR_VIEW";

-- get top 10 companies for a given day
SELECT "Mnemonic", SUM("TradedVolume") AS "TotalTrades" FROM "DBU_DAILY_AGGR_VIEW" WHERE "EntryDate"='2020-06-23'
GROUP BY "Mnemonic" ORDER BY "TotalTrades" DESC LIMIT 10;


