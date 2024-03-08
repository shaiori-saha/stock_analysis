CREATE DATABASE "DBU_COMMON_STOCK";
\c "DBU_COMMON_STOCK";

CREATE TABLE "DBU_DAILY_AGGR_VIEW" (
        "Mnemonic" VARCHAR(10), 
        "MinPrice" REAL, 
        "MaxPrice" REAL, 
        "StartPrice" REAL, 
        "EndPrice" REAL, 
        "TradedVolume" INTEGER,
        "NumTrades" INTEGER, 
        "EntryDate" DATE
        )
