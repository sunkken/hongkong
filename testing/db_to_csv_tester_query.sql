WITH base_query AS (
    SELECT 
        fq.*,
        ha.hkex_stock_code as aof_stock_code,
        ha.aof_qualified_op,
        ha.aof_adverse_op,
        ha.aof_disclaimer_op,
        ha.aof_emphasis_op,
        ha.aof_material_unc_op,
        ha."Unnamed: 16" as aof_comments
    FROM 
        funda_q_isin fq
    LEFT JOIN 
        hkex_auditor_reports_classified ha 
        ON fq.gvkey = ha.cs_gvkey 
        AND fq.datacqtr = ha.cs_datacqtr
        AND ha.cs_fqtr = 4
    GROUP BY 
        fq.gvkey, fq.datacqtr
),
gem_latest AS (
    SELECT 
        stock_code,
        MAX(listing_date) as max_listing_date
    FROM 
        hkex_gem
    GROUP BY 
        stock_code
),
main_latest AS (
    SELECT 
        stock_code,
        MAX(listing_date) as max_listing_date
    FROM 
        hkex_main
    GROUP BY 
        stock_code
)
SELECT 
    bq.*,
    COALESCE(gem.source_file, main.source_file) as hk_source_file,
    COALESCE(gem.stock_code, main.stock_code) as hk_stock_code,
    COALESCE(gem.company, main.company) as hk_company,
    COALESCE(gem.listing_date, main.listing_date) as hk_listing_date,
    COALESCE(gem.sponsors, main.sponsors) as hk_sponsors,
    COALESCE(gem.reporting_accountant, main.reporting_accountant) as hk_reporting_accountant,
    COALESCE(gem.company_isino, main.company_isino) as hk_company_isino,
    COALESCE(gem.isin, main.isin) as hk_isin,
    COALESCE(gem.stock_type, main.stock_type) as hk_stock_type,
    COALESCE(gem.place_of_incorporation, main.place_of_incorporation) as hk_place_of_incorporation,
    COALESCE(gem.national_agency, main.national_agency) as hk_national_agency,
    COALESCE(gem.hkex_co_name, main.hkex_co_name) as hk_hkex_co_name,
    gem.offer_price as gem_offer_price,
    gem.subscription_ratio as gem_subscription_ratio,
    gem.funds_raised as gem_funds_raised,
    gem.shrout_at_listing as gem_shrout_at_listing,
    gem.mcap_at_listing as gem_mcap_at_listing,
    gem.industry as gem_industry,
    gem.listing_method as gem_listing_method,
    gem.place_of_incorporation_isino as gem_place_of_incorporation_isino,
    main.prospectus_date as main_prospectus_date,
    main.valuers as main_valuers,
    main.subscription_price as main_subscription_price,
    main.funds_raised_hk as main_funds_raised_hk,
    main.funds_raised_intl as main_funds_raised_intl,
    main.funds_raised_sg as main_funds_raised_sg
FROM 
    base_query bq
LEFT JOIN 
    hkex_gem gem 
    ON bq.aof_stock_code = gem.stock_code
    AND gem.listing_date = (SELECT max_listing_date FROM gem_latest WHERE stock_code = gem.stock_code)
LEFT JOIN 
    hkex_main main 
    ON bq.aof_stock_code = main.stock_code
    AND main.listing_date = (SELECT max_listing_date FROM main_latest WHERE stock_code = main.stock_code)
ORDER BY 
    bq.gvkey, bq.datadate;
