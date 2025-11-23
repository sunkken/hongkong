# TODOs for hongkong project

### Matching checks to do
#### ✔️ 1. Check isin list assigned by hkex versus the one assigned by other agencies

- https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/ISINs-assigned-by-HKEX/isinsehk.xls

- https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/ISINs-assigned-by-Other-Numbering-Agencies/isino.xls

#### ✔️ 2. Check if we get more matches from securities using standard transfer form combined by ISIN files

- https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls

#### ✔️ 3. Join data from all three files and check for duplicates or mismatches

---

### Check non-matches from Compustat Global to HKEX data
1. ✔️ Check WRDS HKEX List and match against current sample.  
   Link: https://wrds-cloud.wharton.upenn.edu/SASStudio/
2. ✔️ Get list of non-matches from Compustat Global and from HKEX sample.
3. ☐ Manually check non-matches for patterns or issues. Make more matches if possible.

---

### Follow along with plan from Mail #1 and SGX example

#### Mail #1: Här är förslag till workflow för HKEX:
1.	✔️ Masterfil med HKEX-kod–ticker–ISIN för alla bolag på HKEX som sedan kan matchas till Compustat 2015.
2.	Lista över alla filing-rubriker från HKEX-bolag per ID under tidsperioden
3.	Texten från alla filings som innehåller ”audit” som PDF:er så att vi kan identifiera audit opinions per fiscal year

#### SGX example: Sample Construction
1.	✔️ Download all listings from SGX’s web page: https://links.sgx.com/1.0.0/corporate-information/x, where x is 1 to 3,028 (as of December 12, 2024). Generate a unique list of ISINs (sgx-isins.csv) from the list (N=1,864).
2.	❓Less listings that are not regulated by SGX (ASX; GlobalQuote), out of scope (SGX Xtranet), or outdated (CLOB International) (N=1,784).
3.	✔️ Take the ISINs from sgx-isin.csv and use them to screen the Compustat Global database (/wrds/comp/sasdata/d_global/g_fundq). Generate a unique list of ISINs (compustat-isin.csv) from the resulting screen with parameters: indfmt = 'INDL', popsrc = 'I', consol = 'C', datafmt = 'HIST_STD', fyearq >= 2015,  datacqtr ne '' (Nfirms=722; Nobs=21,683).
4.	❓Less observations for fiscal 2024 (Nfirms = 722; Nobs = 20,529).
5. ☐ Manually added missing observations and delistings (Nfirms = 722; Nobs = 20,564).
6.	☐ We classify firms as quarterly reporters (qrt = 1) for each observation as follows. We use Worldscope’s WC05200 variable as our primary data source. When Worldscope data is missing, we use Bloomberg’s EARN_ANN_DT_TIME_HIST_WITH_EPS variable following Finne et al. (2024). We validate the data by manually checking approximately 40% of the observations.

| year | firms | observations | qrt     |
|------|-------|---------------|---------|
| 2015 | 679   | 2577          | 0.67831 |
| 2016 | 665   | 2507          | 0.69234 |
| 2017 | 635   | 2440          | 0.6936  |
| 2018 | 625   | 2392          | 0.69009 |
| 2019 | 611   | 2305          | 0.64757 |
| 2020 | 578   | 2199          | 0.23736 |
| 2021 | 553   | 2129          | 0.19238 |
| 2022 | 532   | 2050          | 0.19268 |
| 2023 | 508   | 1965          | 0.15937 |
