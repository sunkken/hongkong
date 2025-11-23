# 🐍 Hong Kong Securities Data

This project downloads transforms securities data from the Hong Kong Exchange (HKEX) into a clean dataset.
## 📂 Data Sources

### Pages
[Securities Lists Page](https://www.hkex.com.hk/Services/Trading/Securities/Securities-Lists)  
[New Listings Page](https://www2.hkexnews.hk/New-Listings/New-Listing-Information/)  

### Files
[ISINs Assigned by HKEX](https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/ISINs-assigned-by-HKEX/isinsehk.xls)  
[ISINs Assigned by Other Numbering Agencies](https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/ISINs-assigned-by-Other-Numbering-Agencies/isino.xls)  
[Securities Using Standard Transfer Form](https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls)  
[Main Board New Listing Reports](https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/New-Listings/New-Listing-Information/New-Listing-Report/Main/NLR2025_Eng.xlsx) (1994-2025)  
[GEM New Listing Reports](https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/New-Listings/New-Listing-Information/New-Listing-Report/GEM/e_newlistings2025.xlsx) (1999-2025)  

---

### ⚙️ Requirements
- **Python 3.13**
- Optional: **Git** for version control

Check:
```bash
python --version
```

---

### ✅ Folder Setup
```bash
mkdir hongkong
cd hongkong
git pull git@github.com:sunkken/hongkong.git # or https://github.com/sunkken/hongkong.git if not using ssh
python -m venv .venv # Make sure you are running python 3.13 for this command!
.venv\Scripts\Activate.ps1 # or "source .venv/bin/activate on mac/linux"
pip install -r requirements.txt
python main.py
```

### 🔐 WRDS Credentials Setup

This project requires WRDS credentials for downloading additional datasets.  

1. Create a `.env` file in the project root with the following lines (it will **not** be committed to GitHub):

```text
WRDS_USER=your_username_here
WRDS_PASS=your_password_here
DB_PATH=data/hongkong.db (main.py will create db in this location if it doesn't exist)
