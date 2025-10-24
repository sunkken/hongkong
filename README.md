# 🐍 Hong Kong Securities Data

This project downloads and manages securities data from the Hong Kong Exchange (HKEX), including the ISIN file from:  
[HKEX ISIN List](https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/ISINs-assigned-by-Other-Numbering-Agencies/isino.xls)

---

## ⚙️ Setup

### 1. Requirements
- **Python 3.8+**
- Optional: **Git** for version control

Check:
```bash
python --version
```

---

### 2. Create & Activate Virtual Environment
**Windows**
```bash
python -m venv .venv
.\.venv\Scriptsctivate
```

**macOS / Linux**
```bash
python -m venv .venv
source .venv/bin/activate
```

---

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

---

### 4. Run the Script
```bash
python services/download_hkex_isino.py
```

The script:
- Creates `./data/raw/` if missing  
- Downloads `isino.xls`  
- Saves metadata to skip future re-downloads if unchanged  

---

### ✅ Quick Setup Summary
```bash
git clone <your-repo>
cd hongkong
python -m venv .venv
.\.venv\Scriptsactivate        # or "source .venv/bin/activate on mac/linux"
pip install -r requirements.txt
python services/download_hkex_isino.py
```
