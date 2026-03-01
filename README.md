# PPC Competitor Research

SERP snapshots, share-of-voice proxies, ATC drilldowns, landing page intelligence.

## Run locally

1. **Create and activate a virtual environment**
   ```bash
   python -m venv venv
   venv\Scripts\activate   # Windows
   # source venv/bin/activate   # macOS/Linux
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**
   - Copy `.env.example` to `.env` in the project root (same folder as `app.py`).
   - Set `SERPAPI_API_KEY` (required for Run target, Competitors, Auction insights, Trends). Other keys are optional.

4. **Start the app**
   ```bash
   python app.py
   ```
   **Windows:** you can double-click `run.bat` in the project root (it activates venv and runs the app).
   Open http://127.0.0.1:5000

**If you see `ModuleNotFoundError` (e.g. `No module named 'apscheduler'`):** activate the venv and run `pip install -r requirements.txt`.

## Production

- Do **not** use `python app.py` in production. Use a WSGI server, e.g.:
  ```bash
  pip install gunicorn
  gunicorn -w 4 -b 0.0.0.0:5000 "app:app"
  ```
- Set `SECRET_KEY` and `FLASK_DEBUG=0` in the environment.
- Keep `.env` out of version control (it is in `.gitignore`).

## Config reference

See `.env.example` and `config.py`. Keys are loaded from the environment; `.env` is read from the project root.
