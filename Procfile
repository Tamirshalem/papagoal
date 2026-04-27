web: python -c "import main; main.init_db()" && gunicorn main:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120
