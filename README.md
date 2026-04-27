# PapaGoal ⚽

מערכת חכמה לניתוח שוק הימורים בכדורגל בזמן אמת.

## הוראות העלאה ל-Railway

### שלב 1 – GitHub
1. נכנס ל-github.com
2. לחץ "New Repository"
3. שם: `papagoal`
4. לחץ "Create Repository"
5. העלה את כל הקבצים

### שלב 2 – Railway
1. נכנס ל-railway.app
2. פתח פרויקט PapaGoal
3. לחץ על ה-PapaGoal Service
4. לחץ "Settings"
5. לחץ "Connect Repo"
6. בחר את ה-papagoal repo

### שלב 3 – Deploy
Railway יבנה ויפרוס אוטומטית.

### שלב 4 – דומיין
1. לחץ על PapaGoal Service
2. לחץ "Settings"
3. לחץ "Generate Domain"
4. קבל URL לדשבורד!

## משתני סביבה (כבר הוגדרו)
- ODDS_API_KEY
- DATABASE_URL
- PORT

## API Endpoints
- GET / – דשבורד ראשי
- GET /api/stats – סטטיסטיקות
- GET /api/signals – אותות פעילים
- GET /api/odds – יחסים אחרונים
- POST /api/goal – רישום גול ידני
- GET /health – בדיקת בריאות
