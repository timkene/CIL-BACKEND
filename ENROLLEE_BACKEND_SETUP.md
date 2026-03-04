# Enrollee Backend Setup Guide

## Current Status

✅ **Frontend is READY** - The React enrollee module has been created and is waiting for backend data
❌ **Backend NOT READY** - You need to create the enrollee backend files

## What You Have (Frontend)

The following frontend files have been created:

1. ✅ [frontend/src/modules/EnrolleeModule.jsx](frontend/src/modules/EnrolleeModule.jsx) - Complete enrollee UI
2. ✅ [frontend/src/modules/EnrolleeModule.css](frontend/src/modules/EnrolleeModule.css) - Styling
3. ✅ [frontend/src/components/Sidebar.jsx](frontend/src/components/Sidebar.jsx) - Module navigation
4. ✅ [frontend/src/components/Sidebar.css](frontend/src/components/Sidebar.css) - Sidebar styling
5. ✅ [frontend/src/services/api.js](frontend/src/services/api.js) - API service with enrolleeAPI
6. ✅ [frontend/src/App.jsx](frontend/src/App.jsx) - Updated with module routing
7. ✅ [frontend/src/App.css](frontend/src/App.css) - Updated with sidebar layout

## What You Need (Backend)

According to the Claude instructions you provided, you need to create these backend files:

### Required Backend Files

1. **Enrollee Service**
   - File: `services/enrollee_service.py`
   - Contains all business logic for enrollee analytics

2. **Enrollee Routes**
   - File: `api/routes/enrollees.py`
   - Contains all API endpoints

3. **Update main.py**
   - Add enrollee router to main.py

4. **Update schemas**
   - File: `api/models/schemas.py`
   - Add enrollee data models

## Backend Endpoints Required

The frontend expects these endpoints:

```
GET  /api/v1/enrollees/top-by-cost       - Top 50 enrollees by cost
GET  /api/v1/enrollees/top-by-visits     - Top 50 enrollees by visits
GET  /api/v1/enrollees/benefit-violations - Benefit limit violations
GET  /api/v1/enrollees/enrollment-stats   - Monthly enrollment statistics
GET  /api/v1/enrollees/data-quality       - Data quality metrics
GET  /api/v1/enrollees/dashboard          - All-in-one dashboard
```

## How to Set Up Backend

Based on the instructions Claude gave you, follow these steps:

### Option 1: Create Files Yourself

Create the backend files based on the implementation guide Claude provided. The files should implement:

1. **Top 50 by Cost** - Query claims + unclaimed PA, group by enrollee
2. **Top 50 by Visits** - Count distinct PA numbers per enrollee
3. **Benefit Violations** - Compare usage against benefit limits
4. **Enrollment Stats** - Count enrollees added/terminated by month
5. **Data Quality** - Count missing DOB, phone, email, address

### Option 2: Ask Claude to Create Backend

Open a chat with Claude and say:

```
I need you to create the enrollee backend module for my FastAPI app.

My database has these tables:
- all_active_members (enrollee data with firstname, surname, nhislegacynumber, groupname, date_of_birth, phone_number, email, residential_address)
- claims_data (with nhislegacynumber, approved_amount)
- preauthorizations (with nhislegacynumber, cost)

Please create:
1. services/enrollee_service.py - All business logic
2. api/routes/enrollees.py - All API endpoints
3. Update my main.py to include the enrollee router

Endpoints needed:
- GET /api/v1/enrollees/top-by-cost (top 50 by claims + unclaimed PA cost)
- GET /api/v1/enrollees/top-by-visits (top 50 by visit count)
- GET /api/v1/enrollees/benefit-violations (benefit limit violations)
- GET /api/v1/enrollees/enrollment-stats (monthly added/terminated)
- GET /api/v1/enrollees/data-quality (missing data metrics)
- GET /api/v1/enrollees/dashboard (all-in-one endpoint)

My database connection is already set up in core/database.py
```

## After Backend is Ready

### Step 1: Update main.py

Add the enrollee router:

```python
# In main.py
from api.routes import mlr, utilization, health, renewal, enrollees  # Add enrollees

# Include router
app.include_router(enrollees.router, prefix="/api/v1/enrollees", tags=["Enrollee Management"])
```

### Step 2: Start Backend

```bash
cd /Users/kenechukwuchukwuka/Downloads/DLT
python main.py
```

### Step 3: Verify Endpoints

Open browser to: http://localhost:8000/docs

You should see a new section "Enrollee Management" with 6 endpoints.

### Step 4: Test Dashboard Endpoint

```bash
curl http://localhost:8000/api/v1/enrollees/dashboard | python -m json.tool
```

Expected response:
```json
{
  "success": true,
  "dashboard": {
    "top_by_cost": [...],
    "top_by_visits": [...],
    "enrollment_statistics": {
      "added_this_month": 125,
      "terminated_this_month": 43,
      "net_change": 82,
      "active_enrollees": 15234
    },
    "data_quality": {
      "total_active_enrollees": 15234,
      "missing_data": {...}
    }
  }
}
```

### Step 5: Start Frontend

```bash
cd /Users/kenechukwuchukwuka/Downloads/DLT/frontend
npm run dev
```

### Step 6: Access App

Open browser to: http://localhost:5173

You should see:
1. Sidebar with two modules: MLR Analysis and Enrollee Management
2. Click on "Enrollee Management" to see the enrollee dashboard
3. Four stat cards showing enrollment metrics
4. Tabs for Overview, Top by Cost, Top by Visits, and Data Quality

## Quick Test Commands

### Test if backend is running:
```bash
curl http://localhost:8000/api/v1/health
```

### Test enrollee dashboard (after backend is created):
```bash
curl http://localhost:8000/api/v1/enrollees/dashboard
```

### Check frontend dev server:
```bash
cd /Users/kenechukwuchukwuka/Downloads/DLT/frontend
npm run dev
```

## Expected Frontend Behavior

When you click "Enrollee Management" in the sidebar:

1. **Loading State**: Shows spinner while fetching data
2. **Stat Cards**: Display 4 cards with:
   - ➕ Added this month (green)
   - ➖ Terminated this month (red)
   - 📈 Net change (blue)
   - 👥 Total active (purple)
3. **Tabs**: 4 tabs for different views
4. **Overview Tab**: Quick stats and data completeness progress bar
5. **Top by Cost Tab**: Table of highest-cost enrollees
6. **Top by Visits Tab**: Table of most frequent users
7. **Data Quality Tab**: Missing data metrics with cards

## Troubleshooting

### Frontend shows error "Failed to load enrollee data"
- Backend is not running or endpoints don't exist
- Check backend logs
- Verify endpoints in http://localhost:8000/docs

### "Network Error" in browser console
- Backend not running
- CORS issue (check main.py CORS settings)
- Wrong port (should be 8000 for backend)

### "404 Not Found" for enrollee endpoints
- Enrollee router not added to main.py
- Routes file not created
- Check main.py includes: `app.include_router(enrollees.router...)`

### Empty data in frontend
- Database has no data
- Query logic issue in backend
- Check backend logs for SQL errors

## Next Steps

1. ✅ Frontend is complete and ready
2. ❌ Create backend enrollee service and routes
3. ❌ Update main.py to include enrollee router
4. ❌ Test endpoints in http://localhost:8000/docs
5. ❌ Start frontend and test the enrollee module
6. ✅ Add more modules as needed (sidebar structure supports unlimited modules)

## Summary

Your React frontend is **100% ready** with:
- ✅ Modular architecture with sidebar
- ✅ Enrollee module UI complete
- ✅ API integration ready
- ✅ Beautiful responsive design
- ✅ Error handling and loading states

You just need to:
- ❌ Create the backend enrollee files
- ❌ Update main.py
- ❌ Test the integration

The frontend will automatically work once the backend endpoints are available!
