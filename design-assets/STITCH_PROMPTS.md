# Stitch AI Prompts - Ready to Use

Copy and paste these prompts directly into Stitch AI for each module.

---

## 1. Base Design System

```
Create a modern healthcare analytics dashboard design system with:

COLORS:
- Primary: #667eea (purple-blue gradient)
- Success: #10b981 (green)
- Danger: #ef4444 (red)
- Warning: #f59e0b (amber)
- Info: #3b82f6 (blue)
- Background: #f5f7fa (light gray)
- Surface: #ffffff (white)
- Text Primary: #1f2937 (dark gray)
- Text Secondary: #6b7280 (medium gray)
- Border: #e5e7eb (light gray)

TYPOGRAPHY:
- Font Family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif
- Headings: Bold, 24px-32px, line-height 1.2
- Body: Regular, 14px-16px, line-height 1.5
- Small: Regular, 12px, line-height 1.4
- Code/Monospace: 'Monaco', 'Courier New', monospace

SPACING:
- Base unit: 4px
- Small: 8px
- Medium: 16px
- Large: 24px
- XLarge: 32px
- XXLarge: 48px

COMPONENTS:
- Buttons: Rounded 8px, padding 12px 24px, medium shadow on hover
- Cards: White background, rounded 12px, shadow: 0 2px 8px rgba(0,0,0,0.1)
- Tables: Clean borders, alternating row colors, hover effects
- Inputs: Rounded 8px, border 1px solid #e5e7eb, padding 12px, focus: blue border
- Badges: Rounded 16px, padding 4px 12px, small font

LAYOUT:
- Sidebar: Fixed left, 240px width, background #1f2937, text white
- Main content: Flexible, max-width 1400px, padding 24px, background #f5f7fa
- Grid: 12-column responsive grid, 24px gutters

STYLE:
- Modern, clean, professional healthcare theme
- Accessible (WCAG AA compliant)
- Responsive (mobile-first)
- Subtle animations and transitions
```

---

## 2. Login Page

```
Design a modern, professional login page for a healthcare analytics platform:

LAYOUT:
- Full-screen centered layout
- Background: Subtle gradient or solid color (#f5f7fa)
- Centered card: White background, rounded 16px, shadow, max-width 400px
- Logo: Top center, 120px height
- Title: "Welcome Back" (24px, bold)
- Subtitle: "Sign in to access your dashboard" (14px, gray)

FORM:
- Email input: Full width, rounded 8px, padding 12px, email icon on left
- Password input: Full width, rounded 8px, padding 12px, eye icon on right (toggle visibility)
- Remember me: Checkbox with label
- Login button: Full width, primary color (#667eea), white text, rounded 8px, padding 14px, bold
- Error message: Red text, below form, 14px
- Loading state: Spinner on button, disabled state

STYLE:
- Clean, minimal, professional
- Healthcare/medical theme
- Accessible form design
- Smooth transitions
```

---

## 3. Sidebar Navigation

```
Design a fixed left sidebar navigation for healthcare dashboard:

DIMENSIONS:
- Width: 240px
- Fixed position, full height
- Background: #1f2937 (dark gray)
- Text: White (#ffffff)

HEADER:
- Title: "Modules" (18px, bold, white)
- Subtitle: "X of Y modules" (12px, gray #9ca3af)
- Padding: 24px 16px
- Border bottom: 1px solid #374151

NAVIGATION ITEMS:
Each item:
- Icon: 20px, left side
- Label: 14px, bold, white
- Description: 12px, gray (#9ca3af)
- Padding: 12px 16px
- Border radius: 8px
- Margin: 4px 8px

ITEMS:
1. MLR Analysis (📊) - "Medical Loss Ratio"
2. Client Analysis (🏢) - "Client-level analytics"
3. Enrollee Management (👥) - "Enrollee Analytics"
4. Finances (💰) - "Payment & Invoicing"
5. PA & Claims (📋) - "Prior Authorization & Claims"
6. Admin (👥) - "User Management"

STATES:
- Default: Transparent background
- Hover: Background #374151, cursor pointer
- Active: Background #667eea (primary color), white text, left border 3px solid white

FOOTER:
- "Clearline HMO" text (12px, gray)
- Version: "v1.0.0" (10px, gray)
- Padding: 16px
- Border top: 1px solid #374151

STYLE:
- Dark theme, professional
- Clear visual hierarchy
- Smooth hover/active transitions
```

---

## 4. MLR Analysis Module

```
Design a healthcare analytics dashboard for Medical Loss Ratio (MLR) analysis:

HEADER:
- Title: "MLR Analysis" (32px, bold)
- Subtitle: "Medical Loss Ratio Overview" (16px, gray)
- Last updated: "Updated: [timestamp]" (12px, gray)

TOP METRICS (4 cards in row):
Card 1: Total Companies
- Value: Large number (32px, bold)
- Label: "Total Companies" (14px, gray)
- Icon: Building icon
- Background: White, rounded 12px, shadow

Card 2: Average MLR
- Value: Percentage (32px, bold, color-coded)
- Label: "Average MLR" (14px, gray)
- Icon: Chart icon
- Background: White, rounded 12px, shadow

Card 3: High Risk
- Value: Count (32px, bold, red)
- Label: "High Risk Companies" (14px, gray)
- Icon: Warning icon
- Background: White, rounded 12px, shadow

Card 4: Total Medical Cost
- Value: Currency ₦ (32px, bold)
- Label: "Total Medical Cost" (14px, gray)
- Icon: Money icon
- Background: White, rounded 12px, shadow

MAIN TABLE:
- White background, rounded 12px, shadow
- Header row: Dark background (#1f2937), white text, bold, 14px
- Columns:
  * Company Name (left-aligned)
  * Debit Amount (right-aligned, currency)
  * Total Medical Cost (right-aligned, currency)
  * Claims Amount (right-aligned, currency)
  * Unclaimed PA (right-aligned, currency)
  * Commission (right-aligned, currency)
  * MLR % (right-aligned, color-coded)
  * Member Count (right-aligned, number)
  * Avg PMPM (right-aligned, currency)
  * Premium PMPM (right-aligned, currency)

MLR COLOR CODING:
- Green: < 50% (low risk)
- Yellow/Amber: 50-70% (medium risk)
- Red: > 70% (high risk)

TABLE FEATURES:
- Sortable columns (click header to sort)
- Hover effect on rows (light gray background)
- Alternating row colors (subtle)
- Pagination at bottom
- Search/filter bar at top

STYLE:
- Clean, professional, data-dense but organized
- Clear visual hierarchy
- Easy to scan and analyze
```

---

## 5. Client Analysis Module

```
Design a comprehensive client profile page for healthcare analytics:

HEADER SECTION:
- Client name: Large, bold (32px)
- Contract period: "April 2025 - March 2026" (16px, gray)
- Status badge: "Active" (green) or "Inactive" (red), rounded 16px

TOP METRICS (4 cards in row):
Card 1: Total Medical Cost
- Value: Large currency ₦ (40px, bold, primary color)
- Label: "Total Medical Cost" (14px, gray)
- Trend: Up/down arrow with percentage

Card 2: Total Debit Note
- Value: Currency ₦ (32px, bold)
- Label: "Total Debit Note" (14px, gray)

Card 3: MLR %
- Value: Percentage (32px, bold, color-coded)
- Label: "Medical Loss Ratio" (14px, gray)
- Gauge/chart visualization

Card 4: Cash Received
- Value: Currency ₦ (32px, bold)
- Label: "Cash Received" (14px, gray)

TWO-COLUMN LAYOUT:
Left Column: Top 20 Procedures
- Title: "Top 20 Procedures by Cost" (18px, bold)
- Table: Procedure Code, Total Cost, Count
- White card, rounded 12px, shadow

Right Column: Top 20 Enrollees
- Title: "Top 20 Enrollees by Cost" (18px, bold)
- Table: Enrollee ID, Total Cost, Visit Count
- White card, rounded 12px, shadow

BOTTOM SECTION:
- Monthly Medical Cost: Line chart showing trend over contract period
- Top 20 Providers: Table with provider name, total cost, unique enrollees
- Benefit Limit Analysis: Table showing benefit limits and utilization

STYLE:
- Professional, data-rich but organized
- Clear sections with cards
- Easy navigation between different views
```

---

## 6. Enrollee Management Module

```
Design an enrollee management dashboard with tabbed interface:

HEADER:
- Title: "Enrollee Management" (32px, bold)
- Subtitle: "Comprehensive enrollee analytics" (16px, gray)

TAB NAVIGATION:
- Tabs: Overview | Top by Cost | Top by Visits | Data Quality
- Active tab: Primary color background, white text, bottom border 3px
- Inactive tabs: Transparent, gray text, hover effect
- Rounded top corners

TAB 1: OVERVIEW
- 4 stat cards in row:
  * Added This Month (green, + icon)
  * Terminated This Month (red, - icon)
  * Net Change (blue, arrow icon)
  * Total Active (purple, users icon)
- Each card: Large number, label, icon, white background, rounded 12px, shadow

TAB 2: TOP BY COST
- Table: Enrollee ID, Name, Total Cost, Visit Count, Company
- Sortable columns
- Currency formatting
- Hover effects

TAB 3: TOP BY VISITS
- Table: Enrollee ID, Name, Visit Count, Total Cost, Company
- Sortable columns
- Number formatting
- Hover effects

TAB 4: DATA QUALITY
- Metrics grid:
  * Missing DOB: Count and percentage
  * Missing Phone: Count and percentage
  * Missing Email: Count and percentage
  * Missing Address: Count and percentage
- Each metric: Card with icon, value, label, progress bar

STYLE:
- Clean, organized, easy to navigate
- Clear tab states
- Professional data presentation
```

---

## 7. Finance Module

```
Design a finance dashboard for healthcare payment tracking:

HEADER:
- Title: "Finance Dashboard" (32px, bold)
- Month selector: Dropdown (defaults to current month), right-aligned

MAIN TABLE:
- White background, rounded 12px, shadow
- Columns:
  * Company Name (left-aligned)
  * Cash Received (right-aligned, currency ₦)
  * Debit Note Amount (right-aligned, currency ₦)
  * Status (center-aligned, badge)
  * Actions (center-aligned, view button)

STATUS BADGES:
- "Existing": Green badge, rounded 16px
- "New": Blue badge, rounded 16px

STATUS LOGIC:
- "Existing": Company has PA or claims in last 6 months
- "New": No PA or claims history in last 6 months

TABLE FEATURES:
- Sortable columns
- Search/filter bar
- Pagination
- Hover effects on rows

SUMMARY ROW:
- Bottom of table: Totals for Cash Received and Debit Note
- Bold, highlighted background

STYLE:
- Financial theme
- Clear currency formatting
- Professional appearance
- Easy to scan
```

---

## 8. PA & Claims Module

```
Design a Prior Authorization and Claims analytics dashboard:

HEADER:
- Title: "PA & Claims Analytics" (32px, bold)
- Filters: Date range picker, client dropdown, provider dropdown, procedure dropdown
- Filter bar: Horizontal, below header, rounded 8px, white background

TAB NAVIGATION:
- Tabs: Top Clients | Top Providers | Top Procedures | PA Status
- Active tab: Primary color, white text
- Inactive tabs: Transparent, gray text

TAB 1: TOP CLIENTS
- Table: Client Name, Total Cost, Visit Count, Trend
- Sortable columns
- Currency formatting

TAB 2: TOP PROVIDERS
- Table: Provider Name, Total PA Volume, Average Cost, Trend
- Sortable columns
- Number formatting

TAB 3: TOP PROCEDURES
- Table: Procedure Code, Frequency, Total Cost, Average Cost
- Sortable columns
- Number and currency formatting

TAB 4: PA STATUS
- Pie chart: Approved, Pending, Denied
- Legend with percentages
- Summary cards below chart

STYLE:
- Analytics-focused
- Clear data visualization
- Filterable views
- Professional appearance
```

---

## 9. Admin Module

```
Design an admin panel for user and permission management:

HEADER:
- Title: "Admin Panel" (32px, bold)
- Subtitle: "User and Permission Management" (16px, gray)

TAB NAVIGATION:
- Tabs: Users | Department Permissions | Staff-Client Allocations
- Active tab: Primary color, white text
- Inactive tabs: Transparent, gray text

TAB 1: USERS MANAGEMENT
- Header: "Add User" button (primary color, right-aligned)
- Table: User ID, First Name, Last Name, Email, Department, Status, Actions
- Status badges: "Active" (green), "Terminated" (red)
- Actions column: Buttons for Terminate, Reactivate, Change Password, Delete
- Modal: Add User form (First Name, Last Name, Email, Department, Auto-generated Password)

TAB 2: DEPARTMENT PERMISSIONS
- Header: "Add Permissions" button (primary color, right-aligned)
- Table: Department, Assigned Modules (comma-separated), Actions
- Actions: Edit, Delete buttons
- Modal: Edit form with checkboxes for each module

TAB 3: STAFF-CLIENT ALLOCATIONS
- Header: "Edit Allocations" button (primary color, right-aligned)
- Table: Staff Name, Department, Allocated Clients (comma-separated), Actions
- Actions: Edit Allocations, Delete Allocations buttons
- Modal: Edit form with checkboxes for each client

STYLE:
- Admin/management theme
- Clear action buttons
- Modal forms for editing
- Professional, organized layout
```

---

## Usage Instructions

1. Copy the base design system prompt first
2. Generate the design system in Stitch
3. Then use module-specific prompts one by one
4. Reference the design system for consistency
5. Iterate with refinement prompts as needed

Good luck! 🎨
