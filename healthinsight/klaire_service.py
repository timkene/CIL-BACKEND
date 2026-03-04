from __future__ import annotations

import os
import time
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# OpenAI for natural language understanding
try:
    import openai
    from openai import OpenAI
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False


# Get the folder where this script is located
HEALTHINSIGHT_FOLDER = os.path.dirname(os.path.abspath(__file__))
# Parent directory (DLT root)
DLT_ROOT = os.path.dirname(HEALTHINSIGHT_FOLDER)

# Database paths (relative to DLT root)
DB_PATH = os.getenv("HEALTHINSIGHT_DUCKDB", os.path.join(DLT_ROOT, "ai_driven_data.duckdb"))
CHAT_DB_PATH = os.getenv("HEALTHINSIGHT_CHAT_DUCKDB", os.path.join(HEALTHINSIGHT_FOLDER, "healthinsight_chat.duckdb"))
SERVICE_NAME = "KLAIRE"


def ensure_chat_store() -> None:
    con = duckdb.connect(CHAT_DB_PATH)
    con.execute(
        """
        CREATE SCHEMA IF NOT EXISTS healthinsight;
        CREATE TABLE IF NOT EXISTS healthinsight.conversations (
            conversation_id TEXT,
            created_at TIMESTAMP,
            title TEXT
        );
        CREATE TABLE IF NOT EXISTS healthinsight.messages (
            conversation_id TEXT,
            role TEXT,
            content TEXT,
            ts TIMESTAMP
        );
        """
    )
    con.close()


ensure_chat_store()


class ChatRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = None
    # Optional filters the assistant can use against DuckDB (not required now)
    company: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class ChatResponse(BaseModel):
    conversation_id: str
    reply: str
    tokens_used: Optional[int] = None


class HistoryItem(BaseModel):
    role: str
    content: str
    ts: float


app = FastAPI(title="KLAIRE Chat Service - CLEARLINE INTERNATIONAL LIMITED", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _now_ts() -> datetime:
    return datetime.utcnow()


def _get_conversation_title(first_user_message: str) -> str:
    text = first_user_message.strip().replace("\n", " ")
    if len(text) > 64:
        text = text[:61] + "..."
    return text or "Conversation"


def save_message(conversation_id: str, role: str, content: str) -> None:
    con = duckdb.connect(CHAT_DB_PATH)
    con.execute(
        "INSERT INTO healthinsight.messages VALUES (?, ?, ?, ?)",
        [conversation_id, role, content, _now_ts()],
    )
    con.close()


def create_conversation(first_user_message: str) -> str:
    conversation_id = str(uuid.uuid4())
    title = _get_conversation_title(first_user_message)
    con = duckdb.connect(CHAT_DB_PATH)
    con.execute(
        "INSERT INTO healthinsight.conversations VALUES (?, ?, ?)",
        [conversation_id, _now_ts(), title],
    )
    con.close()
    return conversation_id


def run_duckdb_query(sql: str) -> str:
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        # Default to AI DRIVEN DATA schema so unqualified table names resolve
        try:
            con.execute('USE "AI DRIVEN DATA"')
        except Exception:
            pass
        df = con.execute(sql).fetchdf()
        con.close()
    except Exception as e:
        return f"DuckDB error: {e}"

    # Render a compact textual table (first 10 rows)
    if df.empty:
        return "No rows returned."
    preview = df.head(10)
    return preview.to_markdown(index=False)


def get_database_schema() -> str:
    """Get comprehensive database schema information with relationships"""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        
        # Get key tables with their columns
        key_tables = [
            'PA DATA', 'CLAIMS DATA', 'MEMBERS', 'PROVIDERS', 'GROUPS',
            'BENEFITCODES', 'BENEFITCODE_PROCEDURES', 'GROUP_PLANS',
            'TBPADIAGNOSIS', 'PROCEDURE DATA', 'TARIFF'
        ]
        
        schema_info = "DATABASE SCHEMA AND KEY TABLES:\n\n"
        
        for table_name in key_tables:
            try:
                # Get columns
                columns = con.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'AI DRIVEN DATA' 
                      AND table_name = '{table_name}'
                    ORDER BY ordinal_position
                """).fetchall()
                
                if columns:
                    schema_info += f"📊 {table_name}:\n"
                    for col_name, col_type in columns:
                        schema_info += f"   - {col_name} ({col_type})\n"
                    
                    # Get row count
                    try:
                        row_count = con.execute(f'SELECT COUNT(*) FROM "AI DRIVEN DATA"."{table_name}"').fetchone()[0]
                        schema_info += f"   Rows: {row_count:,}\n"
                    except:
                        pass
                    schema_info += "\n"
            except:
                pass
        
        # Key relationships
        schema_info += """
KEY RELATIONSHIPS:
- PA DATA.panumber → CLAIMS DATA.panumber (PA to claims linkage)
- PA DATA.providerid → PROVIDERS.providerid (PA to provider)
- CLAIMS DATA.enrollee_id → MEMBERS.enrollee_id (Claims to members)
- MEMBERS.groupid → GROUPS.groupid (Members to groups)
- CLAIMS DATA.panumber → TBPADIAGNOSIS.panumber (Claims to diagnoses)
- PA DATA.code → PROCEDURE DATA.procedurecode (PA to procedure details)
- TARIFF.tariffname → Used for tariff lookups
- BENEFITCODE_PROCEDURES links procedurecode to benefitcodeid
- BENEFITCODES provides benefit descriptions

IMPORTANT NOTES:
- Enrollee IDs may have variations (e.g., CL/XXX/123/2024-A vs CL/XXX/123/2024-A-E)
- PA DATA uses 'granted' column for spending amounts (not totaltariff)
- CLAIMS DATA uses 'approvedamount' for claim amounts
- Use encounterdatefrom for date filtering in CLAIMS DATA
- Use requestdate for date filtering in PA DATA
"""
        
        con.close()
        return schema_info
    except Exception as e:
        return f"Error getting schema: {e}"

def healthinsight_reasoning(message: str, context: str = "") -> str:
    """
    HEALTHINSIGHT reasoning engine - uses AI to understand questions naturally.
    This service knows about:
    - All tables in ai_driven_data.duckdb
    - Enrollee utilization, diagnoses, procedures
    - Group/company financial analysis
    - PA (Pre-Authorization) data
    - Claims data
    """
    lower = message.lower().strip()

    # Direct SQL execution (user explicitly requests SQL)
    if lower.startswith("sql:"):
        sql = message[4:].strip()
        blocked = ["insert ", "update ", "delete ", "drop ", "create ", "alter ", "attach ", "pragma "]
        if any(b in lower for b in blocked):
            return "For safety, only SELECT queries are allowed."
        return run_duckdb_query(sql)

    # Database update requests
    if "update" in lower and ("table" in lower or "database" in lower or "duckdb" in lower):
        if "all" in lower or "everything" in lower or "derived" in lower:
            try:
                import subprocess
                import sys
                update_script = os.path.join(DLT_ROOT, "auto_update_database.py")
                if not os.path.exists(update_script):
                    return f"Update script not found at {update_script}"
                
                result = subprocess.run(
                    [sys.executable, update_script],
                    cwd=DLT_ROOT,
                    capture_output=True,
                    text=True,
                    timeout=1800
                )
                
                if result.returncode == 0:
                    output_lines = result.stdout.split('\n')
                    summary_lines = [line for line in output_lines if 'DATABASE UPDATE SUMMARY' in line or 'Tables Updated' in line or 'Total Rows Updated' in line or 'Duration' in line or '✅' in line or '❌' in line]
                    summary = '\n'.join(summary_lines[-10:]) if summary_lines else result.stdout[-500:]
                    return f"✅ Database update completed successfully!\n\n{summary}"
                else:
                    return f"❌ Database update failed:\n{result.stderr}\n{result.stdout[-500:]}"
            except subprocess.TimeoutExpired:
                return "⏱️ Update is taking longer than expected. Please check the database_update.log file for progress."
            except Exception as e:
                return f"❌ Error running update: {e}"

    # Use AI to understand the question naturally
    if AI_AVAILABLE:
        try:
            # Get OpenAI API key from environment or secrets.toml
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                # Try to load from secrets.toml (same way as ai_health_analyst.py)
                try:
                    import toml
                    # Try multiple paths for secrets.toml
                    secrets_paths = [
                        os.path.join(HEALTHINSIGHT_FOLDER, 'secrets.toml'),
                        os.path.join(DLT_ROOT, 'secrets.toml'),
                        'secrets.toml'
                    ]
                    for path in secrets_paths:
                        if os.path.exists(path):
                            secrets = toml.load(path)
                            api_key = secrets.get('openai', {}).get('api_key')
                            if api_key:
                                break
                except Exception as e:
                    pass
            
            if not api_key:
                # Try to get from openai module if already set
                try:
                    api_key = openai.api_key
                except:
                    pass
            
            if not api_key:
                # Fallback to pattern matching
                return _fallback_reasoning(message, lower)
            
            client = OpenAI(api_key=api_key)
            
            # Get database schema for context
            schema_info = get_database_schema()
            
            # Load knowledge base if available
            knowledge_base = ""
            knowledge_file = os.path.join(DLT_ROOT, "CLEARLINE_KNOWLEDGE_BASE.md")
            if os.path.exists(knowledge_file):
                with open(knowledge_file, 'r') as f:
                    knowledge_base = f.read()
            
            # Enhanced system prompt matching Cursor AI reasoning style
            system_prompt = f"""You are KLAIRE, an expert AI data analyst for CLEARLINE INTERNATIONAL LIMITED health insurance data. You work exactly like a senior data analyst who understands both the technical aspects and business context.

**IMPORTANT - CONTINUOUS LEARNING**: 
- As you learn new information about Clearline's business, processes, or data during conversations, you should mentally note it
- The system will automatically save new learnings to the knowledge base after each conversation
- Always clarify and confirm understanding when the user explains something new

BUSINESS CONTEXT - CLEARLINE INTERNATIONAL LIMITED:
Clearline is a health insurance company (HMO) that partners with hospitals, takes premiums from clients (groups), and provides health insurance services to enrollees. We sell individual and family plans to clients for 1-year periods.

KEY CONCEPTS:
- Groups = Clients = groupname (all refer to the same thing)
- Enrollees = Members = Customers (IDs: legacycode, enrollee_id, IID, memberid - case insensitive)
- PA (Pre-Authorization): Hospital requests approval, we give panumber (authorization code)
- Claims: Hospital submits for payment after PA (or sometimes without PA - this is normal)
- Providers: Hospitals we partner with, each mapped to a tariff (price list)
- Plans: Individual or Family plans sold to clients, can have multiple plans per client
- Benefits: Classes/buckets of procedures with limits per plan
- Coverage: Installment-based coverage periods within contract dates

IMPORTANT RULES:
1. ALWAYS use contract dates from GROUP_CONTRACT for client analysis (unless user specifies period)
2. Use iscurrent = 1 for current data (MEMBER_PLAN, GROUP_COVERAGE, etc.)
3. Claims can exist WITHOUT panumber - this is normal (hospital-end authorization allowed)
4. Provide claims data for BOTH dates: encounterdatefrom and datesubmitted
5. One member can have multiple plans - always use iscurrent = 1
6. Coverage never exceeds contract period

DATABASE SCHEMA:
{schema_info}

{f'KNOWLEDGE BASE:\n{knowledge_base}' if knowledge_base else ''}

YOUR CAPABILITIES:
1. **Deep Understanding**: You understand database relationships, table structures, and data patterns
2. **Intelligent Querying**: You generate precise SQL queries that answer questions accurately
3. **Context Awareness**: You remember previous conversation context and build on it
4. **Domain Expertise**: You understand Clearline's health insurance business model:
   - PA (Pre-Authorization) vs Claims workflow
   - Enrollee IDs and their variations (legacycode, enrollee_id, IID, memberid)
   - Provider networks and tariff structures
   - Benefit codes and procedure mappings
   - Plan limits and utilization tracking
   - Financial analysis (spending, utilization, trends)

REASONING APPROACH:
1. **Understand Intent**: First understand what the user really wants to know
2. **Plan Query**: Think about which tables and joins are needed
3. **Generate SQL**: Create precise SQL with proper joins, filters, and aggregations
4. **Analyze Results**: Interpret the data and provide insights
5. **Explain Clearly**: Provide clear, actionable answers with context

SPECIAL KNOWLEDGE:
- PA spending uses the 'granted' column, not 'totaltariff'
- Claims spending uses 'approvedamount' column
- Enrollee IDs can have suffixes (-E, -B, ~A, etc.) - try variations if no results
- Date filtering: Use encounterdatefrom for claims, requestdate for PA
- Procedure codes link to diagnoses via panumber (TBPADIAGNOSIS table)
- Groups link to members via groupid
- Always filter by contract dates from GROUP_CONTRACT unless user specifies period

CRITICAL SQL REQUIREMENTS:
- ALWAYS use schema-qualified table names: "AI DRIVEN DATA"."TABLE NAME"
- Example: "AI DRIVEN DATA"."PA DATA" not just "PA DATA"
- Example: "AI DRIVEN DATA"."CLAIMS DATA" not just "CLAIMS DATA"
- This is required for all table references in SQL queries
- Use double quotes for schema and table names: "AI DRIVEN DATA"."PA DATA"

RESPONSE FORMAT:
- If you need to query the database, generate SQL in this format: SQL: [your query]
- After seeing results, provide a clear, natural language answer
- Include specific numbers and percentages
- Provide context and actionable insights
- Explain what the data means, not just what it shows

IMPORTANT: Only generate SELECT queries. Never generate INSERT, UPDATE, DELETE, or DDL statements."""

            # Enhanced user prompt with multi-step reasoning and context
            user_prompt = f"""{context}

Current question: {message}

Think step by step:

STEP 1: Understand the question
- What is the user really asking?
- What data is needed to answer this?
- What time period, filters, or conditions are mentioned?

STEP 2: Plan your approach
- Which tables are needed?
- What joins are required?
- What aggregations or calculations?
- Any special considerations (enrollee ID variations, date ranges, etc.)?

STEP 3: Generate SQL
- Create a precise SQL query that answers the question
- CRITICAL: Always use schema-qualified table names: "AI DRIVEN DATA"."TABLE NAME"
- Use proper joins based on relationships
- Include appropriate filters and aggregations
- **MUST format as**: SQL: [your complete SQL query]
- **DO NOT just show the SQL in a code block - you MUST use the "SQL:" prefix**
- Example: SQL: SELECT SUM(granted) FROM "AI DRIVEN DATA"."PA DATA" WHERE requestdate >= '2025-11-01' AND requestdate < '2025-12-01'
- The SQL will be executed automatically, then you'll analyze the results

STEP 4: Prepare for analysis
- After SQL executes, you'll analyze the results
- Think about what insights to provide
- Consider context and actionable recommendations

Generate the SQL query now. If no database query is needed, provide a direct answer."""

            response = client.chat.completions.create(
                model="gpt-4o",  # Using gpt-4o for better reasoning (matches Cursor AI quality)
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=3000,
                temperature=0.2  # Slightly higher for more creative problem-solving
            )
            
            ai_response = response.choices[0].message.content.strip()
            
            # Check if AI generated SQL to execute
            # Look for "SQL:" prefix or SQL code blocks
            sql_query = None
            if "SQL:" in ai_response:
                # Extract SQL query - handle multiple formats including multi-line
                sql_start = ai_response.find("SQL:") + 4
                remaining = ai_response[sql_start:].strip()
                
                # Remove code blocks if present
                if remaining.startswith('```sql'):
                    remaining = remaining[6:].strip()
                elif remaining.startswith('```'):
                    remaining = remaining[3:].strip()
                
                if remaining.endswith('```'):
                    remaining = remaining[:-3].strip()
                
                # Extract SQL query - could be multi-line
                # Find the SQL query (until next section or end)
                lines = remaining.split('\n')
                sql_lines = []
                in_sql = True
                
                for line in lines:
                    line = line.strip()
                    # Stop if we hit a blank line followed by non-SQL content
                    if not line:
                        # Check if next line looks like explanation
                        if sql_lines:  # We have some SQL already
                            break
                        continue
                    
                    # Stop if line looks like explanation (starts with word, not SQL keyword)
                    if in_sql and line and not any(line.upper().startswith(kw) for kw in 
                        ['SELECT', 'WITH', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 
                         'GROUP', 'ORDER', 'HAVING', 'UNION', 'EXCEPT', 'INTERSECT', '--', '/*']):
                        # Might be explanation, but check if it's part of SQL (e.g., column alias)
                        if not any(char in line for char in [',', '(', ')', '=', '>', '<', 'AND', 'OR']):
                                break
                
                    sql_lines.append(line)
                
                sql_query = ' '.join(sql_lines).strip()
                
                # Clean up common SQL formatting issues
                sql_query = sql_query.replace('\n', ' ').replace('  ', ' ')
            elif '```sql' in ai_response.lower() or '```' in ai_response:
                # Extract SQL from code blocks even without "SQL:" prefix
                # Look for SQL code blocks
                import re
                sql_pattern = r'```(?:sql)?\s*(SELECT.*?)(?:```|$)'
                matches = re.findall(sql_pattern, ai_response, re.DOTALL | re.IGNORECASE)
                if matches:
                    sql_query = matches[0].strip()
                    # Clean up
                    sql_query = sql_query.replace('\n', ' ').replace('  ', ' ')
            elif 'SELECT' in ai_response.upper() and 'FROM' in ai_response.upper():
                # Try to extract SQL even if it's embedded in explanation
                import re
                # Look for SELECT ... FROM ... pattern
                sql_match = re.search(r'(SELECT\s+.*?FROM\s+["\']?[^"\']+["\']?\s+.*?)(?:;|\n\n|\Z)', ai_response, re.DOTALL | re.IGNORECASE)
                if sql_match:
                    sql_query = sql_match.group(1).strip()
                    # Ensure it has schema qualification
                    if '"AI DRIVEN DATA"' not in sql_query and 'FROM' in sql_query:
                        # Try to fix schema qualification
                        sql_query = re.sub(r'FROM\s+"([^"]+)"', r'FROM "AI DRIVEN DATA"."\1"', sql_query, flags=re.IGNORECASE)
            
            if sql_query:
                # Execute SQL
                try:
                    query_result = run_duckdb_query(sql_query)
                    
                    # Enhanced analysis with deeper reasoning
                    analysis_prompt = f"""The user asked: {message}

I executed this SQL query:
{sql_query}

Query results:
{query_result}

Now provide a comprehensive answer:

1. **Direct Answer**: Answer the user's question clearly and directly
2. **Key Findings**: Highlight the most important numbers and patterns
3. **Context**: Explain what these numbers mean in business terms
4. **Insights**: Provide insights that go beyond just the numbers
5. **Recommendations**: If appropriate, suggest actionable next steps

Be specific, use actual numbers from the results, and write like a senior data analyst who understands both the data and the business context."""
                    
                    analysis_response = client.chat.completions.create(
                        model="gpt-4o",  # Using gpt-4o for better analysis quality
                        messages=[
                            {"role": "system", "content": "You are KLAIRE, an expert health insurance data analyst for CLEARLINE INTERNATIONAL LIMITED. You provide deep insights, clear explanations, and actionable recommendations based on data. You think like a senior analyst who understands both technical details and business implications."},
                            {"role": "user", "content": analysis_prompt}
                        ],
                        max_tokens=2500,
                        temperature=0.3  # Balanced creativity for insightful analysis
                    )
                    
                    analysis = analysis_response.choices[0].message.content.strip()
                    final_response = f"{analysis}\n\n**Data:**\n{query_result}"
                    
                    # Extract and save any learnings from this conversation
                    extract_and_save_learnings(message, final_response, client)
                    
                    return final_response
                except Exception as e:
                    return f"I attempted to query the database but encountered an error: {e}\n\nOriginal AI response: {ai_response}"
            
            # Extract and save any learnings from this conversation (if we have client)
            if AI_AVAILABLE and client:
                try:
                    extract_and_save_learnings(message, ai_response, client)
                except Exception as e:
                    # Don't fail the request if learning extraction fails
                    print(f"⚠️ Learning extraction failed: {e}")
            
            return ai_response
            
        except Exception as e:
            # Fallback to pattern matching if AI fails
            return _fallback_reasoning(message, lower)
    else:
        # Fallback to pattern matching if OpenAI not available
        return _fallback_reasoning(message, lower)

def save_learning_to_knowledge_base(learning_text: str) -> bool:
    """
    Save new learning to the knowledge base file.
    Appends to a "LEARNINGS" section at the end of the file.
    """
    try:
        knowledge_file = os.path.join(DLT_ROOT, "CLEARLINE_KNOWLEDGE_BASE.md")
        
        # Read current content
        if os.path.exists(knowledge_file):
            with open(knowledge_file, 'r', encoding='utf-8') as f:
                content = f.read()
        else:
            content = ""
        
        # Check if learning section exists
        if "## Continuous Learning" not in content:
            content += "\n\n## Continuous Learning\n\n"
            content += "This section contains knowledge learned during conversations with KLAIRE.\n\n"
        
        # Append new learning with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        learning_entry = f"\n### Learning - {timestamp}\n\n{learning_text}\n\n"
        
        content += learning_entry
        
        # Write back to file
        with open(knowledge_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return True
    except Exception as e:
        print(f"⚠️ Failed to save learning to knowledge base: {e}")
        return False


def extract_and_save_learnings(message: str, reply: str, client: OpenAI) -> None:
    """
    Analyze the conversation to extract any new learnings and save them to knowledge base.
    """
    if not AI_AVAILABLE or not client:
        return
    
    try:
        # Ask AI to identify if there's new knowledge to save
        extraction_prompt = f"""You are KLAIRE, an expert data analyst for CLEARLINE INTERNATIONAL LIMITED.

The user said: "{message}"

Your response was: "{reply}"

**TASK**: Identify if any NEW business knowledge, rules, or clarifications were learned from this interaction that should be added to the knowledge base.

**Rules for what to save:**
- New business rules or clarifications about Clearline's operations
- Clarifications about table structures, column meanings, or relationships
- New calculation methods or formulas
- Business process explanations
- Important relationships between tables or concepts

**What NOT to save:**
- Query results or data values
- Questions without answers
- Already known information
- Temporary or one-off requests

If you find NEW knowledge to save, format it as a clear, concise explanation that would help future queries. 
If no new knowledge, respond with just: "NO_NEW_KNOWLEDGE"

Format any new knowledge as markdown that fits into a knowledge base."""

        extraction_response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are KLAIRE, an expert at identifying and extracting new knowledge from conversations. Be conservative - only extract truly new information that would help in future analysis."},
                {"role": "user", "content": extraction_prompt}
            ],
            max_tokens=500,
            temperature=0.2
        )
        
        extracted = extraction_response.choices[0].message.content.strip()
        
        if extracted and extracted != "NO_NEW_KNOWLEDGE" and len(extracted) > 20:
            # Save the learning
            save_learning_to_knowledge_base(extracted)
            print(f"📚 New learning saved to knowledge base")
    
    except Exception as e:
        # Don't fail the request if learning extraction fails
        print(f"⚠️ Failed to extract learnings: {e}")


def _fallback_reasoning(message: str, lower: str) -> str:
    """Fallback pattern-based reasoning when AI is not available"""
    # This is now only used if OpenAI is not available
    return (
        f"{SERVICE_NAME} (KLAIRE): AI reasoning is not available. Please set OPENAI_API_KEY environment variable.\n\n"
        "You can still use direct SQL queries with: SQL: [your query]\n"
        "Example: SQL: SELECT COUNT(*) FROM \"AI DRIVEN DATA\".\"CLAIMS DATA\""
    )


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="Empty message")

    conversation_id = req.conversation_id or create_conversation(req.message)

    # Save user message
    save_message(conversation_id, "user", req.message)

    # Get conversation history for context (last 5 messages)
    con = duckdb.connect(CHAT_DB_PATH)
    recent_messages = con.execute("""
        SELECT role, content 
        FROM healthinsight.messages 
        WHERE conversation_id = ? 
        ORDER BY ts DESC 
        LIMIT 10
    """, [conversation_id]).fetchall()
    con.close()
    
    # Build context from recent messages (excluding current message)
    context = ""
    if len(recent_messages) > 1:  # More than just current message
        context_messages = list(reversed(recent_messages[1:]))  # Exclude current, reverse for chronological
        context = "\n\nRecent conversation context:\n"
        for role, content in context_messages[-5:]:  # Last 5 messages
            context += f"{role.upper()}: {content}\n"

    # Generate reply with context
    reply = healthinsight_reasoning(req.message, context=context)

    # Save assistant reply
    save_message(conversation_id, "assistant", reply)
    
    # Note: Learning extraction happens inside healthinsight_reasoning to have access to OpenAI client

    return ChatResponse(conversation_id=conversation_id, reply=reply, tokens_used=None)


@app.get("/history/{conversation_id}")
def history(conversation_id: str) -> Dict[str, Any]:
    con = duckdb.connect(CHAT_DB_PATH)
    msgs = con.execute(
        "SELECT role, content, CAST(epoch(ms(ts)) AS DOUBLE) AS ts FROM healthinsight.messages WHERE conversation_id = ? ORDER BY ts ASC",
        [conversation_id],
    ).fetchall()
    con.close()
    items: List[HistoryItem] = [
        HistoryItem(role=r, content=c, ts=t) for (r, c, t) in msgs
    ]
    return {"conversation_id": conversation_id, "messages": [i.dict() for i in items]}


@app.get("/conversations")
def conversations() -> List[Dict[str, Any]]:
    con = duckdb.connect(CHAT_DB_PATH)
    rows = con.execute(
        "SELECT conversation_id, created_at, title FROM healthinsight.conversations ORDER BY created_at DESC"
    ).fetchdf()
    con.close()
    return rows.to_dict(orient="records")


# Optional utility endpoint to quickly try a SQL query
class SQLRequest(BaseModel):
    sql: str


@app.post("/sql")
def sql_endpoint(req: SQLRequest) -> Dict[str, Any]:
    result = run_duckdb_query(req.sql)
    return {"result": result}


# Run with: uvicorn klaire_service:app --reload --port 8787

