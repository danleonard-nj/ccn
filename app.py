import json
import logging
import os
import urllib.parse
from datetime import datetime, timedelta

import httpx
import pyodbc
import redis.asyncio as redis
from aioauth_client import (FacebookClient, GoogleClient, MicrosoftClient,
                            OAuth2Client)
from faker import Faker
from quart import (Quart, g, session, request, redirect, url_for,
                   render_template_string, make_response)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

##############################################################################
# CONFIGURATION
##############################################################################
DATABASE_URL = "mssql+aioodbc:///?odbc_connect=" + urllib.parse.quote(
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\CCN;DATABASE=CCN;Trusted_Connection=yes;"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

app = Quart(__name__)
app.secret_key = "your_secret_key"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

##############################################################################
# DB ENGINE & SESSION FACTORY
##############################################################################
async_engine = create_async_engine(DATABASE_URL, echo=False)
async_session_factory = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)

##############################################################################
# REDIS CLIENT
##############################################################################
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

##############################################################################
# OAUTH CLIENTS
##############################################################################
GOOGLE_REDIRECT_URI = "http://localhost:5000/auth/google/callback"
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "your-google-client-id")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "your-google-client-secret")
google_client = GoogleClient(
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    redirect_uri=GOOGLE_REDIRECT_URI,
    scope=["openid", "email", "profile"]
)

MICROSOFT_REDIRECT_URI = "http://localhost:5000/auth/microsoft/callback"
MICROSOFT_CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID", "your-microsoft-client-id")
MICROSOFT_CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET", "your-microsoft-client-secret")
microsoft_client = MicrosoftClient(
    client_id=MICROSOFT_CLIENT_ID,
    client_secret=MICROSOFT_CLIENT_SECRET,
    redirect_uri=MICROSOFT_REDIRECT_URI,
    scope=["User.Read"]
)

FACEBOOK_REDIRECT_URI = "http://localhost:5000/auth/facebook/callback"
FACEBOOK_CLIENT_ID = os.getenv("FACEBOOK_CLIENT_ID", "your-facebook-client-id")
FACEBOOK_CLIENT_SECRET = os.getenv("FACEBOOK_CLIENT_SECRET", "your-facebook-client-secret")
facebook_client = FacebookClient(
    client_id=FACEBOOK_CLIENT_ID,
    client_secret=FACEBOOK_CLIENT_SECRET,
    redirect_uri=FACEBOOK_REDIRECT_URI,
    scope=["email"]
)

X_REDIRECT_URI = "http://localhost:5000/auth/x/callback"
X_CLIENT_ID = os.getenv("X_CLIENT_ID", "your-x-client-id")
X_CLIENT_SECRET = os.getenv("X_CLIENT_SECRET", "your-x-client-secret")
x_client = OAuth2Client(
    client_id=X_CLIENT_ID,
    client_secret=X_CLIENT_SECRET,
    redirect_uri=X_REDIRECT_URI,
    authorize_url="https://provider-x.com/oauth2/authorize",
    access_token_url="https://provider-x.com/oauth2/token",
    scope=["email", "profile"]
)

##############################################################################
# HELPER FUNCTIONS
##############################################################################


def current_user():
    if "user_id" not in session:
        return None
    return {"user_id": session["user_id"], "email": session["email"], "role": session.get("role")}


def user_is_admin():
    u = current_user()
    return u is not None and u.get("role") == "Admin"


def login_user(user_id, email, role):
    session["user_id"] = user_id
    session["email"] = email
    session["role"] = role


def logout_user():
    session.clear()


async def get_db_session() -> AsyncSession:
    if not hasattr(g, "db_session"):
        g.db_session = async_session_factory()
    return g.db_session

##############################################################################
# DATABASE SETUP (using pyodbc)
##############################################################################


def ensure_database_exists():
    conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\CCN;DATABASE=CCN;Trusted_Connection=yes;'
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sys.databases WHERE name='CCN'")
            if not cur.fetchone():
                cur.execute("CREATE DATABASE CCN")
                conn.commit()
        with pyodbc.connect(conn_str) as conn:
            cur = conn.cursor()
            cur.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Users')
                CREATE TABLE Users (
                    UserID INT PRIMARY KEY IDENTITY(1,1),
                    FirstName NVARCHAR(50) NOT NULL,
                    LastName NVARCHAR(50) NOT NULL,
                    Email NVARCHAR(255) NOT NULL UNIQUE,
                    RoleType NVARCHAR(50) DEFAULT 'Reader',
                    Address NVARCHAR(200) NOT NULL,
                    City NVARCHAR(100) NOT NULL,
                    StateProvince NVARCHAR(100) NOT NULL,
                    ZipCode NVARCHAR(20) NOT NULL,
                    Country NVARCHAR(100) NOT NULL,
                    CreatedDate DATETIME DEFAULT GETDATE(),
                    IsActive BIT DEFAULT 1,
                    google_id NVARCHAR(255),
                    microsoft_id NVARCHAR(255),
                    facebook_id NVARCHAR(255),
                    x_id NVARCHAR(255)
                );

                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Consultants')
                CREATE TABLE Consultants (
                    ConsultantID INT PRIMARY KEY IDENTITY(1,1),
                    UserID INT NOT NULL UNIQUE,
                    Organization NVARCHAR(200),
                    Summary NVARCHAR(3000)
                );

                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Subscriptions')
                CREATE TABLE Subscriptions (
                    SubscriptionID INT PRIMARY KEY IDENTITY(1,1),
                    UserID INT NOT NULL,
                    SubscriptionLevel NVARCHAR(50) DEFAULT 'Free',
                    SubscriptionStartDate DATETIME NOT NULL,
                    Amount DECIMAL(10,2) DEFAULT 0.00,
                    PayPalTransactionID NVARCHAR(100),
                    IsActive BIT DEFAULT 1
                );

                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Events')
                CREATE TABLE Events (
                    EventID INT PRIMARY KEY IDENTITY(1,1),
                    Title NVARCHAR(200) NOT NULL,
                    EventDateTime DATETIME NOT NULL,
                    Location NVARCHAR(200) NOT NULL,
                    IsPublic BIT DEFAULT 1,
                    CreatedDate DATETIME DEFAULT GETDATE(),
                    Description NVARCHAR(3000)
                );

                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Payments')
                CREATE TABLE Payments (
                    PaymentID INT PRIMARY KEY IDENTITY(1,1),
                    UserID INT NOT NULL,
                    PaymentDate DATETIME NOT NULL DEFAULT GETDATE(),
                    Amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                    PaymentStatus NVARCHAR(50) NOT NULL DEFAULT 'Completed',
                    PaymentMethod NVARCHAR(50) NOT NULL DEFAULT 'PayPal'
                );
            """)
            conn.commit()
            logger.info("Ensured CCN tables exist.")
    except pyodbc.Error as e:
        logger.error("Database setup error: %s", e)
        raise

##############################################################################
# DUMMY DATA POPULATION
##############################################################################


async def populate_dummy_data():
    faker = Faker()
    session_db = await get_db_session()
    try:
        # Create admin user if not exists
        res = await session_db.execute(text("SELECT UserID FROM Users WHERE Email='admin'"))
        if not res.fetchone():
            await session_db.execute(text("""
                INSERT INTO Users (FirstName, LastName, Email, RoleType, Address, City, StateProvince, ZipCode, Country)
                VALUES ('Admin','User','admin','Admin','123 Admin St','AdminCity','AdminState','00000','AdminLand')
            """))
            await session_db.commit()
            logger.info("Admin user created.")

        # Create 50 dummy non-admin users if needed
        res = await session_db.execute(text("SELECT COUNT(*) AS Cnt FROM Users WHERE Email<>'admin'"))
        count = res.fetchone().Cnt
        needed = 50 - count
        if needed > 0:
            for _ in range(needed):
                first = faker.first_name()
                last = faker.last_name()
                email = faker.unique.email()
                address = faker.street_address()
                city = faker.city()
                state = faker.state()
                zip_code = faker.zipcode()
                country = faker.country()
                await session_db.execute(text("""
                    INSERT INTO Users (FirstName, LastName, Email, Address, City, StateProvince, ZipCode, Country)
                    VALUES (:f, :l, :e, :addr, :c, :s, :z, :co)
                """), {"f": first, "l": last, "e": email, "addr": address, "c": city, "s": state, "z": zip_code, "co": country})
            await session_db.commit()

        # Create 20 dummy events if needed
        res = await session_db.execute(text("SELECT COUNT(*) AS EvtCnt FROM Events"))
        ecnt = res.fetchone().EvtCnt
        needed_evt = 20 - ecnt
        if needed_evt > 0:
            for _ in range(needed_evt):
                title = faker.sentence(nb_words=4)
                event_date = faker.date_time_between(start_date="-10d", end_date="+60d")
                location = faker.city()
                desc = faker.paragraph(nb_sentences=5)
                await session_db.execute(text("""
                    INSERT INTO Events (Title, EventDateTime, Location, Description)
                    VALUES (:t, :d, :l, :desc)
                """), {"t": title, "d": event_date, "l": location, "desc": desc})
            await session_db.commit()

        # Create free subscriptions for non-admin users
        res = await session_db.execute(text("SELECT UserID FROM Users WHERE Email<>'admin'"))
        user_ids = [row.UserID for row in res.fetchall()]
        for uid in user_ids:
            sub_res = await session_db.execute(text("SELECT SubscriptionID FROM Subscriptions WHERE UserID=:u"), {"u": uid})
            if not sub_res.fetchone():
                await session_db.execute(text("""
                    INSERT INTO Subscriptions (UserID, SubscriptionLevel, SubscriptionStartDate, Amount, IsActive)
                    VALUES (:uid, 'Free', GETDATE(), 0.00, 1)
                """), {"uid": uid})
        await session_db.commit()

        # Create 5 dummy consultants if needed
        res = await session_db.execute(text("SELECT COUNT(*) AS C FROM Consultants"))
        ccount = res.fetchone().C
        needed_cons = 5 - ccount
        if needed_cons > 0:
            r = await session_db.execute(text("""
                SELECT TOP :nc UserID FROM Users WHERE Email<>'admin' ORDER BY NEWID()
            """), {"nc": needed_cons})
            picks = r.fetchall()
            for p in picks:
                org = faker.company()
                summ = faker.paragraph()
                await session_db.execute(text("""
                    INSERT INTO Consultants (UserID, Organization, Summary)
                    VALUES (:u, :o, :s)
                """), {"u": p.UserID, "o": org, "s": summ})
            await session_db.commit()

    except Exception as e:
        logger.error("populate_dummy_data error: %s", e)
        await session_db.rollback()
    finally:
        await session_db.close()

##############################################################################
# APP LIFECYCLE
##############################################################################


@app.before_serving
async def startup():
    try:
        ensure_database_exists()
        await populate_dummy_data()
    except Exception as e:
        logger.error("Startup error: %s", e)


@app.teardown_appcontext
async def shutdown(exception):
    pass

##############################################################################
# COMMON STYLES & NAVBAR (async)
##############################################################################


def get_common_styles():
    return """
<style>
  body { font-family: Arial, sans-serif; margin: 0; padding: 0; background: #fff; }
  .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
  .navbar { background-color: #0078D7; padding: 10px; margin-bottom: 20px; }
  .navbar-container { display: flex; justify-content: space-between; align-items: center; max-width: 1200px; margin: 0 auto; }
  .nav-left a, .nav-right a { color: #fff; text-decoration: none; margin-right: 15px; }
  .nav-left a:hover, .nav-right a:hover { text-decoration: underline; }
  .user-icon { margin-right: 5px; }
</style>
"""


async def get_navbar_html():
    navbar_template = get_common_styles() + """
<div class="navbar">
  <div class="navbar-container">
    <div class="nav-left">
      <a href="{{ url_for('home') }}">HOME</a>
      <a href="{{ url_for('list_consultants') }}">CONSULTANTS</a>
      <a href="{{ url_for('list_events') }}">EVENTS</a>
      <a href="{{ url_for('home') }}">RESOURCES</a>
      <a href="{{ url_for('home') }}">CCN BLOG</a>
      <a href="{{ url_for('home') }}">ABOUT/CONTACT US</a>
      <a href="{{ url_for('dashboard') }}">MEMBER AREA</a>
    </div>
    <div class="nav-right">
      {% if current_user() %}
        <span class="user-icon">👤</span>
        <span class="username">{{ session.get('email') }}</span>
        {% if user_is_admin() %}
          <a href="{{ url_for('admin_portal') }}">Admin Portal</a>
        {% endif %}
        <a href="{{ url_for('do_logout') }}">Logout</a>
      {% else %}
        <a href="{{ url_for('login') }}">Login</a>
        <a href="{{ url_for('register') }}">Register</a>
      {% endif %}
    </div>
  </div>
</div>
"""
    return await render_template_string(navbar_template)

##############################################################################
# GENERIC EXCEPTION HANDLER
##############################################################################


@app.errorhandler(Exception)
async def handle_exceptions(e):
    logger.error("Unhandled exception: %s", e, exc_info=True)
    styles = get_common_styles()
    return await render_template_string("""
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Error</title>
    {{ styles|safe }}
  </head>
  <body>
    <div class="navbar">
      <div class="navbar-container">
        <div class="nav-left">
          <a href="{{ url_for('home') }}">HOME</a>
        </div>
        <div class="nav-right">
          <a href="{{ url_for('home') }}">Go Home</a>
        </div>
      </div>
    </div>
    <div class="container">
      <h1>Something went wrong!</h1>
      <p>{{ e }}</p>
    </div>
  </body>
</html>
""", styles=styles, e=e), 500

##############################################################################
# SUBSCRIPTION VALIDATION
##############################################################################


async def validate_subscription(user_id):
    session_db = await get_db_session()
    r = await session_db.execute(text("SELECT * FROM Subscriptions WHERE UserID=:uid"), {"uid": user_id})
    sub = r.fetchone()
    if not sub:
        raise ValueError("No subscription found for this user.")
    if not sub.IsActive:
        raise ValueError("Your subscription is not active. Please renew or contact support.")

##############################################################################
# ROUTES
##############################################################################
# HOME (INDEX)


@app.route("/")
async def home():
    navbar_html = await get_navbar_html()
    cache_key = "homepage_events"
    cached = await redis_client.get(cache_key)
    if cached:
        events = json.loads(cached)
    else:
        session_db = await get_db_session()
        r = await session_db.execute(text("SELECT TOP 5 * FROM Events ORDER BY EventDateTime DESC"))
        events = [dict(x) for x in r.mappings().all()]
        await redis_client.setex(cache_key, 300, json.dumps(events, default=str))
    now_dt = datetime.now()
    for e in events:
        if isinstance(e["EventDateTime"], str):
            e["EventDateTime"] = datetime.fromisoformat(e["EventDateTime"].replace("Z", ""))
    upcoming = [evt for evt in events if evt["EventDateTime"] > now_dt]
    return await render_template_string("""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>CCN Home</title>
  </head>
  <body>
    {{ navbar|safe }}
    <div class="container" style="display: flex; flex-wrap: wrap;">
      <div style="flex: 0 0 50%; max-width: 600px; padding-right: 20px;">
        <img src="https://via.placeholder.com/600x400" alt="CCN Homepage Image" style="width:100%; height:auto;">
      </div>
      <div style="flex:1; display:flex; flex-direction:column;">
        <div style="margin-bottom:20px;">
          <a href="{{ url_for('list_consultants') }}" style="display:inline-block; background:#0078D7; color:#fff; padding:15px 30px; margin:10px; text-decoration:none; border-radius:4px;">Find a Consultant</a>
          <a href="{{ url_for('register') }}" style="display:inline-block; background:#0078D7; color:#fff; padding:15px 30px; margin:10px; text-decoration:none; border-radius:4px;">Join CCN</a>
        </div>
        <div style="background:#f9f9f9; padding:20px; border-radius:5px; margin-bottom:20px;">
          <h2>FEATURED MEMBER</h2>
          <h3>Dr. Jerome P. Ferrance</h3>
          <p>JZF-Engineering, Virginia</p>
          <a href="{{ url_for('list_consultants') }}">View member directory</a>
        </div>
        <div>
          <h2 style="color:#0078D7;">UPCOMING EVENTS</h2>
          {% if upcoming %}
            <ul style="list-style:none; padding:0;">
              {% for event in upcoming %}
                <li style="margin:10px 0;">
                  <a href="{{ url_for('event_details', event_id=event['EventID']) }}">{{ event['Title'] }}</a>
                  - {{ event['EventDateTime'] }} at {{ event['Location'] }}
                </li>
              {% endfor %}
            </ul>
          {% else %}
            <p>No events available.</p>
          {% endif %}
        </div>
      </div>
    </div>
    <div class="footer">
      Footer or additional info here
    </div>
  </body>
</html>
""", navbar=navbar_html, upcoming=upcoming)

# LOGIN


@app.route("/login")
async def login():
    navbar_html = await get_navbar_html()
    return await render_template_string("""
<!DOCTYPE html>
<html>
  <head><meta charset="UTF-8"><title>Login</title></head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <h1>Login</h1>
      <ul>
        <li><a href="{{ url_for('oauth_login', provider='google') }}">Login with Google</a></li>
        <li><a href="{{ url_for('oauth_login', provider='microsoft') }}">Login with Microsoft</a></li>
        <li><a href="{{ url_for('oauth_login', provider='facebook') }}">Login with Facebook</a></li>
        <li><a href="{{ url_for('oauth_login', provider='x') }}">Login with Provider X</a></li>
      </ul>
      <p>Don't have an account? <a href="{{ url_for('register') }}">Register</a>.</p>
    </div>
  </body>
</html>
""", navbar=navbar_html)

# OAUTH LOGIN


@app.route("/login/<provider>")
async def oauth_login(provider):
    if provider == "google":
        return redirect(google_client.get_authorize_url())
    elif provider == "microsoft":
        return redirect(microsoft_client.get_authorize_url())
    elif provider == "facebook":
        return redirect(facebook_client.get_authorize_url())
    elif provider == "x":
        return redirect(x_client.get_authorize_url())
    else:
        raise ValueError("Unsupported provider: " + provider)

# OAUTH CALLBACK


@app.route("/auth/<provider>/callback")
async def oauth_callback(provider):
    code = request.args.get("code")
    if not code:
        raise ValueError("Missing OAuth code")
    if provider == "google":
        token = await google_client.get_access_token(code=code)
        user_data = await google_client.get_user_info(token=token)
        oauth_id = user_data.get("id")
    elif provider == "microsoft":
        token = await microsoft_client.get_access_token(code=code)
        user_data = await microsoft_client.get_user_info(token=token)
        oauth_id = user_data.get("id")
    elif provider == "facebook":
        token = await facebook_client.get_access_token(code=code)
        user_data = await facebook_client.get_user_info(token=token)
        oauth_id = user_data.get("id")
    elif provider == "x":
        token = await x_client.get_access_token(code=code)
        user_data = await x_client.get_user_info(token=token)
        oauth_id = user_data.get("id")
    else:
        raise ValueError("Unsupported provider: " + provider)
    email = user_data.get("email")
    first_name = user_data.get("given_name") or user_data.get("first_name") or "OAuth"
    last_name = user_data.get("family_name") or user_data.get("last_name") or "User"
    session_db = await get_db_session()
    r = await session_db.execute(text("SELECT * FROM Users WHERE Email=:em"), {"em": email})
    row = r.fetchone()
    if row:
        user_id = row.UserID
        update_sql = ""
        if provider == "google" and not row.google_id:
            update_sql = "UPDATE Users SET google_id=:oid WHERE UserID=:uid"
        elif provider == "microsoft" and not row.microsoft_id:
            update_sql = "UPDATE Users SET microsoft_id=:oid WHERE UserID=:uid"
        elif provider == "facebook" and not row.facebook_id:
            update_sql = "UPDATE Users SET facebook_id=:oid WHERE UserID=:uid"
        elif provider == "x" and not row.x_id:
            update_sql = "UPDATE Users SET x_id=:oid WHERE UserID=:uid"
        if update_sql:
            await session_db.execute(text(update_sql), {"oid": oauth_id, "uid": user_id})
            await session_db.commit()
        role = row.RoleType
    else:
        col = {"google": "google_id", "microsoft": "microsoft_id", "facebook": "facebook_id", "x": "x_id"}.get(provider, "google_id")
        q = """
        INSERT INTO Users (FirstName, LastName, Email, RoleType, Address, City, StateProvince, ZipCode, Country, {col})
        OUTPUT inserted.UserID
        VALUES (:fn, :ln, :em, 'Reader', 'Unknown', 'Unknown', 'Unknown', '00000', 'Unknown', :oid)
        """.replace("{col}", col)
        r = await session_db.execute(text(q), {"fn": first_name, "ln": last_name, "em": email, "oid": oauth_id})
        user_id = r.fetchone().UserID
        await session_db.commit()
        role = "Reader"
    login_user(user_id, email, role)
    await validate_subscription(user_id)
    return redirect(url_for("dashboard"))

# REGISTER


@app.route("/register", methods=["GET", "POST"])
async def register():
    navbar_html = await get_navbar_html()
    if request.method == "POST":
        form = await request.form
        for field in ["first_name", "last_name", "email", "address", "city", "state_province", "zip_code", "country"]:
            if not form.get(field):
                raise ValueError("Missing required field: " + field)
        session_db = await get_db_session()
        r = await session_db.execute(text("SELECT UserID FROM Users WHERE Email=:e"), {"e": form["email"]})
        if r.fetchone():
            raise ValueError("User already exists with that email.")
        ins_q = """
        INSERT INTO Users (FirstName, LastName, Email, RoleType, Address, City, StateProvince, ZipCode, Country)
        OUTPUT inserted.UserID
        VALUES (:fn, :ln, :em, 'Reader', :ad, :ct, :st, :zc, :co)
        """
        res = await session_db.execute(text(ins_q), {
            "fn": form["first_name"], "ln": form["last_name"], "em": form["email"],
            "ad": form["address"], "ct": form["city"], "st": form["state_province"],
            "zc": form["zip_code"], "co": form["country"]
        })
        new_id = res.fetchone().UserID
        await session_db.commit()
        await session_db.execute(text("""
            INSERT INTO Subscriptions (UserID, SubscriptionLevel, SubscriptionStartDate, Amount, IsActive)
            VALUES (:uid, 'Free', GETDATE(), 0.00, 1)
        """), {"uid": new_id})
        await session_db.commit()
        login_user(new_id, form["email"], "Reader")
        return redirect(url_for("dashboard"))
    return render_template_string("""
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Register</title>
  </head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <h1>Register</h1>
      <form method="POST">
        <label>First Name*</label>
        <input type="text" name="first_name" required>
        <label>Last Name*</label>
        <input type="text" name="last_name" required>
        <label>Email*</label>
        <input type="email" name="email" required>
        <label>Address*</label>
        <input type="text" name="address" required>
        <label>City*</label>
        <input type="text" name="city" required>
        <label>State/Province*</label>
        <input type="text" name="state_province" required>
        <label>Zip Code*</label>
        <input type="text" name="zip_code" required>
        <label>Country*</label>
        <input type="text" name="country" required>
        <button type="submit">Register</button>
      </form>
    </div>
  </body>
</html>
""", navbar=navbar_html)

# LOGOUT


@app.route("/logout")
async def do_logout():
    logout_user()
    return redirect(url_for("home"))

# DASHBOARD


@app.route("/dashboard")
async def dashboard():
    if not current_user():
        return redirect(url_for("login"))
    navbar_html = await get_navbar_html()
    session_db = await get_db_session()
    r = await session_db.execute(text("SELECT TOP 5 * FROM Events ORDER BY EventDateTime DESC"))
    events = [dict(x) for x in r.mappings().all()]
    return render_template_string("""
<!DOCTYPE html>
<html>
  <head><meta charset="UTF-8"><title>Dashboard</title></head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <h1>Welcome, {{ session.get('email') }}!</h1>
      <p><a href="{{ url_for('my_subscription') }}">Manage My Subscription</a></p>
      <h2>Recent Events</h2>
      <ul>
        {% for e in events %}
          <li>{{ e["Title"] }} - {{ e["EventDateTime"] }} at {{ e["Location"] }}</li>
        {% endfor %}
      </ul>
    </div>
  </body>
</html>
""", navbar=navbar_html, events=events)

# MY SUBSCRIPTION (User)


@app.route("/my_subscription", methods=["GET", "POST"])
async def my_subscription():
    if not current_user():
        return redirect(url_for("login"))
    uid = current_user()["user_id"]
    session_db = await get_db_session()
    if request.method == "POST":
        form = await request.form
        action = form.get("action")
        r = await session_db.execute(text("SELECT * FROM Subscriptions WHERE UserID=:u"), {"u": uid})
        sub = r.fetchone()
        if not sub:
            raise ValueError("No subscription found. Contact support.")
        if action == "upgrade":
            await session_db.execute(text("""
                INSERT INTO Payments (UserID, Amount, PaymentStatus, PaymentMethod)
                VALUES (:u, 99.99, 'Completed', 'PayPal')
            """), {"u": uid})
            await session_db.execute(text("""
                UPDATE Subscriptions SET SubscriptionLevel='Paid', Amount=99.99, IsActive=1
                WHERE SubscriptionID=:sid
            """), {"sid": sub.SubscriptionID})
        elif action == "cancel":
            await session_db.execute(text("""
                UPDATE Subscriptions SET IsActive=0 WHERE SubscriptionID=:sid
            """), {"sid": sub.SubscriptionID})
        await session_db.commit()
        return redirect(url_for("my_subscription"))
    r = await session_db.execute(text("SELECT * FROM Subscriptions WHERE UserID=:u"), {"u": uid})
    sub = r.fetchone()
    if not sub:
        raise ValueError("No subscription found. Contact support.")
    return render_template_string("""
<!DOCTYPE html>
<html>
  <head><meta charset="UTF-8"><title>My Subscription</title></head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <h1>My Subscription</h1>
      <div style="background:#f9f9f9; padding:20px; border-radius:5px; margin-bottom:20px;">
        <p>Level: {{ sub.SubscriptionLevel }}</p>
        <p>Start Date: {{ sub.SubscriptionStartDate }}</p>
        <p>Amount: {{ sub.Amount }}</p>
        <p>Status: {{ "Active" if sub.IsActive else "Inactive" }}</p>
      </div>
      <form method="POST">
        {% if sub.SubscriptionLevel == 'Free' and sub.IsActive %}
          <button type="submit" name="action" value="upgrade">Upgrade to Paid ($99.99)</button>
        {% endif %}
        {% if sub.IsActive %}
          <button type="submit" name="action" value="cancel">Cancel Subscription</button>
        {% endif %}
      </form>
    </div>
  </body>
</html>
""", navbar=await get_navbar_html(), sub=sub)

# CONSULTANTS DIRECTORY


@app.route("/consultants")
async def list_consultants():
    navbar_html = await get_navbar_html()
    cache_key = "consultants_list"
    c = await redis_client.get(cache_key)
    if c:
        consultants = json.loads(c)
    else:
        session_db = await get_db_session()
        r = await session_db.execute(text("""
            SELECT c.*, u.FirstName, u.LastName, u.City
            FROM Consultants c JOIN Users u ON c.UserID=u.UserID
        """))
        consultants = [dict(x) for x in r.mappings().all()]
        await redis_client.setex(cache_key, 600, json.dumps(consultants, default=str))
    return render_template_string("""
<!DOCTYPE html>
<html>
  <head><meta charset="UTF-8"><title>Consultants</title></head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <h1>Consultant Directory</h1>
      {% if not consultants %}
        <p>No consultants found.</p>
      {% else %}
        <input type="text" id="search" placeholder="Search consultants..." style="width:100%; padding:8px;">
        <table style="width:100%; border-collapse:collapse; margin-top:20px;">
          <tr style="background:#0078D7; color:#fff;">
            <th style="padding:8px;">Name &amp; Location</th>
            <th style="padding:8px;">Expertise</th>
            <th style="padding:8px;">Services</th>
          </tr>
          {% for cc in consultants %}
          <tr>
            <td style="border:1px solid #ddd; padding:8px;">{{ cc["FirstName"] }} {{ cc["LastName"] }} - {{ cc["City"] }}</td>
            <td style="border:1px solid #ddd; padding:8px;">{{ cc["Summary"] or "N/A" }}</td>
            <td style="border:1px solid #ddd; padding:8px;">{{ cc["Organization"] or "N/A" }}</td>
          </tr>
          {% endfor %}
        </table>
      {% endif %}
    </div>
    <script>
      const searchInput = document.getElementById('search');
      if(searchInput) {
        searchInput.addEventListener('keyup', () => {
          const val = searchInput.value.toLowerCase();
          const rows = document.querySelectorAll('table tr');
          rows.forEach((row, idx) => {
            if(idx===0) return;
            const txt = row.textContent.toLowerCase();
            row.style.display = txt.includes(val) ? '' : 'none';
          });
        });
      }
    </script>
  </body>
</html>
""", navbar=navbar_html, consultants=consultants)

# EVENTS LIST


@app.route("/events")
async def list_events():
    navbar_html = await get_navbar_html()
    key = "events_list"
    cached = await redis_client.get(key)
    if cached:
        events = json.loads(cached)
    else:
        session_db = await get_db_session()
        r = await session_db.execute(text("SELECT * FROM Events ORDER BY EventDateTime DESC"))
        events = [dict(x) for x in r.mappings().all()]
        await redis_client.setex(key, 300, json.dumps(events, default=str))
    now_dt = datetime.now()
    for e in events:
        if isinstance(e["EventDateTime"], str):
            e["EventDateTime"] = datetime.fromisoformat(e["EventDateTime"].replace("Z", ""))
    upcoming = [evt for evt in events if evt["EventDateTime"] > now_dt]
    past = [evt for evt in events if evt["EventDateTime"] <= now_dt]
    return render_template_string("""
<!DOCTYPE html>
<html>
  <head><meta charset="UTF-8"><title>Events</title></head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <h1 style="text-transform:uppercase; color:#0078D7;">Events</h1>
      <h2 style="text-transform:uppercase; color:#0078D7;">Upcoming Events</h2>
      {% if upcoming %}
        <ul style="list-style:none; padding:0;">
          {% for e in upcoming %}
            <li style="margin:8px 0;">
              <span style="font-weight:bold; margin-right:8px;">{{ e["EventDateTime"].strftime("%d %b %Y") }}</span>
              <a href="{{ url_for('event_details', event_id=e['EventID']) }}">{{ e["Title"] }}</a>
            </li>
          {% endfor %}
        </ul>
      {% else %}
        <p><em>No events available.</em></p>
      {% endif %}
      <h2 style="text-transform:uppercase; color:#0078D7;">Past Events</h2>
      {% if past %}
        <ul style="list-style:none; padding:0;">
          {% for e in past %}
            <li style="margin:8px 0;">
              <span style="font-weight:bold; margin-right:8px;">{{ e["EventDateTime"].strftime("%d %b %Y") }}</span>
              <a href="{{ url_for('event_details', event_id=e['EventID']) }}">{{ e["Title"] }}</a>
            </li>
          {% endfor %}
        </ul>
      {% else %}
        <p><em>No past events.</em></p>
      {% endif %}
    </div>
  </body>
</html>
""", navbar=navbar_html, upcoming=upcoming, past=past)

# EVENT DETAILS + ICS


@app.route("/events/<int:event_id>")
async def event_details(event_id):
    navbar_html = await get_navbar_html()
    session_db = await get_db_session()
    r = await session_db.execute(text("SELECT * FROM Events WHERE EventID=:eid"), {"eid": event_id})
    row = r.fetchone()
    if not row:
        raise ValueError("Event not found")
    event = dict(row._mapping)
    if isinstance(event["EventDateTime"], str):
        event["EventDateTime"] = datetime.fromisoformat(event["EventDateTime"].replace("Z", ""))
    speaker_name = "John Fetzer PhD, FRSC"
    display_dt = event["EventDateTime"].strftime("%d %b %Y, %I:%M %p")
    return render_template_string("""
<!DOCTYPE html>
<html>
  <head><meta charset="UTF-8"><title>{{ event.Title }}</title></head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
        <div>
          <a href="{{ url_for('list_events') }}" style="text-decoration:none; color:#0078D7; font-weight:bold;">&larr; Back</a>
        </div>
        <div>
          <a href="{{ url_for('event_ics', event_id=event.EventID) }}" style="background:#0078D7; color:#fff; text-decoration:none; padding:10px 15px; border-radius:4px;" download>Add to My Calendar</a>
        </div>
      </div>
      <div style="display:flex; flex-wrap:wrap;">
        <div style="flex:0 0 250px; margin-right:20px; background:#f9f9f9; padding:20px; border-radius:5px;">
          <p><strong>Date &amp; Time:</strong><br>{{ display_dt }} via Zoom</p>
          <p><strong>Location:</strong><br>{{ event.Location }}</p>
          <div style="margin-top:20px; background:#fff; border:1px solid #ccc; padding:10px; border-radius:5px;">
            <h3>REGISTRATION</h3>
            <p>Attendee<br>Attendee + Email Notification</p>
          </div>
        </div>
        <div style="flex:1;">
          <h1 style="color:#0078D7; margin-top:0;">{{ event.Title }}</h1>
          <h2 style="color:#0078D7;">{{ speaker_name }}</h2>
          <p><strong>Abstract:</strong> {{ event.Description or "No description provided." }}</p>
        </div>
      </div>
    </div>
  </body>
</html>
""", navbar=navbar_html, event=event, display_dt=display_dt, speaker_name=speaker_name)


@app.route("/events/<int:event_id>/ics")
async def event_ics(event_id):
    session_db = await get_db_session()
    r = await session_db.execute(text("SELECT * FROM Events WHERE EventID=:eid"), {"eid": event_id})
    row = r.fetchone()
    if not row:
        raise ValueError("Event not found")
    event = dict(row._mapping)
    if isinstance(event["EventDateTime"], str):
        event["EventDateTime"] = datetime.fromisoformat(event["EventDateTime"].replace("Z", ""))
    start_time = event["EventDateTime"]
    end_time = start_time + timedelta(hours=1)
    ics = (
        "BEGIN:VCALENDAR\n"
        "VERSION:2.0\n"
        "PRODID:-//CCN//EN\n"
        "BEGIN:VEVENT\n"
        f"UID:{event_id}@ccn\n"
        f"DTSTAMP:{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}\n"
        f"DTSTART:{start_time.strftime('%Y%m%dT%H%M%SZ')}\n"
        f"DTEND:{end_time.strftime('%Y%m%dT%H%M%SZ')}\n"
        f"SUMMARY:{event['Title']}\n"
        f"LOCATION:{event['Location']}\n"
        f"DESCRIPTION:{event.get('Description','')}\n"
        "END:VEVENT\n"
        "END:VCALENDAR\n"
    )
    resp = await make_response(ics)
    resp.headers["Content-Type"] = "text/calendar"
    resp.headers["Content-Disposition"] = f'attachment; filename="event_{event_id}.ics"'
    return resp

# ADMIN PORTAL


@app.route("/admin")
async def admin_portal():
    if not user_is_admin():
        raise ValueError("Unauthorized access.")
    navbar_html = await get_navbar_html()
    return render_template_string("""
<!DOCTYPE html>
<html>
  <head><meta charset="UTF-8"><title>Admin Portal</title></head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <h1>Admin Portal</h1>
      <ul style="list-style:none; padding:0;">
        <li><a href="{{ url_for('admin_list_users') }}">List All Users</a></li>
        <li><a href="{{ url_for('list_events') }}">Manage Events</a></li>
        <li><a href="{{ url_for('admin_subscriptions') }}">Manage Subscriptions</a></li>
      </ul>
    </div>
  </body>
</html>
""", navbar=navbar_html)


@app.route("/admin/users")
async def admin_list_users():
    if not user_is_admin():
        raise ValueError("Unauthorized access.")
    navbar_html = await get_navbar_html()
    session_db = await get_db_session()
    r = await session_db.execute(text("SELECT UserID, FirstName, LastName, Email, RoleType, CreatedDate FROM Users ORDER BY CreatedDate DESC"))
    users = [dict(x) for x in r.mappings().all()]
    return render_template_string("""
<!DOCTYPE html>
<html>
  <head><meta charset="UTF-8"><title>Admin - Users</title></head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <h1>All Users</h1>
      <table style="width:100%; border-collapse:collapse; margin-top:20px;">
        <tr style="background:#0078D7; color:#fff;">
          <th style="padding:8px;">ID</th>
          <th style="padding:8px;">Name</th>
          <th style="padding:8px;">Email</th>
          <th style="padding:8px;">Role</th>
          <th style="padding:8px;">Created Date</th>
        </tr>
        {% for u in users %}
        <tr>
          <td style="border:1px solid #ddd; padding:8px;">{{ u.UserID }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ u.FirstName }} {{ u.LastName }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ u.Email }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ u.RoleType }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ u.CreatedDate }}</td>
        </tr>
        {% endfor %}
      </table>
      <p><a href="{{ url_for('admin_portal') }}">Back to Admin</a></p>
    </div>
  </body>
</html>
""", navbar=navbar_html, users=users)


@app.route("/admin/subscriptions", methods=["GET", "POST"])
async def admin_subscriptions():
    if not user_is_admin():
        raise ValueError("Unauthorized access.")
    session_db = await get_db_session()
    navbar_html = await get_navbar_html()
    if request.method == "POST":
        form = await request.form
        sub_id = form.get("subscription_id")
        action = form.get("action")
        if not sub_id:
            raise ValueError("Missing subscription ID")
        r = await session_db.execute(text("SELECT * FROM Subscriptions WHERE SubscriptionID=:sid"), {"sid": sub_id})
        sub = r.fetchone()
        if not sub:
            raise ValueError("Subscription not found.")
        if action == "deactivate":
            await session_db.execute(text("UPDATE Subscriptions SET IsActive=0 WHERE SubscriptionID=:sid"), {"sid": sub_id})
        elif action == "activate":
            await session_db.execute(text("UPDATE Subscriptions SET IsActive=1 WHERE SubscriptionID=:sid"), {"sid": sub_id})
        elif action == "refund":
            await session_db.execute(text("UPDATE Subscriptions SET Amount=0.00, IsActive=0 WHERE SubscriptionID=:sid"), {"sid": sub_id})
        else:
            raise ValueError("Unknown action")
        await session_db.commit()
        return redirect(url_for("admin_subscriptions"))
    r = await session_db.execute(text("""
        SELECT s.SubscriptionID, s.UserID, s.SubscriptionLevel, s.SubscriptionStartDate,
               s.Amount, s.PayPalTransactionID, s.IsActive, u.Email
        FROM Subscriptions s JOIN Users u ON s.UserID=u.UserID
        ORDER BY s.SubscriptionID DESC
    """))
    subs = [dict(x) for x in r.mappings().all()]
    return render_template_string("""
<!DOCTYPE html>
<html>
  <head><meta charset="UTF-8"><title>Admin - Subscriptions</title></head>
  <body>
    {{ navbar|safe }}
    <div class="container">
      <h1>Manage Subscriptions</h1>
      <table style="width:100%; border-collapse:collapse; margin-top:20px;">
        <tr style="background:#0078D7; color:#fff;">
          <th style="padding:8px;">ID</th>
          <th style="padding:8px;">User Email</th>
          <th style="padding:8px;">Level</th>
          <th style="padding:8px;">Start Date</th>
          <th style="padding:8px;">Amount</th>
          <th style="padding:8px;">PayPal Txn</th>
          <th style="padding:8px;">Status</th>
          <th style="padding:8px;">Actions</th>
        </tr>
        {% for sub in subs %}
        <tr>
          <td style="border:1px solid #ddd; padding:8px;">{{ sub.SubscriptionID }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ sub.Email }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ sub.SubscriptionLevel }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ sub.SubscriptionStartDate }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ sub.Amount }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ sub.PayPalTransactionID or "N/A" }}</td>
          <td style="border:1px solid #ddd; padding:8px;">{{ "Active" if sub.IsActive else "Inactive" }}</td>
          <td style="border:1px solid #ddd; padding:8px;">
            <form method="POST" style="display:inline;">
              <input type="hidden" name="subscription_id" value="{{ sub.SubscriptionID }}">
              {% if sub.IsActive %}
                <button type="submit" name="action" value="deactivate">Deactivate</button>
              {% else %}
                <button type="submit" name="action" value="activate">Activate</button>
              {% endif %}
              <button type="submit" name="action" value="refund">Refund</button>
            </form>
          </td>
        </tr>
        {% endfor %}
      </table>
      <p><a href="{{ url_for('admin_portal') }}">Back to Admin</a></p>
    </div>
  </body>
</html>
""", navbar=navbar_html, subs=subs)

##############################################################################
# MAIN ENTRY POINT
##############################################################################
if __name__ == "__main__":
    ensure_database_exists()
    app.run(host="localhost", port=5000)
