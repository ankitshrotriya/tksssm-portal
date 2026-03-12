from flask import Flask, render_template, request, redirect, url_for, flash
from flask_cors import CORS
import psycopg2
from psycopg2 import sql
import os
from datetime import datetime
from decimal import Decimal
from decimal import Decimal, ROUND_HALF_UP
from flask import session
#from waitress import serve
import sys
from datetime import datetime
from flask import request
import json
from flask import jsonify
from datetime import date, timedelta
from psycopg2 import sql
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from flask import Flask, render_template, request, redirect, flash
import psycopg2
import pandas as pd


# --- Flask Setup ---
from flask import Flask

app = Flask(
    __name__,
    static_folder="static",
    static_url_path="/static"
)

app.secret_key = "secret_key"
from flask_cors import CORS

CORS(app, resources={r"/*": {"origins": "*"}})


# 🔵 ADD THIS PART
from union_portal import union
app.register_blueprint(union)

print(app.url_map)




# Folder for file uploads
UPLOAD_FOLDER = os.path.join("static", "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# --- Database Setup ---
def get_db():
    DATABASE_URL = os.environ.get("DATABASE_URL")

    conn = psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )

    return conn


# ✅ Function to generate sequential account numbers
def generate_account_no(cursor):
    cursor.execute("""
        SELECT MAX(CAST(SUBSTRING(account_no FROM 3) AS INTEGER))
        FROM accounts
        WHERE account_no LIKE 'AC%'
    """)
    last_no = cursor.fetchone()[0]
    new_no = (last_no or 0) + 1
    return f"AC{new_no:05d}"

    
# --- LOGOUT ---
@app.route("/logout")
def logout():
    # ✅ Clear the session (remove all saved user data)
    session.clear()

    # ✅ Optional: Flash message for user feedback
    flash("✅ You have been logged out successfully!", "success")

    # ✅ Redirect to login page
    return redirect(url_for("login"))


# --- LOGIN ---
@app.route("/", methods=["GET", "POST"])
def login():
    error = None

    print("🟢 LOGIN ROUTE ACCESSED", flush=True)

    if request.method == "POST":

        print("🟡 LOGIN POST REQUEST RECEIVED", flush=True)

        username = request.form["username"]
        password = request.form["password"]

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM users WHERE username=%s AND password=%s",
            (username, password),
        )
        user = cur.fetchone()
        cur.close()
        conn.close()

        if user:
            session["user_id"] = user[0]
            session["username"] = username

            print(f"🔐 LOGIN SUCCESS → {username} | {datetime.now()} | IP: {request.remote_addr}", flush=True)

            return redirect("/dashboard")

        else:
            print(f"❌ LOGIN FAILED → {username} | {datetime.now()} | IP: {request.remote_addr}", flush=True)
            error = "Invalid username or password"

    return render_template("login.html", error=error)
    
    
    
from datetime import datetime, timedelta
from decimal import Decimal
from dateutil.relativedelta import relativedelta
from apscheduler.schedulers.background import BackgroundScheduler





def auto_renew_rd():
    """
    Auto-renew latest closed RDs per member whose auto_renew=True
    """
    conn = get_db()
    cur = conn.cursor()

    # 1️⃣ Fetch latest closed RDs eligible for auto-renew per member
    cur.execute("""
        SELECT r1.rd_account_no, r1.member_no, r1.member_name, r1.monthly_deposit,
               r1.duration_months, r1.interest_rate, r1.nominee_name, r1.remark, r1.maturity_date
        FROM rd_accounts r1
        WHERE r1.status='Closed' AND r1.auto_renew=True
          AND (r1.maturity_date + INTERVAL '1 month') <= CURRENT_DATE
          AND NOT EXISTS (
              SELECT 1 FROM rd_accounts r2
              WHERE r2.member_no = r1.member_no
                AND r2.start_date > r1.start_date
          )
    """)
    rds = cur.fetchall()

    for rd in rds:
        rd_account_no, member_no, member_name, monthly_deposit, duration_months, interest_rate, nominee_name, remark, maturity_date = rd

        # 🔹 Fetch latest ACTIVE RD interest rate from master table
        cur.execute("""
            SELECT rate
            FROM interest_rates
            WHERE category='RD'
              AND status='Active'
            ORDER BY id DESC
            LIMIT 1
        """)
        rate_row = cur.fetchone()

        if rate_row:
            interest_rate = Decimal(rate_row[0])   # ✅ overwrite old RD rate
        else:
            interest_rate = Decimal(interest_rate) # fallback if table empty

        # 2️⃣ Generate new RD account number
        cur.execute("SELECT MAX(rd_account_no) FROM rd_accounts")
        max_no = cur.fetchone()[0] or "RD00000"
        num = int(max_no.replace("RD", "")) + 1
        new_rd_account_no = f"RD{num:05d}"

        # 3️⃣ Calculate start date, maturity date, maturity amount
        start_dt = maturity_date + relativedelta(months=1, day=maturity_date.day)
        cur.execute("""
            SELECT 1 FROM rd_accounts
            WHERE member_no=%s AND start_date=%s
        """, (member_no, start_dt))

        if cur.fetchone():
            continue   # 🚫 already auto-renewed, skip safely
        maturity_dt = start_dt + relativedelta(months=duration_months)
        maturity_amount = sum([
            monthly_deposit * (1 + (interest_rate / 100) * (duration_months - i + 1) / 12)
            for i in range(1, duration_months + 1)
        ])
        maturity_amount = Decimal(maturity_amount).quantize(Decimal("0.01"))

        # 4️⃣ Insert new RD with installments_paid = 1
        cur.execute("""
            INSERT INTO rd_accounts
            (rd_account_no, member_no, member_name, start_date, duration_months,
             interest_rate, monthly_deposit, total_installments, maturity_date,
             maturity_amount, nominee_name, remark, status, installments_paid, auto_renew)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Active',1, True)
        """, (
            new_rd_account_no, member_no, member_name, start_dt, duration_months,
            interest_rate, monthly_deposit, duration_months, maturity_dt,
            maturity_amount, nominee_name, remark
        ))

        # 5️⃣ Record first installment (debit from saving account)
        cur.execute("""
            SELECT account_no, balance
            FROM accounts
            WHERE member_no=%s AND account_type='Saving Account' AND status='Active'
        """, (member_no,))
        saving = cur.fetchone()
        if not saving or Decimal(saving[1]) < monthly_deposit:
            print(f"[AUTO-RENEW SKIPPED] Member {member_no} insufficient balance")
            continue
        if saving:
            saving_acc_no, balance = saving
            balance = Decimal(balance)
            if balance >= monthly_deposit:
                new_balance = balance - monthly_deposit
                cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s", (new_balance, saving_acc_no))

                # Debit Saving
                cur.execute("""
                    INSERT INTO transactions (member_no, account_no, trans_type, amount, trans_date, remark, created_by)
                    VALUES (%s,%s,'Debit',%s,%s,%s,'admin')
                """, (member_no, saving_acc_no, monthly_deposit, start_dt, f"RD Auto-Renew Created ({new_rd_account_no})"))

                # Credit RD
                cur.execute("""
                    INSERT INTO transactions (member_no, rd_account_no, trans_type, amount, trans_date, remark, created_by)
                    VALUES (%s,%s,'Credit',%s,%s,%s,'system')
                """, (member_no, new_rd_account_no, monthly_deposit, start_dt, f"RD Auto-Renew Credit ({new_rd_account_no})"))

                # ⭐ Save interest for first installment
                monthly_interest = (monthly_deposit * interest_rate / Decimal(100) / Decimal(12)).quantize(Decimal("0.01"))
                cur.execute("""
                    INSERT INTO interest_history
                    (created_on, principal, interest_rate, monthly_interest, month_year,
                     account_type, account_no, member_no, member_name)
                    VALUES (NOW(), %s, %s, %s, %s, 'RD', %s, %s, %s)
                """, (monthly_deposit, interest_rate, monthly_interest, start_dt.strftime("%Y-%m-01"), new_rd_account_no, member_no, member_name))

    conn.commit()
    cur.close()
    conn.close()

scheduler = BackgroundScheduler()
scheduler.add_job(auto_renew_rd, 'cron', hour=2)


# --- DASHBOARD ---
# --- DASHBOARD ---
@app.route("/dashboard")
def dashboard():
    check_fd_maturity()   # 👈 auto check here
    fd_yearly_interest()

    conn = get_db()
    cur = conn.cursor()

    # Count active accounts
    cur.execute("SELECT COUNT(*) FROM accounts WHERE status = 'Active'")
    active_accounts = cur.fetchone()[0]

    # Count active FDs
    cur.execute("SELECT COUNT(*) FROM fd_accounts WHERE status = 'Active'")
    active_fds = cur.fetchone()[0]
    
    # Count active loans
    cur.execute("SELECT COUNT(*) FROM loans WHERE status = 'Active'")
    active_loans = cur.fetchone()[0]
    
    # Count active members
    cur.execute("SELECT COUNT(*) FROM members WHERE status = 'Active'")
    active_members = cur.fetchone()[0]

    cur.close()
    conn.close()

    return render_template("index.html",
                           active_accounts=active_accounts,
                           active_fds=active_fds, active_loans=active_loans, active_members=active_members)




# --- OPEN MEMBER FORM ---
# --- OPEN MEMBER FORM ---
@app.route("/open_member", methods=["GET"])
def open_member():
    conn = get_db()
    cur = conn.cursor()

    # Fetch guarantor list (only members with member_type='Member')
    cur.execute(
        "SELECT member_no, member_name_eng, member_type FROM public.members WHERE member_type = 'Member'"
    )
    guarantors = cur.fetchall()

    # ✅ Generate sequential Member Number (TKSSSM00001 format)
    cur.execute(
        """
        SELECT member_no 
        FROM public.members 
        WHERE member_no LIKE 'TKSSSM%' 
        ORDER BY member_no DESC 
        LIMIT 1;
        """
    )
    last_member = cur.fetchone()

    if last_member and last_member[0]:
        try:
            last_no = int(''.join(filter(str.isdigit, last_member[0])))  # extract numeric part only
        except:
            last_no = 0
        new_no = last_no + 1
    else:
        new_no = 1  # first record

    # Default Member No without suffix
    new_member_no = f"TKSSSM{new_no:05d}"

    cur.close()
    conn.close()

    return render_template(
        "open_member.html", guarantors=guarantors, new_member_no=new_member_no
    )



# --- SAVE MEMBER ---
# --- SAVE MEMBER ---
# --- SAVE MEMBER ---
@app.route("/member/save", methods=["POST"])
def save_member():
    try:
        data = {key: request.form.get(key) for key in request.form.keys()}

        # ✅ Handle boolean fields correctly
        close_status_value = request.form.get("close_status", "").lower()
        if close_status_value in ["true", "yes", "1", "operational"]:
            data["close_status"] = True
        else:
            data["close_status"] = False

        # Default values for optional fields
        data["loan_taken"] = data.get("loan_taken") or "No"
        data["medical_insurance"] = data.get("medical_insurance") or "No"
        data["member_type"] = data.get("member_type") or "Member"
        data["remark"] = data.get("remark") or "Active"
        data["status"] = data.get("status") or "Active"

        # ✅ Handle uploaded files
        file_fields = ["member_photo_path", "member_sign_path", "nominee_photo_path"]
        for field in file_fields:
            file = request.files.get(field)
            if file and file.filename:
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
                file.save(filepath)
                data[field] = filepath
            else:
                data[field] = None

        # --- Insert data into database ---
        conn = get_db()
        cur = conn.cursor()

        columns = list(data.keys())
        values = [data[c] for c in columns]

        insert_query = sql.SQL(
            """
            INSERT INTO public.members ({})
            VALUES ({})
            """
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )

        cur.execute(insert_query, values)

                # =====================================================
        # ✅ AUTO CREATE USER ENTRY (PLAIN PASSWORD ONLY)
        # =====================================================
        username = data.get("member_no")
        mobile = data.get("member_mobile_no")
        
        password = str(mobile) if mobile else "NULL"

        cur.execute(
            """
            INSERT INTO public.users (username, password, role, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (username) DO NOTHING
            """,
            (
                username,
                password,   # 👈 ALWAYS PLAIN
                "user",
                "Active",
            ),
        )


        # ✅ Auto-create accounts depending on member type
        member_type = data.get("member_type")
        member_no = data.get("member_no")
        opening_date = data.get("opening_date")

        if member_type == "Member":
            account_types = ["Anivarya Sanchay", "Share Account", "Saving Account"]
        elif member_type == "Initial":
            account_types = ["Saving Account"]
        else:
            account_types = []

        for acc_type in account_types:
            acc_no = generate_account_no(cur)
            cur.execute(
                """
                INSERT INTO accounts 
                (member_no, account_no, account_type, opening_date, balance, status, remark, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    member_no,
                    acc_no,
                    acc_type,
                    opening_date,
                    0.00,
                    "Active",
                    "Auto-created for new member",
                    "admin",
                ),
            )

        conn.commit()
        cur.close()
        conn.close()

        flash("✅ Member saved successfully with default accounts!", "success")
        return redirect("/dashboard")

    except Exception as e:
        print("❌ Error:", e)
        flash(f"❌ Error saving member: {e}", "danger")
        return redirect("/open_member")



        
       
# --- GET ACCOUNTS BY MEMBER ---
@app.route("/get_accounts/<member_no>")
def get_accounts(member_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT account_no, account_type FROM accounts WHERE member_no = %s", (member_no,))
    accounts = [{"account_no": a[0], "account_type": a[1]} for a in cur.fetchall()]
    cur.close()
    conn.close()
    return {"accounts": accounts}
    
    
 # --- CREDIT / DEBIT FORM ---
# --- CREDIT / DEBIT FORM ---
# --- CREDIT / DEBIT FORM (updated for RD Installment handling) ---
# --- CREDIT / DEBIT FORM (FINAL with RD maturity & auto-transfer) ---
# --- CREDIT / DEBIT FORM (FINAL) ---

@app.route("/credit_debit", methods=["GET", "POST"])
def credit_debit():
    conn = get_db()
    cur = conn.cursor()

    # Load active members for GET
    cur.execute("SELECT member_no, member_name_eng FROM members WHERE status='Active'")
    members = cur.fetchall()

    # Load banks for GET (bank_name dropdown)
    cur.execute("SELECT id, head_name FROM loan_heads WHERE status='Active'")
    banks = cur.fetchall()  # [(id, head_name), ...]

    if request.method == "POST":

        # ========= GET ALL FORM DATA =========
        purpose = request.form.get("purpose", "Normal")
        member_no = request.form.get("member_no")
        account_no = request.form.get("account_no")
        rd_account_no = request.form.get("rd_account_no")
        trans_type = request.form.get("trans_type", "Debit")
        amount = Decimal(request.form.get("amount") or 0)
        remark = request.form.get("remark", "")
        source = request.form.get("source", "")
        voucher_no = request.form.get("voucher_no") or ""
        bank_name = request.form.get("pedi_bank") or ""  # text
        bank_id = None

        # Cheque/DD fields
        cheque_no = request.form.get("cheque_no", "")
        dd_no = request.form.get("dd_no", "")
        issued_bank = request.form.get("issued_bank", "")
        issue_date = request.form.get("issue_date", None)
        if not issue_date:
            issue_date = None  # empty date ko None me convert kar diya, taaki Cash me error na aaye

        # Fetch bank_id from loan_heads
        if bank_name:
            cur.execute("SELECT id FROM loan_heads WHERE head_name=%s", (bank_name,))
            res = cur.fetchone()
            if res:
                bank_id = res[0]

                    # ------------------ CASE 1: RD Installment ------------------
        if purpose == "RD":

            if not rd_account_no:
                flash("❌ Please select RD Account.", "danger")
                cur.close()
                conn.close()
                return redirect("/credit_debit")

            # Check RD status
            cur.execute("SELECT status FROM rd_accounts WHERE rd_account_no=%s", (rd_account_no,))
            rd_status = cur.fetchone()
            if rd_status and rd_status[0] == "Closed":
                flash(f"❌ RD Account {rd_account_no} is already closed!", "danger")
                cur.close()
                conn.close()
                return redirect("/credit_debit")

            # Fetch saving account
            cur.execute("""
                SELECT account_no, balance 
                FROM accounts 
                WHERE member_no=%s AND account_type='Saving Account' AND status='Active'
            """, (member_no,))
            saving = cur.fetchone()

            if not saving:
                flash("❌ No active Saving Account found.", "danger")
                cur.close()
                conn.close()
                return redirect("/credit_debit")

            saving_acc_no, saving_balance = saving
            saving_balance = Decimal(saving_balance)

            if saving_balance < amount:
                flash(f"❌ Insufficient balance! Available ₹{saving_balance}", "danger")
                cur.close()
                conn.close()
                return redirect("/credit_debit")

            # Cheque fields
            cheque_no = request.form.get("cheque_no")
            dd_no = request.form.get("dd_no")
            issued_bank = request.form.get("issued_bank")
            issue_date = request.form.get("issue_date")
            voucher_no = request.form.get("voucher_no")
            bank_name = request.form.get("bank_name")
            bank_id = request.form.get("bank_id")

            if source == "Cash":
                cheque_no = None
                dd_no = None
                issued_bank = None
                issue_date = None

            # Debit Saving
            new_balance = saving_balance - amount
            cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s", (new_balance, saving_acc_no))

            # Increment installment count
            cur.execute("""
                UPDATE rd_accounts 
                SET installments_paid = installments_paid + 1 
                WHERE rd_account_no=%s
            """, (rd_account_no,))

            # Transaction entries
            cur.execute("""
                INSERT INTO transactions 
                (member_no, account_no, trans_type, amount, remark, source, created_by,
                 cheque_no, dd_no, issued_bank, issue_date, bank_name, voucher_no, bank_id)
                VALUES (%s,%s,'Debit',%s,%s,%s,'admin',
                        %s,%s,%s,%s,%s,%s,%s)
            """, (
                member_no, saving_acc_no, amount, f"RD Installment ({rd_account_no})",
                source, cheque_no, dd_no, issued_bank, issue_date, bank_name, voucher_no, bank_id
            ))

            cur.execute("""
                INSERT INTO transactions 
                (member_no, rd_account_no, trans_type, amount, remark, source, created_by,
                 cheque_no, dd_no, issued_bank, issue_date, bank_name, voucher_no, bank_id)
                VALUES (%s,%s,'Credit',%s,%s,%s,'system',
                        %s,%s,%s,%s,%s,%s,%s)
            """, (
                member_no, rd_account_no, amount, f"RD Installment Received ({rd_account_no})",
                source, cheque_no, dd_no, issued_bank, issue_date, bank_name, voucher_no, bank_id
            ))

            # ------------------ INTEREST CALCULATION ------------------
            cur.execute("""
                SELECT monthly_deposit, interest_rate, member_name 
                FROM rd_accounts 
                WHERE rd_account_no = %s
            """, (rd_account_no,))
            rd_info = cur.fetchone()

            if rd_info:
                monthly_deposit, interest_rate, member_name = rd_info
                monthly_deposit = Decimal(monthly_deposit)
                interest_rate = Decimal(interest_rate)

                cur.execute("""
                    SELECT COUNT(*) 
                    FROM interest_history
                    WHERE account_type='RD' AND account_no=%s
                """, (rd_account_no,))
                count = cur.fetchone()[0]

                principal = (count + 1) * monthly_deposit
                month_year = datetime.now().strftime("%Y-%m-01")
                monthly_interest = (principal * interest_rate / 100 / 12).quantize(Decimal("0.01"))

                cur.execute("""
                    INSERT INTO interest_history
                    (account_type, account_no, member_no, member_name, month_year,
                     principal, interest_rate, monthly_interest, created_on)
                    VALUES ('RD', %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    rd_account_no, member_no, member_name, month_year,
                    principal, interest_rate, monthly_interest
                ))

            # ------------------ MATURITY CHECK ------------------
            cur.execute("""
                SELECT member_no, monthly_deposit, installments_paid, total_installments, maturity_amount, status
                FROM rd_accounts 
                WHERE rd_account_no=%s
            """, (rd_account_no,))
            rd = cur.fetchone()

            if rd:
                member_no, monthly_deposit, paid, total, maturity_amount, status = rd
                if paid >= total and status != "Closed":

                    cur.execute("""
                        UPDATE rd_accounts 
                        SET status='Closed', closed_date=NOW() 
                        WHERE rd_account_no=%s
                    """, (rd_account_no,))

                    # 🔻 RD Debit
                    cur.execute("""
                        INSERT INTO transactions 
                        (member_no, rd_account_no, trans_type, amount, remark, source, created_by)
                        VALUES (%s,%s,'Debit',%s,%s,'System','system')
                    """, (
                        member_no,
                        rd_account_no,
                        maturity_amount,
                        f"RD Maturity Paid ({rd_account_no})"
                    ))

                    # 🔻 Saving Credit
                    cur.execute("""
                        SELECT account_no, balance 
                        FROM accounts 
                        WHERE member_no=%s AND account_type='Saving Account' AND status='Active'
                    """, (member_no,))
                    acc = cur.fetchone()

                    if acc:
                        saving_acc_no, bal = acc
                        new_bal = Decimal(bal) + Decimal(maturity_amount)

                        cur.execute(
                            "UPDATE accounts SET balance=%s WHERE account_no=%s",
                            (new_bal, saving_acc_no)
                        )

                        cur.execute("""
                            INSERT INTO transactions 
                            (member_no, account_no, trans_type, amount, remark, source, created_by)
                            VALUES (%s,%s,'Credit',%s,%s,'System','system')
                        """, (
                            member_no,
                            saving_acc_no,
                            maturity_amount,
                            f"RD Maturity Received ({rd_account_no})"
                        ))

                        flash(f"✅ RD {rd_account_no} matured and ₹{maturity_amount} credited!", "success")

            conn.commit()
            cur.close()
            conn.close()

            flash("✅ RD Installment recorded successfully!", "success")
            return redirect("/credit_debit")



            # ------------------ CUMULATIVE INTEREST ------------------
            cur.execute("""
                SELECT monthly_deposit, interest_rate, member_name 
                FROM rd_accounts 
                WHERE rd_account_no = %s
            """, (rd_account_no,))
            rd_info = cur.fetchone()

            if rd_info:
                monthly_deposit = Decimal(rd_info[0])
                interest_rate = Decimal(rd_info[1])
                member_name = rd_info[2]

                # Count installments already paid (interest_history rows)
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM interest_history
                    WHERE account_type='RD' AND account_no=%s
                """, (rd_account_no,))
                count = cur.fetchone()[0]

                # Correct cumulative principal
                principal = (count + 1) * monthly_deposit

                # Correct month_year format
                month_year = datetime.now().strftime("%Y-%m-01")

                # Monthly interest
                monthly_interest = (principal * interest_rate / 100 / 12).quantize(Decimal("0.01"))

                # Insert into interest_history
                cur.execute("""
                    INSERT INTO interest_history
                    (account_type, account_no, member_no, member_name, month_year,
                     principal, interest_rate, monthly_interest, created_on)
                    VALUES ('RD', %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    rd_account_no,
                    member_no,
                    member_name,
                    month_year,
                    principal,
                    interest_rate,
                    monthly_interest
                ))

            # ------------------ RD Maturity Check ------------------
            cur.execute("""
                SELECT member_no, monthly_deposit, installments_paid, total_installments, maturity_amount, status
                FROM rd_accounts 
                WHERE rd_account_no=%s
            """, (rd_account_no,))
            rd = cur.fetchone()

            if rd:
                member_no, monthly_deposit, paid, total, maturity_amount, status = rd

                if paid >= total and status != "Closed":
                    cur.execute("""
                        UPDATE rd_accounts 
                        SET status='Closed', closed_date=NOW() 
                        WHERE rd_account_no=%s
                    """, (rd_account_no,))

                    # Credit maturity to saving
                    cur.execute("""
                        SELECT account_no, balance 
                        FROM accounts 
                        WHERE member_no=%s AND account_type='Saving Account' AND status='Active'
                    """, (member_no,))
                    res = cur.fetchone()

                    if res:
                        saving_acc_no, bal = res
                        new_bal = Decimal(bal) + Decimal(maturity_amount)

                        cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s", (new_bal, saving_acc_no))

                        cur.execute("""
                            INSERT INTO transactions (member_no, account_no, trans_type, amount, remark, source, created_by)
                            VALUES (%s,%s,'Credit',%s,%s,%s,%s)
                        """, (
                            member_no,
                            saving_acc_no,
                            maturity_amount,
                            f"RD Maturity Received ({rd_account_no})",
                            "System",
                            "system"
                        ))

                        flash(f"✅ RD {rd_account_no} matured and ₹{maturity_amount} credited!", "success")

            conn.commit()
            cur.close()
            conn.close()
            flash("✅ RD Installment recorded successfully!", "success")
            return redirect("/credit_debit")


        # ------------------ CASE 2: NORMAL TRANSACTION ------------------
        else:

            if not account_no:
                flash("❌ Please select Account.", "danger")
                return redirect("/credit_debit")

            cur.execute("SELECT balance FROM accounts WHERE account_no=%s", (account_no,))
            acc = cur.fetchone()

            if not acc:
                flash("❌ Account not found!", "danger")
                return redirect("/credit_debit")

            balance = Decimal(acc[0])
            if trans_type == "Debit":
                if balance < amount:
                    flash(f"❌ Insufficient Balance! Available ₹{balance}", "danger")
                    return redirect("/credit_debit")
                new_balance = balance - amount
            else:
                new_balance = balance + amount

            cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s", (new_balance, account_no))

            # Insert Transaction (Normal)
            cur.execute("""
                INSERT INTO transactions
                (member_no, account_no, trans_type, amount, remark, source, bank_name, bank_id, voucher_no, cheque_no, dd_no, issued_bank, issue_date, created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'admin')
            """, (
                member_no,
                account_no,
                trans_type,
                amount,
                remark,
                source,
                bank_name,
                bank_id,
                voucher_no,
                cheque_no,
                dd_no,
                issued_bank,
                issue_date
            ))

            # Update loan_heads & bank_transactions if bank selected
            if bank_id and amount > 0:
                # First check head_name
                cur.execute("SELECT head_name FROM loan_heads WHERE id=%s", (bank_id,))
                result = cur.fetchone()
                head_name = result[0] if result else None

                # Default trans for inserting into bank_transactions
                insert_trans_type = trans_type  

                if head_name == "सिल्लक संस्था":
                    # SAME direction
                    if trans_type == "Debit":
                        cur.execute("UPDATE loan_heads SET amount = amount - %s WHERE id=%s",
                                    (amount, bank_id))
                    else:  # Credit
                        cur.execute("UPDATE loan_heads SET amount = amount + %s WHERE id=%s",
                                    (amount, bank_id))

                else:
                    # OPPOSITE direction
                    if trans_type == "Debit":
                        cur.execute("UPDATE loan_heads SET amount = amount + %s WHERE id=%s",
                                    (amount, bank_id))
                        insert_trans_type = "Credit"
                    else:  # Credit
                        cur.execute("UPDATE loan_heads SET amount = amount - %s WHERE id=%s",
                                    (amount, bank_id))
                        insert_trans_type = "Debit"

                # Insert into bank_transactions with updated type
                cur.execute("""
                    INSERT INTO bank_transactions
                    (loan_no, bank_name, trans_type, amount, voucher_no, member_no, bank_id, remark, created_by, created_on)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'Admin',NOW())
                """, (
                    account_no,
                    bank_name,
                    insert_trans_type,
                    amount,
                    voucher_no,
                    member_no,
                    bank_id,
                    remark
                ))

            conn.commit()
            flash(f"✅ {trans_type} ₹{amount:.2f} completed successfully!", "success")
            return redirect("/credit_debit")

    # =========== GET REQUEST ============
    return render_template("credit_debit.html", members=members, banks=banks)











    
    
# --- FD FORM ---
@app.route("/fd", methods=["GET"])
def fd_form():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT member_no, member_name_eng FROM members WHERE status='Active'")
    members = cur.fetchall()
    cur.close()
    conn.close()

    members_json = json.dumps(members)

    # Auto-generate FD Account No
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT fd_account_no FROM fd_accounts ORDER BY fd_account_no DESC LIMIT 1")
    last = cur.fetchone()
    new_fd_no = f"FD{(int(last[0].replace('FD','')) + 1):05d}" if last else "FD00001"
    cur.close()
    conn.close()

    return render_template("fd.html", members=members, members_json=members_json, new_fd_no=new_fd_no)






# --- SAVE FD ---
@app.route("/fd/save", methods=["POST"])
def save_fd():
    try:
        data = {key: request.form.get(key) for key in request.form.keys()}
        member_no = data["member_no"]
        amount = float(data.get("deposit_amt", 0))
        data["deposit_amount"] = amount
        # Map frontend fields to DB fields
        data["deposit_amount"] = data.get("deposit_amt")
        data["maturity_amount"] = data.get("maturity_amt")
        auto_renew = request.form.get("auto_renew") == "on"
        withdraw_yearly_interest = request.form.get("withdraw_interest") == "on"
        voucher_no = request.form.get("voucher_no")


  # fixed

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT account_no, balance FROM accounts WHERE member_no=%s AND account_type='Saving Account' AND status='Active'",
            (member_no,)
        )
        res = cur.fetchone()

        if not res:
            flash("❌ No active Saving Account found.", "danger")
            cur.close()
            conn.close()
            return redirect("/fd")

        saving_acc_no, balance = res
        balance = float(balance)

        if balance < amount:
            flash(f"❌ Insufficient balance! Available ₹{balance}", "danger")
            cur.close()
            conn.close()
            return redirect("/fd")

        # Deduct balance
        new_balance = balance - amount
        cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s", (new_balance, saving_acc_no))

        # Record Debit transaction
        cur.execute("""
            INSERT INTO transactions 
            (member_no, account_no, trans_type, amount, trans_date, remark, created_by,fd_account_no,voucher_no)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (member_no, saving_acc_no, "Debit", amount, datetime.now().date(), f"FD Created ({data['fd_account_no']})", "admin",data["fd_account_no"],voucher_no))
        
        

        # Save FD record
        cur.execute("""
            INSERT INTO fd_accounts 
            (fd_account_no, member_no, member_name, deposit_amount, interest_rate, 
             start_date, maturity_date, maturity_amount, fd_duration, 
             nominee_name, remark, status,auto_renew,withdraw_yearly_interest)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            data["fd_account_no"], member_no, data["member_name"], amount, data["interest_rate"],
            data["start_date"] , data["maturity_date"], data["maturity_amount"], data["fd_duration"],
            data.get("nominee_name", ""), data.get("remark", "Active"), "Active",auto_renew,withdraw_yearly_interest
        ))
        
        conn.commit()
        start_date_obj = datetime.strptime(data["start_date"], "%Y-%m-%d").date()
        duration_days = int(data["fd_duration"])

        generate_fd_monthly_interest(
            data["fd_account_no"],
            member_no,
            data["member_name"],
            amount,
            Decimal(str(data["interest_rate"])),
            start_date_obj,
            duration_days
        )
        cur.close()
        conn.close()

        flash("✅ FD created successfully! (Amount debited from Saving Account)", "success")
        return redirect("/dashboard")

    except Exception as e:
        print("❌ Error while saving FD:", e)
        flash(f"❌ Error creating FD: {e}", "danger")
        return redirect("/fd")

        


       


        
        
# ✅ Manual button to check and close matured FDs
@app.route("/fd/check_maturity")
def fd_check_maturity():
    count = check_fd_maturity()
    flash(f"✅ {count} matured FDs closed and transferred successfully!", "success")
    return redirect("/dashboard")


def generate_fd_no(cur):
    cur.execute("""
        SELECT MAX(CAST(SUBSTRING(fd_account_no,3) AS INTEGER))
        FROM fd_accounts
    """)

    last = cur.fetchone()[0]

    if last:
        new_no = last + 1
    else:
        new_no = 1

    return f"FD{new_no:05d}"        

        
        
        
        
from decimal import Decimal
        
# ✅ Auto-check FD maturity and transfer amount to Saving Account
def check_fd_maturity():
    conn = get_db()
    cur = conn.cursor()

    today = datetime.now().date()

    # 1️⃣ Find all matured FDs which are Active
    cur.execute("""
        SELECT fd_account_no, member_no, maturity_amount
        FROM fd_accounts
        WHERE maturity_date <= %s AND status = 'Active'
    """, (today,))
    matured_fds = cur.fetchall()

    for fd_account_no, member_no, maturity_amount in matured_fds:

        # 🔍 Fetch more details including auto_renew
        cur.execute("""
            SELECT deposit_amount, interest_rate, fd_duration, member_name,
                   nominee_name, auto_renew,withdraw_yearly_interest, last_interest_date, start_date
            FROM fd_accounts
            WHERE fd_account_no = %s
        """, (fd_account_no,))
        fd = cur.fetchone()

        deposit_amount, interest_rate, fd_duration, member_name, nominee_name, auto_renew,withdraw_yearly_interest, last_interest_date, start_date = fd

        # 🔍 Get saving account number
        cur.execute("""
            SELECT account_no FROM accounts 
            WHERE member_no=%s AND account_type='Saving Account'
        """, (member_no,))
        saving_acc = cur.fetchone()
        
        # 2️⃣ Calculate correct maturity amount
        deposit_amount = Decimal(deposit_amount)
        interest_rate = Decimal(interest_rate)
        fd_duration_days = int(fd_duration)

        if withdraw_yearly_interest:
            # Interest already paid yearly, only remaining interest since last credited
            last_date = last_interest_date or start_date
            days_for_interest = (today - last_date).days
            remaining_interest = (deposit_amount * interest_rate / Decimal('100') * Decimal(days_for_interest) / Decimal('365')).quantize(Decimal("0.01"))
            maturity_amount_to_credit = deposit_amount + remaining_interest
        else:
            # Full interest + principal
            duration_years = Decimal(fd_duration_days) / Decimal('365')
            total_interest = (deposit_amount * interest_rate / Decimal('100') * duration_years).quantize(Decimal("0.01"))
            maturity_amount_to_credit = deposit_amount + total_interest

        # 2️⃣ Add maturity amount to Saving Account
        if saving_acc:
            cur.execute("""
                UPDATE accounts 
                SET balance = balance + %s 
                WHERE member_no = %s AND account_type = 'Saving Account'
            """, (maturity_amount_to_credit, member_no))

            # Record Credit transaction
            cur.execute("""
                INSERT INTO transactions (member_no, account_no, trans_type, amount, trans_date, remark, created_by,fd_account_no)
                VALUES (%s, %s, 'Credit', %s, %s, %s, 'system',%s)
            """, (
                member_no,
                saving_acc[0],
                maturity_amount_to_credit,
                today,
                f"FD Matured ({fd_account_no})",
                fd_account_no
            ))

        # 3️⃣ Close OLD FD
        cur.execute("""
            UPDATE fd_accounts 
            SET status='Closed', is_closed=TRUE, closed_at=%s, remark='Matured'
            WHERE fd_account_no=%s
        """, (today, fd_account_no))

        # 4️⃣ IF AUTO RENEW = TRUE → Create new FD
        # 4️⃣ IF AUTO RENEW = TRUE → Create new FD
        if auto_renew:

            # Generate new FD number
            new_fd_no = generate_fd_no(cur)

            new_start = today

            # Detect FD Type (Days or Years)
            fd_duration_days = int(fd_duration)
            duration_years = Decimal(fd_duration_days) / Decimal('365')
            
            # ⭐ Decide tenure label based on days  
            #  YE BHI ABHI ADD KIYA HAI 
            if 46 <= fd_duration_days <= 179:
                tenure_label = "46 - 179 Days"
            elif 180 <= fd_duration_days <= 364:
                tenure_label = "180 - 364 Days"
            elif 365 <= fd_duration_days < 730:
                tenure_label = "1 Year"
            elif 730 <= fd_duration_days < 1095:
                tenure_label = "2 Years"
            else:
                tenure_label = "3 Years"
                
            cur.execute("""
                SELECT rate
                FROM interest_rates
                WHERE category = 'FD'
                AND tenure = %s
                AND status = 'Active'
                ORDER BY id DESC
                LIMIT 1
            """, (tenure_label,))

            rate_row = cur.fetchone()

            if rate_row:
                interest_rate = Decimal(rate_row[0])   # ⭐ Renewed FD ab current rate pe banegi



            if fd_duration_days < 365:
                new_end = today + timedelta(days=fd_duration_days)
            else:
                new_end = today.replace(year=today.year + int(duration_years))
                
            renew_interest = deposit_amount * (interest_rate / Decimal('100')) * duration_years

                
            if withdraw_yearly_interest:
                # ✅ Auto renew principal only, interest already yearly credited
                renew_interest = Decimal('0')
                new_principal = deposit_amount
            else:
                # New principal = principal + interest
                new_principal = deposit_amount + renew_interest
                
            next_cycle_interest = new_principal * (interest_rate / Decimal('100')) * duration_years
            new_maturity_amount = new_principal + next_cycle_interest

                
            

            # Insert new FD
            cur.execute("""
                INSERT INTO fd_accounts
                (fd_account_no, member_no, member_name, deposit_amount, interest_rate,
                 start_date, maturity_date, maturity_amount, fd_duration,
                 nominee_name, remark, status, auto_renew, withdraw_yearly_interest, last_interest_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Auto Renewed', 'Active', TRUE,%s,%s)
            """, (
                new_fd_no, member_no, member_name, new_principal, interest_rate,
                new_start, new_end, new_maturity_amount, fd_duration,
                nominee_name,withdraw_yearly_interest,  today  # ⭐ पुरानी FD वाली value SAME copy
   
            ))

            # Debit money again for new FD
            if saving_acc:
                cur.execute("""
                    INSERT INTO transactions (member_no, account_no, trans_type, amount, trans_date, remark, created_by,fd_account_no)
                    VALUES (%s, %s, 'Debit', %s, %s, %s, 'system',%s)
                """, (
                    member_no,
                    saving_acc[0],
                    new_principal,
                    today,
                    f"FD Auto Renewed ({new_fd_no})",
                    new_fd_no
                ))
                # YE ABHI BAAD ME DAALA HAI     
                cur.execute("""
                    UPDATE accounts
                    SET balance = balance - %s
                    WHERE account_no = %s
                    AND balance >= %s
                """, (
                    new_principal,
                    saving_acc[0],
                    new_principal
                ))

                




                conn.commit()
                generate_fd_monthly_interest(
                    new_fd_no,
                    member_no,
                    member_name,
                    new_principal,
                    interest_rate,
                    new_start,
                    fd_duration_days
               )
    conn.commit()
    cur.close()
    conn.close()

    return len(matured_fds)
    
    
# --- FD Certificate Page ---
@app.route("/fd_certificate", methods=["GET"])
def fd_certificate():
    conn = get_db()
    cur = conn.cursor()

    # Fetch all ACTIVE members
    cur.execute("SELECT member_no, member_name_eng FROM members WHERE status = 'Active'")
    members = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("fd_certificate.html", members=members)
    
from flask import jsonify

# --- FD LIST BY MEMBER (used by JS: /get_fd_by_member/<member_no>) ---
@app.route("/get_fd_by_member/<member_no>")
def get_fd_by_member(member_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT fd_account_no 
        FROM fd_accounts 
        WHERE member_no = %s AND status = 'Active'
        ORDER BY fd_account_no;
    """, (member_no,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([{"fd_account_no": r[0]} for r in rows])


# --- FD DETAILS BY FD NO (used by JS: /get_fd_details/<fd_no>) ---
@app.route("/get_fd_details/<fd_no>")
def get_fd_details(fd_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT f.fd_account_no, f.member_no, f.deposit_amount, f.interest_rate,
               f.start_date, f.maturity_date, m.member_name_eng
        FROM fd_accounts f
        JOIN members m ON f.member_no = m.member_no
        WHERE f.fd_account_no = %s;
    """, (fd_no,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "No FD found"}), 404

    data = {
        "fd_account_no": row[0],
        "member_no": row[1],
        "amount": row[2],
        "interest_rate": row[3],
        "start_date": row[4].strftime("%Y-%m-%d") if row[4] else "",
        "end_date": row[5].strftime("%Y-%m-%d") if row[5] else "",
        "member_name_eng": row[6]
    }
    return jsonify(data)
    


# --- Get FD Accounts by Member ---
# --- Get FD list by Member No. (for FD Certificate) ---
@app.route("/get_fd_accounts/<member_no>")
def get_fd_accounts(member_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT fd_account_no 
        FROM fd_accounts 
        WHERE member_no = %s AND status = 'Active'
        ORDER BY fd_account_no
    """, (member_no,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    fds = [r[0] for r in rows]
    return jsonify({"fds": fds})
    
# --- FD Pre-Close Form Page ---
@app.route("/fd_pre_close")
def fd_pre_close():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT member_no, member_name_eng FROM members ORDER BY member_name_eng")
    members = cur.fetchall()  # [(member_no, member_name), ...]
    cur.close()
    conn.close()
    return render_template("fd_pre_close.html", members=members)


# --- AJAX: Get Active FDs by Member ---
@app.route("/get_fdac_by_member/<member_no>")
def get_fdac_by_member(member_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT fd_account_no, deposit_amount, maturity_date
        FROM fd_accounts
        WHERE member_no=%s AND status='Active'
        ORDER BY maturity_date
    """, (member_no,))
    fds = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([{
        "fd_account_no": fd[0],
        "deposit_amount": str(fd[1]),
        "maturity_date": str(fd[2])
    } for fd in fds])


# --- Prematurely Close FD ---
@app.route("/fd/premature_close", methods=["POST"])
def fd_premature_close():
    from decimal import Decimal
    from datetime import datetime, timedelta

    member_no = request.form.get("member_no")
    fd_account_no = request.form.get("fd_account_no")
    today = datetime.now().date()

    conn = get_db()
    cur = conn.cursor()

    # Fetch FD details
    cur.execute("""
        SELECT deposit_amount, fd_duration, start_date, member_name, nominee_name
        FROM fd_accounts
        WHERE fd_account_no=%s AND member_no=%s AND status='Active'
    """, (fd_account_no, member_no))
    fd = cur.fetchone()

    if not fd:
        flash("❌ FD not found or already closed!", "danger")
        cur.close()
        conn.close()
        return redirect("/fd_pre_close")

    deposit_amount, fd_duration, start_date, member_name, nominee_name = fd
    deposit_amount = Decimal(deposit_amount)
    fd_duration = int(fd_duration)

    # Calculate total days FD held
    days_held = (today - start_date).days
    principal = deposit_amount
    interest_amount = Decimal('0')

    # --- Slab-wise calculation ---
    remaining_days = days_held

    # Handle full years first
    for year in range(1, 4):  # max 3 years
        if remaining_days >= 365:
            if year == 1:
                rate = Decimal('7.25')
            elif year == 2:
                rate = Decimal('7.5')
            else:
                rate = Decimal('8')
            interest = principal * rate / Decimal('100')
            principal += interest
            interest_amount += interest
            remaining_days -= 365
        else:
            break

    # Handle remaining days
    if remaining_days > 0:
        if remaining_days <= 45:
            rate = Decimal('0')
        elif 46 <= remaining_days <= 179:
            rate = Decimal('3')
        elif 180 <= remaining_days <= 364:
            rate = Decimal('4')
        else:
            rate = Decimal('0')  # should not occur
        interest = principal * rate / Decimal('100') * Decimal(remaining_days) / Decimal('365')
        interest_amount += interest
        principal += interest

    maturity_amount = deposit_amount + interest_amount

    # Add amount to saving account
    cur.execute("""
        SELECT account_no FROM accounts
        WHERE member_no=%s AND account_type='Saving Account'
    """, (member_no,))
    saving_acc = cur.fetchone()
    if saving_acc:
        cur.execute("""
            UPDATE accounts SET balance = balance + %s WHERE account_no=%s
        """, (maturity_amount, saving_acc[0]))

        # Insert transaction
        cur.execute("""
            INSERT INTO transactions
            (member_no, account_no, trans_type, amount, trans_date, remark, created_by,fd_account_no)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
        """, (
            member_no,
            saving_acc[0],
            'Credit',
            maturity_amount,
            today,
            f"FD Prematurely Closed ({fd_account_no})",
            'system',
            fd_account_no
        ))

    # Close the FD
    cur.execute("""
        UPDATE fd_accounts SET status='Closed', is_closed=TRUE, closed_at=%s,
            remark='Prematurely Closed'
        WHERE fd_account_no=%s
    """, (today, fd_account_no))

    conn.commit()
    cur.close()
    conn.close()

    flash(f"✅ FD {fd_account_no} closed successfully! Amount ₹{maturity_amount:.2f} transferred to Saving Account.", "success")
    return redirect("/fd_pre_close")
    


from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from math import ceil


def generate_fd_monthly_interest(fd_account_no, member_no, member_name, principal, rate, start_date, duration_days):
    conn = get_db()
    cur = conn.cursor()

    rate = Decimal(str(rate))
    principal = Decimal(str(principal))

    # NEW LINE – FIX
    created_on = datetime.now()

    is_year_fd = (duration_days % 365 == 0)

    if is_year_fd:
        years = duration_days // 365
        total_months = years * 12
    else:
        total_months = ceil(duration_days / 30)

    days_left = duration_days

    for i in range(total_months):

        month_date = start_date + relativedelta(months=i+1)

        # FD CLOSED CHECK
        cur.execute("SELECT status FROM fd_accounts WHERE fd_account_no=%s", (fd_account_no,))
        res = cur.fetchone()
        if not res:
            break

        status = res[0]
        if status == "Closed":
            break
            
        if month_date > date.today():
            break

        # DAYS BASED FD
        if not is_year_fd:

            month_days = min(30, days_left)
            days_left -= month_days

            daily_interest = (principal * rate / Decimal(100)) / Decimal(365)
            monthly_interest = (daily_interest * month_days).quantize(Decimal("0.01"))

        else:
            # YEAR BASED FD
            monthly_interest = (
                principal * rate / Decimal(100) / Decimal(12)
            ).quantize(Decimal("0.01"))

        # INSERT ROW — FIXED created_on ADDED
        cur.execute("""
            INSERT INTO fd_monthly_interest
            (fd_account_no, member_no, member_name, month_date,
             principal, interest_rate, monthly_interest, created_on)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            fd_account_no, member_no, member_name, month_date,
            principal, rate, monthly_interest, created_on
        ))

        if days_left <= 0:
            break

    conn.commit()
    cur.close()
    conn.close()
    
def generate_all_fd_interest():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT fd_account_no, member_no, member_name,
               principal, rate, start_date, duration_days
        FROM fd_accounts
        WHERE status='Active'
    """)
    fds = cur.fetchall()

    for fd in fds:
        generate_fd_monthly_interest(*fd)

    cur.close()
    conn.close()
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()

scheduler.add_job(
    generate_all_fd_interest,   # ye function sab FD ke liye call kare
    'cron',
    day=1,
    hour=1
)




@app.route("/fd/yearly_interest")
def fd_yearly_interest():
    today = datetime.now().date()
    conn = get_db()
    cur = conn.cursor()

    # Select all active FDs with yearly interest enabled
    cur.execute("""
        SELECT fd_account_no, member_no, deposit_amount, interest_rate, start_date, fd_duration
        FROM fd_accounts
        WHERE withdraw_yearly_interest = TRUE AND status='Active'
    """)
    fds = cur.fetchall()

    for fd_no, member_no, principal, rate, start_date, fd_duration in fds:
        start_date = start_date
        # कितने साल पूरे हुए
        years_passed = (today - start_date).days // 365
        if years_passed <= 0:
            continue

        # Last interest transfer date check
        cur.execute("SELECT last_interest_date FROM fd_accounts WHERE fd_account_no=%s", (fd_no,))
        last_transfer = cur.fetchone()[0]
        last_transfer_date = last_transfer or start_date
        years_to_pay = (today - last_transfer_date).days // 365
        if years_to_pay <= 0:
            continue

        yearly_interest = principal * (rate / 100) * years_to_pay

        # Transfer interest to Saving account
        cur.execute("SELECT account_no FROM accounts WHERE member_no=%s AND account_type='Saving Account'", (member_no,))
        saving_acc = cur.fetchone()
        if saving_acc:
            cur.execute("""
                UPDATE accounts SET balance = balance + %s WHERE account_no=%s
            """, (yearly_interest, saving_acc[0]))

            # Transaction record
            cur.execute("""
                INSERT INTO transactions (member_no, account_no, trans_type, amount, trans_date, remark, created_by, fd_account_no)
                VALUES (%s, %s, 'Credit', %s, %s, %s, 'system', %s)
            """, (member_no, saving_acc[0], yearly_interest, today, f"Yearly Interest FD {fd_no}", fd_no))

            # Update last transfer date
            cur.execute("""
                UPDATE fd_accounts SET last_interest_date=%s WHERE fd_account_no=%s
            """, (today, fd_no))

    conn.commit()
    cur.close()
    conn.close()
    return "✅ Yearly interest credited successfully!"


    
# --- Get Active Members for FD Certificate ---
from flask import jsonify

@app.route("/get_active_members")
def get_active_members():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT member_no, member_name_eng
        FROM members
        WHERE status = 'Active'
        ORDER BY member_no;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    members = [{"member_no": r[0], "member_name_eng": r[1]} for r in rows]
    return jsonify(members)   # ✅ FIXED — always return JSON

    
    
# --- MEMBERSHIP CERTIFICATE PAGE ---
@app.route("/membership_certificate")
def membership_certificate():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT member_no, member_name_eng FROM members WHERE status='Active' ORDER BY member_no;")
    members = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("membership_certificate.html", members=members)


# --- GET MEMBER DETAILS (for Membership Certificate) ---
@app.route("/get_member_details/<member_no>")
def get_member_details(member_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT member_no, member_name_eng, opening_date
        FROM members
        WHERE member_no = %s;
    """, (member_no,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "Member not found"}), 404

    data = {
        "member_no": row[0],
        "member_name_eng": row[1],
        "opening_date": row[2].strftime("%Y-%m-%d") if row[2] else ""
    }
    return jsonify(data)
    
    
# --- PASSBOOK REPORT PAGE ---
# --- PASSBOOK REPORT PAGE ---
@app.route("/passbook")
def passbook_report():
    conn = get_db()
    cur = conn.cursor()

    # fetch active members
    cur.execute("SELECT member_no, member_name_eng FROM members WHERE status='Active'")
    members = cur.fetchall()

    cur.close()
    conn.close()
    return render_template("passbook.html", members=members)


# --- Get account numbers based on member + account type ---
@app.route("/get_accounts_by_type/<member_no>/<acc_type>")
def get_accounts_by_type(member_no, acc_type):
    conn = get_db()
    cur = conn.cursor()

    accounts = []

    try:
        if acc_type in ["Anivarya Sanchay", "Saving Account", "Share Account","Home Loan Account"]:
            # Normal accounts
            cur.execute("""
                SELECT account_no 
                FROM accounts 
                WHERE member_no=%s 
                  AND account_type=%s 
                  AND status IN ('Active', 'Closed')
            """, (member_no, acc_type))
            accounts = [r[0] for r in cur.fetchall()]

        elif acc_type == "FD":
            # FD Accounts (Active + Closed)
            cur.execute("""
                SELECT fd_account_no 
                FROM fd_accounts 
                WHERE member_no=%s 
                  AND status IN ('Active', 'Closed')
                ORDER BY fd_account_no
            """, (member_no,))
            accounts = [r[0] for r in cur.fetchall()]

        elif acc_type == "RD":
            # RD Accounts (Active + Closed)
            cur.execute("""
                SELECT rd_account_no 
                FROM rd_accounts 
                WHERE member_no=%s 
                  AND status IN ('Active', 'Closed')
                ORDER BY rd_account_no
            """, (member_no,))
            accounts = [r[0] for r in cur.fetchall()]

    except Exception as e:
        print("❌ Error in get_accounts_by_type:", e)

    finally:
        cur.close()
        conn.close()

    return {"accounts": accounts}


    
 
# --- Get Passbook Transactions ---
# --- Get Passbook Transactions (with Transaction ID & Source) ---
# --- Get Passbook Transactions (with Transaction ID & Source) ---
@app.route("/get_passbook/<member_no>/<account_no>")
def get_passbook(member_no, account_no):
    conn = get_db()
    cur = conn.cursor()

    if account_no.startswith("FD"):
        cur.execute("""
            SELECT id, trans_date,
            CASE 
                WHEN trans_type='Debit' THEN 'Credit'
                WHEN trans_type='Credit' THEN 'Debit'
                ELSE trans_type
            END AS trans_type,
            amount, remark, source
            FROM transactions
            WHERE member_no=%s AND (fd_account_no=%s OR remark ILIKE %s)
            ORDER BY trans_date ASC, id ASC
        """, (member_no, account_no, f"%{account_no}%"))

    elif account_no.startswith("RD"):
        cur.execute("""
            SELECT id, trans_date, trans_type, amount, remark, source
            FROM transactions
            WHERE member_no=%s AND rd_account_no=%s
            ORDER BY trans_date ASC, id ASC
        """, (member_no, account_no))

    else:
        cur.execute("""
            SELECT id, trans_date, trans_type, amount, remark, source
            FROM transactions
            WHERE member_no=%s AND account_no=%s
            ORDER BY trans_date ASC, id ASC
        """, (member_no, account_no))

    rows = cur.fetchall()

    balance = 0
    transactions = []

    for r in rows:

        trans_type = r[2].lower()
        amount = float(r[3])

        credit = 0
        debit = 0

        if trans_type == "credit":
            credit = amount
            balance += amount
        else:
            debit = amount
            balance -= amount

        transactions.append({
            "id": r[0],
            "date": r[1].strftime("%Y-%m-%d") if r[1] else "",
            "remark": r[4],
            "trans_no": r[0],
            "credit": credit,
            "debit": debit,
            
            "balance": balance
        })

    cur.close()
    conn.close()

    return {"transactions": transactions}
    
    
# --- RD FORM ---
@app.route("/rd", methods=["GET"])
def rd_form():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT member_no, member_name_eng FROM members WHERE status='Active'")
    members = cur.fetchall()

    # Auto-generate RD Account No
    cur.execute("SELECT rd_account_no FROM rd_accounts ORDER BY id DESC LIMIT 1")
    last = cur.fetchone()
    new_rd_no = f"RD{(int(last[0].replace('RD','')) + 1):05d}" if last else "RD00001"

    cur.close()
    conn.close()
    return render_template("rd.html", members=members, new_rd_no=new_rd_no)
    
    
    
@app.route("/rd/save", methods=["POST"])
def save_rd():
    try:
        data = {key: request.form.get(key) for key in request.form.keys()}
        member_no = data["member_no"]
        member_name = data["member_name"]
        rd_account_no = data["rd_account_no"]
        monthly_deposit = Decimal(data["deposit_amount"])
        duration_months = int(data["duration_months"])
        interest_rate = Decimal(data["interest_rate"])
        start_date = data["start_date"]
        nominee_name = data.get("nominee_name", "")
        remark = data.get("remark", "")
        # ✅ Voucher No from hidden field
        voucher_no = data.get("voucher_no", "").strip()
        if not voucher_no:
            flash("❌ Voucher No. is required!", "danger")
            return redirect("/rd")
        auto_renew = True if data.get("auto_renew") == "on" else False

        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        today = datetime.now().date()

        conn = get_db()
        cur = conn.cursor()

        # Fetch saving account
        cur.execute("SELECT account_no, balance FROM accounts WHERE member_no=%s AND account_type='Saving Account'", (member_no,))
        res = cur.fetchone()
        if not res:
            flash("❌ Saving Account not found.", "danger")
            cur.close(); conn.close()
            return redirect("/rd")
        saving_acc_no, balance = res
        balance = Decimal(balance)

        if start_dt <= today and balance < monthly_deposit:
            flash(f"❌ Insufficient balance! Available ₹{balance}", "danger")
            cur.close(); conn.close()
            return redirect("/rd")

        # Calculate maturity
        maturity_dt = start_dt + timedelta(days=30 * duration_months)
        total_installments = duration_months
        maturity_amount = sum([
            monthly_deposit * (1 + (interest_rate / 100) * (duration_months - i + 1) / 12)
            for i in range(1, duration_months + 1)
        ])
        maturity_amount = Decimal(maturity_amount).quantize(Decimal("0.01"))

        # Save RD account
        installments_paid = 0
        cur.execute("""
            INSERT INTO rd_accounts 
            (rd_account_no, member_no, member_name, start_date, duration_months,
             interest_rate, monthly_deposit, total_installments, maturity_date,
             maturity_amount, nominee_name, remark, status, installments_paid, auto_renew)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Active',%s,%s)
        """, (rd_account_no, member_no, member_name, start_dt, duration_months, interest_rate, monthly_deposit,
              total_installments, maturity_dt, maturity_amount, nominee_name, remark, installments_paid, auto_renew))

        # FIRST installment
        if start_dt <= today:
            new_balance = balance - monthly_deposit
            cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s", (new_balance, saving_acc_no))
            # Debit Saving
            cur.execute("""
                INSERT INTO transactions
                (member_no, account_no, trans_type, amount, trans_date, remark, created_by)
                VALUES (%s,%s,'Debit',%s,%s,%s,'admin')
            """, (member_no, saving_acc_no, monthly_deposit, start_dt, f"RD Created ({rd_account_no})"))
            # Credit RD
            cur.execute("""
                INSERT INTO transactions
                (member_no, rd_account_no, trans_type, amount, trans_date, remark, created_by, voucher_no)
                VALUES (%s,%s,'Credit',%s,%s,%s,'system',%s)
            """, (member_no, rd_account_no, monthly_deposit, start_dt, f"RD Created Credit ({rd_account_no})", voucher_no))

            installments_paid = 1
            cur.execute("UPDATE rd_accounts SET installments_paid=%s WHERE rd_account_no=%s", (installments_paid, rd_account_no))

            # Save interest
            monthly_interest = (monthly_deposit * interest_rate / Decimal(100) / Decimal(12)).quantize(Decimal("0.01"))
            cur.execute("""
                INSERT INTO interest_history
                (created_on, principal, interest_rate, monthly_interest, month_year,
                 account_type, account_no, member_no, member_name)
                VALUES (NOW(), %s, %s, %s, %s, 'RD', %s, %s, %s)
            """, (monthly_deposit, interest_rate, monthly_interest, start_dt.strftime("%Y-%m-01"), rd_account_no, member_no, member_name))

        conn.commit()
        cur.close(); conn.close()

        if installments_paid == 1:
            flash(f"✅ RD {rd_account_no} created! First installment deducted. Maturity ₹{maturity_amount}", "success")
        else:
            flash(f"✅ RD {rd_account_no} created! Installments will start on {start_date}.", "success")

        return redirect("/rd")

    except Exception as e:
        print("❌ Error saving RD:", e)
        flash(f"❌ Error saving RD: {e}", "danger")
        return redirect("/rd")





        
        
# --- GET RD LIST BY MEMBER (used by Credit/Debit form) ---
@app.route("/get_rd_by_member/<member_no>")
def get_rd_by_member(member_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT rd_account_no 
        FROM rd_accounts 
        WHERE member_no = %s AND status = 'Active'
        ORDER BY rd_account_no;
    """, (member_no,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([{"rd_account_no": r[0]} for r in rows])
    
# --- GET RD DETAILS BY RD ACCOUNT NO ---
@app.route("/get_rd_details/<rd_no>")
def get_rd_details(rd_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT rd_account_no, monthly_deposit 
        FROM rd_accounts 
        WHERE rd_account_no = %s
    """, (rd_no,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "RD not found"}), 404

    return jsonify({
        "rd_account_no": row[0],
        "monthly_deposit": float(row[1])
    })
    
    
    
@app.route("/member_report", methods=["GET"])
def member_report():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT member_no, member_name_eng 
        FROM members 
        WHERE status='Active'
        ORDER BY member_no;
    """)
    members = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("member_report.html", members=members)
    
    
from flask import jsonify

@app.route("/get_member_full/<member_no>")
def get_member_full(member_no):
    try:
        print("🧩 get_member_full called for:", member_no)   # debug - remove later if you want
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, member_no, member_name_eng, member_name_hin, member_type,
                   opening_date, dob, age, father_name, gender, marrital_status,
                   religion, caste, close_status, loan_taken, medical_insurance,
                   identity_1, identity_2, include_15h, include_15g, compulsory_deposit_amt,
                   guarantor_no, guarantor_name, guarantor_type,
                   permanent_address, present_address,
                   region, circle, division, dc_zone,
                   nominee_name, nominee_relationship,
                   member_photo_path, member_sign_path, nominee_photo_path,
                   member_adhaar_id, member_pan_id, member_email, member_mobile_no,
                   creator_remark, created_by, created_on, updated_by, updated_on,
                   status, remark, bank_acct_no, bank_ifsc_code, bank_name, bank_branch_address,
                   old_member_no, employee_no
            FROM members
            WHERE member_no = %s
            LIMIT 1;
        """, (member_no,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return jsonify({"error": "Member not found"}), 404

        cols = [
            "id","member_no","member_name_eng","member_name_hin","member_type",
            "opening_date","dob","age","father_name","gender","marrital_status",
            "religion","caste","close_status","loan_taken","medical_insurance",
            "identity_1","identity_2","include_15h","include_15g","compulsory_deposit_amt",
            "guarantor_no","guarantor_name","guarantor_type",
            "permanent_address","present_address",
            "region","circle","division","dc_zone",
            "nominee_name","nominee_relationship",
            "member_photo_path","member_sign_path","nominee_photo_path",
            "member_adhaar_id","member_pan_id","member_email","member_mobile_no",
            "creator_remark","created_by","created_on","updated_by","updated_on",
            "status","remark","bank_acct_no","bank_ifsc_code","bank_name","bank_branch_address",
            "old_member_no","employee_no"
        ]

        # Convert datetimes/dates to string where needed
        record = dict(zip(cols, row))
        # Safe convert date/datetime to isoformat strings if present
        for k, v in record.items():
            if hasattr(v, "strftime"):
                record[k] = v.strftime("%Y-%m-%d %H:%M:%S") if getattr(v, "hour", None) is not None else v.strftime("%Y-%m-%d")

        # Return only the fields frontend needs (but it's okay to return all)
        # Frontend expects: member_no, member_name_eng, present_address, member_email, member_mobile_no, member_pan_id, member_adhaar_id, member_type, status etc.
        return jsonify(record)

    except Exception as e:
        print("❌ get_member_full error:", e)
        return jsonify({"error": str(e)}), 500


from datetime import date, datetime, timedelta
from decimal import Decimal
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import date, timedelta

def get_last_day_of_month(d):
    if d.month == 12:
        next_month = date(d.year + 1, 1, 1)
    else:
        next_month = date(d.year, d.month + 1, 1)
    last_day = next_month - timedelta(days=1)
    return last_day


scheduler = BackgroundScheduler()
scheduler.start()

# ================================
# MONTHLY INTEREST CALCULATION
# ================================
def calculate_monthly_interest():
    conn = get_db()
    cur = conn.cursor()

    today = date.today()
    month_year = get_last_day_of_month(today)

    # Stop duplicate runs
    cur.execute("""
        SELECT COUNT(*) FROM interest_history
        WHERE month_year = %s AND account_type = 'Anivarya'
    """, (month_year,))
    if cur.fetchone()[0] > 0:
        print(f"Monthly interest already stored for {month_year}")
        conn.close()
        return

    print(f"📌 Storing monthly interest for {month_year}")

    cur.execute("""
        SELECT a.account_no, a.member_no, a.balance, m.member_name_eng
        FROM accounts a
        JOIN members m ON a.member_no = m.member_no
        WHERE a.account_type = 'Anivarya Sanchay' AND a.status = 'Active'
    """)
    accounts = cur.fetchall()

    monthly_rate = Decimal('0.0066')  # 0.66%

    for account_no, member_no, balance, member_name in accounts:
        balance = Decimal(balance)
        interest = (balance * monthly_rate).quantize(Decimal('0.01'))

        cur.execute("""
            INSERT INTO interest_history
            (account_type, account_no, member_no, member_name, month_year,
             principal, interest_rate, monthly_interest, added_to_loan)
            VALUES ('Anivarya', %s, %s, %s, %s, %s, %s, %s, false)
        """, (
            account_no, member_no, member_name,
            month_year, balance,
            Decimal('0.66'), interest
        ))

    conn.commit()
    conn.close()
    print("✔️ Monthly interest stored successfully")



# ================================
# ANNUAL INTEREST FINAL CREDIT
# ================================
# ================================
# ANNUAL INTEREST FINAL CREDIT (FIXED)
# ================================
def credit_annual_interest():
    conn = get_db()
    cur = conn.cursor()

    today = date.today()

    # Fix Financial Year Calculation
    if today.month > 3:
        fy_start_year = today.year
    else:
        fy_start_year = today.year - 1

    fy_start = date(fy_start_year, 4, 1)
    fy_end = date(fy_start_year + 1, 3, 31)
    financial_year = f"{fy_start_year}-{fy_start_year + 1}"

    print(f"Processing final interest credit for FY {financial_year}")

    # Fetch SUM of all monthly interest not yet credited
    cur.execute("""
        SELECT account_no, member_no, SUM(monthly_interest)
        FROM interest_history
        WHERE added_to_loan = false
          AND month_year >= %s AND month_year <= %s
          AND account_type = 'Anivarya'
        GROUP BY account_no, member_no
    """, (fy_start, fy_end))

    payouts = cur.fetchall()

    for account_no, member_no, total_interest in payouts:
        total_interest = Decimal(total_interest).quantize(Decimal('0.01'))

        # Add interest to account balance
        cur.execute("""
            UPDATE accounts
            SET balance = balance + %s
            WHERE account_no = %s
        """, (total_interest, account_no))

        # Mark these specific FY months as credited
        cur.execute("""
            UPDATE interest_history
            SET added_to_loan = true
            WHERE account_no = %s
              AND month_year >= %s AND month_year <= %s
              AND added_to_loan = false
        """, (account_no, fy_start, fy_end))

        # Insert transaction for ledger tracking
        cur.execute("""
            INSERT INTO transactions
            (member_no, account_no, trans_type, amount, trans_date, remark, created_by)
            VALUES (%s, %s, 'Credit', %s, %s, %s, 'system')
        """, (
            member_no, account_no,
            total_interest, today,
            f"Annual Interest Credited FY {financial_year}"
        ))

    conn.commit()
    print("Annual interest credited successfully.")
    cur.close()
    conn.close()



# ================================
# SCHEDULER TASKS
# ================================

# Run every month last day at 23:59
scheduler.add_job(calculate_monthly_interest, 'cron', day='last', hour=23, minute=59)

# Run yearly: 31 March 23:59
scheduler.add_job(credit_annual_interest, 'cron',
                  month='3', day='31', hour=23, minute=59)


# ================================
# MANUAL API FOR TESTING
# ================================
@app.route('/manual/monthly-interest', methods=['POST'])
def manual_monthly():
    try:
        calculate_monthly_interest()
        return jsonify({"status": "ok", "message": "Monthly interest calculated successfully"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/manual/annual-interest', methods=['POST'])
def manual_annual():
    try:
        credit_annual_interest()
        return jsonify({"status": "ok", "message": "Annual interest credited successfully"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
   
    
    
    
@app.route("/update_member_contact", methods=["POST"])
def update_member_contact():
    data = request.form
    member_no = data.get("member_no")

    # Sabhi editable fields ko form se get karo
    member_name_eng = data.get("member_name_eng")
    member_name_hin = data.get("member_name_hin")
    member_type = data.get("member_type")
    opening_date = data.get("opening_date") or None
    dob = data.get("dob") or None
    age = data.get("age") or None
    father_name = data.get("father_name")
    gender = data.get("gender")
    marrital_status = data.get("marrital_status")
    religion = data.get("religion")
    caste = data.get("caste")
    mobile = data.get("member_mobile_no")
    email = data.get("member_email")
    pan = data.get("member_pan_id")
    adhar = data.get("member_adhaar_id")
    present_address = data.get("present_address")
    permanent_address = data.get("permanent_address")
    nominee_name = data.get("nominee_name")
    nominee_relationship = data.get("nominee_relationship")
    bank_acct_no = data.get("bank_acct_no")
    bank_ifsc_code = data.get("bank_ifsc_code")
    bank_name = data.get("bank_name")
    bank_branch_address = data.get("bank_branch_address")
    remark = data.get("remark")
    employee_no = data.get("employee_no")

    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        UPDATE members
        SET member_name_eng = %s,
            member_name_hin = %s,
            member_type = %s,
            opening_date = %s,
            dob = %s,
            age = %s,
            father_name = %s,
            gender = %s,
            marrital_status = %s,
            religion = %s,
            caste = %s,
            member_mobile_no = %s,
            member_email = %s,
            member_pan_id = %s,
            member_adhaar_id = %s,
            present_address = %s,
            permanent_address = %s,
            nominee_name = %s,
            nominee_relationship = %s,
            bank_acct_no = %s,
            bank_ifsc_code = %s,
            bank_name = %s,
            bank_branch_address = %s,
            remark = %s,
            employee_no = %s,
            updated_on = NOW()
        WHERE member_no = %s
    """, (
        member_name_eng, member_name_hin, member_type, opening_date, dob, age, father_name, gender, marrital_status,
        religion, caste, mobile, email, pan, adhar, present_address, permanent_address,
        nominee_name, nominee_relationship, bank_acct_no, bank_ifsc_code, bank_name, bank_branch_address,
        remark,employee_no, member_no
    ))

    # 👇 NEW PART – password bhi update karo
    if mobile:
        cur.execute("""
            UPDATE users
            SET password = %s
            WHERE username = %s
        """, (str(mobile), member_no))

    conn.commit()
    cur.close()
    conn.close()

    flash(f"✅ Member {member_no} details updated successfully!", "success")
    return redirect("/member_report")

    
@app.route("/get_rd_details_full/<rd_no>")
def get_rd_details_full(rd_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT rd_account_no, member_no, member_name, monthly_deposit, interest_rate,
               start_date, maturity_date
        FROM rd_accounts
        WHERE rd_account_no = %s;
    """, (rd_no,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "RD not found"}), 404

    data = {
        "rd_account_no": row[0],
        "member_no": row[1],
        "member_name": row[2],
        "monthly_deposit": float(row[3]),
        "interest_rate": float(row[4]),
        "start_date": row[5].strftime("%Y-%m-%d") if row[5] else "",
        "maturity_date": row[6].strftime("%Y-%m-%d") if row[6] else ""
    }
    return jsonify(data)
    
@app.route("/rd_certificate", methods=["GET"])
def rd_certificate():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT member_no, member_name_eng FROM members WHERE status='Active'")
    members = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("rd_certificate.html", members=members)
    
@app.route("/check_balance/<member_no>/<account_no>")
def check_balance(member_no, account_no):
    conn = get_db()
    cur = conn.cursor()
    
    # Determine table & column based on account type prefix
    if account_no.startswith("FD"):
        cur.execute("SELECT maturity_amount FROM fd_accounts WHERE fd_account_no=%s", (account_no,))
    elif account_no.startswith("RD"):
        cur.execute("SELECT monthly_deposit * total_installments AS total FROM rd_accounts WHERE rd_account_no=%s", (account_no,))
    else:
        # Saving / Share / Anivarya etc.
        cur.execute("SELECT balance FROM accounts WHERE member_no=%s AND account_no=%s AND status='Active'", (member_no, account_no))
    
    row = cur.fetchone()
    balance = float(row[0]) if row and row[0] is not None else 0.0

    cur.close()
    conn.close()
    return jsonify({"balance": balance})
    
    
@app.route("/check_balance_amount/<member_no>/<amount>")
def check_balance_amount(member_no, amount):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT balance 
        FROM accounts 
        WHERE member_no=%s AND account_type='Saving Account' AND status='Active'
    """, (member_no,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    balance = float(row[0]) if row else 0.0
    ok = balance >= float(amount)
    return jsonify({"ok": ok, "balance": balance})
    
    
@app.route("/transfer", methods=["GET", "POST"])
def transfer():
    conn = get_db()
    cur = conn.cursor()

    # --- LOAD PAGE ---
    if request.method == "GET":
        cur.execute("SELECT member_no, member_name_eng FROM members WHERE status='Active'")
        members = cur.fetchall()
        return render_template("transfer.html", members=members, now=datetime.now())

    # --- PROCESS TRANSFER ---
    try:
        from_member = request.form["from_member"]
        from_acc_no = request.form["from_acc_no"]
        from_acc_type = request.form["from_acc_type"]

        to_member = request.form["to_member"]
        to_acc_no = request.form["to_acc_no"]
        to_acc_type = request.form["to_acc_type"]

        amount = Decimal(request.form["amount"])
        trans_date = request.form["trans_date"]
        remark = request.form.get("remark") or ""
        source = request.form.get("source") or "Manual"

        # ------------- VALIDATION -------------
        if from_acc_no == to_acc_no:
            flash("❌ Same account transfer नहीं हो सकता", "danger")
            return redirect("/transfer")

        if amount <= 0:
            flash("❌ Amount invalid है", "danger")
            return redirect("/transfer")

        # Identify account type by prefix
        def kind(acc):
            if acc.startswith("RD"):
                return "RD"
            if acc.startswith("FD"):
                return "FD"
            if acc.startswith("LN"):
                return "LOAN"
            return "REG"

        from_kind = kind(from_acc_no)
        to_kind = kind(to_acc_no)

        # ❌ SAFE RULES
        if from_kind in ("RD", "FD", "LOAN"):
            flash("❌ RD/FD/Loan से सीधे money निकालना allowed नहीं है", "danger")
            return redirect("/transfer")

        if to_kind == "FD":
            flash("❌ FD में direct credit allowed नहीं है", "danger")
            return redirect("/transfer")

        # ------------- START DB TRANSACTION -------------
        conn.autocommit = False

        # ---- 1. LOCK Source Account ----
        cur.execute("""
            SELECT balance, member_no FROM accounts
            WHERE account_no=%s FOR UPDATE
        """, (from_acc_no,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            flash("❌ Source account नहीं मिला", "danger")
            return redirect("/transfer")

        src_balance, src_member_db = row
        src_balance = Decimal(src_balance)

        if src_balance < amount:
            conn.rollback()
            flash(f"❌ Balance कम है. Available = ₹{src_balance}", "danger")
            return redirect("/transfer")

        # Update Source Balance
        new_src = src_balance - amount
        cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s", (new_src, from_acc_no))

        # Insert Debit Entry
        cur.execute("""
            INSERT INTO transactions (member_no, account_no, trans_type, amount, trans_date, remark, source)
            VALUES (%s,%s,'Debit',%s,%s,%s,%s)
        """, (from_member, from_acc_no, amount, trans_date, f"Transfer to {to_acc_no} | {remark}", source))

        # ---- 2. TARGET logic ----
        # ============== A. REGULAR ACCOUNT ==============
        if to_kind == "REG":
            cur.execute("""
                SELECT balance FROM accounts
                WHERE account_no=%s FOR UPDATE
            """, (to_acc_no,))
            row = cur.fetchone()

            if not row:
                conn.rollback()
                flash("❌ Target account नहीं मिला", "danger")
                return redirect("/transfer")

            tgt_balance = Decimal(row[0])
            new_tgt = tgt_balance + amount

            cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s", (new_tgt, to_acc_no))

            cur.execute("""
                INSERT INTO transactions (member_no, account_no, trans_type, amount, trans_date, remark, source)
                VALUES (%s,%s,'Credit',%s,%s,%s,%s)
            """, (to_member, to_acc_no, amount, trans_date, f"Transfer from {from_acc_no} | {remark}", source))

        # ============== B. RD ACCOUNT (Installment) ==============
        elif to_kind == "RD":
            cur.execute("""
                SELECT monthly_deposit, installments_paid
                FROM rd_accounts WHERE rd_account_no=%s FOR UPDATE
            """, (to_acc_no,))
            rd = cur.fetchone()

            if not rd:
                conn.rollback()
                flash("❌ RD account नहीं मिला", "danger")
                return redirect("/transfer")

            monthly_deposit, inst = rd

            # CREDIT RD
            cur.execute("""
                INSERT INTO transactions (member_no, rd_account_no, trans_type, amount, trans_date, remark, source)
                VALUES (%s,%s,'Credit',%s,%s,%s,%s)
            """, (to_member, to_acc_no, amount, trans_date, f"RD Installment | {remark}", source))

            # Auto Installment Count
            if Decimal(monthly_deposit) == amount:
                cur.execute("""
                    UPDATE rd_accounts
                    SET installments_paid = installments_paid + 1
                    WHERE rd_account_no=%s
                """, (to_acc_no,))

        # ============== C. LOAN ACCOUNT (Repayment) ==============
        elif to_kind == "LOAN":
            cur.execute("""
                INSERT INTO transactions (member_no, account_no, trans_type, amount, trans_date, remark, source)
                VALUES (%s,%s,'Credit',%s,%s,%s,%s)
            """, (to_member, to_acc_no, amount, trans_date, f"Loan Repayment | {remark}", source))

            # Loan balance update optional — बताना हो तो मैं add कर दूँ

        # ============== OTHER (not allowed) ==============
        else:
            conn.rollback()
            flash("❌ Unsupported transfer type", "danger")
            return redirect("/transfer")

        # COMMIT ALL
        conn.commit()
        flash("✅ Transfer Success!", "success")
        return redirect("/transfer")

    except Exception as e:
        conn.rollback()
        flash(f"❌ Error: {e}", "danger")
        return redirect("/transfer")
    finally:
        cur.close()
        conn.close()
        
@app.route("/rd/pre_close", methods=["GET", "POST"])
def rd_pre_close():
    conn = get_db()
    cur = conn.cursor()

    if request.method == "GET":
        cur.execute("SELECT member_no, member_name_eng FROM members WHERE status='Active'")
        members = cur.fetchall()
        cur.close()
        conn.close()
        return render_template("rd_pre_close.html", members=members)

    try:
        member_no = request.form.get("member_no")
        rd_account_no = request.form.get("rd_account_no")

        if not member_no or not rd_account_no:
            return jsonify({"status": "error", "message": "Member / RD not selected"})

        cur.execute("""
            SELECT monthly_deposit, installments_paid, status 
            FROM rd_accounts 
            WHERE rd_account_no=%s
        """, (rd_account_no,))
        rd = cur.fetchone()

        if not rd:
            return jsonify({"status": "error", "message": "RD Account not found!"})

        monthly_deposit, installments_paid, status = rd
        monthly_deposit = Decimal(monthly_deposit)
        installments_paid = int(installments_paid)

        if status == "Closed":
            return jsonify({"status": "error", "message": "RD already closed!"})

        # Load RD History
        cur.execute("""
            SELECT principal, monthly_interest 
            FROM interest_history 
            WHERE account_no=%s
            ORDER BY month_year
        """, (rd_account_no,))
        rows = cur.fetchall()

        if not rows:
            return jsonify({"status": "error", "message": "No installment history!"})

        completed_years = installments_paid // 12
        eligible_months = completed_years * 12

        if eligible_months > 0:
            principal_interest_part = Decimal(rows[eligible_months - 1][0])
            interest_amount = sum(Decimal(r[1]) for r in rows[:eligible_months])
        else:
            principal_interest_part = Decimal(0)
            interest_amount = Decimal(0)

        remaining_months = installments_paid % 12
        remaining_principal = remaining_months * monthly_deposit

        total_payout = principal_interest_part + interest_amount + remaining_principal

        cur.execute("""
            SELECT account_no, balance 
            FROM accounts 
            WHERE member_no=%s AND account_type='Saving Account' AND status='Active'
        """, (member_no,))
        saving = cur.fetchone()

        if not saving:
            return jsonify({"status": "error", "message": "Saving Account missing!"})

        saving_acc_no, saving_balance = saving
        new_balance = Decimal(saving_balance) + total_payout

        cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s",
                    (new_balance, saving_acc_no))

        cur.execute("""
            UPDATE rd_accounts 
            SET status='Pre Closed', closed_date=NOW()
            WHERE rd_account_no=%s
        """, (rd_account_no,))

        cur.execute("""
            INSERT INTO transactions 
            (member_no, account_no, trans_type, amount, remark, source, created_by)
            VALUES (%s,%s,'Credit',%s,%s,%s,%s)
        """, (member_no, saving_acc_no, total_payout,
              f"RD Pre-Closure ({rd_account_no})", "System", "system"))

        conn.commit()

        return jsonify({
            "status": "success",
            "rd": rd_account_no,
            "installments_paid": installments_paid,
            "interest_amount": float(round(interest_amount, 2)),
            "remaining_principal": float(round(remaining_principal, 2)),
            "total_payout": float(round(total_payout, 2)),
        })

    except Exception as e:
        conn.rollback()
        return jsonify({"status": "error", "message": str(e)})

    finally:
        cur.close()
        conn.close()





    

    
def save_fd_monthly_interest(fd_data):
    conn = get_db()
    cur = conn.cursor()

    amount = Decimal(fd_data['deposit_amount'])
    rate = Decimal(fd_data['interest_rate'])
    start = fd_data['start_date']
    duration = int(fd_data['duration_months'])
    member_no = fd_data['member_no']
    member_name = fd_data['member_name']
    fd_no = fd_data['fd_account_no']

    monthly_interest = (amount * rate / Decimal(100) / Decimal(12)).quantize(Decimal("0.01"))

    # loop for each month
    for i in range(duration):
        month_date = (start + timedelta(days=30 * i))

        cur.execute("""
            INSERT INTO interest_history
            (account_type, account_no, member_no, member_name, month_year, 
             principal, interest_rate, monthly_interest)
            VALUES ('FD', %s, %s, %s, %s, %s, %s, %s)
        """, (
            fd_no, member_no, member_name,
            month_date, amount, rate, monthly_interest
        ))

    conn.commit()
    cur.close()
    conn.close()
    
    
@app.route("/manual_auto_renew", methods=["POST"])
def manual_auto_renew():
    try:
        auto_renew_rd()
        flash("✅ Auto-renew executed successfully!", "success")
    except Exception as e:
        flash(f"❌ Auto-renew failed: {e}", "danger")
    return redirect("/rd")  # RD listing page ya dashboard
    
    
    
    

        

from datetime import datetime


def credit_account(cur, member_no, account_type, amount):
    """
    Credit the member account. If it doesn't exist, create it.
    """
    cur.execute("""
        SELECT id, balance FROM accounts
        WHERE member_no=%s AND account_type=%s
    """, (member_no, account_type))
    row = cur.fetchone()

    if row:
        cur.execute("""
            UPDATE accounts
            SET balance = balance + %s
            WHERE id=%s
        """, (amount, row[0]))
    else:
        account_no = f"{member_no}_{account_type}"
        cur.execute("""
            INSERT INTO accounts
            (member_no, account_no, account_type, opening_date, balance, status, created_at)
            VALUES (%s, %s, %s, NOW(), %s, 'Active', NOW())
        """, (member_no, account_no, account_type, amount))


def record_transaction(cur, loan_no, trans_type, amount, remark,
                       member_no=None, account_no=None, account_type=None, gl_head=None):
    """
    Insert a transaction record into loan_transactions table with GL and member/account details.
    """
    cur.execute("""
        INSERT INTO loan_transactions
        (loan_no, trans_type, amount, remark, member_no, account_no, account_type, gl_head, created_on)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """, (loan_no, trans_type, amount, remark, member_no, account_no, account_type, gl_head))

@app.route("/loan/new", methods=["GET", "POST"])
def loan_new():
    if request.method == "POST":
        conn = None
        try:
            conn = get_db()
            cur = conn.cursor()

            member_no = request.form.get("member_no")
            member_name = request.form.get("member_name")
            loan_head = request.form.get("loan_head")
            principal = float(request.form.get("principal", 0))
            interest_rate = float(request.form.get("interest_rate", 0))
            tenure_months = int(request.form.get("tenure_months", 0))
            emi_amount = float(request.form.get("emi_amount", 0))

            if not member_no or principal <= 0:
                flash("Member No or Loan Amount missing!", "error")
                return redirect("/loan/new")

            disbursement_date = datetime.now()
            loan_no = "LN" + str(int(disbursement_date.timestamp()))

            saving_amount = 0
            share_amount = 0
            home_loan_amount = 0
            home_acc_no = None

            # ============ जमानती क़र्ज़ LOGIC ============
            if loan_head == "जमानती क़र्ज़":
                required_share = principal * 0.10  
                cur.execute("""
                    SELECT balance FROM accounts 
                    WHERE member_no=%s AND account_type='Share Account' LIMIT 1
                """, (member_no,))
                row = cur.fetchone()
                current_share_balance = float(row[0]) if row else 0.0

                if current_share_balance >= required_share:
                    saving_amount = principal
                else:
                    share_amount = required_share - current_share_balance
                    if share_amount < 0: share_amount = 0
                    saving_amount = principal - share_amount

            # ============ पावती तरन क़र्ज़ (Loan Against FD) ============
            elif loan_head == "पावती तरन क़र्ज़":
                cur.execute("""
                    SELECT COALESCE(SUM(deposit_amount), 0)
                    FROM fd_accounts
                    WHERE member_no=%s AND status='Active'
                """, (member_no,))
                fd_total = float(cur.fetchone()[0])

                if fd_total <= 0:
                    flash("❌ इस सदस्य के पास कोई Active FD नहीं है!", "danger")
                    return redirect("/loan/new")

                allowable_loan = fd_total * 0.75  # 75%

                if principal > allowable_loan:
                    flash(f"❌ अधिकतम FD Loan Limit: ₹{allowable_loan:.2f}", "danger")
                    return redirect("/loan/new")

                saving_amount = principal  # पूरा Saving में

            # ============ माकन तरन क़र्ज़ LOGIC ============
            elif loan_head == "माकन तरन क़र्ज़":
                required_home_amount = principal * 0.05

                cur.execute("""
                    SELECT account_no, balance FROM accounts
                    WHERE member_no=%s AND account_type='Home Loan Account' LIMIT 1
                """, (member_no,))
                row = cur.fetchone()

                if row:
                    home_acc_no, current_home_balance = row
                    current_home_balance = float(current_home_balance)
                else:
                    home_acc_no = None
                    current_home_balance = 0

                if current_home_balance >= required_home_amount:
                    saving_amount = principal
                else:
                    home_loan_amount = required_home_amount - current_home_balance
                    saving_amount = principal - home_loan_amount

                    if home_acc_no is None:
                        home_acc_no = f"HLA{int(datetime.now().timestamp())}"
                        cur.execute("""
                            INSERT INTO accounts
                            (member_no, account_no, account_type, balance, created_by)
                            VALUES (%s,%s,'Home Loan Account',0,%s)
                        """, (member_no, home_acc_no, session.get('user','system')))

            else:
                saving_amount = principal  # बाकी Loans Full Saving में

            # =========== GL Head Code ===========
            cur.execute("SELECT id FROM loan_heads WHERE head_name=%s", (loan_head,))
            loan_head_id = cur.fetchone()[0]
            
            # ---------------- GUARANTORS ----------------
            guarantors = []

            for i in range(1, 5):
                g_no = request.form.get(f"guarantor{i}_member_no")
                g_name = request.form.get(f"guarantor{i}_name")

                if g_no:
                    guarantors.append((g_no, g_name))

            # ---- VALIDATION ----
            #if len(guarantors) != 4:
            #    raise Exception("Exactly 4 guarantors are required")

            borrower_no = member_no
            seen = set()

            for g_no, _ in guarantors:
                if g_no == borrower_no:
                    raise Exception("Borrower khud guarantor nahi ho sakta")

                if g_no in seen:
                    raise Exception("Same guarantor dobara allowed nahi hai")

                seen.add(g_no)
            g1_no, g1_name = guarantors[0] if len(guarantors) > 0 else (None, None)
            g2_no, g2_name = guarantors[1] if len(guarantors) > 1 else (None, None)
            g3_no, g3_name = guarantors[2] if len(guarantors) > 2 else (None, None)
            g4_no, g4_name = guarantors[3] if len(guarantors) > 3 else (None, None)
            
            # =========== INSERT Loan Record ===========
            cur.execute("""
                INSERT INTO loans
                (loan_no, member_no, member_name, loan_head, gl_code,
                 principal, interest_rate, tenure_months, emi,
                 disbursed_amount, disbursed_date, outstanding_principal,
                 status, total_payable,first_guarantor_member_no, first_guarantor_name,
                 second_guarantor_member_no, second_guarantor_name,
                 third_guarantor_member_no, third_guarantor_name,
                 fourth_guarantor_member_no, fourth_guarantor_name)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Active',%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                loan_no, member_no, member_name, loan_head, loan_head_id,
                principal, interest_rate, tenure_months, emi_amount,
                principal, disbursement_date, principal, principal,
                g1_no, g1_name,
                g2_no, g2_name,
                g3_no, g3_name,
                g4_no, g4_name
            ))

            # =========== Loan Head Debit ===========
            cur.execute("""
                INSERT INTO loan_transactions
                (loan_no, trans_type, amount, trans_date, remark, created_by,
                 created_on, member_no, gl_head)
                VALUES (%s,'Debit',%s,NOW(),'Loan disbursed from Loan Head',
                        %s,NOW(),%s,%s)
            """, (
                loan_no, principal,
                session.get('user','system'), member_no, loan_head_id
            ))

            cur.execute("UPDATE loan_heads SET amount = amount - %s WHERE id=%s",
                        (principal, loan_head_id))

            # =========== Saving Credit ===========
            if saving_amount > 0:
                credit_account(cur, member_no, "Saving Account", saving_amount)

                cur.execute("SELECT account_no FROM accounts WHERE member_no=%s AND account_type='Saving Account' LIMIT 1",
                            (member_no,))
                saving_acc_no = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO transactions
                    (member_no, account_no, trans_type, amount, trans_date,
                     remark, created_by, source, gl_head)
                    VALUES (%s,%s,'Credit',%s,NOW(),
                    'Loan credited to Saving Account',
                    %s,'Loan Disbursement',%s)
                """, (
                    member_no, saving_acc_no, saving_amount,
                    session.get('user','system'), loan_head_id
                ))

                cur.execute("""
                    INSERT INTO loan_transactions
                    (loan_no, trans_type, amount, trans_date,
                     remark, created_by, created_on, member_no, account_no, account_type, gl_head)
                    VALUES (%s,'Credit',%s,NOW(),'Saving Account Credit',
                    %s,NOW(),%s,%s,'Saving Account',%s)
                """, (
                    loan_no, saving_amount,
                    session.get('user','system'), member_no, saving_acc_no, loan_head_id
                ))

            # =========== Share Credit ===========
            if share_amount > 0:
                credit_account(cur, member_no, "Share Account", share_amount)

                cur.execute("SELECT account_no FROM accounts WHERE member_no=%s AND account_type='Share Account' LIMIT 1",
                            (member_no,))
                share_acc_no = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO transactions
                    (member_no, account_no, trans_type, amount, trans_date,
                     remark, created_by, source, gl_head)
                    VALUES (%s,%s,'Credit',%s,NOW(),
                    'Loan credited to Share Account',
                    %s,'Loan Disbursement',%s)
                """, (
                    member_no, share_acc_no, share_amount,
                    session.get('user','system'), loan_head_id
                ))

                cur.execute("""
                    INSERT INTO loan_transactions
                    (loan_no, trans_type, amount, trans_date, remark,
                     created_by, created_on, member_no, account_no, account_type, gl_head)
                    VALUES (%s,'Credit',%s,NOW(),'Share A/c Credit',
                    %s,NOW(),%s,%s,'Share Account',%s)
                """, (
                    loan_no, share_amount,
                    session.get('user','system'),
                    member_no, share_acc_no, loan_head_id
                ))

            # =========== Home Loan Credit ===========
            if home_loan_amount > 0:
                cur.execute("UPDATE accounts SET balance = balance + %s WHERE account_no=%s",
                            (home_loan_amount, home_acc_no))

                cur.execute("""
                    INSERT INTO transactions
                    (member_no, account_no, trans_type, amount, trans_date,
                     remark, created_by, source, gl_head)
                    VALUES (%s,%s,'Credit',%s,NOW(),
                    'Loan credited to Home Loan Account',
                    %s,'Loan Disbursement',%s)
                """, (
                    member_no, home_acc_no, home_loan_amount,
                    session.get('user','system'), loan_head_id
                ))

                cur.execute("""
                    INSERT INTO loan_transactions
                    (loan_no, trans_type, amount, trans_date, remark,
                     created_by, created_on, member_no, account_no, account_type, gl_head)
                    VALUES (%s,'Credit',%s,NOW(),'Home Loan A/c Credit',
                    %s,NOW(),%s,%s,'Home Loan Account',%s)
                """, (
                    loan_no, home_loan_amount,
                    session.get('user','system'),
                    member_no, home_acc_no, loan_head_id
                ))

            conn.commit()
            flash(f"Loan Created Successfully! Loan No: {loan_no}", "success")

        except Exception as e:
            if conn: conn.rollback()
            print("❌ ERROR:", e)
            flash("Error Occurred: " + str(e), "error")

        finally:
            if conn: conn.close()

        return redirect("/loan/new")

    return render_template("disburse_loan.html",
                           heads=["जमानती क़र्ज़", "विविध क़र्ज़", "चिकित्सा क़र्ज़",
                                  "माकन तरन क़र्ज़", "दोपहिया वाहन क़र्ज़", "पावती तरन क़र्ज़"])
                                  
        
        
@app.route("/api/get_member_fd/<member_no>")
def get_member_fd(member_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT member_name_eng FROM members WHERE member_no=%s", (member_no,))
    r = cur.fetchone()
    name = r[0] if r else ""

    cur.execute("""
        SELECT deposit_amount 
        FROM fd_accounts
        WHERE member_no=%s AND status='Active'
    """, (member_no,))
    rows = cur.fetchall()

    fd_total = sum(float(x[0]) for x in rows) if rows else 0
    allowed = fd_total * 0.75

    return jsonify({
        "name": name,
        "allowed": allowed
    })






    
    
    
    





    







from datetime import datetime, date
from decimal import Decimal
from psycopg2 import sql

def add_month(dt):
    """Return date object for first day of next month."""
    year = dt.year + (dt.month // 12)
    month = dt.month % 12 + 1
    return date(year, month, 1)

@app.route("/loan/repay", methods=["GET", "POST"])
def emi_repay():
    conn = get_db()
    cur = conn.cursor()

    if request.method == "POST":
        try:
            loan_no = request.form.get("loan_no")
            pay_date_str = request.form.get("pay_date")
            pay_date = datetime.strptime(pay_date_str, "%Y-%m-%d") if pay_date_str else datetime.now()
            pay_mode = request.form.get("pay_mode")

            cur.execute("""
                SELECT loan_no, member_no, member_name, interest_rate, emi,
                       outstanding_principal, status, gl_code, loan_head,
                       total_payable, principal, disburse_date
                FROM loans WHERE loan_no=%s FOR UPDATE
            """, (loan_no,))
            row = cur.fetchone()

            if not row:
                flash("Loan not found.", "error")
                return redirect(url_for("emi_repay"))

            (loan_no_db, member_no, member_name, int_rate, emi_val,
             outstanding, status_db, gl_code, loan_head_name,
             total_payable, principal_amount, disburse_date) = row

            emi_val = Decimal(str(emi_val))
            outstanding = Decimal(str(outstanding))
            total_payable = Decimal(str(total_payable))
            principal_amount = Decimal(str(principal_amount))

            if status_db and status_db.lower() == "closed":
                flash("Loan already closed.", "error")
                return redirect(url_for("emi_repay"))

            # 🔴 CHANGE START
            skip_interest = disburse_date is not None
            accum_interest = Decimal("0.00")
            pending_interest = Decimal("0.00")
            # 🔴 CHANGE END

            # ======================================================
            # INTEREST LOGIC (ONLY IF disburse_date IS NULL)
            # ======================================================
            if not skip_interest:

                cur.execute("""
                    SELECT month_year FROM interest_history
                    WHERE account_no=%s
                    ORDER BY month_year DESC
                    LIMIT 1
                """, (loan_no_db,))
                last_row = cur.fetchone()

                if last_row and last_row[0]:
                    last_month = last_row[0]
                    if isinstance(last_month, datetime):
                        last_month = last_month.date()
                    next_month_year = add_month(last_month)
                else:
                    cur.execute("""
                        SELECT created_on FROM loans WHERE loan_no=%s
                    """, (loan_no_db,))
                    loan_start_date = cur.fetchone()[0]
                    if isinstance(loan_start_date, datetime):
                        loan_start_date = loan_start_date.date()

                    last_month = date(loan_start_date.year, loan_start_date.month, 1)
                    next_month_year = add_month(last_month)

                months_gap = (pay_date.year - last_month.year) * 12 + (pay_date.month - last_month.month)

                if months_gap > 1:
                    missing_date = last_month
                    for i in range(1, months_gap):
                        missing_date = add_month(missing_date)

                        cur.execute("""
                            SELECT 1 FROM interest_history
                            WHERE account_no=%s AND month_year=%s
                            LIMIT 1
                        """, (loan_no_db, missing_date))
                        if cur.fetchone():
                            continue

                        missing_interest = (
                            outstanding * Decimal(str(int_rate)) /
                            Decimal('12') / Decimal('100')
                        ).quantize(Decimal("0.01"))

                        cur.execute("""
                            INSERT INTO interest_history
                            (account_type, account_no, member_no, member_name,
                             month_year, principal, interest_rate,
                             monthly_interest, added_to_loan, created_on)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,false,NOW())
                        """, (
                            "Loan", loan_no_db, member_no, member_name,
                            missing_date, outstanding,
                            Decimal(str(int_rate)), missing_interest
                        ))

                        if missing_date.month == 3 and loan_head_name != "विविध क़र्ज़":
                            fy_year = missing_date.year

                            cur.execute("""
                                SELECT COALESCE(SUM(monthly_interest),0)
                                FROM interest_history
                                WHERE account_no=%s
                                  AND month_year >= %s
                                  AND month_year <= %s
                                  AND added_to_loan = false
                            """, (
                                loan_no_db,
                                date(fy_year - 1, 4, 1),
                                date(fy_year, 3, 31)
                            ))
                            fy_interest = Decimal(cur.fetchone()[0] or 0).quantize(Decimal("0.01"))

                            if fy_interest > 0:
                                outstanding += fy_interest
                                total_payable += fy_interest

                                cur.execute("""
                                    UPDATE interest_history
                                    SET added_to_loan = true
                                    WHERE account_no=%s
                                      AND month_year >= %s
                                      AND month_year <= %s
                                      AND added_to_loan = false
                                """, (
                                    loan_no_db,
                                    date(fy_year - 1, 4, 1),
                                    date(fy_year, 3, 31)
                                ))

                                cur.execute("""
                                    UPDATE loans
                                    SET outstanding_principal=%s,
                                        total_payable=%s,
                                        updated_on=NOW()
                                    WHERE loan_no=%s
                                """, (outstanding, total_payable, loan_no_db))

                    next_month_year = add_month(missing_date)

                monthly_interest = (
                    outstanding * Decimal(str(int_rate)) /
                    Decimal('12') / Decimal('100')
                ).quantize(Decimal("0.01"))

                cur.execute("""
                    SELECT 1 FROM interest_history
                    WHERE account_no=%s AND month_year=%s
                    LIMIT 1
                """, (loan_no_db, next_month_year))

                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO interest_history
                        (account_type, account_no, member_no, member_name,
                         month_year, principal, interest_rate,
                         monthly_interest, added_to_loan, created_on)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,false,NOW())
                    """, (
                        "Loan", loan_no_db, member_no, member_name,
                        next_month_year, outstanding,
                        Decimal(str(int_rate)), monthly_interest
                    ))

                if next_month_year.month == 3 and loan_head_name != "विविध क़र्ज़":
                    fy_year = next_month_year.year

                    cur.execute("""
                        SELECT COALESCE(SUM(monthly_interest),0)
                        FROM interest_history
                        WHERE account_no=%s
                          AND month_year >= %s
                          AND month_year <= %s
                          AND added_to_loan = false
                    """, (
                        loan_no_db,
                        date(fy_year - 1, 4, 1),
                        date(fy_year, 3, 31)
                    ))
                    accum_interest = Decimal(cur.fetchone()[0] or 0).quantize(Decimal("0.01"))

                    if accum_interest > 0:
                        outstanding += accum_interest
                        total_payable += accum_interest

                        cur.execute("""
                            UPDATE interest_history
                            SET added_to_loan = true
                            WHERE account_no=%s
                              AND month_year >= %s
                              AND month_year <= %s
                              AND added_to_loan = false
                        """, (
                            loan_no_db,
                            date(fy_year - 1, 4, 1),
                            date(fy_year, 3, 31)
                        ))

                        cur.execute("""
                            UPDATE loans
                            SET outstanding_principal=%s,
                                total_payable=%s,
                                updated_on=NOW()
                            WHERE loan_no=%s
                        """, (outstanding, total_payable, loan_no_db))

                future_outstanding = outstanding - min(emi_val, outstanding)

                if future_outstanding <= 0 and loan_head_name != "विविध क़र्ज़":
                    cur.execute("""
                        SELECT COALESCE(SUM(monthly_interest),0)
                        FROM interest_history
                        WHERE account_no=%s
                          AND added_to_loan = false
                    """, (loan_no_db,))
                    pending_interest = Decimal(cur.fetchone()[0] or 0).quantize(Decimal("0.01"))

                    if pending_interest > 0:
                        outstanding += pending_interest
                        total_payable += pending_interest

                        cur.execute("""
                            UPDATE interest_history
                            SET added_to_loan = true
                            WHERE account_no=%s
                              AND added_to_loan = false
                        """, (loan_no_db,))

            # ======================================================
            # PRINCIPAL PAYMENT (UNCHANGED)
            # ======================================================
            pay_principal = min(emi_val, outstanding)
            outstanding -= pay_principal
            total_payable -= pay_principal

            if outstanding < 0: outstanding = Decimal("0.00")
            if total_payable < 0: total_payable = Decimal("0.00")

            account_no_used = None
            account_type_used = "Cash"

            if pay_mode == "Saving":
                cur.execute("""
                    SELECT account_no, balance FROM accounts
                    WHERE member_no=%s AND account_type='Saving Account'
                    LIMIT 1 FOR UPDATE
                """, (member_no,))
                sav_acc, sav_bal = cur.fetchone()

                sav_bal = Decimal(str(sav_bal))
                if sav_bal < pay_principal:
                    raise Exception("Insufficient balance")

                cur.execute("""
                    UPDATE accounts
                    SET balance = balance - %s
                    WHERE account_no=%s
                """, (pay_principal, sav_acc))

                account_no_used = sav_acc
                account_type_used = "Saving Account"

                cur.execute("""
                    INSERT INTO transactions
                    (member_no, account_no, trans_type, amount,
                     trans_date, remark, created_by, source, gl_head)
                    VALUES (%s,%s,'Debit',%s,%s,%s,%s,'EMI Payment',%s)
                """, (
                    member_no, sav_acc, pay_principal,
                    pay_date.date(),
                    f'Principal EMI deduction for Loan {loan_no_db}',
                    session.get('user','system'), gl_code
                ))

            remark_text = (
                f"Principal paid {pay_principal}, "
                f"Interest added (FY accrual): {accum_interest}, "
                f"Pending interest added on closure: {pending_interest}"
            )

            cur.execute("""
                INSERT INTO loan_transactions
                (loan_no, trans_type, amount, trans_date, remark,
                 created_by, created_on, member_no,
                 account_no, account_type, gl_head)
                VALUES (%s,'REPAY',%s,%s,%s,%s,NOW(),%s,%s,%s,%s)
            """, (
                loan_no_db, pay_principal, pay_date.date(),
                remark_text, session.get('user','system'),
                member_no, account_no_used,
                account_type_used, gl_code
            ))

            loan_status = "Closed" if outstanding <= 0 else "Active"

            cur.execute("""
                UPDATE loans
                SET outstanding_principal=%s,
                    total_payable=%s,
                    total_paid=COALESCE(total_paid,0)+%s,
                    updated_on=NOW(),
                    status=%s
                WHERE loan_no=%s
            """, (
                outstanding, total_payable,
                pay_principal, loan_status,
                loan_no_db
            ))

            conn.commit()
            flash("EMI paid successfully", "success")
            return redirect(url_for("emi_repay"))

        except Exception as e:
            conn.rollback()
            flash("ERROR: " + str(e), "error")
            return redirect(url_for("emi_repay"))

    cur.execute("""
        SELECT loan_no, member_no, member_name,
               outstanding_principal, emi, loan_head
        FROM loans
        WHERE COALESCE(outstanding_principal,0) > 0
        ORDER BY created_on DESC
    """)
    loans = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("loan_repay.html", loans=loans)



from apscheduler.schedulers.background import BackgroundScheduler
from datetime import date
from decimal import Decimal


def auto_generate_monthly_loan_interest():
    conn = get_db()
    cur = conn.cursor()

    try:
        today = date.today()
        import calendar
        last_day = calendar.monthrange(today.year, today.month)[1]
        if today.day != last_day:
            return   # 👈 sirf last date pe hi chalega
            
        current_month = date(today.year, today.month, 1)

        # sirf active loans
        cur.execute("""
            SELECT loan_no, member_no, member_name,
                   interest_rate, outstanding_principal, loan_head
            FROM loans
            WHERE status='Active' AND COALESCE(outstanding_principal,0) > 0
        """)
        loans = cur.fetchall()

        for loan_no, member_no, member_name, rate, outstanding, loan_head in loans:
            outstanding = Decimal(str(outstanding))
            rate = Decimal(str(rate))

            # check: is month ka interest already hai ya nahi
            cur.execute("""
                SELECT 1 FROM interest_history
                WHERE account_no=%s AND month_year=%s
                LIMIT 1
            """, (loan_no, current_month))

            if cur.fetchone():
                continue  # already generated

            monthly_interest = (
                outstanding * rate / Decimal('12') / Decimal('100')
            ).quantize(Decimal('0.01'))

            cur.execute("""
                INSERT INTO interest_history
                (account_type, account_no, member_no, member_name,
                 month_year, principal, interest_rate,
                 monthly_interest, added_to_loan, created_on)
                VALUES
                ('Loan', %s, %s, %s,
                 %s, %s, %s,
                 %s, false, NOW())
            """, (
                loan_no, member_no, member_name,
                current_month, outstanding, rate,
                monthly_interest
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("AUTO INTEREST ERROR:", e)

    finally:
        cur.close()
        conn.close()


scheduler = BackgroundScheduler()
scheduler.add_job(
    auto_generate_monthly_loan_interest,
    trigger='cron',        # har mahine 1 tareekh
    hour=23,
    minute=55
)

@app.route("/test/run-interest-job", methods=["POST"])
def test_interest_job():
    auto_generate_monthly_loan_interest()
    flash("Interest job successfully executed", "success")
    return redirect(url_for("emi_repay"))













    
    
    
@app.route('/api/get_member/<member_no>')
def get_member_api(member_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT member_name_eng FROM members WHERE member_no=%s", (member_no,))
    r = cur.fetchone()

    if not r:
        return jsonify({"status": "not_found"})

    name = r[0]

    cur.execute("""
        SELECT balance 
        FROM accounts 
        WHERE member_no=%s AND account_type='Anivarya Sanchay'
    """, (member_no,))
    a = cur.fetchone()
    anivarya = float(a[0]) if a else 0

    return jsonify({
        "status": "ok",
        "name": name,
        "anivarya_balance": anivarya
    })
    
@app.route('/api/search_members')
def search_members():
    text = request.args.get("q", "")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT member_no, member_name_eng 
        FROM members 
        WHERE member_no ILIKE %s OR member_name_eng ILIKE %s
        ORDER BY member_no
        LIMIT 10
    """, (f"%{text}%", f"%{text}%"))

    data = [{"member_no": r[0], "name": r[1]} for r in cur.fetchall()]
    return jsonify(data)



# ---- ऊपर का code यहाँ खत्म होता है ----
from decimal import Decimal, ROUND_HALF_UP

@app.route("/interest_form", methods=["GET", "POST"])
def interest_form():
    conn = get_db()
    cur = conn.cursor()

    if request.method == "POST":
        try:
            member_no = request.form.get("member_no")
            loan_no = request.form.get("loan_no")
            credit_account = request.form.get("credit_account")  # loan_head ID from dropdown

            # 1️⃣ Fetch loan outstanding
            cur.execute("SELECT total_payable FROM loans WHERE loan_no = %s", (loan_no,))
            loan_row = cur.fetchone()
            if not loan_row:
                flash(f"Loan {loan_no} not found!", "danger")
                return redirect(url_for("interest_form"))

            outstanding = Decimal(loan_row[0])

            # 2️⃣ Fetch member's saving account
            cur.execute("""
                SELECT account_no, balance 
                FROM accounts 
                WHERE member_no = %s AND account_type = 'Saving Account'
            """, (member_no,))
            acc_row = cur.fetchone()
            if not acc_row:
                flash(f"Saving account for member {member_no} not found!", "danger")
                return redirect(url_for("interest_form"))

            debit_account_no = acc_row[0]
            current_balance = Decimal(acc_row[1])

            if current_balance < outstanding:
                flash("Insufficient balance in saving account!", "danger")
                return redirect(url_for("interest_form"))

            # 3️⃣ Debit saving account
            new_balance = current_balance - outstanding
            cur.execute("""
                UPDATE accounts
                SET balance = %s
                WHERE account_no = %s
            """, (new_balance, debit_account_no))

            # 4️⃣ Update loan total_payable
            remaining_payable = Decimal(loan_row[0]) - outstanding
            status = 'closed' if remaining_payable <= 0 else 'active'
            cur.execute("""
                UPDATE loans
                SET total_payable = %s, status = %s
                WHERE loan_no = %s
            """, (remaining_payable, status, loan_no))

            # 5️⃣ Credit loan_head (loan_transactions) with required columns
            cur.execute("""
                INSERT INTO loan_transactions 
                (loan_no, member_no, gl_head, amount, trans_type, remark, created_by, account_no, account_type, created_on)
                VALUES (%s, %s, %s, %s, 'Credit', %s, %s, %s, %s, NOW())
            """, (
                loan_no,
                member_no,
                credit_account,                  # gl_head = loan_head ID
                outstanding,
                f"Interest payment for loan {loan_no}",  # remark
                'system',                         # created_by
                debit_account_no,                 # account_no
                'Saving Account'                  # account_type
            ))

            # --- NEW: Update loan_heads.amount ---
            cur.execute("""
                UPDATE loan_heads
                SET amount = COALESCE(amount, 0) + %s
                WHERE id = %s
            """, (outstanding, credit_account))

            # 6️⃣ Record transaction (saving account debit)
            cur.execute("""
                INSERT INTO transactions 
                (member_no, account_no, trans_type, amount, remark, created_by, trans_date, gl_head)
                VALUES (%s, %s, 'Debit', %s, %s, %s, NOW(), %s)
            """, (member_no, debit_account_no, outstanding, f"Interest payment for loan {loan_no}", 'system', credit_account))

            conn.commit()
            flash("Interest payment completed successfully!", "success")

        except Exception as e:
            conn.rollback()
            flash(f"Error: {str(e)}", "danger")

        finally:
            cur.close()
            conn.close()

        return redirect(url_for("interest_form"))

    # ---------- GET ----------
    cur.execute("SELECT member_no, member_name_eng FROM members ORDER BY member_no")
    members = cur.fetchall()

    cur.execute("""
        SELECT loan_no, member_no, loan_head, total_payable
        FROM loans
        WHERE LOWER(status) = 'active'
    """)
    loan_rows = cur.fetchall()
    loans = [{"loan_no": r[0], "member_no": r[1], "loan_head": r[2], "total_payable": float(r[3]), "status": "1"} for r in loan_rows]

    cur.execute("SELECT id, head_name FROM loan_heads ORDER BY head_name")
    loan_heads = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("interest_form.html", members=members, loans=loans, loan_heads=loan_heads)
    
    
@app.route("/view_loans")
def view_loans():
    conn = get_db()
    cur = conn.cursor()

    # Fetch loans
    cur.execute("""
        SELECT loan_no, member_no, member_name, loan_head, principal,
               interest_rate, tenure_months, emi, outstanding_principal,
               total_paid, status, total_payable
        FROM loans
        ORDER BY id DESC
    """)
    loans = cur.fetchall()

    # Fetch members for filter
    cur.execute("SELECT member_no, member_name_eng FROM members ORDER BY member_no")
    members_list = cur.fetchall()

    cur.close()
    conn.close()
    return render_template("view_loan.html", loans=loans, members_list=members_list)
    
    
    
@app.route("/loan_passbook")
def loan_passbook():
    return render_template("loan_passbook.html", title="Loan Passbook")
    
    
@app.route("/get_loans_by_member/<member_no>")
def get_loans_by_member(member_no):
    conn = get_db()
    cur = conn.cursor()

    # सभी loans दिखेंगे चाहे status जो भी हो (Open/Active/Closed)
    cur.execute("""
        SELECT loan_no, loan_head 
        FROM loans
        WHERE TRIM(member_no)=TRIM(%s)
        ORDER BY id DESC
    """, (member_no,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [{"loan_no": r[0], "loan_head": r[1]} for r in rows]



@app.route("/get_loan_passbook/<loan_no>")
def get_loan_passbook_data(loan_no):

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, trans_type, amount, trans_date, remark
        FROM loan_transactions
        WHERE loan_no=%s
        ORDER BY trans_date ASC, id ASC
    """, (loan_no,))

    rows = cur.fetchall()

    balance = 0
    transactions = []

    for r in rows:

        trans_type = r[1].lower()
        amount = float(r[2])

        credit = 0
        debit = 0

        if trans_type == "credit":
            credit = amount
            balance += amount
        else:
            debit = amount
            balance -= amount

        transactions.append({
            "date": r[3].strftime("%Y-%m-%d"),
            "narration": r[4],
            "trans_no": r[0],
            "credit": credit,
            "debit": debit,
            "balance": balance
        })

    conn.close()

    return {"transactions": transactions}
    
    
@app.route("/head_passbook")
def head_passbook():
    return render_template("head_passbook.html")


@app.route("/get_loan_heads")
def get_loan_heads():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT id, head_name FROM loan_heads ORDER BY head_name")

        data = [{"id": r[0], "head_name": r[1]} for r in cur.fetchall()]
        return jsonify({"status": "success", "data": data})

    except Exception as e:
        print("Error in get_loan_heads:", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/get_head_passbook/<int:head_id>")
def get_head_passbook(head_id):
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                id,
                COALESCE(trans_date, NOW()),
                COALESCE(trans_type, 'N/A'),
                COALESCE(amount, 0),
                COALESCE(remark, ''),
                COALESCE(loan_no, '')
            FROM loan_transactions
            WHERE gl_head::text = %s
            ORDER BY id DESC
        """, (str(head_id),))

        rows = cur.fetchall()

        result = [
            {
                "id": r[0],
                "trans_date": r[1].strftime("%d-%m-%Y") if r[1] else "",
                "trans_type": r[2],
                "amount": float(r[3]),
                "remark": r[4],
                "loan_no": r[5]
            }
            for r in rows
        ]

        return jsonify({"status": "success", "data": result})

    except Exception as e:
        print("Error in get_head_passbook:", e)
        return jsonify({"status": "error", "message": str(e)}), 500
        
        
        
        


from datetime import datetime, date
from flask import request, jsonify

@app.route("/loan/preclose/calc", methods=["POST"])
def preclose_calc():
    try:
        loan_no = request.form.get("loan_no")
        if not loan_no:
            return jsonify({"error": "Loan No Missing!"})

        conn = get_db()
        cur = conn.cursor()

        # -------------------------------------------------------
        # 1️⃣ Loan data fetch
        # -------------------------------------------------------
        cur.execute("""
            SELECT 
                loan_no,
                outstanding_principal,
                interest_rate,
                COALESCE(disbursed_date::date, created_on::date) AS start_date
            FROM loans
            WHERE loan_no = %s
        """, (loan_no,))
        row = cur.fetchone()

        if not row:
            return jsonify({"error": "Loan not found!"})

        loan_no, outstanding, rate, start_date = row

        # -------------------------------------------------------
        # 2️⃣ Fetch Recovered Interest (interest_history)
        # -------------------------------------------------------
        cur.execute("""
            SELECT COALESCE(SUM(monthly_interest),0)
            FROM interest_history
            WHERE account_no=%s AND added_to_loan=FALSE
        """, (loan_no,))
        recovered_interest = cur.fetchone()[0] or 0

        # -------------------------------------------------------
        # 3️⃣ Final Payable
        # -------------------------------------------------------
        total = float(outstanding) + float(recovered_interest)

        return jsonify({
            "outstanding": float(outstanding),
            "interest": float(recovered_interest),
            "total": total
        })

    except Exception as e:
        print("Preclose Calc Error:", e)
        return jsonify({"error": "Server Error in Preclose Calc!"})


from decimal import Decimal

@app.route("/loan/preclose/confirm", methods=["POST"])
def confirm_preclose():
    try:
        loan_no = request.form.get("loan_no")
        user = request.form.get("user") or "System"

        if not loan_no:
            return jsonify(error="Loan No missing"), 400

        conn = get_db()
        cur = conn.cursor()

        # -----------------------------
        # 1️⃣ Fetch loan main data
        # -----------------------------
        cur.execute("""
            SELECT 
                loan_no,
                member_no,
                outstanding_principal
            FROM loans
            WHERE loan_no=%s
        """, (loan_no,))
        row = cur.fetchone()

        if not row:
            return jsonify(error="Loan not found"), 404

        loan_no, member_no, outstanding = row

        # -----------------------------
        # 2️⃣ Fetch pending interest
        # -----------------------------
        cur.execute("""
            SELECT COALESCE(SUM(monthly_interest),0)
            FROM interest_history
            WHERE account_no=%s AND added_to_loan=FALSE
        """, (loan_no,))
        recovered_interest = cur.fetchone()[0] or 0

        # -----------------------------
        # 3️⃣ Final Payable
        # -----------------------------
        total_pay = float(outstanding) + float(recovered_interest)

        # -----------------------------
        # 4️⃣ Fetch saving account
        # -----------------------------
        cur.execute("""
            SELECT account_no, balance
            FROM accounts
            WHERE member_no=%s AND account_type='Saving Account'
            LIMIT 1
        """, (member_no,))
        account = cur.fetchone()
        if not account:
            return jsonify(error="Saving Account not found!"), 404

        account_no, balance = account

        if balance < total_pay:
            return jsonify(error="Insufficient balance!"), 400

        # -----------------------------
        # 5️⃣ Fetch GL Head
        # -----------------------------
        cur.execute("""
            SELECT lh.id
            FROM loan_heads lh
            JOIN loans l ON l.loan_head = lh.head_name
            WHERE l.loan_no=%s
        """, (loan_no,))
        gl = cur.fetchone()
        gl_head_id = gl[0] if gl else None

        # -----------------------------
        # 6️⃣ Debit saving account
        # -----------------------------
        new_balance = balance - Decimal(str(total_pay))

        cur.execute("""
            UPDATE accounts SET balance=%s WHERE account_no=%s
        """, (new_balance, account_no))

        # -----------------------------
        # 7️⃣ INSERT ONLY DEBIT in transactions
        # -----------------------------
        cur.execute("""
            INSERT INTO transactions
                (member_no, account_no, trans_type, amount, trans_date, remark, created_by, source, gl_head)
            VALUES (%s,%s,'Debit',%s,CURRENT_DATE,%s,%s,'Loan',%s)
        """, (
            member_no,
            account_no,
            total_pay,
            f"Loan Preclose Debit for Loan {loan_no}",
            user,
            gl_head_id
        ))

        # -----------------------------
        # 8️⃣ LOAN_TRANSACTION as CREDIT ENTRY (your requirement)
        # -----------------------------
        cur.execute("""
            INSERT INTO loan_transactions
                (loan_no, trans_type, amount, trans_date, remark, created_by, created_on,
                 member_no, account_no, account_type, gl_head)
            VALUES (%s,'LOAN_PRECLOSED',%s,CURRENT_DATE,
                    'Loan Preclosed (Credit)',%s,NOW(),
                    %s,%s,'Saving Account',%s)
        """, (
            loan_no, total_pay, user,
            member_no, account_no, gl_head_id
        ))

        # -----------------------------
        # 9️⃣ loan_heads amount update
        # -----------------------------
        cur.execute("""
            UPDATE loan_heads
            SET amount = COALESCE(amount,0) + %s
            WHERE id = %s
        """, (total_pay, gl_head_id))

        # -----------------------------
        # 🔟 Close the Loan
        # -----------------------------
        cur.execute("""
            UPDATE loans
            SET status='Closed',
                outstanding_principal=0,
                total_payable=0,
                total_paid = COALESCE(total_paid,0) + %s
            WHERE loan_no=%s
        """, (total_pay, loan_no))

        conn.commit()

        return jsonify(
            success=True,
            total_debited=float(total_pay),
            new_balance=float(new_balance)
        )

    except Exception as e:
        conn.rollback()
        print("Preclose Confirm Error:", e)
        return jsonify(error=str(e)), 500



        
# MEMBER SEARCH
# ------------------------------
#      LOAN PART PAYMENT ROUTES
# ------------------------------

# 1) MEMBER SEARCH
@app.route("/api/members/search")
def member_search():
    query = request.args.get("query", "")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT member_no, member_name_eng
        FROM members
        WHERE member_no ILIKE %s OR member_name_eng ILIKE %s
        ORDER BY member_no
        LIMIT 10
    """, (f"%{query}%", f"%{query}%"))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    data = [{"member_no": r[0], "member_name": r[1]} for r in rows]
    return jsonify(data)



# 2) LOANS BY MEMBER
@app.route("/api/loans/by-member/<member_no>", methods=["GET"])
def loans_by_member(member_no):

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT loan_no, loan_head
        FROM loans
        WHERE member_no = %s
        ORDER BY loan_no
    """, (member_no,))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {"loan_no": r[0], "loan_type": r[1]} 
        for r in rows
    ])



# 3) LOAN DETAILS
@app.route("/api/loan/details/<loan_no>", methods=["GET"])
def loan_details(loan_no):

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT total_payable, outstanding_principal, total_paid
        FROM loans
        WHERE loan_no = %s
    """, (loan_no,))
    
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "Loan not found"}), 404

    return jsonify({
        "total_payable": float(row[0]) if row[0] is not None else 0,
        "principal_outstanding": float(row[1]) if row[1] is not None else 0,
        "total_paid": float(row[2]) if row[2] is not None else 0
    })






# 5) PART PAYMENT PAGE (GET)
@app.route("/loan/part-payment", methods=["GET"])
def loan_part_payment_page():
    return render_template("loan_part_payment.html")
    
   
   
   
from datetime import datetime

@app.route("/loan/part-payment", methods=["POST"])
def loan_part_payment():
    """
    Expected JSON:
    {
      "member_no": "M001",
      "loan_no": "LN12345",
      "part_amount": 5000.00
    }
    """
    try:
        data = request.get_json() or {}
        member_no = data.get("member_no")
        loan_no = data.get("loan_no")
        part_amount = data.get("part_amount")

        # Basic validation
        if not member_no or not loan_no or part_amount is None:
            return jsonify({"status": "error", "message": "Missing required fields"}), 400

        try:
            part_amount = float(part_amount)
        except:
            return jsonify({"status": "error", "message": "Invalid amount"}), 400

        if part_amount <= 0:
            return jsonify({"status": "error", "message": "Amount must be greater than zero"}), 400

        conn = get_db()
        cur = conn.cursor()

        # 1) Lock and fetch loan row
        cur.execute("""
            SELECT loan_no, member_no, outstanding_principal, total_paid, total_payable, loan_head, gl_code, status
            FROM loans
            WHERE loan_no = %s
            FOR UPDATE
        """, (loan_no,))
        loan_row = cur.fetchone()
        if not loan_row:
            conn.rollback()
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Loan not found"}), 404

        (_loan_no, loan_member_no, outstanding_principal, total_paid, total_payable, loan_head_name, gl_code, loan_status) = loan_row

        # optional: check member_no matches loan
        if str(loan_member_no).strip() != str(member_no).strip():
            conn.rollback()
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Member does not match loan"}), 400

        # ensure loan active (optional)
        if loan_status and loan_status.lower() in ('closed', 'closed ' , 'closed\n'):
            conn.rollback()
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Loan already closed"}), 400

        # 2) Lock and fetch saving account (single)
        cur.execute("""
            SELECT account_no, balance
            FROM accounts
            WHERE member_no=%s AND account_type='Saving Account' AND status='Active'
            LIMIT 1
            FOR UPDATE
        """, (member_no,))
        acc_row = cur.fetchone()
        if not acc_row:
            conn.rollback()
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Saving account not found for member"}), 404

        saving_acc_no, saving_balance = acc_row
        saving_balance = float(saving_balance or 0)

        # 3) Check sufficient balance
        if saving_balance < part_amount:
            conn.rollback()
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Insufficient balance in saving account"}), 400

        # 4) Compute new values
        new_saving_balance = round(saving_balance - part_amount, 2)
        new_outstanding = round((float(outstanding_principal or 0) - part_amount), 2)
        new_total_paid = round((float(total_paid or 0) + part_amount), 2)
        new_total_payable = round((float(total_payable or 0) - part_amount), 2)

        # 5) Update accounts (debit saving)
        cur.execute("""
            UPDATE accounts
            SET balance = %s
            WHERE account_no = %s
        """, (new_saving_balance, saving_acc_no))

        # 6) Insert into transactions (DEBIT)
        # Use created_by = "System", source = 'Loan Part Payment'
        # Put gl_head as loan_head id if available (we'll fetch below)
        # For now find loan_head id:
        cur.execute("SELECT id FROM loan_heads WHERE head_name = %s LIMIT 1", (loan_head_name,))
        lh = cur.fetchone()
        loan_head_id = lh[0] if lh else None

        cur.execute("""
            INSERT INTO transactions
            (member_no, account_no, trans_type, amount, trans_date, remark, created_by, source, gl_head)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            member_no,
            saving_acc_no,
            'Debit',
            part_amount,
            datetime.now().date(),
            f'Loan Part Payment for {loan_no}',
            'System',
            'Loan Part Payment',
            str(loan_head_id) if loan_head_id is not None else None
        ))

        # 7) Update loans table (outstanding, total_paid, total_payable)
        cur.execute("""
            UPDATE loans
            SET outstanding_principal = %s,
                total_paid = %s,
                total_payable = %s,
                updated_on = NOW()
            WHERE loan_no = %s
        """, (new_outstanding, new_total_paid, new_total_payable, loan_no))

        # 8) Insert into loan_transactions — CREDIT entry to loan (loan side)
        cur.execute("""
            INSERT INTO loan_transactions
            (loan_no, trans_type, amount, trans_date, remark, created_by, created_on, member_no, account_no, account_type, gl_head)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)
        """, (
            loan_no,
            'Credit',
            part_amount,
            datetime.now().date(),
            f'Part Payment credited to loan {loan_no}',
            'System',
            member_no,
            None,
            None,
            str(loan_head_id) if loan_head_id is not None else loan_head_name
        ))

        
        # 10) Update loan_heads.amount (credit)
        if loan_head_id is not None:
            cur.execute("""
                UPDATE loan_heads
                SET amount = COALESCE(amount,0) + %s
                WHERE id = %s
            """, (part_amount, loan_head_id))
        else:
            # If loan_head_id not found, try updating by head_name (safer)
            if loan_head_name:
                cur.execute("""
                    UPDATE loan_heads
                    SET amount = COALESCE(amount,0) + %s
                    WHERE head_name = %s
                """, (part_amount, loan_head_name))

        # COMMIT
        conn.commit()

        # Close cursor/conn
        cur.close()
        conn.close()

        # Return useful info
        return jsonify({
            "status": "success",
            "message": "Part payment successful",
            "loan_no": loan_no,
            "member_no": member_no,
            "paid_amount": part_amount,
            "new_saving_balance": new_saving_balance,
            "new_outstanding": new_outstanding,
            "new_total_paid": new_total_paid,
            "new_total_payable": new_total_payable
        })

    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        # log error server-side
        print("ERROR in part-payment:", e)
        return jsonify({"status": "error", "message": "Server Error: " + str(e)}), 500
        
    
    
    
# Page: Modify EMI form
@app.route("/api/members", methods=["GET"])
def api_members():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT member_no, member_name_eng FROM members ORDER BY member_name_eng")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    members = [{"member_no": r[0], "member_name": r[1]} for r in rows]
    return jsonify(members)

# API: return active loans for a member
@app.route("/api/member_loans", methods=["GET"])
def api_member_loans():
    member_no = request.args.get("member_no")
    if not member_no:
        return jsonify([])
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT loan_no, loan_head, outstanding_principal, emi, tenure_months
        FROM loans
        WHERE member_no=%s AND status='Active'
        ORDER BY created_on DESC
    """, (member_no,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    loans = []
    for r in rows:
        loans.append({
            "loan_no": r[0],
            "loan_head": r[1],
            "outstanding": float(r[2] or 0),
            "emi": float(r[3] or 0),
            "tenure_months": int(r[4] or 0)
        })
    return jsonify(loans)

# API: return loan details
@app.route("/api/loan_details", methods=["GET"])
def api_loan_details():
    loan_no = request.args.get("loan_no")
    if not loan_no:
        return jsonify({"error": "Loan missing"}), 400
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT loan_no, member_no, member_name, outstanding_principal, emi, tenure_months
        FROM loans
        WHERE loan_no=%s
        LIMIT 1
    """, (loan_no,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return jsonify({"error": "Loan not found"}), 404
    return jsonify({
        "loan_no": row[0],
        "member_no": row[1],
        "member_name": row[2],
        "outstanding": float(row[3] or 0),
        "emi": float(row[4] or 0),
        "tenure_months": int(row[5] or 0)
    })

# Page: Modify EMI form
@app.route("/loan/modify", methods=["GET"])
def loan_modify_form():
    # page renders empty; front-end will call /api/members
    return render_template("loan_modify.html")


# POST: apply modification
@app.route("/loan/modify", methods=["POST"])
def loan_modify_apply():
    try:
        loan_no = request.form.get("loan_no")
        new_emi_raw = request.form.get("new_emi", "").strip()
        new_outstanding_raw = request.form.get("new_outstanding", "").strip()
        new_tenure_raw = request.form.get("new_tenure", "").strip()
        changed_by = session.get("user", "system")

        if not loan_no:
            flash("Loan not selected", "danger")
            return redirect(url_for("loan_modify_form"))

        conn = get_db()
        cur = conn.cursor()

        # fetch current values
        cur.execute("""
            SELECT loan_no, member_no, outstanding_principal, emi, tenure_months
            FROM loans WHERE loan_no=%s FOR UPDATE
        """, (loan_no,))
        row = cur.fetchone()
        if not row:
            flash("Loan not found", "danger")
            return redirect(url_for("loan_modify_form"))

        loan_no_db, member_no, outstanding_db, emi_db, tenure_db = row
        outstanding_db = Decimal(str(outstanding_db or 0))
        emi_db = Decimal(str(emi_db or 0))
        tenure_db = int(tenure_db or 0)

        # parse inputs (allow empty)
        new_emi = None
        new_outstanding = None
        new_tenure = None

        try:
            if new_emi_raw:
                new_emi = Decimal(new_emi_raw)
                if new_emi <= 0:
                    raise InvalidOperation
        except (InvalidOperation, ValueError):
            flash("New EMI invalid", "danger")
            cur.close(); conn.close()
            return redirect(url_for("loan_modify_form"))

        try:
            if new_outstanding_raw:
                new_outstanding = Decimal(new_outstanding_raw)
                if new_outstanding < 0:
                    raise InvalidOperation
        except (InvalidOperation, ValueError):
            flash("New Outstanding invalid", "danger")
            cur.close(); conn.close()
            return redirect(url_for("loan_modify_form"))

        try:
            if new_tenure_raw:
                new_tenure = int(new_tenure_raw)
                if new_tenure <= 0:
                    raise ValueError
        except Exception:
            flash("New Tenure invalid", "danger")
            cur.close(); conn.close()
            return redirect(url_for("loan_modify_form"))

        # --- Decide final applied values
        # Priority rules:
        # - If user provided new_outstanding explicitly -> use that.
        # - Else if new_emi and new_tenure both provided -> use both.
        # - Else if new_emi only -> compute new_tenure = ceil(outstanding / new_emi)
        # - Else if new_tenure only -> compute new_emi = ceil(outstanding / new_tenure)
        # - Else nothing -> error
        # --- Apply values EXACTLY as user entered (NO CALCULATION)

        final_outstanding = outstanding_db
        final_emi = emi_db
        final_tenure = tenure_db

        if new_outstanding is not None:
            final_outstanding = new_outstanding

        if new_emi is not None:
            final_emi = new_emi   # 👈 EMI EXACTLY USER INPUT

        if new_tenure is not None:
            final_tenure = new_tenure

        # at least one value must change
        if new_outstanding is None and new_emi is None and new_tenure is None:
            flash("Koi naya value provide nahi kiya.", "warning")
            cur.close(); conn.close()
            return redirect(url_for("loan_modify_form"))


        # Save log in loan_transactions (EMI_MODIFIED)
        remark = (f"EMI_MODIFIED: old_emi={emi_db}, old_tenure={tenure_db}, "
                  f"old_outstanding={outstanding_db} -> new_emi={final_emi}, new_tenure={final_tenure}, new_outstanding={final_outstanding}")

        cur.execute("""
            INSERT INTO loan_transactions
            (loan_no, trans_type, amount, trans_date, remark, created_by, created_on, member_no, account_no, account_type, gl_head)
            VALUES (%s, 'EMI_MODIFIED', %s, CURRENT_DATE, %s, %s, NOW(), %s, %s, %s, %s)
        """, (
            loan_no_db, final_emi, remark, changed_by, member_no, None, None, None
        ))

        # Update loans table
        cur.execute("""
            UPDATE loans
            SET emi=%s,
                tenure_months=%s,
                outstanding_principal=%s,
                updated_on=NOW()
            WHERE loan_no=%s
        """, (final_emi, final_tenure, final_outstanding, loan_no_db))

        conn.commit()
        cur.close(); conn.close()

        flash("Loan EMI/tenure updated successfully.", "success")
        return redirect(url_for("loan_modify_form"))

    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        print("Modify EMI Error:", e)
        flash("Error updating loan: " + str(e), "danger")
        return redirect(url_for("loan_modify_form"))
    
    
    
    

# PYTHON BACKEND ROUTES

from flask import jsonify

@app.route("/get_pedi_banks")
def get_pedi_banks():
    conn=get_db();cur=conn.cursor()
    cur.execute("SELECT head_name FROM loan_heads where head_type='Bank' ORDER BY id")
    rows=cur.fetchall()
    return jsonify([{ "head_name":r[0] } for r in rows])
    
    
    
    
    
@app.route("/bulk_transaction", methods=["GET"])
def bulk_transaction():
    conn = get_db()
    cur = conn.cursor()

    # Pedi Heads
    cur.execute("""
        SELECT id, head_name
        FROM loan_heads
        WHERE status='Active'
        ORDER BY head_name
    """)
    pedi_heads = cur.fetchall()

    # Active Members
    cur.execute("""
        SELECT member_no, member_name_eng
        FROM members
        WHERE status='Active'
        ORDER BY member_no
    """)
    members = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "bulk_transaction.html",
        pedi_heads=pedi_heads,
        members=members
    )


@app.route("/get_account_no_bulk/<member_no>/<account_type>")
def get_account_no_bulk(member_no, account_type):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT account_no
        FROM accounts
        WHERE member_no=%s
          AND account_type=%s
          AND status='Active'
        ORDER BY account_no
        LIMIT 1
    """, (member_no, account_type))

    row = cur.fetchone()
    cur.close()
    conn.close()

    return jsonify({
        "accounts": [row[0]] if row else []
    })


from decimal import Decimal
from flask import request, jsonify
from datetime import datetime

@app.route("/bulk_transaction/save", methods=["POST"])
def bulk_transaction_save():
    conn = None
    cur = None
    try:
        rows = request.json
        if not rows:
            return jsonify({"status": "error", "message": "No data received"}), 400

        conn = get_db()
        cur = conn.cursor()

        for r in rows:
            pedi_head      = r.get("pedi_head")
            target_pedi    = r.get("target_pedi_head")
            member_no      = r.get("member_no")
            account_no     = r.get("account_no")          # Loan No when Loan
            account_type   = r.get("account_type")
            amount         = Decimal(str(r.get("amount")))
            voucher_no     = r.get("voucher_no")
            txn_direction  = r.get("txn_direction", "P2M")

            if not pedi_head or not amount or not voucher_no:
                raise Exception("Invalid row data received")

            # =====================================================
            # 🔐 LOCK SOURCE PEDI
            # =====================================================
            cur.execute("""
                SELECT id, amount
                FROM loan_heads
                WHERE head_name=%s
                FOR UPDATE
            """, (pedi_head,))
            src = cur.fetchone()

            if not src:
                raise Exception(f"Pedi Head not found: {pedi_head}")

            src_id, src_balance = src
            src_balance = Decimal(src_balance)

            # =====================================================
            # 🟢 PEDI ➝ PEDI
            # =====================================================
            if txn_direction == "P2P":

                if not target_pedi:
                    raise Exception("Target Pedi Head required")

                if pedi_head == target_pedi:
                    raise Exception("Source & Target Pedi cannot be same")

                cur.execute("""
                    SELECT id, amount
                    FROM loan_heads
                    WHERE head_name=%s
                    FOR UPDATE
                """, (target_pedi,))
                tgt = cur.fetchone()

                if not tgt:
                    raise Exception("Target Pedi not found")

                tgt_id, tgt_balance = tgt
                tgt_balance = Decimal(tgt_balance)

                if src_balance < amount:
                    raise Exception("Insufficient balance in Source Pedi")

                cur.execute("UPDATE loan_heads SET amount=%s WHERE id=%s",
                            (src_balance - amount, src_id))
                cur.execute("UPDATE loan_heads SET amount=%s WHERE id=%s",
                            (tgt_balance + amount, tgt_id))

                cur.execute("""
                    INSERT INTO bank_transactions
                    (loan_no, bank_name, trans_type, amount, voucher_no,
                     remark, created_by, bank_id, created_on)
                    VALUES (%s,%s,'Debit',%s,%s,%s,%s,%s,NOW())
                """, ("P2P", pedi_head, amount, voucher_no,
                      f"Pedi to Pedi Transfer → {target_pedi}", "admin", src_id))

                cur.execute("""
                    INSERT INTO bank_transactions
                    (loan_no, bank_name, trans_type, amount, voucher_no,
                     remark, created_by, bank_id, created_on)
                    VALUES (%s,%s,'Credit',%s,%s,%s,%s,%s,NOW())
                """, ("P2P", target_pedi, amount, voucher_no,
                      f"Pedi to Pedi Transfer ← {pedi_head}", "admin", tgt_id))

                continue

            # =====================================================
            # 🔥 MEMBER ➝ PEDI (LOAN REPAYMENT)
            # =====================================================
            if txn_direction == "M2P" and account_type == "Loan":

                loan_no = account_no

                cur.execute("""
                    SELECT loan_no, member_no,
                           outstanding_principal, total_payable,
                           total_paid, status, gl_code
                    FROM loans
                    WHERE loan_no=%s
                    FOR UPDATE
                """, (loan_no,))
                loan = cur.fetchone()

                if not loan:
                    raise Exception(f"Loan not found: {loan_no}")

                (loan_no_db, loan_member,
                 outstanding, total_payable,
                 total_paid, status_db, gl_code) = loan

                outstanding   = Decimal(outstanding or 0)
                total_payable = Decimal(total_payable or 0)
                total_paid    = Decimal(total_paid or 0)

                if status_db and status_db.lower() == "closed":
                    raise Exception("Loan already closed")

                if amount > outstanding:
                    raise Exception("Amount greater than outstanding principal")

                new_outstanding = outstanding - amount
                new_total       = total_payable - amount
                new_status      = "Closed" if new_outstanding <= 0 else "Active"

                # 🔄 UPDATE LOANS
                cur.execute("""
                    UPDATE loans
                    SET outstanding_principal=%s,
                        total_payable=%s,
                        total_paid=%s,
                        status=%s,
                        updated_on=NOW()
                    WHERE loan_no=%s
                """, (
                    new_outstanding,
                    new_total,
                    total_paid + amount,
                    new_status,
                    loan_no_db
                ))

                # 🔺 CREDIT PEDI
                cur.execute("""
                    UPDATE loan_heads SET amount=%s WHERE id=%s
                """, (src_balance + amount, src_id))

                # 📒 PEDI LEDGER
                cur.execute("""
                    INSERT INTO bank_transactions
                    (loan_no, bank_name, trans_type, amount, voucher_no,
                     remark, created_by, member_no, bank_id, created_on)
                    VALUES
                    (%s,%s,'Credit',%s,%s,%s,%s,%s,%s,NOW())
                """, (
                    loan_no_db,
                    pedi_head,
                    amount,
                    voucher_no,
                    f"Loan Repayment {loan_no_db}",
                    "admin",
                    loan_member,
                    src_id
                ))

                # 📘 LOAN TRANSACTION
                cur.execute("""
                    INSERT INTO loan_transactions
                    (loan_no, trans_type, amount, trans_date, remark,
                     created_by, member_no, account_no, account_type,
                     gl_head, bank_name, voucher_no, bank_id)
                    VALUES
                    (%s,'REPAY',%s,CURRENT_DATE,%s,
                     %s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    loan_no_db,
                    amount,
                    "Bulk Loan Repayment",
                    "admin",
                    loan_member,
                    loan_no_db,
                    "Loan",
                    gl_code,
                    pedi_head,
                    voucher_no,
                    src_id
                ))

                # 🔐 LOCK & DEDUCT SAVING ACCOUNT
                cur.execute("""
                    SELECT account_no, balance
                    FROM accounts
                    WHERE member_no=%s
                      AND account_type='Saving Account'
                      AND status='Active'
                    FOR UPDATE
                    LIMIT 1
                """, (loan_member,))
                sav = cur.fetchone()

                if not sav:
                    raise Exception("Active Saving Account not found")

                saving_acc_no, saving_balance = sav
                saving_balance = Decimal(saving_balance)

                if saving_balance < amount:
                    raise Exception("Insufficient balance in Saving Account")

                cur.execute("""
                    UPDATE accounts
                    SET balance=%s
                    WHERE account_no=%s
                """, (saving_balance - amount, saving_acc_no))

                # 📗 MEMBER TRANSACTION LEDGER
                cur.execute("""
                    INSERT INTO transactions
                    (member_no, account_no, trans_type, amount, trans_date,
                     remark, created_by, source, bank_name, voucher_no, bank_id)
                    VALUES
                    (%s,%s,'Debit',%s,CURRENT_DATE,
                     %s,%s,%s,%s,%s,%s)
                """, (
                    loan_member,
                    saving_acc_no,
                    amount,
                    f"Loan Repayment {loan_no_db}",
                    "admin",
                    "Bulk",
                    pedi_head,
                    voucher_no,
                    src_id
                ))

                continue

            # =====================================================
            # 🔐 MEMBER ACCOUNT (OLD LOGIC – UNCHANGED)
            # =====================================================
            if not member_no or not account_no:
                raise Exception("Member/Account required")

            cur.execute("""
                SELECT balance
                FROM accounts
                WHERE account_no=%s
                FOR UPDATE
            """, (account_no,))
            acc = cur.fetchone()

            if not acc:
                raise Exception("Account not found")

            acc_balance = Decimal(acc[0])

            if txn_direction == "P2M":
                if src_balance < amount:
                    raise Exception("Insufficient Pedi balance")

                cur.execute("UPDATE loan_heads SET amount=%s WHERE id=%s",
                            (src_balance - amount, src_id))
                cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s",
                            (acc_balance + amount, account_no))
                pedi_trans_type, member_trans_type = "Debit", "Credit"
            else:
                if acc_balance < amount:
                    raise Exception("Insufficient Member balance")

                cur.execute("UPDATE accounts SET balance=%s WHERE account_no=%s",
                            (acc_balance - amount, account_no))
                cur.execute("UPDATE loan_heads SET amount=%s WHERE id=%s",
                            (src_balance + amount, src_id))
                pedi_trans_type, member_trans_type = "Credit", "Debit"

            cur.execute("""
                INSERT INTO bank_transactions
                (loan_no, bank_name, trans_type, amount, voucher_no,
                 remark, created_by, member_no, bank_id, created_on)
                VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """, (
                account_no, pedi_head, pedi_trans_type,
                amount, voucher_no,
                f"Multi Transaction {txn_direction}",
                "admin", member_no, src_id
            ))

            cur.execute("""
                INSERT INTO transactions
                (member_no, account_no, trans_type, amount, trans_date,
                 remark, created_by, source, bank_name, voucher_no, bank_id)
                VALUES
                (%s,%s,%s,%s,CURRENT_DATE,
                 %s,%s,%s,%s,%s,%s)
            """, (
                member_no, account_no, member_trans_type,
                amount,
                f"Multi Transaction {txn_direction} {pedi_head}",
                "admin", "Bulk", pedi_head, voucher_no, src_id
            ))

        conn.commit()
        return jsonify({"status": "success"})

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()







@app.route("/generate_voucher")
def generate_voucher():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT nextval('voucher_seq')")
    seq = cur.fetchone()[0]

    year = datetime.now().year
    voucher_no = f"MT/{year}/{str(seq).zfill(6)}"

    cur.close()
    conn.close()

    return jsonify({"voucher_no": voucher_no})
    
    
@app.route('/get_loan_no_by_member/<member_no>')
def get_loan_no_by_member(member_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT loan_no, loan_head
        FROM loans
        WHERE member_no = %s
        ORDER BY loan_no
    """, (member_no,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({
        "loan_nos": [
            {
                "loan_no": r[0],
                "loan_head": r[1]
            }
            for r in rows
        ]
    })
    
    
    

    
from flask import request, jsonify

@app.route("/api/transactions/<member_no>/<account_no>")
def api_transactions(member_no, account_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT trans_date, trans_type, amount, remark
        FROM transactions
        WHERE member_no=%s AND account_no=%s
        ORDER BY trans_date DESC
    """, (member_no, account_no))
    rows = cur.fetchall()
    cur.close(); conn.close()

    return jsonify([
        {
            "date": r[0].strftime("%Y-%m-%d"),
            "type": r[1],
            "amount": float(r[2]),
            "remark": r[3]
        } for r in rows
    ])

        
        
from flask import Flask, request, jsonify

from flask import request, jsonify
from werkzeug.security import check_password_hash

@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, username, role
        FROM users
        WHERE username=%s AND password=%s
    """, (username, password))

    user = cur.fetchone()
    cur.close()
    conn.close()

    if user:
        return jsonify({
            "status": "success",
            "username": user[1],
            "member_no": user[1]   # 🔥 SAME AS LOGIN USERNAME
        }), 200
    else:
        return jsonify({
            "status": "error",
            "message": "Invalid credentials"
        }), 401






        
        
@app.route("/api/accounts/<member_no>")
def api_accounts(member_no):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT account_no, account_type, balance
        FROM accounts
        WHERE member_no=%s
    """, (member_no,))
    rows = cur.fetchall()
    cur.close(); conn.close()

    return jsonify([
        {
            "account_no": r[0],
            "account_type": r[1],
            "balance": float(r[2])
        } for r in rows
    ])
    
    
@app.route("/api/saving-accounts/<member_no>")
def api_saving_accounts(member_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT account_no, account_type, balance
        FROM accounts
        WHERE account_type = 'Saving Account'
          AND member_no = %s
    """, (member_no,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {
            "account_no": r[0],
            "account_type": r[1],
            "balance": float(r[2])
        } for r in rows
    ])
    
    
@app.route("/api/account-types/<member_no>")
def api_account_types(member_no):
    types = []

    conn = get_db()
    cur = conn.cursor()

    # ===============================
    # Saving / Share / Anivarya
    # ===============================
    cur.execute("""
        SELECT DISTINCT account_type
        FROM accounts
        WHERE member_no = %s
          AND status = 'Active'
    """, (member_no,))
    rows = cur.fetchall()
    types.extend([r[0] for r in rows])

    # ===============================
    # Loan Account
    # ===============================
    cur.execute("""
        SELECT 1
        FROM loans
        WHERE member_no = %s
          AND status IN ('Active','Closed')
        LIMIT 1
    """, (member_no,))
    if cur.fetchone():
        types.append("Loan Account")

    # ===============================
    # RD Account
    # ===============================
    cur.execute("""
        SELECT 1
        FROM rd_accounts
        WHERE member_no = %s
          AND status IN ('Active','Closed')
        LIMIT 1
    """, (member_no,))
    if cur.fetchone():
        types.append("RD")

    # ===============================
    # FD Account
    # ===============================
    cur.execute("""
        SELECT 1
        FROM fd_accounts
        WHERE member_no = %s
          AND status IN ('Active','Closed')
        LIMIT 1
    """, (member_no,))
    if cur.fetchone():
        types.append("FD Account")

    cur.close()
    conn.close()

    # ===============================
    # FINAL UNIQUE LIST
    # ===============================
    return jsonify(sorted(list(set(types))))




@app.route("/api/accounts/<member_no>/<account_type>")
def api_accounts_by_type(member_no, account_type):
    conn = get_db()
    cur = conn.cursor()

    # ===== LOAN ACCOUNT =====
    if "loan" in account_type.lower():
        cur.execute("""
            SELECT
                loan_no,
                loan_head,
                principal,
                emi,
                outstanding_principal,
                total_paid,          -- ✅ ADD THIS
                emi_paid,
                emi_remaining,
                total_emi,
                interest_rate,
                status
            FROM loans
            WHERE member_no = %s
        """, (member_no,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        return jsonify([
            {
                "loan_no": r[0],
                "loan_head": r[1],
                "principal": str(r[2]) if r[2] is not None else "0",
                "emi": str(r[3]) if r[3] is not None else "0",
                "outstanding_principal": str(r[4]) if r[4] is not None else "0",

                # 🔑 THIS IS WHAT YOU WANT
                "total_paid": str(r[5]) if r[5] is not None else "0",

                "emi_paid": r[6] if r[6] is not None else 0,
                "emi_remaining": r[7] if r[7] is not None else 0,
                "total_emi": r[8] if r[8] is not None else 0,
                "interest_rate": str(r[9]) if r[9] is not None else "0",
                "status": r[10]
            }
            for r in rows
        ])

    # ===== OTHER ACCOUNTS (unchanged) =====
    cur.execute("""
        SELECT
            a.account_no,
            a.account_type,
            a.balance,
            a.member_no
        FROM accounts a
        WHERE a.member_no = %s
          AND a.account_type = %s
          AND a.status = 'Active'
    """, (member_no, account_type))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {
            "account_no": r[0],
            "account_type": r[1],
            "balance": float(r[2]) if r[2] else 0,
            "member_no": r[3]
        }
        for r in rows
    ])







    
@app.route("/api/rd-accounts/<member_no>")
def api_rd_accounts(member_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            rd_account_no,
            monthly_deposit,
            total_installments,
            installments_paid,
            duration_months,
            interest_rate,
            deposit_amount,
            maturity_amount,
            start_date,
            maturity_date,
            auto_renew,
            status
        FROM public.rd_accounts
        WHERE member_no = %s
          AND status IN ('Active','Closed')
        ORDER BY start_date DESC
    """, (member_no,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
    {
        "rd_no": r[0],

        "monthly_deposit": float(r[1]) if r[1] is not None else 0,
        "total_installments": r[2] if r[2] is not None else 0,
        "installments_paid": r[3] if r[3] is not None else 0,
        "duration_months": r[4] if r[4] is not None else 0,

        "interest_rate": float(r[5]) if r[5] is not None else 0,
        "deposit_amount": float(r[6]) if r[6] is not None else 0,
        "maturity_amount": float(r[7]) if r[7] is not None else 0,

        "start_date": r[8].strftime("%d-%m-%Y") if r[8] else "",
        "maturity_date": r[9].strftime("%d-%m-%Y") if r[9] else "",

        "auto_renew": r[10],
        "status": r[11]
    } for r in rows
])


@app.route("/api/member-profile/<member_no>")
def api_member_profile(member_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            member_no,
            member_name_hin,
            member_mobile_no
        FROM members
        WHERE member_no = %s
    """, (member_no,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return jsonify({}), 404

    return jsonify({
        "member_no": row[0],
        "member_name": row[1],      # 👈 Hindi Name
        "mobile_no": row[2]
    })
    
    
@app.route("/api/fd/account/data", methods=["GET"])
def get_fd_account_data():
    try:
        loginid = request.args.get("loginid")
        fd_status = request.args.get("fd_status", "operational")

        if not loginid:
            return jsonify({
                "status": "error",
                "message": "loginid required"
            }), 400

        conn = get_db()
        cur = conn.cursor()

        # 🔹 status logic
        if fd_status == "closed":
            status_condition = "is_closed = TRUE"
        else:
            status_condition = "is_closed = FALSE"

        query = f"""
            SELECT
                fd_account_no,
                deposit_amount,
                interest_rate,
                start_date,
                maturity_date,
                maturity_amount,
                status
            FROM fd_accounts
            WHERE member_no = %s
              AND {status_condition}
            ORDER BY start_date DESC
        """

        cur.execute(query, (loginid,))
        rows = cur.fetchall()

        data = []
        for r in rows:
            data.append({
                "fd_account_no": r[0],
                "deposit_amount": float(r[1]),
                "interest_rate": float(r[2]),
                "start_date": r[3].strftime("%Y-%m-%d"),
                "maturity_date": r[4].strftime("%Y-%m-%d"),
                "maturity_amount": float(r[5]),
                "status": r[6]
            })

        return jsonify({
            "status": "success",
            "count": len(data),
            "data": data
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
        
@app.route("/api/accounts/<member_no>/FD Account")
def api_fd_accounts_for_app(member_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            fd_account_no,
            deposit_amount,
            interest_rate,
            maturity_amount,
            TO_CHAR(start_date, 'DD-MM-YYYY')    AS fd_opening_date,
            TO_CHAR(maturity_date, 'DD-MM-YYYY') AS fd_maturity_date,
            status
        FROM fd_accounts
        WHERE member_no = %s
        ORDER BY start_date DESC
    """, (member_no,))

    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

    data = [dict(zip(colnames, r)) for r in rows]

    cur.close()
    conn.close()

    return jsonify(data)
    
from math import floor

@app.route("/api/accounts/<member_no>/Loan Account", methods=["GET"])
def api_loan_accounts_for_app(member_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            loan_no,
            loan_head,
            principal,
            interest_rate,
            emi,
            tenure_months,
            total_paid,
            outstanding_principal,
            status
        FROM loans
        WHERE member_no = %s
        ORDER BY created_on DESC
    """, (member_no,))

    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

    data = []

    for r in rows:
        row = dict(zip(colnames, r))

        emi_amount = float(row.get("emi") or 0)
        total_paid = float(row.get("total_paid") or 0)
        tenure = int(row.get("tenure_months") or 0)

        emi_paid = floor(total_paid / emi_amount) if emi_amount > 0 else 0
        emi_remaining = max(tenure - emi_paid, 0)

        row["emi_paid"] = emi_paid
        row["emi_remaining"] = emi_remaining
        row["total_emi"] = tenure

        # Optional cleanup
        row.pop("tenure_months", None)
        row.pop("total_paid", None)

        data.append(row)

    cur.close()
    conn.close()

    return jsonify(data)
    
    
@app.route("/api/statement/saving/<member_no>/<account_no>", methods=["GET"])
def saving_account_statement(member_no, account_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            TO_CHAR(trans_date, 'DD-MM-YYYY') AS trans_date,
            trans_type,id,
            amount,
            remark,
            voucher_no,
            cheque_no
        FROM transactions
        WHERE member_no = %s
          AND account_no = %s
        ORDER BY trans_date DESC, id DESC
    """, (member_no, account_no))

    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    data = [dict(zip(cols, row)) for row in rows]

    cur.close()
    conn.close()

    return jsonify(data)
    
    
@app.route("/api/statement/anivarya/<member_no>/<account_no>")
def anivarya_statement(member_no, account_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id,
            trans_date,
            trans_type,
            amount,
            remark,
            voucher_no,
            cheque_no
        FROM transactions
        WHERE member_no = %s
          AND account_no = %s
        ORDER BY trans_date ASC
    """, (member_no, account_no))

    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    data = [dict(zip(cols, r)) for r in rows]

    cur.close()
    conn.close()

    return jsonify(data)
    
@app.route("/api/share-statement/<member_no>/<account_no>")
def api_share_statement(member_no, account_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            TO_CHAR(trans_date, 'DD-MM-YYYY') AS trans_date,
            trans_type,
            amount,
            remark,
            voucher_no,
            cheque_no
        FROM transactions
        WHERE member_no = %s
          AND account_no = %s
        ORDER BY trans_date ASC, id ASC
    """, (member_no, account_no))

    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

    data = [dict(zip(colnames, row)) for row in rows]

    cur.close()
    conn.close()

    return jsonify(data)
    
@app.route("/api/rd-statement/<member_no>/<rd_account_no>")
def rd_statement(member_no, rd_account_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            trans_date,
            trans_type,
            amount,
            remark,
            voucher_no,
            cheque_no
        FROM transactions
        WHERE member_no = %s
          AND rd_account_no = %s
        ORDER BY trans_date DESC, id DESC
    """, (member_no, rd_account_no))

    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    data = [dict(zip(cols, r)) for r in rows]

    cur.close()
    conn.close()

    return jsonify(data)
    
@app.route("/api/loan-statement/<member_no>/<loan_no>")
def loan_statement(member_no, loan_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            loan_no,
            trans_type,
            amount,
            trans_date,
            remark,
            voucher_no
        FROM loan_transactions
        WHERE member_no = %s
          AND loan_no = %s
        ORDER BY trans_date ASC
    """, (member_no, loan_no))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for r in rows:
        result.append({
            "id": r[0],
            "loan_no": r[1],
            "trans_type": r[2],
            "amount": float(r[3]),
            "trans_date": r[4].strftime("%d-%m-%Y"),  # ✅ formatting here
            "remark": r[5],
            "voucher_no": r[6],
        })

    return jsonify(result)


    
    
# 🔥 Latest 20 Saving Account Transactions (Latest First)
@app.route("/api/saving/latest/<member_no>/<account_no>")
def latest_saving_transactions(member_no, account_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT trans_date, trans_type, amount, remark
        FROM transactions
        WHERE member_no = %s
          AND account_no = %s
        ORDER BY trans_date DESC
        LIMIT 20
    """, (member_no, account_no))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {
            "date": r[0].strftime("%Y-%m-%d"),
            "type": r[1],
            "amount": float(r[2]),
            "remark": r[3]
        }
        for r in rows
    ])


@app.route("/api/directors")
def get_directors():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT name, mobile_no, designation
        FROM directors
        WHERE status = 'ACTIVE'
        ORDER BY id
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {
            "name": r[0],
            "mobile": r[1],
            "designation": r[2]
        }
        for r in rows
    ])
    
    
@app.route("/api/member-personal/<member_no>")
def member_personal(member_no):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            member_no,
            member_name_eng,
            member_name_hin,
            member_email,
            member_mobile_no,
            opening_date,
            employee_no,
            father_name,
            gender,
            dob,
            present_address,
            permanent_address
        FROM members
        WHERE member_no = %s
    """, (member_no,))

    row = cur.fetchone()

    if not row:
        return jsonify({}), 404

    data = {
        "member_no": row[0],
        "member_name_eng": row[1],
        "member_name_hin": row[2],
        "email": row[3],
        "mobile": row[4],
        "opening_date": row[5].strftime("%d-%m-%Y") if row[5] else "",
        "employee_no": row[6],
        "father_name": row[7],
        "gender": row[8],
        "dob": row[9].strftime("%d-%m-%Y") if row[9] else "",
        "present_address": row[10],
        "permanent_address": row[11],
    }

    return jsonify(data)
    
@app.route("/api/interest-rates")
def api_interest_rates():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT category, title, tenure, rate
        FROM interest_rates
        WHERE status='Active'
        ORDER BY category, id
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    data = {}
    for cat, title, tenure, rate in rows:
        if cat not in data:
            data[cat] = {
                "title": title,
                "rates": []
            }
        data[cat]["rates"].append({
            "tenure": tenure,
            "rate": f"{rate:.2f} %"
        })

    return jsonify(data)
    
@app.route("/api/gallery")
def api_gallery():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT image_name, image_url
        FROM gallery_images
        WHERE status = 'Active'
        ORDER BY id DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {
            "image_name": r[0],
            "image_url": r[1]
        } for r in rows
    ])
    
@app.route("/api/news", methods=["GET"])
def get_news():
    conn = get_db()

    cur = conn.cursor()

    cur.execute("""
        SELECT id, title, description, created_on
        FROM news
        WHERE status = '1'
        ORDER BY created_on DESC
    """)

    rows = cur.fetchall()

    news_list = []
    for r in rows:
        news_list.append({
            "id": r[0],
            "title": r[1],
            "description": r[2],
            "created_on": r[3].strftime("%d-%m-%Y")
        })

    cur.close()
    conn.close()

    return jsonify(news_list)
    
# --- NEWS ADMIN PAGE ---
@app.route("/news", methods=["GET", "POST"])
def news_admin():
    if request.method == "POST":
        title = request.form.get("title")
        description = request.form.get("description")
        status = request.form.get("status", "1")

        if not title or not description:
            flash("❌ Title और Description जरूरी है", "danger")
            return redirect("/news")

        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO news (title, description, status, created_on)
            VALUES (%s, %s, %s, NOW())
        """, (title, description, status))

        conn.commit()
        cur.close()
        conn.close()

        flash("✅ समाचार सफलतापूर्वक जोड़ा गया", "success")
        return redirect("/news")

    # GET – show existing news
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, title, status, created_on
        FROM news
        ORDER BY created_on DESC
    """)
    news_list = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("news_admin.html", news_list=news_list)
    
# --- DELETE NEWS ---
@app.route("/news/delete/<int:news_id>")
def delete_news(news_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("DELETE FROM news WHERE id = %s", (news_id,))

    conn.commit()
    cur.close()
    conn.close()

    flash("🗑️ समाचार delete कर दिया गया", "success")
    return redirect("/news")
  
import base64
from flask import jsonify

@app.route("/api/qr", methods=["GET"])
def get_qr():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT upi_id, image
        FROM qr_code
        WHERE status = '1'
        ORDER BY id DESC
        LIMIT 1
    """)
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "QR not found"}), 404

    upi_id, image_bytes = row

    qr_base64 = base64.b64encode(image_bytes).decode("utf-8")

    return jsonify({
        "upi_id": upi_id,
        "qr_image": qr_base64
    })


from datetime import datetime
from flask import request, render_template, redirect, flash

from datetime import datetime
from flask import request, render_template, redirect, flash

@app.route("/deduction_form", methods=["GET", "POST"])
def deduction_form():
    conn = get_db()
    cur = conn.cursor()

    # =========================
    # 🔹 POST : SAVE DATA
    # =========================
    if request.method == "POST":

        month = int(request.form.get("month"))
        year  = int(request.form.get("year"))

        # 🔴 DUPLICATE CHECK (MONTH + YEAR)
        cur.execute("""
            SELECT COUNT(*)
            FROM employee_deduction_schedule
            WHERE month = %s
              AND year = %s
              AND status = 'Active'
        """, (month, year))

        if cur.fetchone()[0] > 0:
            cur.close()
            conn.close()
            flash(f"❌ {month}/{year} की कटौती पहले से मौजूद है", "danger")
            return redirect("/deduction_form")

        # FORM DATA
        dept_codes   = request.form.getlist("dept_code[]")
        employee_nos = request.form.getlist("employee_no[]")
        member_nos   = request.form.getlist("member_no[]")
        member_names = request.form.getlist("member_name[]")
        anivaryas    = request.form.getlist("anivarya[]")
        rds          = request.form.getlist("rd[]")

        # Loan heads
        cur.execute("""
            SELECT DISTINCT loan_head
            FROM loans
            WHERE TRIM(LOWER(status)) <> 'closed'
            ORDER BY loan_head
        """)
        loan_types = [r[0] for r in cur.fetchall()]

        loan_data = {
            lt: request.form.getlist(f"loan_{lt}[]")
            for lt in loan_types
        }

        saved_count = 0

        for i in range(len(member_nos)):

            # 🔹 अनिवार्य संचय
            if float(anivaryas[i] or 0) > 0:
                cur.execute("""
                    INSERT INTO employee_deduction_schedule
                    (dept_code, employee_no, member_no, member_name_hin,
                     deduction_head, deduction_type, amount,
                     month, year, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'Active')
                """, (
                    dept_codes[i],
                    employee_nos[i],
                    member_nos[i],
                    member_names[i],
                    "अनिवार्य संचय",
                    "ANIVARYA",
                    anivaryas[i],
                    month,
                    year
                ))
                saved_count += 1

            # 🔹 RD
            if float(rds[i] or 0) > 0:
                cur.execute("""
                    INSERT INTO employee_deduction_schedule
                    (dept_code, employee_no, member_no, member_name_hin,
                     deduction_head, deduction_type, amount,
                     month, year, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'Active')
                """, (
                    dept_codes[i],
                    employee_nos[i],
                    member_nos[i],
                    member_names[i],
                    "आवर्ती जमा",
                    "RD",
                    rds[i],
                    month,
                    year
                ))
                saved_count += 1

            # 🔹 LOANS
            for lt in loan_types:
                emi = loan_data[lt][i]
                if float(emi or 0) > 0:
                    cur.execute("""
                        INSERT INTO employee_deduction_schedule
                        (dept_code, employee_no, member_no, member_name_hin,
                         deduction_head, deduction_type, amount,
                         month, year, status)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'Active')
                    """, (
                        dept_codes[i],
                        employee_nos[i],
                        member_nos[i],
                        member_names[i],
                        lt,
                        "LOAN",
                        emi,
                        month,
                        year
                    ))
                    saved_count += 1

        conn.commit()
        cur.close()
        conn.close()

        flash(f"✅ कटौती सफलतापूर्वक सहेजी गई | Total Entries: {saved_count}", "success")
        return redirect("/deduction_form")

    # =========================
    # 🔹 GET : LOAD PAGE
    # =========================

    # 🔹 AUTO CLOSE RD WHERE installments_paid == duration_months
    cur.execute("""
        UPDATE rd_accounts
        SET status = 'Closed'
        WHERE status = 'Active'
          AND duration_months IS NOT NULL
          AND installments_paid IS NOT NULL
          AND duration_months = installments_paid
    """)
    conn.commit()

    # 🔹 Loan types
    cur.execute("""
        SELECT DISTINCT loan_head
        FROM loans
        WHERE TRIM(LOWER(status)) <> 'closed'
        ORDER BY loan_head
    """)
    loan_types = [r[0] for r in cur.fetchall()]

    # 🔹 Members (RD amount = SUM of all ACTIVE RD)
    cur.execute("""
        SELECT
            m.member_no,
            m.member_name_hin,
            m.employee_no,
            d.departcod,
            COALESCE(m.compulsory_deposit_amt,0),
            COALESCE(
                SUM(
                    CASE
                        WHEN r.monthly_deposit > 0 THEN r.monthly_deposit
                        WHEN r.deposit_amount > 0 AND r.duration_months > 0
                        THEN r.deposit_amount / r.duration_months
                        ELSE 0
                    END
                ),0
            )
        FROM members m
        LEFT JOIN department_master_new d
            ON d.subd_cod::text = m.division
        LEFT JOIN rd_accounts r
            ON r.member_no = m.member_no
           AND r.status = 'Active'
        WHERE m.member_type = 'Member'
          AND m.status = 'Active'
          AND m.employee_no IS NOT NULL
          AND m.employee_no <> ''
          AND TRIM(LOWER(m.old_member_no)) = 'true'
        GROUP BY
            m.member_no,
            m.member_name_hin,
            m.employee_no,
            d.departcod,
            m.compulsory_deposit_amt
        ORDER BY m.employee_no
    """)
    members = cur.fetchall()

    # 🔹 Loan EMI map
    cur.execute("""
        SELECT member_no, loan_head, emi
        FROM loans
        WHERE TRIM(LOWER(status)) <> 'closed'
    """)
    loan_map = {}
    for m, h, e in cur.fetchall():
        loan_map.setdefault(m, {})[h] = e or 0

    cur.close()
    conn.close()

    return render_template(
        "deduction_form.html",
        members=members,
        loan_types=loan_types,
        loan_map=loan_map
    )



@app.route("/save_deductions_batch", methods=["POST"])
def save_deductions_batch():

    data = request.json
    month = int(data["month"])
    year  = int(data["year"])

    conn = get_db()
    cur = conn.cursor()

    # 🔴 DUPLICATE CHECK (MONTH + YEAR)
    cur.execute("""
        SELECT COUNT(*)
        FROM employee_deduction_schedule
        WHERE month = %s
          AND year  = %s
          AND status = 'Active'
    """, (month, year))

    if cur.fetchone()[0] > 0:
        cur.close()
        conn.close()
        return {"error": "duplicate month/year"}, 409

    # ✅ INSERT DATA
    for r in data["rows"]:

        if r["anivarya"] > 0:
            cur.execute("""
                INSERT INTO employee_deduction_schedule
                (dept_code, employee_no, member_no, member_name_hin,
                 deduction_head, deduction_type, amount,
                 month, year, status)
                VALUES (%s,%s,%s,%s,'अनिवार्य संचय','ANIVARYA',%s,%s,%s,'Active')
            """, (
                r["dept"], r["emp"], r["member"], r["name"],
                r["anivarya"], month, year
            ))

        if r["rd"] > 0:
            cur.execute("""
                INSERT INTO employee_deduction_schedule
                (dept_code, employee_no, member_no, member_name_hin,
                 deduction_head, deduction_type, amount,
                 month, year, status)
                VALUES (%s,%s,%s,%s,'आवर्ती जमा','RD',%s,%s,%s,'Active')
            """, (
                r["dept"], r["emp"], r["member"], r["name"],
                r["rd"], month, year
            ))

        for h, emi in r["loans"].items():
            if emi > 0:
                cur.execute("""
                    INSERT INTO employee_deduction_schedule
                    (dept_code, employee_no, member_no, member_name_hin,
                     deduction_head, deduction_type, amount,
                     month, year, status)
                    VALUES (%s,%s,%s,%s,%s,'LOAN',%s,%s,%s,'Active')
                """, (
                    r["dept"], r["emp"], r["member"], r["name"],
                    h, emi, month, year
                ))

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "ok"}

    
from flask import request, render_template, redirect, flash
import pandas as pd
from werkzeug.utils import secure_filename
import os

@app.route("/deduction/upload", methods=["GET", "POST"])
def deduction_upload():

    if request.method == "POST":

        file = request.files.get("file")

        if not file or file.filename == "":
            flash("❌ कोई फ़ाइल select नहीं की गई", "danger")
            return redirect("/deduction/upload")

        # Excel read
        df = pd.read_excel(file)

        # 🔧 Column clean
        df.columns = (
            df.columns
              .str.strip()
              .str.lower()
              .str.replace(" ", "_")
        )

        required_cols = {
            "upload_row_id",
            "employee_no",
            "employee_name",
            "total_deducted_amount",
            "month",
            "year"
        }

        missing = required_cols - set(df.columns)
        if missing:
            flash(f"❌ Excel में ये column missing हैं: {', '.join(missing)}", "danger")
            return redirect("/deduction/upload")

        # 🔴 IMPORTANT: Month & Year duplicate check
        upload_month = int(df.iloc[0]["month"])
        upload_year  = int(df.iloc[0]["year"])

        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*)
            FROM employee_deduction_upload
            WHERE month = %s
              AND year  = %s
        """, (upload_month, upload_year))

        if cur.fetchone()[0] > 0:
            cur.close()
            conn.close()
            flash(
                f"❌ {upload_month}/{upload_year} की deduction पहले से upload हो चुकी है",
                "danger"
            )
            return redirect("/deduction/upload")

        # ✅ INSERT DATA
        inserted = 0
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO employee_deduction_upload
                (upload_row_id, employee_no, employee_name,
                 total_deducted_amount, month, year, status)
                VALUES (%s,%s,%s,%s,%s,%s,'PENDING')
            """, (
                int(row["upload_row_id"]),
                str(row["employee_no"]),
                str(row["employee_name"]),
                float(row["total_deducted_amount"]),
                int(row["month"]),
                int(row["year"])
            ))
            inserted += 1

        conn.commit()
        cur.close()
        conn.close()

        flash(
            f"✅ {inserted} records upload हुए | Month-Year: {upload_month}/{upload_year}",
            "success"
        )
        return redirect("/deduction/upload")

    return render_template("deduction_upload.html")

@app.route("/deduction/upload", methods=["GET"])
def deduction_upload_page():
    return render_template("deduction_upload.html")
    

@app.route("/deduction/status")
def deduction_status():
    month = request.args.get("month")
    year  = request.args.get("year")

    conn = get_db()
    cur = conn.cursor()

    query = """
        SELECT 
            u.employee_no,
            u.month,
            u.year,
            u.total_deducted_amount,
            COALESCE(SUM(s.amount),0) AS scheduled_amount,
            CASE 
                WHEN COALESCE(SUM(s.amount),0) = u.total_deducted_amount 
                THEN 'COMPLETE'
                ELSE 'PARTIAL'
            END AS status
        FROM employee_deduction_upload u
        LEFT JOIN employee_deduction_schedule s
            ON s.employee_no = u.employee_no
           AND s.month = u.month
           AND s.year = u.year
    """

    cond = []
    params = []

    if month:
        cond.append("u.month = %s")
        params.append(month)

    if year:
        cond.append("u.year = %s")
        params.append(year)

    if cond:
        query += " WHERE " + " AND ".join(cond)

    query += """
        GROUP BY u.employee_no, u.month, u.year, u.total_deducted_amount
        ORDER BY u.employee_no
    """

    cur.execute(query, params)
    rows = cur.fetchall()

    complete = []
    partial = []

    for r in rows:
        data = {
            "employee_no": r[0],
            "month": r[1],
            "year": r[2],
            "uploaded_amount": r[3],
            "scheduled_amount": r[4]
        }

        if r[5] == "COMPLETE":
            complete.append(data)
        else:
            partial.append(data)

    cur.close()
    conn.close()

    return render_template(
        "deduction_status.html",
        complete=complete,
        partial=partial,
        complete_count=len(complete),
        partial_count=len(partial),
        sel_month=month,
        sel_year=year
    )


@app.route("/deduction/posting/anivarya", methods=["POST"])
def post_anivarya_deduction():
    try:
        conn = get_db()
        cur = conn.cursor()

        # 1️⃣ Fetch un-posted Anivarya deductions (month+year wise)
        cur.execute("""
            SELECT employee_no, month, year, SUM(amount) AS total_amount
            FROM employee_deduction_schedule
            WHERE deduction_head = 'अनिवार्य संचय'
              AND COALESCE(posted, false) = false
            GROUP BY employee_no, month, year
        """)
        rows = cur.fetchall()

        posted_count = 0

        for emp_no, month, year, total_amount in rows:

            # 2️⃣ Get member_no from members
            cur.execute("""
                SELECT member_no
                FROM members
                WHERE employee_no = %s
            """, (emp_no,))
            res = cur.fetchone()
            if not res:
                continue

            member_no = res[0]

            # 3️⃣ Get Anivarya Sanchay account
            cur.execute("""
                SELECT account_no, balance
                FROM accounts
                WHERE member_no = %s
                  AND account_type = 'Anivarya Sanchay'
                  AND status = 'Active'
            """, (member_no,))
            acc = cur.fetchone()
            if not acc:
                continue

            account_no, balance = acc
            new_balance = balance + total_amount

            # 4️⃣ Update account balance
            cur.execute("""
                UPDATE accounts
                SET balance = %s
                WHERE account_no = %s
            """, (new_balance, account_no))

            # 5️⃣ Insert transaction (CREDIT)
            cur.execute("""
                INSERT INTO transactions
                (member_no, account_no, trans_type, amount, trans_date, remark, created_by)
                VALUES (%s, %s, 'Credit', %s, CURRENT_DATE, %s, 'system')
            """, (
                member_no,
                account_no,
                total_amount,
                f"MPPKVVCL deduction ({month}/{year})"
            ))

            # 6️⃣ Mark deduction rows as posted
            cur.execute("""
                UPDATE employee_deduction_schedule
                SET posted = true, posted_on = NOW()
                WHERE employee_no = %s
                  AND month = %s
                  AND year = %s
                  AND deduction_head = 'अनिवार्य संचय'
            """, (emp_no, month, year))

            posted_count += 1

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "message": f"✅ {posted_count} Anivarya postings completed successfully"
        }

    except Exception as e:
        print("❌ Posting Error:", e)
        return {
            "status": "error",
            "message": str(e)
        }, 500
        
@app.route("/deduction/posting/all", methods=["POST"])
def post_all_deductions():
    from decimal import Decimal
    from datetime import datetime

    try:
        conn = get_db()
        cur = conn.cursor()

        anivarya_count = 0
        rd_count = 0

        # =========================================================
        # 🔹 PART 1: ANIVARYA SANCHAY POSTING
        # =========================================================
        cur.execute("""
            SELECT employee_no, month, year, SUM(amount)
            FROM employee_deduction_schedule
            WHERE deduction_head='अनिवार्य संचय'
              AND COALESCE(posted,false)=false
            GROUP BY employee_no, month, year
        """)
        rows = cur.fetchall()

        for emp_no, month, year, total_amount in rows:

            cur.execute("SELECT member_no FROM members WHERE employee_no=%s", (emp_no,))
            r = cur.fetchone()
            if not r:
                continue
            member_no = r[0]

            cur.execute("""
                SELECT account_no, balance
                FROM accounts
                WHERE member_no=%s
                  AND account_type='Anivarya Sanchay'
                  AND status='Active'
            """, (member_no,))
            acc = cur.fetchone()
            if not acc:
                continue

            acc_no, bal = acc
            new_bal = Decimal(bal) + Decimal(total_amount)

            cur.execute("""
                UPDATE accounts
                SET balance=%s
                WHERE account_no=%s
            """, (new_bal, acc_no))

            cur.execute("""
                INSERT INTO transactions
                (member_no, account_no, trans_type, amount, remark, created_by)
                VALUES (%s,%s,'Credit',%s,%s,'system')
            """, (
                member_no,
                acc_no,
                total_amount,
                f"MPPKVVCL Deduction ({month}/{year})"
            ))

            cur.execute("""
                UPDATE employee_deduction_schedule
                SET posted=true, posted_on=NOW()
                WHERE employee_no=%s
                  AND month=%s AND year=%s
                  AND deduction_head='अनिवार्य संचय'
            """, (emp_no, month, year))

            anivarya_count += 1

                    # =========================================================
            # 🔹 PART 2: RD POSTING (MULTI-RD SAFE)
            # =========================================================
            cur.execute("""
                SELECT employee_no, month, year
                FROM employee_deduction_schedule
                WHERE deduction_head='आवर्ती जमा'
                  AND COALESCE(posted,false)=false
                GROUP BY employee_no, month, year
            """)
            rows = cur.fetchall()

            for emp_no, month, year in rows:

                # member
                cur.execute("SELECT member_no FROM members WHERE employee_no=%s", (emp_no,))
                r = cur.fetchone()
                if not r:
                    continue
                member_no = r[0]

                # 🔹 ALL ACTIVE RD ACCOUNTS (IMPORTANT FIX)
                cur.execute("""
                    SELECT rd_account_no, monthly_deposit, interest_rate, member_name
                    FROM rd_accounts
                    WHERE member_no=%s AND status='Active'
                    ORDER BY rd_account_no
                """, (member_no,))
                rd_accounts = cur.fetchall()

                if not rd_accounts:
                    continue

                for rd_no, monthly_dep, ir, member_name in rd_accounts:

                    monthly_dep = Decimal(monthly_dep or 0)
                    ir = Decimal(ir or 0)

                    if monthly_dep <= 0:
                        continue

                    # ✅ increment installment for THIS RD
                    cur.execute("""
                        UPDATE rd_accounts
                        SET installments_paid = COALESCE(installments_paid,0) + 1
                        WHERE rd_account_no=%s
                    """, (rd_no,))

                    # ✅ credit ONLY this RD's amount
                    cur.execute("""
                        INSERT INTO transactions
                        (member_no, rd_account_no, trans_type, amount, remark, source, created_by)
                        VALUES (%s,%s,'Credit',%s,%s,'DeductionUpload','system')
                    """, (
                        member_no,
                        rd_no,
                        monthly_dep,
                        f"RD Installment Received ({rd_no}) - MPPKVVCL Deduction"
                    ))

                    # 🔹 interest_history (same logic as credit_debit)
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM interest_history
                        WHERE account_type='RD' AND account_no=%s
                    """, (rd_no,))
                    count = cur.fetchone()[0]

                    principal = (count + 1) * monthly_dep
                    month_year = datetime.now().strftime("%Y-%m-01")
                    monthly_interest = (principal * ir / 100 / 12).quantize(Decimal("0.01"))

                    cur.execute("""
                        INSERT INTO interest_history
                        (account_type, account_no, member_no, member_name,
                         month_year, principal, interest_rate, monthly_interest)
                        VALUES ('RD',%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        rd_no,
                        member_no,
                        member_name,
                        month_year,
                        principal,
                        ir,
                        monthly_interest
                    ))

                # 🔹 mark deduction rows posted ONCE per employee/month/year
                cur.execute("""
                    UPDATE employee_deduction_schedule
                    SET posted=true, posted_on=NOW()
                    WHERE employee_no=%s
                      AND month=%s AND year=%s
                      AND deduction_head='आवर्ती जमा'
                """, (emp_no, month, year))

                rd_count += 1


        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "message": (
                f"✅ Posting Completed\n"
                f"Anivarya: {anivarya_count}\n"
                f"RD: {rd_count}"
            )
        }

    except Exception as e:
        print("POSTING ERROR:", e)
        return {"status": "error", "message": str(e)}, 500

@app.route("/print_passbook_page")
def print_passbook_page():
    member_no = request.args.get("member")
    account_no = request.args.get("acc")
    start_date = request.args.get("start")
    end_date = request.args.get("end")

    conn = get_db()
    cur = conn.cursor()

    # 🔹 NORMAL ACCOUNTS
    cur.execute("""
        SELECT trans_date, trans_type, amount, remark
        FROM transactions
        WHERE member_no=%s
          AND account_no=%s
        ORDER BY trans_date ASC
    """, (member_no, account_no))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    # Date filter Python side
    if start_date:
        rows = [r for r in rows if str(r[0]) >= start_date]
    if end_date:
        rows = [r for r in rows if str(r[0]) <= end_date]

    return render_template(
        "passbook_print.html",
        rows=rows,
        member_no=member_no,
        account_no=account_no,
        start_date=start_date,
        end_date=end_date
    )

@app.route("/ping")
def ping():
    return "OK"

import requests
from apscheduler.schedulers.background import BackgroundScheduler

def self_ping():
    try:
        requests.get("https://tksssm-portal.onrender.com/ping")
    except:
        pass

scheduler = BackgroundScheduler()
scheduler.add_job(self_ping, "interval", minutes=10)
scheduler.start()



















































































    
    
    



    
    


    
 








            




























        











    
    
 
    


# --- RUN APP ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)


# --- RUN APP 
#if __name__ == '__main__':
 #   print("🚀 Starting Waitress server on http://tech.mpwin.co.in:5001/ ...")
  #  serve(app, host="0.0.0.0", port=5001)

