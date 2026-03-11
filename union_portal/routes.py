from . import union
from flask import render_template, request, redirect, url_for
from datetime import datetime
import psycopg2
import os
from werkzeug.utils import secure_filename

# 🔹 Database Connection (separate DB for union)
def get_union_db():
    return psycopg2.connect(
        host="localhost",
        database="union_portal",
        user="postgres",
        password="root"
    )

# 🔹 Upload Folder
UPLOAD_FOLDER = os.path.join("union_portal", "static", "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@union.route("/")
def home():
    return redirect(url_for("union.membership"))


@union.route("/membership", methods=["GET", "POST"])
def membership():

    conn = get_union_db()

    if request.method == "POST":

        form_data = request.form

        file = request.files.get("payment_attachment")
        filename = ""

        if file and file.filename:
            filename = str(datetime.now().timestamp()).replace(".", "") + "_" + secure_filename(file.filename)
            file.save(os.path.join(UPLOAD_FOLDER, filename))

        cur = conn.cursor()

        insert_query = """
        INSERT INTO members
        (name, father_name, permanent_address, email, mobile, dob, district,
        company_name, position, office_name, membership_fee,
        class_of_employee, pincode, utr_number, payment_attachment, created_on)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cur.execute(insert_query, (
            form_data["name"],
            form_data["father_name"],
            form_data["permanent_address"],   # ✅ NEW
            form_data["email"],
            form_data["mobile"],
            form_data["dob"],
            form_data["district"],
            form_data["company_name"],
            form_data["position"],
            form_data["office_name"],
            form_data["membership_fee"],
            form_data["class_of_employee"],
            form_data["pincode"],
            form_data["utr_number"],
            filename,
            datetime.now()
        ))

        conn.commit()
        cur.close()
        conn.close()

        return redirect(url_for("union.membership", success="1"))

    success = request.args.get("success")
    conn.close()
    return render_template("union/membership.html", success=success)
