# auto_renew_runner.py
from app import auto_renew_rd  # app.py me define function

if __name__ == "__main__":
    try:
        auto_renew_rd()
        print("✅ Auto-renew executed successfully!")
    except Exception as e:
        print(f"❌ Auto-renew failed: {e}")
