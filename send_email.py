# send_email.py
import os
import glob
import base64
import requests
from dotenv import load_dotenv

# -------------------------
# Load environment variables
# -------------------------
load_dotenv()

BREVO_API_KEY = os.getenv("BREVO_API_KEY")
BREVO_SENDER = os.getenv("BREVO_SENDER")
BREVO_RECEIVER = os.getenv("BREVO_RECEIVER")

ALLOWED_EXTENSIONS = (".csv", ".xlsx", ".pdf")


def send_email(subject: str, body: str, attachment_dir: str = None):
    print("BREVO_SENDER:", BREVO_SENDER)
    print("BREVO_RECEIVER:", BREVO_RECEIVER)

    if not all([BREVO_API_KEY, BREVO_SENDER, BREVO_RECEIVER]):
        print("❌ Missing Brevo API key, sender, or receiver in .env")
        return False

    url = "https://api.brevo.com/v3/smtp/email"
    headers = {
        "accept": "application/json",
        "api-key": BREVO_API_KEY,
        "Content-Type": "application/json",
    }

    data = {
        "sender": {"name": "SEO Reports", "email": BREVO_SENDER},
        "to": [{"email": BREVO_RECEIVER}],
        "subject": subject,
        "htmlContent": body,
    }

    # -------------------------
    # Attachments (FILTERED)
    # -------------------------
    if attachment_dir and os.path.exists(attachment_dir):
        data["attachment"] = []

        files = glob.glob(os.path.join(attachment_dir, "**/*.*"), recursive=True)

        for file_path in files:
            filename = os.path.basename(file_path)

            if not filename.lower().endswith(ALLOWED_EXTENSIONS):
                print(f"⏭️ Skipped unsupported file: {filename}")
                continue

            try:
                with open(file_path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode()
                    data["attachment"].append(
                        {"name": filename, "content": encoded}
                    )
                    print(f"📎 Attached: {filename}")
            except Exception as e:
                print(f"⚠️ Failed to attach {filename}: {e}")

    # -------------------------
    # SEND EMAIL VIA BREVO
    # -------------------------
    try:
        response = requests.post(url, headers=headers, json=data)

        if response.status_code in (200, 201):
            print("✅ Email sent successfully via Brevo!")
            return True
        else:
            print(f"❌ Failed to send email: {response.status_code} {response.text}")
            return False

    except Exception as e:
        print(f"❌ Exception while sending email: {e}")
        return False


# -------------------------
# Quick test
# -------------------------
if __name__ == "__main__":
    send_email(
        subject="Test Email from Brevo",
        body="<p>This is a test email.</p>",
        attachment_dir=None,  # or your OUTPUT_DIR
    )
