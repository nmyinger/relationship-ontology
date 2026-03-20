import json
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow

TEMPLATE_PATH = Path("credentials/google_oauth_template.json")
OUTPUT_PATH = Path("credentials/google_oauth.json")

def main():
    with open(TEMPLATE_PATH, "r") as f:
        config = json.load(f)

    client_id = config["client_id"]
    client_secret = config["client_secret"]
    scopes = config["scopes"]
    token_uri = config.get("token_uri", "https://oauth2.googleapis.com/token")

    if "YOUR_CLIENT_ID" in client_id or "YOUR_CLIENT_SECRET" in client_secret:
        raise ValueError(
            "Fill in client_id and client_secret in credentials/google_oauth_template.json first"
        )

    flow = InstalledAppFlow.from_client_config(
        {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": token_uri,
            }
        },
        scopes,
    )

    creds = flow.run_local_server(
        port=8081,
        access_type="offline",
        prompt="consent",
    )

    output = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": token_uri,
        "client_id": client_id,
        "client_secret": client_secret,
        "scopes": scopes,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Credentials saved to: {OUTPUT_PATH}")
    print("Use this same file for both Gmail and Calendar.\n")

if __name__ == "__main__":
    main()