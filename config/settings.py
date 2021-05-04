import os

from pathlib import Path
from dotenv import load_dotenv

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path, verbose=True)

# SSO Service
SERVICE_SSO_DOMAIN = os.getenv("SERVICE_SSO_DOMAIN")
SERVICE_SSO_SECRET_TOKEN = os.getenv("SERVICE_SSO_SECRET_TOKEN")
SERVICE_SSO_SERVICE_TOKEN = os.getenv("SERVICE_SSO_SERVICE_TOKEN")
SERVICE_SSO_SECRET_KEY = os.getenv("SERVICE_SSO_SECRET_KEY")

# Attachment
SERVICE_ATTACHMENT_HOST = os.getenv("SERVICE_ATTACHMENT_HOST")
SERVICE_ATTACHMENT_SECRET_KEY = os.getenv("SERVICE_ATTACHMENT_SECRET_KEY")
SERVICE_ATTACHMENT_SECRET_TOKEN = os.getenv("SERVICE_ATTACHMENT_SECRET_TOKEN")

# Notification
SERVICE_NOTIFICATION_HOST = os.getenv("SERVICE_NOTIFICATION_HOST")
SERVICE_NOTIFICATION_SECRET_KEY = os.getenv("SERVICE_NOTIFICATION_SECRET_KEY")
