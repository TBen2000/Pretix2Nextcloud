# Pretix2Nextcloud
This tool retrieves registration data for a specific event from Pretix, compiles it into the desired Excel formats, and uploads it to Nextcloud. This project was programmed for the KV-Stuttgart of the SWD-EC and is therefore not universally applicable.

## Setting up Pretix2Nextcloud via Docker

### Step 1:
Install Docker from https://docs.docker.com/get-docker/

<br>

### Step 2:
Create a new file called `.env` and store your environment variables there like this:

    PRETIX_EVENT_SLUG=your_event_slug
    PRETIX_ORGANIZER_SLUG=your_organizer_slug
    PRETIX_URL=https://pretix.yourdomain.com
    NEXTCLOUD_URL=https://nextcloud.yourdomain.com
    NEXTCLOUD_UPLOAD_DIR=/path/in/nextcloud/
    TIMEZONE=Europe/Berlin
    RUN_ONCE=false
    NEXTCLOUD_USERNAME=nextcloud_user
    NEXTCLOUD_PASSWORD=MySecurePassword123!
    PRETIX_API_TOKEN=zkv2p7eja7j8axbe77d3ye85wgyruofcmuphf7gjngbdgepttsejmmpwrgyezdbs

> [!NOTE]  
> NEXTCLOUD_PASSWORD takes leading and trailing whitespaces into account. Make sure there are none if your password doesn't contain them.

<br>

If you want to encode your environment variables with base64, you need to use the prefix `BASE64:` like e.g. 

    NEXTCLOUD_PASSWORD=BASE64:TXlTZWN1cmVQYXNzd29yZDEyMyE=

> [!WARNING]  
> Base64 is not a form of encryption and provides no meaningful security. It merely obscures the value to avoid storing secrets in plain text, reducing the chance that someone running the Docker image will casually see them.

<br>

### Step 3:
Run desired docker image with following command:

    docker run --name p2n-kv-stuttgart-jungschartag -d --restart unless-stopped --env-file .env ghcr.io/tben2000/pretix2nextcloud-kv-stuttgart-jungschartag:latest

<br>

### Step 4:
Delete `.env` file after starting the container to avoid leaking sensitive data.

<br>

### Alternative for advandced users:
If you're running Docker Swarm and want to use docker secrets instead of environment variables for sensitive data like API tokens and passwords, use following names for the secrets:

- `nextcloud_username` instead of environment variable `NEXTCLOUD_USERNAME`
- `nextcloud_password` instead of environment variable `NEXTCLOUD_PASSWORD`
- `pretix_api_token` instead of environment variable `PRETIX_API_TOKEN`

The same base64 possibility applies to the content of the secret files.
