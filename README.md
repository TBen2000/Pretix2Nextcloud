# Pretix2Nextcloud

This tool retrieves registration data for a specific event from Pretix, compiles it into the desired Excel formats, and uploads it to Nextcloud. This project was programmed for the KV-Stuttgart of the SWD-EC and is therefore not universally applicable.

## Setting up Pretix2Nextcloud via Docker

### Step 1:
Install Docker from https://docs.docker.com/get-docker/

<br>

### Step 2:
Create a new file called `.env` and store your environment variables there like this:

    PRETIX_EVENT_SLUG=your_event_slug
    PRETIX_API_TOKEN=zkv2p7eja7j8axbe77d3ye85wgyruofcmuphf7gjngbdgepttsejmmpwrgyezdbs
    NEXTCLOUD_UPLOAD_DIR=/path/in/nextcloud/
    NEXTCLOUD_USERNAME=nextcloud_user
    NEXTCLOUD_PASSWORD=MySecurePassword123!
    

> [!NOTE]  
> NEXTCLOUD_PASSWORD takes leading and trailing whitespaces into account. Make sure there are none if your password doesn't contain them.

<br>

If you want to encode your environment variables with base64, you need to use the prefix `BASE64:` like e.g. 

    NEXTCLOUD_PASSWORD=BASE64:TXlTZWN1cmVQYXNzd29yZDEyMyE=

> [!WARNING]  
> Base64 is not a form of encryption and provides no meaningful security. It merely obscures the value to avoid storing secrets in plain text, reducing the chance that someone running the Docker image will casually see them.

<br>

You can also add following optional environment variables if you need to customize the behavior apart from the defaults (normally not necessary):

| environment variable    | default value              |
| ----------------------- | -------------------------- |
| `PRETIX_ORGANIZER_SLUG` | `kv-stuttgart`             |
| `PRETIX_URL`            | `https://tickets.swdec.de` |
| `NEXTCLOUD_URL`         | `https://jcloud.swdec.de`  |
| `TZ`                    | `Europe/Berlin`            |
| `RUN_ONCE`              | `false`                    |
| `INTERVAL_MINUTES`      | `15`                       |
| `CHECK_INTERVAL_SECONDS`| `60`                       |
| `LOGGING_LEVEL`         | `INFO`                     |

`INTERVAL_MINUTES` defines how long the tool waits between two runs. `CHECK_INTERVAL_SECONDS` defines how often the tool checks if it's time to run again. Both are only relevant if `RUN_ONCE` is set to `false`.

`LOGGING_LEVEL` can be set to one of the following values: `DEBUG`, `INFO`, `WARNING`, `ERROR`.

<br>

### Step 3:
Run the desired Docker image with one of the following commands:

#### For KV Stuttgart Jungschartag:

    docker run --name p2n-kv-stuttgart-jungschartag -d --restart unless-stopped --env-file .env ghcr.io/tben2000/pretix2nextcloud-kv-stuttgart-jungschartag:latest

#### For KV Stuttgart Teencamp:

    docker run --name p2n-kv-stuttgart-teencamp -d --restart unless-stopped --env-file .env ghcr.io/tben2000/pretix2nextcloud-kv-stuttgart-teencamp:latest

#### For KV Stuttgart Zeltlager Jungs:

    docker run --name p2n-kv-stuttgart-zeltlager-jungs -d --restart unless-stopped --env-file .env ghcr.io/tben2000/pretix2nextcloud-kv-stuttgart-zeltlager-jungs:latest

#### For KV Stuttgart Zeltlager Mädels:

    docker run --name p2n-kv-stuttgart-zeltlager-maedels -d --restart unless-stopped --env-file .env ghcr.io/tben2000/pretix2nextcloud-kv-stuttgart-zeltlager-maedels:latest

<br>

### Step 4:
Delete `.env` file after starting the container to avoid leaking sensitive data.

<br>

### Alternative for advandced users:
If you're running Docker Swarm and want to use Docker secrets instead of environment variables for sensitive data like API tokens and passwords, you can set your secrets and hand over their names in following environment variables:

- `NEXTCLOUD_USERNAME_SECRET_NAME` instead of `NEXTCLOUD_USERNAME`
- `NEXTCLOUD_PASSWORD_SECRET_NAME` instead of `NEXTCLOUD_PASSWORD`
- `PRETIX_API_TOKEN_SECRET_NAME` instead of `PRETIX_API_TOKEN`

The same base64 possibility applies to the content of the secret files.
