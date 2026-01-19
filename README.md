# Pretix2Nextcloud

<br>

<img src="logo.png" width="1000"/>

<br>

This tool retrieves registration data for a specific event from Pretix, converts it to the desired Excel format and uploads it to Nextcloud. This project was developed for the SWD-EC, so the Docker images are not universally applicable. However, the Python code can be used as a reference implementation for similar use cases.

For the file uploads, a Nextcloud instance is not required. Any storage server that supports WebDAV can be used.

<br>

## Setting up Pretix2Nextcloud via Docker

### Step 1:
Install Docker from <a href="https://docs.docker.com/get-docker/" target="_blank">https://docs.docker.com/get-docker/</a>

<br>

### Step 2:
Create a new file called `.env` and store your environment variables there like this:

    PRETIX_EVENT_SLUG=pretix-event-slug
    PRETIX_API_TOKEN=zkv2p7eja7j8axbe77d3ye85wgyruofcmuphf7gjngbdgepttsejmmpwrgyezdbs
    NEXTCLOUD_UPLOAD_DIR=/path/in/nextcloud/
    NEXTCLOUD_USERNAME=nextcloud-user
    NEXTCLOUD_PASSWORD=MySecurePassword123!
    

> [!NOTE]  
> `NEXTCLOUD_PASSWORD` takes leading and trailing whitespaces into account. Make sure there are none if your password doesn't contain them.

<br>

#### _Base64 encoding (optionally):_
If you want to encode your environment variables with base64, you need to use the prefix `BASE64:` like this:

    NEXTCLOUD_PASSWORD=BASE64:TXlTZWN1cmVQYXNzd29yZDEyMyE=

> [!WARNING]  
> Base64 is not a form of encryption and provides no meaningful security. It merely obscures the value to avoid storing secrets in plain text, reducing the chance that someone running the Docker image will casually see them.

<br>

#### _More variables (optionally):_
You can also add following optional environment variables if you need to customize the behavior apart from the defaults (_normally not necessary_):

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

#### For SWDEC - KV Stuttgart - Jungschartag:

    docker run -d \
    --name p2n-swdec-kvstuttgart-jungschartag \
    --restart unless-stopped \
    --env-file .env \
    ghcr.io/tben2000/p2n-swdec-kvstuttgart-jungschartag:latest

#### For SWDEC - KV Stuttgart - Teencamp:

    docker run -d \
    --name p2n-swdec-kvstuttgart-teencamp \
    --restart unless-stopped \
    --env-file .env \
    ghcr.io/tben2000/p2n-swdec-kvstuttgart-teencamp:latest

#### For SWDEC - KV Stuttgart - Zeltlager Jungs:

    docker run -d \
    --name p2n-swdec-kvstuttgart-jungslager \
    --restart unless-stopped \
    --env-file .env \
    ghcr.io/tben2000/p2n-swdec-kvstuttgart-jungslager:latest

#### For SWDEC - KV Stuttgart - Zeltlager Mädels:

    docker run -d \
    --name p2n-swdec-kvstuttgart-maedelslager \
    --restart unless-stopped \
    --env-file .env \
    ghcr.io/tben2000/p2n-swdec-kvstuttgart-maedelslager:latest

<br>

> These commands are structured as multiline commands. You can copy the whole command and paste it to your command line even though it contains multiple lines.

> [!TIP]
> If you want to use <a href="https://docs.docker.com/compose/" target="_blank">**Docker Compose**</a> instead of Docker, you can use this <a href="https://it-tools.tech/docker-run-to-docker-compose-converter" target="_blank">Converter</a> to convert the above commands. Paste the output to a `docker-compose.yaml` file and run `docker compose up -d`.

<br>

### Step 4:
Delete `.env` file after starting the container to avoid leaking sensitive data.

<br>

### Alternative for advanced users:
If you're running Docker Swarm and want to use <a href="https://docs.docker.com/engine/swarm/secrets/" target="_blank">**Docker secrets**</a> instead of environment variables for sensitive data like API tokens and passwords, you can set your secrets and hand over their names in following environment variables:

- `NEXTCLOUD_USERNAME_SECRET_NAME` instead of `NEXTCLOUD_USERNAME`
- `NEXTCLOUD_PASSWORD_SECRET_NAME` instead of `NEXTCLOUD_PASSWORD`
- `PRETIX_API_TOKEN_SECRET_NAME` instead of `PRETIX_API_TOKEN`

The same base64 possibility applies to the content of the secret files.
