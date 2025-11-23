import requests
import pandas as pd
import os
import logging
from requests.auth import HTTPBasicAuth
import schedule
import time
from datetime import datetime
import pytz
import tempfile
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
import base64

LOGGING_LEVEL = logging.INFO
SCHEDULE_INTERVAL_MINUTES = 15
SCHEDULE_CHECK_INTERVAL_SECONDS = 60
OLD_FILE_THRESHOLD_MINUTES = 50
EXCEL_COLUMN_WIDTH_MAX = 30
TEMP_DIR_NAME = "jungschartag_automation"
DEFAULT_PRETIX_URL = "https://tickets.swdec.de"
DEFAULT_NEXTCLOUD_URL = "https://jcloud.swdec.de"
DEFAULT_UPLOAD_DIR = "Jungschartag_Anmeldungen"
DEFAULT_TIMEZONE = "Europe/Berlin"

logging.basicConfig(level=LOGGING_LEVEL)

success_on_last_run = False  # Track if the last run was successful


def get_env(name: str, default: str = "", strip: bool = True) -> str:
    """
    Get environment variable with optional default value.

    Logs a warning when default is used and an error when a required
    environment variable is missing.
    """

    env = os.getenv(name)
    
    if strip is True and env is not None:
        env = env.strip()

    if not env:
        if default:
            logging.info(
                f"Environment Variable '{name}' is not set. Using default '{default}'."
            )
            env = default
        else:
            logging.error(
                f"Environment Variable '{name}' is not set and has no default."
            )

    env = _decode_if_base64(env, prefix="BASE64:")

    return env


def get_secret(filepath: str) -> str:
    """
    Read a docker-style secret from a file and return its contents. Handles base64-encoded secrets if prefixed with "BASE64:".

    Returns an empty string and logs an error on failures.
    """

    try:
        with open(filepath, "r") as f:
            secret = f.read().strip("\n")

    except Exception as e:
        logging.error(f"Error reading secret from {filepath}: {e}")
        secret = ""

    secret = _decode_if_base64(secret, prefix="BASE64:")

    return secret


def _decode_if_base64(data: str, prefix: str) -> str:
    """
    Decode the given data from base64 if it starts with the specified prefix.
    """

    if data.startswith(prefix):
        try:
            b64_content = data[len(prefix) :]
            data = base64.b64decode(b64_content).decode("utf-8").strip("\n")
        except Exception as e:
            logging.error(f"Error decoding base64: {e}")
            data = ""

    return data


class Dataframe:
    last_raw_df = pd.DataFrame()  # global variable to store last fetched raw dataframe

    def __init__(self, success_on_last_run: bool = False):
        """
        Initialize, fetch data from Pretix API and load into different desired dataframes.
        """

        pretix_event, pretix_organizer, pretix_url, pretix_api_token, time_zone = self._get_env_variables()

        self.time_zone = time_zone

        self.pretix_api_url = os.path.join(
            pretix_url, "api/v1/organizers", pretix_organizer, "events", pretix_event
        )
        self.headers = {"Authorization": f"Token {pretix_api_token}"}
        self.raw_df = self._get_raw_df()

        if success_on_last_run and self.__class__.last_raw_df.equals(self.raw_df):
            raise Exception("No changes in data since last fetch.")
        self.__class__.last_raw_df = self.raw_df

        self.sorted_df = self._get_sorted_df()

        self.towns_list = self._get_question_choices_by_text("Ich melde mich über folgende Ortschaft an")
        self.town_dfs = self._get_town_dfs()
        self.numbers_overview = self._get_numbers_df()

    def _get_env_variables(self):
        """
        Get and validate required environment variables.
        """

        pretix_event = get_env("PRETIX_EVENT_SLUG")

        pretix_organizer = get_env("PRETIX_ORGANIZER_SLUG", default="kv-stuttgart")
        pretix_url = get_env("PRETIX_URL", default=DEFAULT_PRETIX_URL)
        if not (pretix_url.startswith("http://") or pretix_url.startswith("https://")):
            pretix_url = f"https://{pretix_url}"

        try:
            pretix_api_token = get_env("PRETIX_API_TOKEN")
        except Exception:
            # get from docker secret file
            pretix_api_token = get_secret("/run/secrets/pretix_api_token")

        time_zone = get_env("TZ", default=DEFAULT_TIMEZONE)

        return pretix_event, pretix_organizer, pretix_url, pretix_api_token, time_zone

    def _get_questions(self) -> dict:
        """
        Fetch all questions from Pretix API and return a mapping of question ID to question text.
        """

        url = f"{self.pretix_api_url}/questions/"
        questions = {}

        while url:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            for q in data["results"]:
                question_text = q["question"].get("de") or next(iter(q["question"].values()))
                questions[q["id"]] = question_text

            url = data["next"]  # Pagination

        return questions

    def _get_question_choices_by_text(self, question_str: str) -> list:
        """
        Given the clear-text question name (question_str), find all question IDs that
        have that visible text and fetch their possible answer options from the
        Pretix API. If multiple question IDs match the same question_str, the
        returned answer options are merged uniquely and this fact is logged.

        Returns a sorted list of unique answer option strings.
        """

        if not question_str.strip():
            logging.error("Empty question text provided to get_question_choices_by_text")
            return []

        # build a fresh mapping of id -> text (ensures up-to-date data)
        question_map = self._get_questions()

        # case-insensitive match on the visible question text
        target = question_str.strip().lower()
        matching_qids = [qid for qid, text in question_map.items() if (text or "").strip().lower() == target]

        if not matching_qids:
            logging.warning(f"No question IDs found for question text '{question_str}'")
            return []

        all_choices = set()

        for qid in matching_qids:
            url = f"{self.pretix_api_url}/questions/{qid}/"
            try:
                resp = requests.get(url, headers=self.headers)
                resp.raise_for_status()
                data = resp.json()

                # pretix may expose options under different keys depending on API version/implementation
                options = []
                for key in ("options", "choices", "answers", "options_list", "question_options"):
                    if key in data and isinstance(data[key], list):
                        options = data[key]
                        break

                # fallback: sometimes the question detail may include nested structures
                if not options:
                    # try to find any list-valued field in response that looks like options
                    for v in data.values():
                        if isinstance(v, list) and v and isinstance(v[0], (str, dict)):
                            options = v
                            break

                # extract a human-readable string from each option entry.
                def _extract_choice_text(opt) -> str | None:
                    # plain string option
                    if isinstance(opt, str):
                        return opt.strip() or None

                    if not isinstance(opt, dict):
                        return None

                    # prefer common direct string keys
                    for k in ("label", "text", "answer", "name", "title", "display"):
                        v = opt.get(k)
                        if isinstance(v, str) and v.strip():
                            return v.strip()

                        # if the value is a translations dict (e.g. {"de": "..."}), prefer German then English
                        if isinstance(v, dict):
                            for lang in ("de", "de-DE", "en", "en-US"):
                                if (
                                    lang in v
                                    and isinstance(v[lang], str)
                                    and v[lang].strip()
                                ):
                                    return v[lang].strip()
                            # fallback to first available string in translations
                            for vv in v.values():
                                if isinstance(vv, str) and vv.strip():
                                    return vv.strip()

                    # if no labelled text found, try to find any string value in the dict
                    for vv in opt.values():
                        if isinstance(vv, str) and vv.strip():
                            return vv.strip()

                    # nothing human-readable found
                    return None

                for opt in options:
                    text_val = _extract_choice_text(opt)
                    if text_val:
                        all_choices.add(text_val)

            except Exception as e:
                logging.error(f"Error fetching choices for question id {qid}: {e}")

        if len(matching_qids) > 1:
            logging.info(f"Multiple question IDs {matching_qids} map to the same question text '{question_str}'. Merged unique choices ({len(all_choices)} unique).")

        return sorted(all_choices)

    def _get_items(self) -> dict:
        """
        Fetch all items from Pretix API and return a mapping of item ID to item name.
        """

        url = f"{self.pretix_api_url}/items/"
        items = {}

        while url:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            for i in data["results"]:
                item_name = i["name"].get("de") or next(iter(i["name"].values()))
                items[i["id"]] = item_name

            url = data["next"]  # Pagination

        return items

    def _get_orders(self) -> list:
        """
        Fetch all orders from Pretix API and return as a list of order dicts.
        """

        url = f"{self.pretix_api_url}/orders/"
        orders = []

        while url:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            orders.extend(data["results"])
            url = data["next"]  # Pagination

        return orders

    def _get_unique_column_name(self, base_name: str, existing_columns: list) -> str:
        """
        Generate a unique column name by adding (#2), (#3), etc. suffix if needed.

        Args:
            base_name: The desired column name
            existing_columns: List of column names that already exist

        Returns:
            Unique column name (either base_name or base_name with " (#n)" suffix)
        """

        if base_name not in existing_columns:
            return base_name

        # Find the next available number
        counter = 2
        while f"{base_name} (#{counter})" in existing_columns:
            counter += 1

        return f"{base_name} (#{counter})"

    def _orders_to_dataframe(self, orders: list, question_map: dict, item_map: dict) -> pd.DataFrame:
        """
        Convert orders to a pandas DataFrame, resolving question and item names.
        """

        rows = []
        # Track which question texts we've already seen to handle duplicate question texts
        question_text_mapping = {}  # maps original qtext -> unique column name

        for order in orders:
            invoice = order.get("invoice_address", {}) or {}
            order_info = {
                "order_code": order["code"],
                "status": order["status"],
                "email": order["email"],
                "total": order["total"],
                "date": order["datetime"],
                "invoice_name": invoice.get("name", ""),
                "invoice_company": invoice.get("company", ""),
                "invoice_street": invoice.get("street", ""),
                "invoice_zipcode": invoice.get("zipcode", ""),
                "invoice_city": invoice.get("city", ""),
                "invoice_country": invoice.get("country", ""),
                "invoice_vat_id": invoice.get("vat_id", ""),
            }

            for position in order["positions"]:
                # resolve item id
                item_id = position.get("item")
                if isinstance(item_id, dict):
                    item_name = item_id["name"].get("de") or next(
                        iter(item_id["name"].values())
                    )
                else:
                    item_name = item_map.get(item_id, f"Item {item_id}")

                # attendee name from attendee_name_parts
                name_parts = position.get("attendee_name_parts", {}) or {}
                attendee_firstname = name_parts.get("given_name", "")
                attendee_lastname = name_parts.get("family_name", "")

                pos_info = {
                    "position_id": position["id"],
                    "item_id": item_id
                    if not isinstance(item_id, dict)
                    else item_id["id"],
                    "item_name": item_name,
                    "price": position["price"],
                    "attendee_firstname": attendee_firstname,
                    "attendee_lastname": attendee_lastname
                }

                # answers for questions
                questions = {}
                for answer in position.get("answers", []):
                    qid = answer.get("question")
                    answer_text = answer.get("answer", "")

                    if isinstance(qid, dict):
                        qid = qid["id"]

                    qtext = question_map.get(qid, f"question_{qid}")

                    # Generate unique column name for this question text
                    if qtext not in question_text_mapping:
                        question_text_mapping[qtext] = self._get_unique_column_name(qtext, list(question_text_mapping.values()))

                    unique_col_name = question_text_mapping[qtext]
                    questions[unique_col_name] = answer_text

                row = {**order_info, **pos_info, **questions}
                rows.append(row)

        df = pd.DataFrame(rows)

        # Ensure all questions from Pretix are present as columns even if
        # no attendee has answered them yet. Handle duplicate question texts with (#2), (#3), etc.
        for qid, qtext in question_map.items():
            # Generate unique column name for this question text
            if qtext not in question_text_mapping:
                unique_col_name = self._get_unique_column_name(
                    qtext, list(question_text_mapping.values()) + list(df.columns)
                )
                question_text_mapping[qtext] = unique_col_name
            else:
                unique_col_name = question_text_mapping[qtext]

            # Add empty column if it doesn't exist yet
            if unique_col_name not in df.columns:
                df[unique_col_name] = ""

        return df

    def _get_raw_df(self) -> pd.DataFrame:
        """
        Fetch raw data from Pretix API and return as a pandas DataFrame.
        """

        question_map = self._get_questions()
        item_map = self._get_items()
        orders = self._get_orders()
        df = self._orders_to_dataframe(orders, question_map, item_map)

        logging.info("Fetched raw data from Pretix API.")

        return df

    def _get_sorted_df(self) -> pd.DataFrame:
        """
        Process raw dataframe to create a sorted dataframe with required columns.
        """

        df = self.raw_df

        # removed all cancelled registrations
        df = df[df["status"] != "c"]

        # rename needed columns
        renames = {
            "order_code": "Bestellnummer",
            "email": "E-Mail",
            "date": "Anmeldedatum",
            "item_name": "Art",
            "attendee_firstname": "Vorname",
            "attendee_lastname": "Nachname",
            "Essensunverträglichkeiten": "Essensunverträglichkeiten Ja/Nein",
            "Welche Unverträglichkeiten?": "Essensunverträglichkeiten",
            "Ich melde mich über folgende Ortschaft an": "Ortschaft",
            "Ich biete eine Fahrgemeinschaft an": "Fahrer Angebot",
            "Ich bin Ortsverantwortlicher.": "Ortsverantwortlicher",
            "Telefonnummer": "Telefonnummer Mitarbeiter"
        }
        df = df.rename(columns=renames)

        # combine "Telefonnummer der Eltern" and "Telefonnummer Mitarbeiter" to one column "Telefonnummer"
        df["Telefonnummer"] = df["Telefonnummer der Eltern"].combine_first(df["Telefonnummer Mitarbeiter"])

        # filter for columns and set their order
        wanted_columns = [
            "Ortschaft",
            "Art",
            "Nachname",
            "Vorname",
            "Telefonnummer",
            "E-Mail",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Sonstiges",
            "Ortsverantwortlicher",
            "Fahrer Angebot",
            "Anmeldedatum",
            "Bestellnummer"
        ]
        df = df.filter(wanted_columns)

        # change date format
        df["Anmeldedatum"] = (pd.to_datetime(df["Anmeldedatum"], utc=True).dt.tz_convert(self.time_zone).dt.strftime("%Y-%m-%d %H:%M"))

        # sort (by "Ortschaft", then by "Art", then by "Nachname" and then by "Vorname") and reset index numbers
        df = df.sort_values(by=["Ortschaft", "Art", "Nachname", "Vorname"], ascending=True)
        df.index = range(1, len(df) + 1)

        logging.info("Sorted raw data.")

        return df

    def _get_town_dfs(self) -> dict[str, pd.DataFrame]:
        """
        Process sorted dataframe to create a dictionary of dataframes filtered by town.
        """

        df = self.sorted_df

        # filter for columns and set their order
        wanted_columns = [
            "Ortschaft",
            "Art",
            "Nachname",
            "Vorname",
            "Telefonnummer",
            "E-Mail",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Sonstiges",
            "Fahrer Angebot",
            "Anmeldedatum"
        ]
        df = df.filter(wanted_columns)

        # sort by town:
        df_by_town_dict = {}
        for town in self.towns_list:
            # filter by town, drop column "Ortschaft" and reset index numbers
            town_df = df[df["Ortschaft"] == town]
            town_df = town_df.drop(columns=["Ortschaft"])
            town_df.index = range(1, len(town_df) + 1)

            df_by_town_dict[town] = town_df

        logging.info("Filtered sorted data by town.")

        return df_by_town_dict

    def _get_numbers_df(self) -> pd.DataFrame:
        """
        Calculate and return a dataframe with counts of "Jungscharler", "Mitarbeiter", and total by town.
        """

        numbers_df = pd.DataFrame(
            {"Ortschaft": [], "Jungscharler": [], "Mitarbeiter": [], "Gesamt": []}
        )

        # make Ortschaft the index
        numbers_df.set_index("Ortschaft", inplace=True)

        df = self.sorted_df

        number_of_kids = len(df[df["Art"].str.contains("Jungscharler", na=False)])
        number_of_staff = len(df[df["Art"].str.contains("Mitarbeiter", na=False)])
        number_total = number_of_kids + number_of_staff

        # add row to numbers_df
        numbers_df.loc["GESAMT"] = [number_of_kids, number_of_staff, number_total]

        # filter by town:
        for town in self.towns_list:
            town_df = df[df["Ortschaft"] == town]

            town_kids = len(
                town_df[town_df["Art"].str.contains("Jungscharler", na=False)]
            )
            town_staff = len(
                town_df[town_df["Art"].str.contains("Mitarbeiter", na=False)]
            )
            town_total = town_kids + town_staff

            # add row to numbers_df
            numbers_df.loc[town] = [town_kids, town_staff, town_total]

        return numbers_df


class Excel:
    def __init__(self):
        """
        Initialize the Excel helper class, setting up a temporary directory for Excel files.
        """

        self.temp_dir = os.path.join(tempfile.gettempdir(), TEMP_DIR_NAME)
        Path(self.temp_dir).mkdir(parents=True, exist_ok=True)
        
    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize the filename by replacing or removing invalid characters.
        """
        filename = filename.strip()
        filename = filename.replace('\\n', ' ')
        filename = filename.replace('\\r', ' ')
        filename = filename.replace('\\t', ' ')
        filename = filename.replace('<', '_')
        filename = filename.replace('>', '_')
        filename = filename.replace(':', '')
        filename = filename.replace('"', '')
        filename = filename.replace('/', '+')
        filename = filename.replace('\\', '_')
        filename = filename.replace('|', '_')
        filename = filename.replace('?', '')
        filename = filename.replace('*', '')
    
        return filename

    def save_to_excel(self, df: pd.DataFrame, filename: str) -> str:
        """
        Save the given DataFrame to an Excel file with the specified filename in the temporary directory.
        Returns the path to the saved Excel file.
        """
        
        filename = self._sanitize_filename(filename)

        if not filename.endswith(".xlsx"):
            filename += ".xlsx"

        sheet_name = filename.removesuffix(".xlsx")

        path = os.path.join(self.temp_dir, filename)

        # write dataframe to excel
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=True, freeze_panes=(1, 1))
            worksheet = writer.sheets[sheet_name]

            # adjust column widths including index column and header
            for idx, col in enumerate(df.columns, start=2):  # start=2 because index is in column 1
                max_length = max(
                    len(str(col)),  # header length
                    df[col].astype(str).map(len).max()  # data length
                )
                length = min(max_length + 2, EXCEL_COLUMN_WIDTH_MAX)  # cap the width via constant
                
                worksheet.column_dimensions[worksheet.cell(row=1, column=idx).column_letter].width = length

            # adjust index column width
            max_index_length = max(
                len(str(df.index.name or "")),  # index name length
                df.index.astype(str).map(len).max()  # index data length
            )
            worksheet.column_dimensions["A"].width = max_index_length + 2

        logging.info(f"Created Excel file '{path}'.")

        return path

    def add_filter(self, path_to_excel_file: str) -> str:
        """
        Add filtering function to an existing Excel file.
        """

        path = path_to_excel_file

        if not os.path.isfile(path):
            logging.warning(f"Cannot add filter to '{path}': No such file.")
            return path
        
        elif not path.endswith(".xlsx"):
            logging.warning(f"Cannot add filter to '{path}': File is not an excel file.")
            return path

        wb = load_workbook(path)
        ws = wb.active

        # Determine the range of the data
        min_col = 2  # assuming index is in column A
        min_row = 1
        max_col = ws.max_column
        max_row = ws.max_row
        table_range = f"{ws.cell(row=min_row, column=min_col).coordinate}:{ws.cell(row=max_row, column=max_col).coordinate}"

        # Create a table
        tab = Table(displayName="Table1", ref=table_range)

        # Add a default style with striped rows and banded columns
        style = TableStyleInfo(
            name="TableStyleMedium4",
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False
        )
        tab.tableStyleInfo = style

        # Add the table to the worksheet
        ws.add_table(tab)

        wb.save(path)

        logging.info(f"Added filter to Excel file '{path}'.")

        return path

    def delete_excel(self, path_to_excel_file: str) -> None:
        """
        Delete a temporary Excel file.
        """
        try:
            if os.path.isfile(path_to_excel_file):
                os.remove(path_to_excel_file)
                logging.info(f"Deleted temporary Excel file '{path_to_excel_file}'.")
        except Exception as e:
            logging.error(f"Error deleting file '{path_to_excel_file}': {e}")


class Nextcloud:
    def __init__(self):
        """
        Initialize Nextcloud connection with environment variables.
        """
        nextcloud_url, username, password, upload_dir, time_zone = self._get_env_variables()

        self.username = username
        self.password = password
        self.upload_dir = upload_dir
        self.time_zone = time_zone
        
        self.base_url = os.path.join(nextcloud_url, "remote.php/dav/files", self.username)
        
        self.upload_dir_url = os.path.join(self.base_url, self.upload_dir)

    def _get_env_variables(self) -> tuple:
        """
        Get and validate required environment variables.
        """

        nextcloud_url = get_env("NEXTCLOUD_URL", default=DEFAULT_NEXTCLOUD_URL)
        if not nextcloud_url.startswith("http://") and not nextcloud_url.startswith("https://"):
            nextcloud_url = f"https://{nextcloud_url}"

        try:
            username = get_env("NEXTCLOUD_USERNAME")
        except Exception:
            username = get_secret("/run/secrets/nextcloud_username")
        
        try:
            password = get_env("NEXTCLOUD_PASSWORD", strip=False)
        except Exception:
            password = get_secret("/run/secrets/nextcloud_password")

        upload_dir = get_env("NEXTCLOUD_UPLOAD_DIR", default=DEFAULT_UPLOAD_DIR)
        upload_dir = upload_dir.strip("/\\")

        time_zone = get_env("TZ", default=DEFAULT_TIMEZONE)

        return nextcloud_url, username, password, upload_dir, time_zone
    
    def _get_parent_directories(self, path: str) -> list[str]:
        """
        Get all parent directories of a given path.
        e.g. for path "A/B/C" it returns ["A", "A/B", "A/B/C"]
        """
        
        # ensure that the path does not end with a slash
        path = path.rstrip(os.sep)
        
        parent_dirs = []
        
        # walk through the path and build the parent directories
        while path != os.path.dirname(path):
            parent_dirs.append(path)
            path = os.path.dirname(path)

        # reverse the list to have the directories in hierarchical order from top to bottom.
        return parent_dirs[::-1]

    def create_upload_directory(self) -> None:
        """
        Create the upload directory on Nextcloud if it does not exist.
        """
        
        # check if directory exists:
        r = requests.request(
            method="PROPFIND",
            url=self.upload_dir_url,
            auth=HTTPBasicAuth(self.username, self.password)
        )
        
        if r.status_code == 207:
            logging.info(f"Upload directory already exists ('{self.upload_dir_url}')")
            return
        
        # ceate directory
        try:
            for dir in self._get_parent_directories(self.upload_dir):
                
                r = requests.request(
                    method="MKCOL",
                    url=os.path.join(self.base_url, dir),
                    auth=HTTPBasicAuth(self.username, self.password)
                )
                
                if r.status_code not in [201, 405]:
                    logging.error(f"Error creating upload directory: {r.status_code} - {r.text}")
                    return
        
            logging.info(f"Upload directory created ('{self.upload_dir_url}')")
            
        except Exception as e:
            logging.error(f"Error creating upload directory: {e}")

    def _upload_file(self, filename: str, data: bytes) -> None:
        """
        Upload a file to Nextcloud via WebDAV.
        """

        try:
            r = requests.put(
                url=os.path.join(self.upload_dir_url, filename),
                data=data,
                auth=HTTPBasicAuth(self.username, self.password)
            )
            
            if r.status_code in (200, 201, 204):
                logging.info(f"File '{filename}' uploaded successfully.")
            else:
                logging.error(f"Error uploading file '{filename}': {r.status_code} - {r.text}")
                
        except Exception as e:
            logging.error(f"Network error uploading file '{filename}': {e}")

    def upload_excel(self, source_file: str) -> None:
        """
        Upload an Excel file to Nextcloud.
        """

        try:
            filename = os.path.basename(source_file)
            with open(source_file, "rb") as f:
                data = f.read()
            self._upload_file(filename, data)
        except Exception as e:
            logging.error(f"Error uploading Excel file '{source_file}': {e}")

    def upload_last_updated(self) -> None:
        """
        Upload a timestamp file indicating the last update time.
        """

        filename = "Last_Updated.txt"

        berlin_tz = pytz.timezone("Europe/Berlin")
        data = datetime.now(tz=berlin_tz).strftime("%d.%m.%Y %H:%M").encode("utf-8")

        self._upload_file(filename, data)


def main():
    """
    Main function to generate Excel files and upload them to Nextcloud.
    """

    global success_on_last_run

    try:
        dataframe = Dataframe(success_on_last_run)
        excel = Excel()
        nc = Nextcloud()

        nc.create_upload_directory()

        # generate, upload and delete raw data file
        filepath = excel.save_to_excel(dataframe.raw_df, "Raw_Data")
        nc.upload_excel(filepath)
        excel.delete_excel(filepath)

        # generate, upload and delete all attendees file
        filepath = excel.save_to_excel(dataframe.sorted_df, "Alle")
        filepath = excel.add_filter(filepath)
        nc.upload_excel(filepath)
        excel.delete_excel(filepath)

        # generate, upload and delete town-wise attendees files
        for town in dataframe.town_dfs:
            df = dataframe.town_dfs[town]

            filepath = excel.save_to_excel(df, town)
            nc.upload_excel(filepath)
            excel.delete_excel(filepath)

        # generate, upload and delete all numbers_overview file
        filepath = excel.save_to_excel(dataframe.numbers_overview, "Anmeldezahlen")
        nc.upload_excel(filepath)
        excel.delete_excel(filepath)

        nc.upload_last_updated()

        success_on_last_run = True

    except Exception as e:
        if str(e) == "No changes in data since last fetch.":
            logging.info("No changes detected since last fetch. Skipping upload.")
            try:
                nc = Nextcloud()
                nc.upload_last_updated()
            except Exception as e:
                logging.error(f"An error occurred while uploading last updated timestamp: {e}")

        else:
            logging.error(f"An error occurred during execution: {e}")
            success_on_last_run = False


if __name__ == "__main__":
    # Run main immediately on startup
    main()

    if get_env("RUN_ONCE", default="false").lower().strip() == "true":
        logging.info("RUN_ONCE is set to true. Exiting now after single run.")
        exit(0)

    # Schedule main to run at configured intervals
    schedule.every(SCHEDULE_INTERVAL_MINUTES).minutes.do(main)

    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(SCHEDULE_CHECK_INTERVAL_SECONDS)  # Check periodically if a task needs to run
