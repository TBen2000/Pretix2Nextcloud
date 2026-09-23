# Customized Pretix2Nextcloud instance for kv-stuttgart-kv-freizeit
# Needs P2N.py to function

import pandas as pd
import logging
import sys
import os

# Add current directory
sys.path.append(os.path.dirname(__file__))
# Add parent directory
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# import P2N.py
try:
    from P2N import Main, Environment, PretixAPI
except ImportError:
    raise ImportError("Could not import P2N.py from current or parent directory.")


Environment().set_defaults(
    default_pretix_url="https://tickets.swdec.de",
    default_pretix_organizer_slug="kv-stuttgart",
    default_excel_max_column_width=30,
    default_nextcloud_url="https://jcloud.swdec.de",
    default_nextcloud_upload_dir="Anmeldungen_KV-Freizeit",
    default_timezone="Europe/Berlin",
    default_interval_minutes=15,
    default_check_interval_seconds=60,
    default_run_once=False,
    default_logging_level="INFO",
)


class Dataframe:
    last_raw_df = pd.DataFrame()  # global variable to store last fetched raw dataframe

    def __init__(self, success_on_last_run: bool = False):
        """
        Initialize, fetch data from Pretix API and load into different desired dataframes.
        """
        pretix = PretixAPI()
        env = Environment()

        self.time_zone = env.get_timezone()

        self.raw_df = pretix.get_raw_df()
        
        # check for new fetched data and raise exception if no new data occured so that Main can skip this run
        pretix.check_for_new_fetched_data(self.raw_df, success_on_last_run)
        
        self.towns_list = pretix.get_answer_choices_from_question("EC-Jugendarbeit")
        self.towns_list = [town.replace("EC-Jugendarbeit ", "") for town in self.towns_list]
        self.towns_list.remove("sonstige Jugendarbeit")
        self.towns_list.append("Sonstige")

        self.debloated_df = self._get_debloated_df()
        self.attendees_df = self._get_attendees_df()
        self.town_dfs = self._get_town_dfs()
        self.numbers_overview = self._get_numbers_df()
        self.orders_df = self._get_orders_df()


    def _get_debloated_df(self) -> pd.DataFrame:
        """
        Process raw dataframe to create a sorted dataframe with all possibly required columns.
        This debloated dataframe acts as a starting point for all following dataframe processing.
        """
        
        df = self.raw_df.copy()
        
        # rename needed columns
        renames = {
            "order_code": "Bestellnummer",
            "status": "Bestellstatus",
            "email": "E-Mail",
            "total": "Gesamtpreis",
            "date": "Anmeldedatum",
            "item_name": "Art",
            "price": "Preis",
            "attendee_firstname": "Vorname",
            "attendee_lastname": "Nachname",
            "Anrede": "Geschlecht",
            "Spitzname fürs Namensschild": "Spitzname",
            "Massenlager (wir versuchen alle Wünsche zu berücksichtigen)": "Massenlager",
            "Bist du Vegetarier?": "Vegetarier",
            "Das möchte ich noch mitteilen:": "Sonstiges",
            "Für alle U18 jährigen: Einverständniserklärung ausfüllen und bei der Anmeldung am Freitag Abend vorzeigen.": "Einverständniserklärung",
        }
        df = df.rename(columns=renames)
        

        # combine "Straße", "Hausnummer", "Postleitzahl" and "Ort" to "Adresse"
        df["Adresse"] = (
            df["Straße"].fillna("").str.strip() + " " +
            df["Hausnummer"].fillna("").str.strip() + ", " +
            df["Postleitzahl"].fillna("").str.strip() + " " +
            df["Ort"].fillna("").str.strip()
        )
        
        
        # simplify values in column "Anrede"
        # rename all values "Herr" to "männlich" and all values "Frau" to "weiblich":
        df["Anrede"] = df["Anrede"].replace(
            {
                "Herr": "männlich",
                "Frau": "weiblich",
            }
        )
        
        # simplify values in column "Einverständniserklärung"
        # rename all values that conatain any text to "hochgeladen":
        df.loc[
            df["Einverständniserklärung"].notna() & 
            (df["Einverständniserklärung"].astype(str).str.strip() != ""),
            "Einverständniserklärung"
        ] = "hochgeladen"
       
        
        # rename values in "Bestellstatus" from acronyms to the complete meaning
        # rename values "c" to "storniert", "n" to "unbezahlt" und "p" to "bezahlt"
        df["Bestellstatus"] = df["Bestellstatus"].replace(
            {
                "c": "storniert",
                "n": "unbezahlt",
                "p": "bezahlt",
            }
        )

        # change date format
        df["Anmeldedatum"] = (
            pd.to_datetime(df["Anmeldedatum"], utc=True)
            .dt.tz_convert(self.time_zone)
            .dt.strftime("%Y-%m-%d %H:%M")
        )

        # change all values "sonstige Jugendarbeit" in column "EC-Jugendarbeit" to "Sonstige"
        df["EC-Jugendarbeit"] = df["EC-Jugendarbeit"].replace("sonstige Jugendarbeit", "Sonstige")

        # remove all substrings "EC-Jugendarbeit " from all values in column "EC-Jugendarbeit"
        df["EC-Jugendarbeit"] = df["EC-Jugendarbeit"].str.replace("EC-Jugendarbeit ", "", regex=False)

        # filter for columns and set their order
        wanted_columns = [
            "Bestellnummer",
            "Bestellstatus",
            "E-Mail",
            "Gesamtpreis",
            "Anmeldedatum",
            "Adresse",
            "Art",
            "Preis",
            "Vorname",
            "Nachname",
            "Spitzname",
            "Geburtsdatum",
            "Geschlecht",
            "EC-Jugendarbeit",
            "Massenlager",
            "Vegetarier",
            "Sonstiges",
            "Einverständniserklärung",
        ]
        df = df.filter(wanted_columns)

        
        # strip leading/trailing whitespace from all string values in df
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        df[str_cols] = (
            df[str_cols]
            .apply(lambda col: col.str.strip())
            .replace("", pd.NA)
        )
        
        logging.info("Removed bloat from raw data.")
        
        return df
        
        
    def _get_attendees_df(self) -> pd.DataFrame:
        """
        Process debloated dataframe to create a sorted dataframe for attendees with required columns.
        """

        df = self.debloated_df.copy()

        # removed all cancelled registrations
        df = df[df["Bestellstatus"] != "storniert"]

        # filter for columns and set their order
        wanted_columns = [
            "Nachname",
            "Vorname",
            "Spitzname",
            "Geburtsdatum",
            "Geschlecht",
            "EC-Jugendarbeit",
            "Sonstiges",
            "Massenlager",
            "Vegetarier",
            "Einverständniserklärung",
            "E-Mail",
            "Adresse",
            "Bestellnummer",
            "Anmeldedatum",
            "Art",
        ]
        df = df.filter(wanted_columns)

        # sort (by "Nachname" and then by "Vorname") and reset index numbers
        df = df.sort_values(
            by=["Nachname", "Vorname"], ascending=True
        )
        df.index = range(1, len(df) + 1)

        logging.info("Sorted debloated data into attendees data.")

        return df

    def _get_town_dfs(self) -> dict[str, pd.DataFrame]:
        """
        Process sorted dataframe for attendees to create a dictionary of dataframes filtered by town.
        """

        df = self.attendees_df.copy()

        # filter for columns and set their order
        wanted_columns = [
            "Nachname",
            "Vorname",
            "Geburtsdatum",
            "Geschlecht",
            "EC-Jugendarbeit",
            "Sonstiges",
            "Massenlager",
            "Vegetarier",
            "Einverständniserklärung",
            "E-Mail",
            "Adresse",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)
        
        # sort (by "Nachname" and then by "Vorname")
        df = df.sort_values(
            by=["Nachname", "Vorname"], ascending=True
        )

        # sort by town:
        df_by_town_dict = {}
        df_towns = (df["EC-Jugendarbeit"].dropna().astype(str).str.strip().unique())
        towns = sorted(set(self.towns_list) | set(df_towns))
        for town in towns:
            # filter by town, drop column "EC-Jugendarbeit" and reset index numbers
            town_df = df[df["EC-Jugendarbeit"] == town]
            town_df = town_df.drop(columns=["EC-Jugendarbeit"])
            town_df.index = range(1, len(town_df) + 1)

            df_by_town_dict[town] = town_df

        logging.info("Filtered attendees data by town.")

        return df_by_town_dict

    def _get_numbers_df(self) -> pd.DataFrame:
        """
        Calculate and return a dataframe with counts attendees by town.
        """

        numbers_df = pd.DataFrame(
            {"EC-Jugendarbeit": [], "Anmeldungen": []}
        )

        # make Ortschaft the index
        numbers_df = numbers_df.set_index("EC-Jugendarbeit")

        df = self.attendees_df

        # add row to numbers_df
        numbers_df.loc["GESAMT"] = [len(df)]

        # filter by town:
        for town in self.towns_list:
            town_df = df[df["EC-Jugendarbeit"] == town]

            # add row to numbers_df
            numbers_df.loc[town] = [len(town_df)]

        return numbers_df
    
    def _get_orders_df(self) -> pd.DataFrame:
        """
        Process debloated dataframe to create a sorted dataframe for orders with required columns.
        """
        
        df = self.debloated_df.copy()
        
        # filter for columns and set their order
        wanted_columns = [
            "Bestellnummer",
            "Bestellstatus",
            "E-Mail",
            "Gesamtpreis",
            "Anmeldedatum",
            "Rechnung - Empfänger",
            "Rechnung - Adresse",
            "Zuschuss beantragt",
        ]
        wanted_columns = [
            "Bestellnummer",
            "Bestellstatus",
            "E-Mail",
            "Gesamtpreis",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)
        
        # combine rows with same Bestellnummer (every entry with the same "Bestellnummer" has the same values for all the other columns)
        df = df.groupby("Bestellnummer", as_index=False).first()
        
        
        # sort for "Bestellstatus" first, then "Anmeldeddatum"
        # define custom order
        status_order = {
            "unbezahlt": 0,
            "bezahlt": 1,
            "storniert": 2,
        }

        df["_status_sort"] = df["Bestellstatus"].map(status_order)

        df = (
            df.sort_values(
                by=["_status_sort", "Anmeldedatum"],
                ascending=[True, True],
                na_position="last",
            )
            .drop(columns="_status_sort")
        )
                
        # reset index numbers
        df.index = range(1, len(df) + 1)

        logging.info("Sorted debloated data into orders.")
        
        return df
    

class CustomMain(Main):
    def main(self):
        """
        Main function to generate Excel files and upload them to Nextcloud.
        """
        
        # fetch and sort data
        dataframe = Dataframe(self.success_on_last_run)

        # generate and upload excel file for raw data
        self.upload(dataframe.raw_df, "Rohdaten", subdir="Unsortiert", filterable=True)

        # generate and upload excel file for all all debloated data 
        self.upload(dataframe.debloated_df, "Alles", subdir="Unsortiert", filterable=True)
        
        # generate and upload excel file for all attendees
        self.upload(dataframe.attendees_df, "Teilnehmerdaten", filterable=True, freeze_panes=(1,3))
        
        # generate and upload excel file for town-wise attendees
        for town, df in dataframe.town_dfs.items():
            self.upload(df, town, subdir="Nach_Orten", filterable=True)

        # generate and upload excel file for numbers overview
        self.upload(dataframe.numbers_overview, "Anmeldezahlen")
        
        # generate and upload excel file for all orders
        self.upload(dataframe.orders_df, "Bestellungen", filterable=True)
        
        self.upload_dir_tech_details = "Technische_Details"  # set upload directory for technical files like Last_updated.txt, error logs and docker image version info.
        
        self.cloud.upload_last_updated(subdir=self.upload_dir_tech_details)
        
        self.cloud.upload_docker_image_version(subdir=self.upload_dir_tech_details)


if __name__ == "__main__":
    CustomMain().run()
