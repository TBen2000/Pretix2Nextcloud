# Customized Pretix2Nextcloud instance for kv-heilbronn-bubenzeltlager
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
    default_pretix_organizer_slug="kv-heilbronn",
    default_excel_max_column_width=30,
    default_nextcloud_url="https://jcloud.swdec.de",
    default_nextcloud_upload_dir="Anmeldungen_Bubenzeltlager",
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
        
        self.busstop_list = pretix.get_answer_choices_from_question("Zu-/Ausstieg")

        self.debloated_df = self._get_debloated_df()
        self.attendees_df = self._get_attendees_df()
        self.busstop_dfs = self._get_busstop_dfs()
        self.numbers_overview = self._get_numbers_df()
        self.orders_df = self._get_orders_df()
        self.contacts_df = self._get_contacts_df()
        self.medical_info_df = self._get_medical_info_df()
        self.diet_info_df = self._get_diet_info_df()


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
            "Geburtsdatum": "Geburtsdatum",
            "attendee_street": "Straße",
            "attendee_zipcode": "PLZ",
            "attendee_city": "Ort",
            "Ortsteil": "Ortsteil",
            "attendee_country": "Land",
            "Erreichbarkeit des/der Sorgeberechtigten": "Sorgeberechtigter",
            "Verwandte/Freunde, die im Notfall weiterhelfen können - bitte Telefonnummer mit angeben!": "Verwandte/Freunde",
            "Gesundheitsfürsorge - Krankenversicherung": "Krankenversicherung",
            "Name der Krankenkasse und Versicherungsnummer": "Versicherungsnummer",
            "Vor- und Nachname des/der Familienangehörigen, über den Ihr Kind versichert ist": "Versicherungsnehmer",
            "Name und Adresse des Hausarztes": "Hausarzt",
            "Ich stimme der Verabreichung rezeptfreier Medikamente zu": "Verabreichung rezeptfreier Medikamente",
            "Mein Kind ist gegen Tetanus (Wundstarrkrampf) geimpft": "Tetanusimpfung",
            "Letztes Impfdatum Tetanus:": "Impfdatum Tetanus",
            "Mein Kind ist gegen FSME (Zecken) geimpft": "FSME Impfung",
            "Letztes Impfdatum FSME:": "Impfdatum FSME",
            "Ich bin damit einverstanden, dass ein:e Mitarbeiter:in eine Zecke bei meinem Kind entfernen darf": "Zecken entfernen",
            "Mein Kind ernährt sich vegetarisch.": "Vegetarier",
            "Mein Kind kann schwimmen und darf unter Aufsicht im Freibad oder See baden gehen:": "Schwimmerlaubnis",
            "Mein Kind darf für Programmzwecke und für den Fall einer medizinischen Abklärung in einem privaten PKW mitfahren.": "Mitfahrerlaubnis",
            "Zu-/Ausstieg": "Zu-/Ausstieg",
            "Zuschussantrag": "Zuschussantrag",
            "Einverständnis zur Verwendung von entstandenen Video- und Bildaufnahmen": "Bildrechte",
            "Dürfen nach dem Lager Flyer/Einladungen an Ihr Kind verschickt werden?": "Erlaubnis für Einladungen",
            "Mein Kind besucht folgende Jungschar": "Jungschar",
            "Einverständniserklärung": "Einverständniserklärung",
            "Zustimmung zu den AGBs": "Zustimmung AGBs",
            "Medikamente - Name des Medikaments und Dosierung": "Medikamente",
            "Medikamenteneinnahme": "Medikamenteneinnahme",
            "Zeitpunkt Medikamentengabe": "Zeitpunkt Medikamentengabe",
            "Worauf muss besonders geachtet werden?": "Medizinische Informationen",
            "Was das Zeltlager-Leitungs-Team sonst noch wissen sollte:": "Sonstiges",
            "Mein Kind hat folgende Lebensmittelunverträglichkeiten/ Essgewohnheiten": "Lebensmittelunverträglichkeiten",
        }
        df = df.rename(columns=renames)        
        
        # filter for columns
        wanted_columns = list(renames.values())
        df = df.filter(wanted_columns)

        # combine "Straße", "PLZ", "Ort", Ortsteil and "Land" to "Adresse"
        ortsteil = df["Ortsteil"].fillna("").str.strip()
        ortsteil_part = ortsteil.apply(lambda x: f" ({x})" if x else "")

        df["Adresse"] = (
            df["Straße"].fillna("").str.strip() + ", " +
            df["PLZ"].fillna("").str.strip() + " " +
            df["Ort"].fillna("").str.strip() + ortsteil_part + ", " +
            df["Land"].fillna("").str.strip()
        )
        df["Adresse"] = df["Adresse"].str.replace(r"\s+", " ", regex=True).str.strip()

        
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

        
        # replace boolean values with "Ja" and "Nein"
        bool_columns = [
            "Verabreichung rezeptfreier Medikamente",
            "Tetanusimpfung",
            "FSME Impfung",
            "Zecken entfernen",
            "Vegetarier",
            "Schwimmerlaubnis",
            "Mitfahrerlaubnis",
            "Zuschussantrag",
            "Bildrechte",
            "Erlaubnis für Einladungen",
            "Einverständniserklärung",
            "Zustimmung AGBs",
        ]
        df[bool_columns] = df[bool_columns].replace(
            {
                "True": "Ja",
                "False": "Nein",
            }
        )

        
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
            "Geburtsdatum",
            "Adresse",
            "E-Mail",
            "Sorgeberechtigter",
            "Verwandte/Freunde",
            "Krankenversicherung",
            "Versicherungsnummer",
            "Versicherungsnehmer",
            "Hausarzt",
            "Verabreichung rezeptfreier Medikamente",
            "Tetanusimpfung",
            "Impfdatum Tetanus",
            "FSME Impfung",
            "Impfdatum FSME",
            "Zecken entfernen",
            "Medikamente",
            "Medikamenteneinnahme",
            "Zeitpunkt Medikamentengabe",
            "Medizinische Informationen",
            "Sonstiges",
            "Vegetarier",
            "Lebensmittelunverträglichkeiten",
            "Schwimmerlaubnis",
            "Mitfahrerlaubnis",
            "Zu-/Ausstieg",
            "Zuschussantrag",
            "Bildrechte",
            "Erlaubnis für Einladungen",
            "Jungschar",
            "Bestellnummer",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)

        # sort (by "Nachname" and then by "Vorname") and reset index numbers
        df = df.sort_values(
            by=["Nachname", "Vorname"], ascending=True
        )
        df.index = range(1, len(df) + 1)

        logging.info("Sorted debloated data into attendees data.")

        return df

    def _get_busstop_dfs(self) -> dict[str, pd.DataFrame]:
        """
        Process sorted dataframe for attendees to create a dictionary of dataframes filtered by busstop.
        """

        df = self.debloated_df.copy()

        # filter for columns and set their order
        wanted_columns = [
            "Nachname",
            "Vorname",
            "Geburtsdatum",
            "Zu-/Ausstieg",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)
        
        # sort (by "Nachname" and then by "Vorname")
        df = df.sort_values(
            by=["Nachname", "Vorname"], ascending=True
        )

        # sort by busstop:
        df_by_busstop_dict = {}
        df_busstops = (df["Zu-/Ausstieg"].dropna().astype(str).str.strip().unique())
        busstops = sorted(set(self.busstop_list) | set(df_busstops))
        for busstop in busstops:
            # filter by busstop, drop column "Zu-/Ausstieg" and reset index numbers
            busstop_df = df[df["Zu-/Ausstieg"] == busstop]
            busstop_df = busstop_df.drop(columns=["Zu-/Ausstieg"])
            busstop_df.index = range(1, len(busstop_df) + 1)

            df_by_busstop_dict[busstop] = busstop_df

        logging.info("Filtered attendees data by busstop.")

        return df_by_busstop_dict


    def _get_numbers_df(self) -> pd.DataFrame:
        """
        Calculate and return a dataframe with counts attendees by busstop.
        """

        numbers_df = pd.DataFrame(
            {"Zu-/Ausstieg": [], "Anmeldungen": []}
        )

        # make busstop the index
        numbers_df = numbers_df.set_index("Zu-/Ausstieg")

        df = self.debloated_df

        # add row to numbers_df
        numbers_df.loc["GESAMT"] = [len(df)]

        # filter by busstop:
        for busstop in self.busstop_list:
            busstop_df = df[df["Zu-/Ausstieg"] == busstop]

            # add row to numbers_df
            numbers_df.loc[busstop] = [len(busstop_df)]

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
            "Zuschussantrag",
        ]
        df = df.filter(wanted_columns)
        
        # combine rows with same Bestellnummer (every entry with the same "Bestellnummer" has the same values for all the other columns. Differences in "Zuschussantrag" will always be combined to "Ja")
        df = (
            df.groupby("Bestellnummer", as_index=False)
            .agg({
                **{col: "first" for col in df.columns if col != "Zuschussantrag"},
                "Zuschussantrag": lambda x: "Ja" if (x == "Ja").any() else "Nein"
            })
        )
        
        
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
    
    
    def _get_contacts_df(self) -> pd.DataFrame:
        """
        Process debloated dataframe to create a sorted dataframe for emergency contacts with required columns.
        """

        df = self.attendees_df.copy()
        
        wanted_columns = [
            "Nachname",
            "Vorname",
            "Geburtsdatum",
            "Adresse",
            "E-Mail",
            "Sorgeberechtigter",
            "Verwandte/Freunde",
            "Hausarzt",
            "Krankenversicherung",
            "Versicherungsnummer",
            "Versicherungsnehmer",
        ]
        df = df.filter(wanted_columns)
        
        # sort (by "Nachname" and then by "Vorname") and reset index numbers
        df = df.sort_values(
            by=["Nachname", "Vorname"], ascending=True
        )
        df.index = range(1, len(df) + 1)
        
        return df
    
    def _get_medical_info_df(self) -> pd.DataFrame:
        """
        Process debloated dataframe to create a sorted dataframe for medical information with required columns.
        """

        df = self.attendees_df.copy()
        
        # filter for columns and set their order
        wanted_columns = [
            "Nachname",
            "Vorname",
            "Geburtsdatum",
            "Medikamente",
            "Medikamenteneinnahme",
            "Zeitpunkt Medikamentengabe",
            "Medizinische Informationen",
            "Sonstiges",
            "Lebensmittelunverträglichkeiten",
            "Verabreichung rezeptfreier Medikamente",
            "Tetanusimpfung",
            "Impfdatum Tetanus",
            "FSME Impfung",
            "Impfdatum FSME",
            "Zecken entfernen",
            "Ortsteil",
            "E-Mail",
            "Sorgeberechtigter",
            "Verwandte/Freunde",
            "Krankenversicherung",
            "Versicherungsnummer",
            "Versicherungsnehmer",
            "Hausarzt",
            "Mitfahrerlaubnis",
            "Adresse"
        ]
        df = df.filter(wanted_columns)
        
        # sort (by "Nachname" and then by "Vorname") and reset index numbers
        df = df.sort_values(
            by=["Nachname", "Vorname"], ascending=True
        )
        df.index = range(1, len(df) + 1)

        return df
    
    def _get_diet_info_df(self) -> pd.DataFrame:
        """
        Process debloated dataframe to create a sorted dataframe for diet restrictions with required columns.
        """

        df = self.attendees_df.copy()
        
        # filter for columns and set their order
        wanted_columns = [
            "Nachname",
            "Vorname",
            "Geburtsdatum",
            "Vegetarier",
            "Lebensmittelunverträglichkeiten",
            "E-Mail",
            "Sorgeberechtigter",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)
        
        # print contact information only if "Lebensmittelunverträglichkeiten" is not empty
        # create a mask: True if column contains real content (not empty, not just whitespace)
        mask = df["Lebensmittelunverträglichkeiten"].fillna("").str.strip().ne("")

        # columns to clear if no intolerance is given
        columns_to_clear = [
            "E-Mail",
            "Anmeldedatum",
            "Sorgeberechtigter",
        ]
        # set values to NaN where mask is False
        df.loc[~mask, columns_to_clear] = pd.NA
        
        # sort (by "Nachname" and then by "Vorname") and reset index numbers
        df = df.sort_values(
            by=["Nachname", "Vorname"], ascending=True
        )
        df.index = range(1, len(df) + 1)

        return df

class CustomMain(Main):
    def main(self):
        """
        Main function to generate Excel files and upload them to Nextcloud.
        """

        self.upload_dir_tech_details = "Technische_Details"  # set upload directory for technical files like Last_updated.txt, error logs and docker image version info.
        
        # fetch and sort data
        dataframe = Dataframe(self.success_on_last_run)

        # generate and upload excel file for raw data
        self.upload(dataframe.raw_df, "Rohdaten", subdir="Unsortiert", filterable=True)

        # generate and upload excel file for all all debloated data 
        self.upload(dataframe.debloated_df, "Alles", subdir="Unsortiert", filterable=True)
        
        # generate and upload excel file for all attendees
        self.upload(dataframe.attendees_df, "Teilnehmerdaten", filterable=True, freeze_panes=(1,3))
        
        # generate and upload excel file for town-wise attendees
        for busstop, df in dataframe.busstop_dfs.items():
            self.upload(df, busstop, subdir="Nach_Orten", filterable=True)

        # generate and upload excel file for numbers overview
        self.upload(dataframe.numbers_overview, "Anmeldezahlen")
        
        # generate and upload excel file for all orders
        self.upload(dataframe.orders_df, "Bestellungen", subdir="Finanzen", filterable=True)
        
        # generate and upload excel file for emergency contacts
        self.upload(dataframe.contacts_df, "Notfallkontakte", filterable=True, freeze_panes=(1,3))

        # generate and upload excel file for medical information of attendees
        self.upload(dataframe.medical_info_df, "Sani", filterable=True)
        
        # generate and upload excel file for diet information of attendees
        self.upload(dataframe.diet_info_df, "Küche", filterable=True)
        
        self.cloud.upload_last_updated(subdir=self.upload_dir_tech_details)
        
        self.cloud.upload_docker_image_version(subdir=self.upload_dir_tech_details)


if __name__ == "__main__":
    CustomMain().run()
