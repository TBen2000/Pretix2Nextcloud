# Customized Pretix2Nextcloud instance for kv-stuttgart-jungschartag
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
    default_nextcloud_upload_dir="Anmeldungen_Zeltlager-Jungs",
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
        
        print("\n\n\n\n")
        print(self.raw_df["Wir melden uns über folgenden Ort an"].unique())
        print("\n\n\n\n")
        
        # check for new fetched data and raise exception if no new data occured so that Main can skip this run
        pretix.check_for_new_fetched_data(self.raw_df, success_on_last_run)
        
        self.towns_list = pretix.get_answer_choices_from_question("Wir melden uns über folgenden Ort an")
        self.towns_list.remove("Keiner (ortsunabhängige Anmeldung)")
        self.towns_list.append("ortsunabhängig")

        self.debloated_df = self._get_debloated_df()
        self.attendees_df = self._get_attendees_df()
        self.town_dfs = self._get_town_dfs()
        self.numbers_overview = self._get_numbers_df()
        self.orders_df = self._get_orders_df()
        self.donataions_df = self._get_donations_df()
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
            "invoice_name": "Rechnung - Name",
            "invoice_company": "Rechnung - Firma",
            "invoice_street": "Rechnung - Straße",
            "invoice_zipcode": "Rechnung - PLZ",
            "invoice_city": "Rechnung - Stadt",
            "invoice_country": "Rechnung - Land",
            "item_name": "Art",
            "price": "Preis",
            "attendee_firstname": "Vorname",
            "attendee_lastname": "Nachname",
            "Gültige Tetanusimpfung vorhanden": "Tetanusimpfung",
            "Geht Ihr Kind in eine Jungschar?": "Besucht Jungschar",
            "Essensunverträglichkeiten": "Essensunverträglichkeiten Ja/Nein",
            "Welche Unverträglichkeiten?": "Essensunverträglichkeiten",
            "Welche Medikamente?": "Medikamente",
            "Worauf muss außerdem besonders geachtet werden?": "Medizinische Besonderheiten",
            "Zuschuss beantragen": "Zuschuss beantragt",
        }
        df = df.rename(columns=renames)
        
        # combine "Rechnung - Name" and "Rechnung - Straße" to one column "Rechnung - Empfänger"
        name = df["Rechnung - Name"].fillna("").str.strip()
        company = df["Rechnung - Firma"].fillna("").str.strip()
        df["Rechnung - Empfänger"] = (
            name.where(name.eq("") | company.eq(""), name + " (" + company + ")")
                .where(~name.eq(""), company)
        )
        
        # combine "Rechnung - Straße", "Rechnung - PLZ", "Rechnung - Stadt" and "Rechnung - Land" to "Rechnung - Adresse"
        df["Rechnung - Adresse"] = (
            df["Rechnung - Straße"].fillna("").str.strip() + ", " +
            df["Rechnung - PLZ"].fillna("").str.strip() + " " +
            df["Rechnung - Stadt"].fillna("").str.strip() + ", " +
            df["Rechnung - Land"].fillna("").str.strip()
        )
        
        
        # simplify values in column "Krankenversicherung"
        # rename all values "Privat krankenversichert" to "privat" and all values "Gesetzlich krankenversichert (z.B. AOK)" to "gesetzlich":
        df["Krankenversicherung"] = df["Krankenversicherung"].replace(
            {
                "Privat krankenversichert": "privat",
                "Gesetzlich krankenversichert (z.B. AOK)": "gesetzlich",
            }
        )
        
        # simplify values in column "Einverständniserklärung"
        # rename all values that conatain any text to "hochgeladen":
        df.loc[
            df["Einverständniserklärung"].notna() & 
            (df["Einverständniserklärung"].astype(str).str.strip() != ""),
            "Einverständniserklärung"
        ] = "hochgeladen"
       
        # combine "Wo geht Ihr Kind in die Jungschar?" and "Wir melden uns über folgenden Ort an" and "Wir melden uns über folgenden Ort an (#2)" to "Ort"
        col1 = "Wo geht Ihr Kind in die Jungschar?"
        col2 = "Wir melden uns über folgenden Ort an"
        col3 = "Wir melden uns über folgenden Ort an (#2)"
        # Make sure columns exist (avoid KeyError)
        for col in [col1, col2, col3]:
            if col not in df.columns:
                df[col] = pd.NA
        # Condition: col1 usable (not empty, not NA, not "Sonstige")
        use_col1 = (
            df[col1].notna() &
            (df[col1] != "") &
            (df[col1] != "Sonstige")
        )
        # Build Ort column
        df["Ort"] = df[col1].where(use_col1, df[col2])
        df["Ort"] = df["Ort"].where(
            df["Ort"].notna() & (df["Ort"] != ""),
            df[col3]
        )
        
        # rename values in "Bestellstatus" from acronyms to the complete meaning
        # rename values "c" to "storniert", "n" to "unbezahlt" und "p" to "bezahlt"
        df["Bestellstatus"] = df["Bestellstatus"].replace(
            {
                "c": "storniert",
                "n": "unbezahlt",
                "p": "bezahlt",
            }
        )
        
        
        # filter for columns and set their order
        wanted_columns = [
            "Bestellnummer",
            "Bestellstatus",
            "E-Mail",
            "Gesamtpreis",
            "Anmeldedatum",
            "Rechnung - Empfänger",
            "Rechnung - Adresse",
            "Art",
            "Preis",
            "Vorname",
            "Nachname",
            "Geburtsdatum",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Tetanusimpfung",
            "Krankenversicherung",
            "Splitter und Zecken dürfen vom Sani des Lagers entfernt werden",
            "Verabreichung rezeptfreier Medikamente durch den Sani des Lagers",
            "Medikamente",
            "Medizinische Besonderheiten",
            "Notfall-Telefonnummern",
            "T-Shirt Größe",
            "Schwimmer",
            "Besucht Jungschar",
            "Ort",
            "Sonstiges",
            "Zuschuss beantragt",
            "Einverständniserklärung",
        ]
        df = df.filter(wanted_columns)

        # change date format
        df["Anmeldedatum"] = (
            pd.to_datetime(df["Anmeldedatum"], utc=True)
            .dt.tz_convert(self.time_zone)
            .dt.strftime("%Y-%m-%d %H:%M")
        )
        
        # change all values "Keiner (ortsunabhängige Anmeldung)" in column "Ort" to "ortsunanhängig"
        df["Ort"] = df["Ort"].replace("Keiner (ortsunabhängige Anmeldung)", "ortsunabhängig")
        
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
        
        # remove all donation entries (entries with "Spende Zeltlagerarbeit" in column "Art")
        df = df[df["Art"] != "Spende Zeltlagerarbeit"]

        # removed all cancelled registrations
        df = df[df["Bestellstatus"] != "storniert"]

        # filter for columns and set their order
        wanted_columns = [
            "Nachname",
            "Vorname",
            "Geburtsdatum",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Tetanusimpfung",
            "Krankenversicherung",
            "Splitter und Zecken dürfen vom Sani des Lagers entfernt werden",
            "Verabreichung rezeptfreier Medikamente durch den Sani des Lagers",
            "Medikamente",
            "Medizinische Besonderheiten",
            "Notfall-Telefonnummern",
            "T-Shirt Größe",
            "Schwimmer",
            "Besucht Jungschar",
            "Ort",
            "Sonstiges",
            "Zuschuss beantragt",
            "Einverständniserklärung",
            "Bestellnummer",
            "Anmeldedatum",
            "E-Mail",
            "Rechnung - Empfänger",
            "Rechnung - Adresse",
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
            "Besucht Jungschar",
            "Ort",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)
        
        # sort (by "Nachname" and then by "Vorname")
        df = df.sort_values(
            by=["Nachname", "Vorname"], ascending=True
        )

        # sort by town:
        df_by_town_dict = {}
        df_towns = (df["Ort"].dropna().astype(str).str.strip().unique())
        towns = sorted(set(self.towns_list) | set(df_towns))
        for town in towns:
            # filter by town, drop column "Ort" and reset index numbers
            town_df = df[df["Ort"] == town]
            town_df = town_df.drop(columns=["Ort"])
            town_df.index = range(1, len(town_df) + 1)

            df_by_town_dict[town] = town_df

        logging.info("Filtered attendees data by town.")

        return df_by_town_dict

    def _get_numbers_df(self) -> pd.DataFrame:
        """
        Calculate and return a dataframe with counts attendees by town.
        """

        numbers_df = pd.DataFrame(
            {"Ort": [], "Anmeldungen": []}
        )

        # make Ortschaft the index
        numbers_df = numbers_df.set_index("Ort")

        df = self.attendees_df

        # add row to numbers_df
        numbers_df.loc["GESAMT"] = [len(df)]

        # filter by town:
        for town in self.towns_list:
            town_df = df[df["Ort"] == town]

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
        df = df.filter(wanted_columns)
        
        # combine rows with same Bestellnummer (every entry with the same "Bestellnummer" has the same values for all the other columns. Differences in "Zuschuss beantragt" will always be combined to "Ja")
        df = (
            df.groupby("Bestellnummer", as_index=False)
            .agg({
                **{col: "first" for col in df.columns if col != "Zuschuss beantragt"},
                "Zuschuss beantragt": lambda x: "Ja" if (x == "Ja").any() else "Nein"
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
    
    def _get_donations_df(self) -> pd.DataFrame:
        """
        Process debloated dataframe to create a sorted dataframe for donations with required columns.
        """
        
        df = self.debloated_df.copy()
        
        # filter for "Spende Zeltlagerarbeit" in column "Art"
        df = df[df["Art"] == "Spende Zeltlagerarbeit"]
        
        # filter for columns and set their order
        wanted_columns = [
            "Art",
            "Preis",
            "Bestellstatus",
            "Anmeldedatum",
            "E-Mail",
            "Rechnung - Empfänger",
            "Rechnung - Adresse",
            "Bestellnummer",
            "Zuschuss beantragt",
        ]
        df = df.filter(wanted_columns)        
        
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
            "Ort",
            "Notfall-Telefonnummern",
            "E-Mail",
            "Rechnung - Adresse",
            "Rechnung - Empfänger",
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
            "Ort",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Medikamente",
            "Medizinische Besonderheiten",
            "Sonstiges",
            "Verabreichung rezeptfreier Medikamente durch den Sani des Lagers",
            "Splitter und Zecken dürfen vom Sani des Lagers entfernt werden",
            "Tetanusimpfung",
            "Krankenversicherung",
            "Notfall-Telefonnummern",
            "E-Mail",
            "Rechnung - Adresse",
            "Rechnung - Empfänger",
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
            "Ort",
            "Ernährung",
            "Essensunverträglichkeiten",
            "E-Mail",
            "Rechnung - Empfänger",
            "Notfall-Telefonnummern",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)
        
        # print contact information only if "Essensunverträglichkeiten" is not empty
        # create a mask: True if column contains real content (not empty, not just whitespace)
        mask = df["Essensunverträglichkeiten"].fillna("").str.strip().ne("")

        # columns to clear if no intolerance is given
        columns_to_clear = [
            "E-Mail",
            "Anmeldedatum",
            "Rechnung - Empfänger",
            "Notfall-Telefonnummern",
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
        
        # fetch and sort data
        dataframe = Dataframe(self.success_on_last_run)

        # generate and upload excel file for raw data
        self.upload(dataframe.raw_df, "Rohdaten", subdir="Unsortiert", filterable=True)

        # generate and upload excel file for all all debloated data 
        self.upload(dataframe.debloated_df, "Alles", subdir="Unsortiert", filterable=True)
        
        # generate and upload excel file for all attendees
        self.upload(dataframe.attendees_df, "Teilnehmerdaten", filterable=True, freeze_panes=(1,3))
        
        # generate and upload excel file for town-wise attendees
        for town in dataframe.town_dfs:
            df = dataframe.town_dfs[town]
            self.upload(df, town, subdir="Nach_Orten")

        # generate and upload excel file for numbers overview
        self.upload(dataframe.numbers_overview, "Anmeldezahlen", subdir="Nach_Orten")
        
        # generate and upload excel file for all orders
        self.upload(dataframe.orders_df, "Bestellungen", subdir="Finanzen", filterable=True)
        
        # generate and upload excel file for all donations
        self.upload(dataframe.donataions_df, "Spenden", subdir="Finanzen", filterable=True)
        
        # generate and upload excel file for emergency contacts
        self.upload(dataframe.contacts_df, "Notfallkontakte", filterable=True, freeze_panes=(1,3))

        # generate and upload excel file for medical information of attendees
        self.upload(dataframe.medical_info_df, "Sani", filterable=True)
        
        # generate and upload excel file for diet information of attendees
        self.upload(dataframe.diet_info_df, "Küche", filterable=True)
        
        self.cloud.upload_last_updated()
        
        self.cloud.upload_docker_image_version()


if __name__ == "__main__":
    CustomMain().run()
